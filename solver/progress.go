package solver

import (
	"context"
	"errors"
	"io"
	"slices"
	"time"

	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/progress/logs"

	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/util/progress"
	digest "github.com/opencontainers/go-digest"
)

func (j *Job) Status(ctx context.Context, statsStream bool, ch chan *client.SolveStatus) error {
	vs := &vertexStream{cache: map[digest.Digest]*client.Vertex{}, wasCached: make(map[digest.Digest]struct{})}
	pr := j.pr.Reader(ctx)
	defer func() {
		rootCause, hasRootCause := j.RootCause()
		if enc := vs.encore(rootCause, hasRootCause); len(enc) > 0 {
			ch <- &client.SolveStatus{Vertexes: enc}
		}
		close(ch)
	}()

	if !statsStream { // earthly-specific: don't stream stats back to old clients (which will cause them to print binary data to stderr)
		pr = progress.NewFilteredReader(pr, func(ctx context.Context, p *progress.Progress) (bool, error) {
			if vl, ok := p.Sys.(client.VertexLog); ok {
				if vl.Stream == logs.StatsStream {
					return true, nil
				}
			}
			return false, nil
		})
	}

	for {
		p, err := pr.Read(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		ss := &client.SolveStatus{}
		for _, p := range p {
			switch v := p.Sys.(type) {
			case client.Vertex:
				ss.Vertexes = append(ss.Vertexes, vs.append(v)...)

			case progress.Status:
				vtx, ok := p.Meta("vertex")
				if !ok {
					bklog.G(ctx).Warnf("progress %s status without vertex info", p.ID)
					continue
				}
				vs := &client.VertexStatus{
					ID:        p.ID,
					Vertex:    vtx.(digest.Digest),
					Name:      v.Action,
					Total:     int64(v.Total),
					Current:   int64(v.Current),
					Timestamp: p.Timestamp,
					Started:   v.Started,
					Completed: v.Completed,
				}
				ss.Statuses = append(ss.Statuses, vs)
			case client.VertexLog:
				vtx, ok := p.Meta("vertex")
				if !ok {
					bklog.G(ctx).Warnf("progress %s log without vertex info", p.ID)
					continue
				}
				if v.Vertex == "" {
					v.Vertex = vtx.(digest.Digest)
				}
				v.Timestamp = p.Timestamp
				ss.Logs = append(ss.Logs, &v)
			case client.VertexWarning:
				vtx, ok := p.Meta("vertex")
				if !ok {
					bklog.G(ctx).Warnf("progress %s warning without vertex info", p.ID)
					continue
				}
				if v.Vertex == "" {
					v.Vertex = vtx.(digest.Digest)
				}
				ss.Warnings = append(ss.Warnings, &v)
			}
		}
		slices.SortFunc(ss.Vertexes, func(a, b *client.Vertex) int {
			if a.Started == nil {
				return -1
			}
			if b.Started == nil {
				return 1
			}
			return a.Started.Compare(*b.Started)
		})
		slices.SortFunc(ss.Statuses, func(a, b *client.VertexStatus) int {
			return a.Timestamp.Compare(b.Timestamp)
		})
		slices.SortFunc(ss.Logs, func(a, b *client.VertexLog) int {
			return a.Timestamp.Compare(b.Timestamp)
		})

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case ch <- ss:
		}
	}
}

type vertexStream struct {
	cache     map[digest.Digest]*client.Vertex
	wasCached map[digest.Digest]struct{}
}

func (vs *vertexStream) append(v client.Vertex) []*client.Vertex {
	var out []*client.Vertex
	vs.cache[v.Digest] = &v
	if v.Started != nil {
		for _, inp := range v.Inputs {
			if inpv, ok := vs.cache[inp]; ok {
				if !inpv.Cached && inpv.Completed == nil {
					inpv.Cached = true
					inpv.Started = v.Started
					inpv.Completed = v.Started
					out = append(out, vs.append(*inpv)...)
					delete(vs.cache, inp)
				}
			}
		}
	}
	if v.Cached {
		vs.markCached(v.Digest)
	}

	vcopy := v
	return append(out, &vcopy)
}

func (vs *vertexStream) markCached(dgst digest.Digest) {
	if v, ok := vs.cache[dgst]; ok {
		if _, ok := vs.wasCached[dgst]; !ok {
			for _, inp := range v.Inputs {
				vs.markCached(inp)
			}
		}
		vs.wasCached[dgst] = struct{}{}
	}
}

func (vs *vertexStream) encore(rootCause RootCause, hasRootCause bool) []*client.Vertex {
	var out []*client.Vertex
	hasSpecificError := false
	for _, v := range vs.cache {
		if v.Error != "" && !isGenericCancellationString(v.Error) {
			hasSpecificError = true
			break
		}
	}
	rootCauseApplied := false
	for _, v := range vs.cache {
		if v.Started != nil && v.Completed == nil {
			now := time.Now()
			v.Completed = &now
			if _, ok := vs.wasCached[v.Digest]; !ok && v.Error == "" {
				if hasRootCause && !hasSpecificError && !rootCauseApplied && (rootCause.VertexDigest == "" || rootCause.VertexDigest == v.Digest) {
					// Earthbuild: attach the recorded root cause to the final
					// canceled status so clients can show the active operation.
					v.Error = rootCause.Error()
					rootCauseApplied = true
				} else {
					v.Error = context.Canceled.Error()
				}
			}
			out = append(out, v)
		}
	}
	if hasRootCause && !hasSpecificError && !rootCauseApplied {
		now := time.Now()
		started := rootCause.RecordedAt
		if started.IsZero() {
			started = now
		}
		dgst := rootCause.VertexDigest
		if dgst == "" {
			dgst = digest.FromBytes([]byte(rootCause.Error()))
		}
		// Earthbuild: if the root-cause vertex was not still active, emit a
		// compact synthetic vertex so status consumers still receive it.
		out = append(out, &client.Vertex{
			Digest:    dgst,
			Name:      rootCause.VertexName,
			Started:   &started,
			Completed: &now,
			Error:     rootCause.Error(),
		})
	}
	return out
}

func isGenericCancellationString(s string) bool {
	return s == context.Canceled.Error()
}
