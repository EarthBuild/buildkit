package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/remotes"
	cerrdefs "github.com/containerd/errdefs"
	cacheconfig "github.com/moby/buildkit/cache/config"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/contentutil"
	"github.com/moby/buildkit/worker"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// Cross-machine single-flight: the client half.
//
// Two independent buildkitds would otherwise happily build the same vertex at
// the same time. Before executing, we claim the vertex's content-addressed lease
// key (solver.LeaseKey) with an external coordinator. The first claimant builds
// and publishes; the rest block and are handed its output as an OCI descriptor
// chain, which they materialize locally through worker.FromRemote — the same
// path every remote-cache hit already takes, so the resulting ref is LAZY and
// nothing is downloaded until a downstream op actually mounts it.
//
// Configuration is by environment, deliberately: threading a new option through
// worker construction would touch every Worker implementation, and this is a
// fork.
//
//	BUILDKIT_SINGLEFLIGHT_URL       coordinator base, e.g. http://127.0.0.1:5000
//	BUILDKIT_SINGLEFLIGHT_REGISTRY  repo the layers live under, e.g. "cache"
//
// Unset => the feature is off and ExecOp behaves exactly as upstream.
//
// FAIL-OPEN, everywhere. If the coordinator is unreachable, slow, or confused,
// we build the vertex ourselves. Duplicate work is the thing this feature
// exists to avoid, but it is always CORRECT; a stall or a wrong layer is not.
type coordinator struct {
	base     string
	repo     string
	client   *http.Client
	registry string
}

// publishedResult is what a leader hands the coordinator: one descriptor chain
// per output of the vertex. An ExecOp can have several (a rootfs plus mounts),
// and edge.execOp indexes into them, so the shape must be preserved.
type publishedResult struct {
	Outputs [][]ocispecs.Descriptor `json:"outputs"`
}

func coordinatorFromEnv() *coordinator {
	base := os.Getenv("BUILDKIT_SINGLEFLIGHT_URL")
	if base == "" {
		return nil
	}
	repo := os.Getenv("BUILDKIT_SINGLEFLIGHT_REGISTRY")
	if repo == "" {
		repo = "cache"
	}
	return &coordinator{
		base: base,
		repo: repo,
		// No overall timeout: a follower legitimately blocks for as long as the
		// leader takes to build. The coordinator is responsible for never
		// blocking forever — it holds a lease TTL and tells us to rebuild if the
		// leader dies.
		client: &http.Client{},
	}
}

// claim asks who should build `key`.
//
// Returns (nil, false) if WE must build it — including every failure path,
// because building it ourselves is always safe. Returns (result, true) if a peer
// already built it.
func (c *coordinator) claim(ctx context.Context, key string) (*publishedResult, bool) {
	url := fmt.Sprintf("%s/_rebuck/lease/claim/%s", c.base, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		// Coordinator unreachable: build it. A fleet that loses its coordinator
		// degrades to plain BuildKit, which is exactly right.
		return nil, false
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if resp.Header.Get("X-Rebuck-Lease") == "leader" {
			return nil, false
		}
		var pr publishedResult
		if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
			// The leader published something we cannot read. Rebuild rather than
			// guess at what it meant.
			return nil, false
		}
		return &pr, true
	default:
		// 409 (the leader died — re-claim), 5xx, anything else: build it. We do
		// not loop re-claiming; one wasted build beats a retry storm, and the
		// next vertex will coordinate normally.
		io.Copy(io.Discard, resp.Body)
		return nil, false
	}
}

func (c *coordinator) publish(ctx context.Context, key string, pr *publishedResult) {
	body, err := json.Marshal(pr)
	if err != nil {
		c.abandon(ctx, key)
		return
	}
	c.post(ctx, "release", key, body)
}

// abandon frees our followers to rebuild. Called when we led and FAILED (or were
// cancelled): they must not wait out the lease TTL for a result that is never
// coming.
func (c *coordinator) abandon(ctx context.Context, key string) {
	// The build context is likely already cancelled — this must still get out.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	c.post(ctx, "abandon", key, nil)
}

func (c *coordinator) post(ctx context.Context, op, key string, body []byte) {
	url := fmt.Sprintf("%s/_rebuck/lease/%s/%s", c.base, op, key)
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, r)
	if err != nil {
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// adoptLeaderResult materializes what a peer built, WITHOUT executing anything.
//
// This is not a special path: it is exactly what a remote-cache hit does
// (cache/remotecache/v1/cachestorage.go Load -> worker.FromRemote ->
// worker.NewWorkerRefResult). The ref that comes back is LAZY — the layers are
// not pulled here, only when a downstream op actually mounts the snapshot — so a
// follower whose result is never used pays nothing.
//
// One output chain per vertex output, in order: edge.execOp indexes into the
// slice it gets back, so the shape has to survive the round trip.
func (e *ExecOp) adoptLeaderResult(ctx context.Context, pub *publishedResult) ([]solver.Result, error) {
	if pub == nil || len(pub.Outputs) == 0 {
		return nil, errors.Errorf("leader published no outputs")
	}
	results := make([]solver.Result, 0, len(pub.Outputs))
	for i, descs := range pub.Outputs {
		if len(descs) == 0 {
			// A genuinely empty output (scratch). NewWorkerRefResult on a nil ref
			// is how upstream represents that (see the exec.go output loop).
			results = append(results, worker.NewWorkerRefResult(nil, e.w))
			continue
		}
		ref, err := e.w.FromRemote(ctx, e.sf.remoteFor(descs))
		if err != nil {
			return nil, errors.Wrapf(err, "adopting leader's output %d", i)
		}
		results = append(results, worker.NewWorkerRefResult(ref, e.w))
	}
	return results, nil
}

// publishable turns our own results into the descriptor chains a follower can
// materialize.
//
// createIfNeeded=true: a freshly-executed snapshot has no layer blob yet, so
// this is what compresses it and puts it in the content store. It costs the
// LEADER latency on its critical path — the followers are already waiting, so
// better here than a second round of everybody rebuilding.
//
// Best-effort: if we cannot describe our result, we publish nothing and the
// followers rebuild. Slower, never wrong.
func (e *ExecOp) publishable(ctx context.Context, jobCtx solver.JobContext, results []solver.Result) *publishedResult {
	pub := &publishedResult{Outputs: make([][]ocispecs.Descriptor, 0, len(results))}
	for _, res := range results {
		wr, ok := res.Sys().(*worker.WorkerRef)
		if !ok || wr.ImmutableRef == nil {
			pub.Outputs = append(pub.Outputs, nil) // scratch output
			continue
		}
		remotes, err := wr.GetRemotes(ctx, true, e.refCfg(), false, jobCtx.Session())
		if err != nil || len(remotes) == 0 {
			bklog.G(ctx).Warnf("single-flight: cannot describe our result for publication (%v); followers will rebuild", err)
			return nil
		}
		pub.Outputs = append(pub.Outputs, remotes[0].Descriptors)
	}
	return pub
}

// refCfg mirrors worker/cacheresult.go: the default compression, which is what
// the registry cache exporter uses — so the layers a leader publishes are the
// same blobs its --cache-to would have pushed anyway, and both sides of the
// fleet agree on their digests.
func (e *ExecOp) refCfg() cacheconfig.RefConfig {
	return cacheconfig.RefConfig{Compression: compression.New(compression.Default)}
}

// remoteFor turns a leader's descriptor chain into a solver.Remote the local
// worker can materialize.
//
// solver.Remote.Provider must be a content.InfoReaderProvider — Info AND
// ReaderAt — and worker.FromRemote calls Info on every descriptor up front to
// check reachability. contentutil.FromFetcher gives us ReaderAt but no Info, so
// we synthesize Info from the descriptors themselves: the leader already told us
// each digest and size, and asking the registry again would be a round-trip to
// re-learn what we were just handed. This is the same trick the remote-cache
// importer plays with v1.DescriptorProviderPair.
func (c *coordinator) remoteFor(descs []ocispecs.Descriptor) *solver.Remote {
	if len(descs) == 0 {
		return nil
	}
	byDigest := make(map[string]ocispecs.Descriptor, len(descs))
	for _, d := range descs {
		byDigest[d.Digest.String()] = d
	}
	return &solver.Remote{
		Descriptors: descs,
		Provider: &descriptorProvider{
			descs: byDigest,
			inner: contentutil.FromFetcher(&blobFetcher{
				base:   c.base,
				repo:   c.repo,
				client: c.client,
			}),
		},
	}
}

type descriptorProvider struct {
	descs map[string]ocispecs.Descriptor
	inner content.Provider
}

func (p *descriptorProvider) Info(_ context.Context, dgst digest.Digest) (content.Info, error) {
	d, ok := p.descs[dgst.String()]
	if !ok {
		return content.Info{}, errors.Wrapf(cerrdefs.ErrNotFound, "descriptor %s not published by the leader", dgst)
	}
	return content.Info{Digest: d.Digest, Size: d.Size}, nil
}

func (p *descriptorProvider) ReaderAt(ctx context.Context, desc ocispecs.Descriptor) (content.ReaderAt, error) {
	return p.inner.ReaderAt(ctx, desc)
}

// blobFetcher pulls a layer straight from the coordinator's OCI registry over
// plain HTTP. Deliberately NOT going through BuildKit's resolver: that needs
// RegistryHosts (which lives on the concrete *base.Worker, not the Worker
// interface) plus a session, and we already know exactly where the bytes are.
type blobFetcher struct {
	base   string
	repo   string
	client *http.Client
}

func (f *blobFetcher) Fetch(ctx context.Context, desc ocispecs.Descriptor) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/v2/%s/blobs/%s", f.base, f.repo, desc.Digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "fetch %s", desc.Digest)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, errors.Errorf("fetch %s: registry returned %s", desc.Digest, resp.Status)
	}
	return resp.Body, nil
}

var _ remotes.Fetcher = &blobFetcher{}
