package llbsolver

import (
	"context"
	"encoding/json"
	"fmt"

	cacheconfig "github.com/moby/buildkit/cache/config"
	"github.com/moby/buildkit/exporter"
	"github.com/moby/buildkit/exporter/containerimage/exptypes"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/result"
	"github.com/moby/buildkit/worker"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

type earthlyInlineCacheItem struct {
	Key        digest.Digest `json:"cacheKey"`
	Descriptor digest.Digest `json:"descriptor"`
}

// earthlyInlineCache attaches custom "inline cache" metadata which can be used
// by a new build to load image layer blobs and use them as cache results.
func earthlyInlineCache(ctx context.Context, job *solver.Job, exp exporter.ExporterInstance, cached *result.Result[solver.CachedResult]) (map[string][]byte, error) {
	if cached.Ref != nil {
		return nil, errors.New("unexpected ref")
	}

	meta := map[string][]byte{}

	err := inBuilderContext(ctx, job, "preparing layers for inline cache", job.SessionID+"-cache-inline", func(ctx context.Context, _ session.Group) error {
		for k, res := range cached.Refs {
			val, err := earthlyInlineCacheDigests(ctx, job, exp, res)
			if err != nil {
				return err
			}
			meta[fmt.Sprintf("%s/%s", exptypes.EarthlyInlineCache, k)] = val
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return meta, nil
}

// earthlyInlineCacheDigests creates a map of computed cache keys to manifest
// layer hashes which will be used to load inline cache blobs.
func earthlyInlineCacheDigests(ctx context.Context, job *solver.Job, exp exporter.ExporterInstance, res solver.CachedResult) ([]byte, error) {
	workerRef, ok := res.Sys().(*worker.WorkerRef)
	if !ok {
		return nil, errors.Errorf("invalid reference: %T", res.Sys())
	}

	sess := session.NewGroup(job.SessionID)

	remotes, err := workerRef.GetRemotes(ctx, true, cacheconfig.RefConfig{Compression: exp.Config().Compression()}, false, sess)
	if err != nil || len(remotes) == 0 {
		return nil, nil
	}

	if len(remotes) < 1 {
		return nil, errors.New("expected at least one remote")
	}

	remote := remotes[0]

	cacheItems := []earthlyInlineCacheItem{}

	if len(remote.Descriptors) > len(res.CacheKeys()) {
		return nil, errors.New("insufficient cache keys")
	}

	for i, desc := range remote.Descriptors {
		cacheItems = append(cacheItems, earthlyInlineCacheItem{Key: res.CacheKeys()[i].Digest(), Descriptor: desc.Digest})
	}

	val, err := json.Marshal(cacheItems)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func hasInlineCacheExporter(exporters []RemoteCacheExporter) bool {
	for _, exp := range exporters {
		if _, ok := asInlineCache(exp.Exporter); ok {
			return true
		}
	}
	return false
}
