package worker

import (
	"context"
	"sync"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	digest "github.com/opencontainers/go-digest"
)

// RefIDSource allows the caller to translate between a cache key and a worker ref ID.
type RefIDSource interface {
	Get(ctx context.Context, cacheKey digest.Digest) (string, bool, error)
}

// WorkerResultSource abstracts the work involved in loading a Result from a
// worker using a ref ID.
type WorkerResultSource struct {
	wc  *Controller
	ids RefIDSource
}

// NewWorkerResultSource creates and returns a new *WorkerResultSource.
func NewWorkerResultSource(wc *Controller, ids RefIDSource) *WorkerResultSource {
	return &WorkerResultSource{wc: wc, ids: ids}
}

// Load a cached result from a worker.
func (w *WorkerResultSource) Load(ctx context.Context, cacheKey digest.Digest) (solver.Result, bool, error) {
	id, ok, err := w.ids.Get(ctx, cacheKey)
	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, false, nil
	}

	workerID, refID, err := parseWorkerRef(id)
	if err != nil {
		return nil, false, err
	}

	worker, err := w.wc.Get(workerID)
	if err != nil {
		return nil, false, err
	}

	ref, err := worker.LoadRef(ctx, refID, false)
	if err != nil {
		if cache.IsNotFound(err) {
			bklog.G(ctx).Warnf("could not load ref from worker: %v", err)
			return nil, false, nil
		}
		return nil, false, err
	}

	return NewWorkerRefResult(ref, worker), true, nil
}

var _ solver.ResultSource = &WorkerResultSource{}

// FinalizeRef is a convenience function that calls Finalize on a Result's
// ImmutableRef. The 'worker' package cannot be imported by 'solver' due to an
// import cycle, so this function is passed in with solver.SolverOpt.
func FinalizeRef(ctx context.Context, res solver.Result) error {
	sys := res.Sys()
	if w, ok := sys.(*WorkerRef); ok {
		err := w.ImmutableRef.Finalize(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// WorkerRemoteSource can be used to fetch a remote worker source.
type WorkerRemoteSource struct {
	worker  Worker
	remotes map[digest.Digest]*solver.Remote
	mu      sync.Mutex
}

// NewWorkerRemoteSource creates and returns a remote result source.
func NewWorkerRemoteSource(worker Worker) *WorkerRemoteSource {
	return &WorkerRemoteSource{
		worker:  worker,
		remotes: map[digest.Digest]*solver.Remote{},
	}
}

// Load a Result from the worker.
func (w *WorkerRemoteSource) Load(ctx context.Context, cacheKey digest.Digest) (solver.Result, bool, error) {
	w.mu.Lock()
	remote, ok := w.remotes[cacheKey]
	w.mu.Unlock()

	if !ok {
		return nil, false, nil
	}

	ref, err := w.worker.FromRemote(ctx, remote)
	if err != nil {
		return nil, false, err
	}

	return NewWorkerRefResult(ref, w.worker), true, nil
}

// AddResult adds a solver.Remote source for the given cache key.
func (w *WorkerRemoteSource) AddResult(ctx context.Context, cacheKey digest.Digest, remote *solver.Remote) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.remotes[cacheKey] = remote
}

var _ solver.ResultSource = &WorkerRemoteSource{}

// CombinedResultSource implements solver.ResultSource over a list of sources.
type CombinedResultSource struct {
	sources []solver.ResultSource
}

// NewCombinedResultSource creates and returns a new source from a list of sources.
func NewCombinedResultSource(sources ...solver.ResultSource) *CombinedResultSource {
	return &CombinedResultSource{sources: sources}
}

// Load attempts to load a Result from all underlying sources.
func (c *CombinedResultSource) Load(ctx context.Context, cacheKey digest.Digest) (solver.Result, bool, error) {
	for _, source := range c.sources {
		res, ok, err := source.Load(ctx, cacheKey)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return res, true, nil
		}
	}
	return nil, false, nil
}

var _ solver.ResultSource = &CombinedResultSource{}
