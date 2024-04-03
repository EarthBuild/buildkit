package worker

import (
	"context"

	"github.com/containerd/nydus-snapshotter/pkg/errdefs"
	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/solver"
)

// WorkerResultGetter abstracts the work involved in loading a Result from a
// worker using a ref ID.
type WorkerResultGetter struct {
	wc *Controller
}

// NewWorkerResultGetter creates and returns a new *WorkerResultGetter.
func NewWorkerResultGetter(wc *Controller) *WorkerResultGetter {
	return &WorkerResultGetter{wc: wc}
}

// Get a cached results from a worker.
func (w *WorkerResultGetter) Get(ctx context.Context, id string) (solver.Result, error) {
	workerID, refID, err := parseWorkerRef(id)
	if err != nil {
		return nil, err
	}

	worker, err := w.wc.Get(workerID)
	if err != nil {
		return nil, err
	}

	ref, err := worker.LoadRef(ctx, refID, false)
	if err != nil {
		if cache.IsNotFound(err) {
			return nil, errdefs.ErrNotFound
		}
		return nil, err
	}

	return NewWorkerRefResult(ref, worker), nil
}

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
