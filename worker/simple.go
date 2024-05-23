package worker

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

const refIDPrunePeriod = 30 * time.Minute

// WorkerResultSource abstracts the work involved in loading a Result from a
// worker using a ref ID.
type WorkerResultSource struct {
	wc          *Controller
	ids         *refIDStore
	prunePeriod time.Duration
}

// NewWorkerResultSource creates and returns a new *WorkerResultSource.
func NewWorkerResultSource(wc *Controller, rootDir string) (*WorkerResultSource, error) {
	ids, err := newRefIDStore(rootDir)
	if err != nil {
		return nil, err
	}
	w := &WorkerResultSource{
		wc:          wc,
		ids:         ids,
		prunePeriod: refIDPrunePeriod,
	}
	go w.pruneLoop(context.Background())
	return w, nil
}

// Load a cached result from a worker.
func (w *WorkerResultSource) Load(ctx context.Context, cacheKey digest.Digest) (solver.Result, bool, error) {
	fullID, ok, err := w.ids.get(cacheKey)
	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, false, nil
	}

	workerID, refID, err := parseWorkerRef(fullID)
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
			bklog.G(ctx).Warnf("worker ref not found: %v", err)
			return nil, false, nil
		}
		return nil, false, err
	}

	return NewWorkerRefResult(ref, worker), true, nil
}

// Link a simple solver cache key to the worker ref.
func (w *WorkerResultSource) Link(ctx context.Context, cacheKey digest.Digest, refID string) error {
	return w.ids.set(cacheKey, refID)
}

// pruneLoop attempts to prune stale red IDs from the BoltDB database every prunePeriod.
func (w *WorkerResultSource) pruneLoop(ctx context.Context) {
	tick := time.NewTicker(w.prunePeriod)
	for range tick.C {
		start := time.Now()
		examined, pruned, err := w.prune(ctx)
		if err != nil {
			bklog.G(ctx).Warnf("failed to prune ref IDs: %v", err)
		} else {
			bklog.G(ctx).Warnf("examined %d, pruned %d stale ref IDs in %s", examined, pruned, time.Now().Sub(start))
		}
	}
}

// exists determines if the worker ref ID exists. It's important that the ref is
// released after being loaded.
func (w *WorkerResultSource) exists(ctx context.Context, fullID string) (bool, error) {
	workerID, refID, err := parseWorkerRef(fullID)
	if err != nil {
		return false, err
	}

	worker, err := w.wc.Get(workerID)
	if err != nil {
		return false, err
	}

	ref, err := worker.LoadRef(ctx, refID, false)
	if err != nil {
		if cache.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	ref.Release(ctx)

	return true, nil
}

// prune ref IDs by identifying and purging all stale IDs. Ref IDs are unique
// and will not be rewritten by a fresh build. Hence, it's safe to delete these
// items once a worker ref has been pruned by BuildKit.
func (w *WorkerResultSource) prune(ctx context.Context) (int, int, error) {
	var deleteIDs []digest.Digest
	var examined, pruned int

	err := w.ids.walk(func(d digest.Digest, id string) error {
		exists, err := w.exists(ctx, id)
		if err != nil {
			return err
		}
		examined++
		if !exists {
			deleteIDs = append(deleteIDs, d)
		}
		return nil
	})
	if err != nil {
		return examined, 0, err
	}

	for _, deleteID := range deleteIDs {
		err = w.ids.del(deleteID)
		if err != nil {
			return examined, pruned, err
		}
		pruned++
	}

	return examined, pruned, nil
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

func (c *WorkerRemoteSource) Link(ctx context.Context, cacheKey digest.Digest, refID string) error {
	return nil // noop
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

// Link a cache key to a ref ID. Only used by the worker result source.
func (c *CombinedResultSource) Link(ctx context.Context, cacheKey digest.Digest, refID string) error {
	for _, source := range c.sources {
		err := source.Link(ctx, cacheKey, refID)
		if err != nil {
			return nil
		}
	}
	return nil
}

var _ solver.ResultSource = &CombinedResultSource{}

// refIDStore uses a BoltDB database to store links from computed cache keys to
// worker ref IDs.
type refIDStore struct {
	db          *bolt.DB
	rootDir     string
	bucket      string
	prunePeriod time.Duration
}

// newRefIDStore creates and returns a new store and initializes a BoltDB
// instance in the specified root directory.
func newRefIDStore(rootDir string) (*refIDStore, error) {
	r := &refIDStore{
		bucket:      "ids",
		rootDir:     rootDir,
		prunePeriod: refIDPrunePeriod,
	}
	err := r.init()
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *refIDStore) init() error {
	db, err := bolt.Open(filepath.Join(r.rootDir, "ids.db"), 0755, nil)
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(r.bucket))
		return err
	})
	r.db = db
	return nil
}

// set a cache key digest to the value of the worker ref ID. It also sets the
// access time for the key.
func (r *refIDStore) set(cacheKey digest.Digest, id string) error {
	err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(r.bucket))
		return b.Put([]byte(cacheKey), []byte(id))
	})
	if err != nil {
		return errors.Wrap(err, "failed to set ref ID")
	}
	return nil
}

// get a worker ref ID given a cache key digest. It also sets the
// access time for the key.
func (r *refIDStore) get(cacheKey digest.Digest) (string, bool, error) {
	var id string
	err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(r.bucket))
		id = string(b.Get([]byte(cacheKey)))
		return nil
	})
	if err != nil {
		return "", false, errors.Wrap(err, "failed to load ref ID")
	}
	if id == "" {
		return "", false, nil
	}
	return id, true, nil
}

// del removes a single cache key from the ref ID database.
func (r *refIDStore) del(cacheKey digest.Digest) error {
	err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(r.bucket))
		return b.Delete([]byte(cacheKey))
	})
	if err != nil {
		return errors.Wrap(err, "failed to delete key")
	}
	return nil
}

// walk all items in the database using a callback function.
func (r *refIDStore) walk(fn func(digest.Digest, string) error) error {

	all := map[digest.Digest]string{}

	err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(r.bucket))
		err := b.ForEach(func(k, v []byte) error {
			d := digest.Digest(string(k))
			all[d] = string(v)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to iterate ref IDs")
	}

	// The callback needs to be invoked outside of the BoltDB View
	// transaction. Update is an option, but it's much slower. ForEach cannot
	// modify results inside the loop.
	for d, id := range all {
		err = fn(d, id)
		if err != nil {
			return err
		}
	}

	return nil
}
