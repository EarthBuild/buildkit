package solver

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/util/tracing"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

const (
	runOnceLRUSize    = 2000
	parallelGuardWait = time.Millisecond * 100
)

// CommitRefFunc can be used to finalize a Result's ImmutableRef.
type CommitRefFunc func(ctx context.Context, result Result) error

// IsRunOnceFunc determines if the vertex represents an operation that needs to
// be run at least once.
type IsRunOnceFunc func(Vertex, Builder) (bool, error)

// ResultSource can be any source (local or remote) that allows one to load a
// Result using a cache key digest.
type ResultSource interface {
	Load(ctx context.Context, cacheKey digest.Digest) (Result, bool, error)
}

// runOnceCtrl is a simple wrapper around an LRU cache. It's used to ensure that
// an operation is only run once per job. However, this is not guaranteed, as
// the struct uses a reasonable small LRU size to preview excessive memory use.
type runOnceCtrl struct {
	lru *simplelru.LRU
	mu  sync.Mutex
}

func newRunOnceCtrl() *runOnceCtrl {
	lru, _ := simplelru.NewLRU(runOnceLRUSize, nil) // Error impossible on positive first argument.
	return &runOnceCtrl{lru: lru}
}

// hasRun: Here, we use an LRU cache to whether we need to execute the source
// operation for this job. The jobs may be re-run if the LRU size is exceeded,
// but this shouldn't have a big impact on the build. The trade-off is
// worthwhile given the memory-friendliness of LRUs.
func (s *runOnceCtrl) hasRun(d digest.Digest, sessionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s:%s", sessionID, d)
	ret := s.lru.Contains(key)

	s.lru.Add(key, struct{}{})

	return ret
}

type simpleSolver struct {
	resolveOpFunc   ResolveOpFunc
	isRunOnceFunc   IsRunOnceFunc
	commitRefFunc   CommitRefFunc
	solver          *Solver
	parallelGuard   *parallelGuard
	refIDStore      *RefIDStore
	resultSource    ResultSource
	cacheKeyManager *cacheKeyManager
	runOnceCtrl     *runOnceCtrl
}

func newSimpleSolver(
	resolveOpFunc ResolveOpFunc,
	commitRefFunc CommitRefFunc,
	solver *Solver,
	refIDStore *RefIDStore,
	resultSource ResultSource,
	isRunOnceFunc IsRunOnceFunc,
) *simpleSolver {
	return &simpleSolver{
		cacheKeyManager: newCacheKeyManager(),
		parallelGuard:   newParallelGuard(parallelGuardWait),
		resolveOpFunc:   resolveOpFunc,
		commitRefFunc:   commitRefFunc,
		solver:          solver,
		refIDStore:      refIDStore,
		resultSource:    resultSource,
		isRunOnceFunc:   isRunOnceFunc,
		runOnceCtrl:     newRunOnceCtrl(),
	}
}

func (s *simpleSolver) build(ctx context.Context, job *Job, e Edge) (CachedResult, error) {

	// Ordered list of vertices to build.
	digests, vertices := s.exploreVertices(e)

	var ret Result
	var expKeys []ExportableCacheKey

	for _, d := range digests {
		vertex, ok := vertices[d]
		if !ok {
			return nil, errors.Errorf("digest %s not found", d)
		}

		res, cacheKey, err := s.buildOne(ctx, d, vertex, job, e)
		if err != nil {
			return nil, err
		}

		// Release previous result as this is not the final return value.
		if ret != nil {
			err := ret.Release(ctx)
			if err != nil {
				return nil, err
			}
		}

		ret = res

		// Hijack the CacheKey type in order to export a reference from the new cache key to the ref ID.
		expKeys = append(expKeys, ExportableCacheKey{
			CacheKey: &CacheKey{
				ID:     res.ID(),
				digest: cacheKey,
			},
			Exporter: nil, // We're not using an exporter here.
		})
	}

	err := s.commitRefFunc(ctx, ret)
	if err != nil {
		return nil, err
	}

	return NewCachedResult(ret, expKeys), nil
}

func (s *simpleSolver) buildOne(ctx context.Context, d digest.Digest, vertex Vertex, job *Job, e Edge) (Result, digest.Digest, error) {
	st := s.state(vertex, job)

	// Add cache opts to context as they will be accessed by cache retrieval.
	ctx = withAncestorCacheOpts(ctx, st)

	// CacheMap populates required fields in SourceOp.
	cm, err := st.op.CacheMap(ctx, int(e.Index))
	if err != nil {
		return nil, "", err
	}

	inputs, err := s.preprocessInputs(ctx, st, vertex, cm.CacheMap, job)
	if err != nil {
		notifyError(ctx, st, false, err)
		return nil, "", err
	}

	cacheKey, err := s.cacheKeyManager.cacheKey(ctx, d)
	if err != nil {
		return nil, "", err
	}

	// Ensure we don't have multiple threads working on the same operation. The
	// computed cache key needs to be used here instead of the vertex
	// digest. This is because the vertex can sometimes differ for the same
	// operation depending on its ancestors.
	wait, done := s.parallelGuard.acquire(ctx, cacheKey)
	defer done()
	<-wait

	isRunOnce, err := s.isRunOnceFunc(vertex, job)
	if err != nil {
		return nil, "", err
	}

	// Special case for source operations. They need to be run once per build or
	// content changes will not be reliably detected.
	mayLoadCache := !isRunOnce || isRunOnce && s.runOnceCtrl.hasRun(cacheKey, job.SessionID)

	if mayLoadCache {
		v, ok, err := s.resultSource.Load(ctx, cacheKey)
		if err != nil {
			return nil, "", err
		}

		if ok && v != nil {
			notifyError(ctx, st, true, nil)
			return v, cacheKey, nil
		}
	}

	results, _, err := st.op.Exec(ctx, inputs)
	if err != nil {
		return nil, "", err
	}

	res := results[int(e.Index)]

	for i := range results {
		if i != int(e.Index) {
			err = results[i].Release(ctx)
			if err != nil {
				return nil, "", err
			}
		}
	}

	err = s.refIDStore.Set(ctx, cacheKey, res.ID())
	if err != nil {
		return nil, "", err
	}

	return res, cacheKey, nil
}

func notifyError(ctx context.Context, st *state, cached bool, err error) {
	ctx = progress.WithProgress(ctx, st.mpw)
	notifyCompleted := notifyStarted(ctx, &st.clientVertex, cached)
	notifyCompleted(err, cached)
}

func (s *simpleSolver) state(vertex Vertex, job *Job) *state {
	s.solver.mu.Lock()
	defer s.solver.mu.Unlock()
	if st, ok := s.solver.actives[vertex.Digest()]; ok {
		st.jobs[job] = struct{}{}
		return st
	}
	return s.createState(vertex, job)
}

// createState creates a new state struct with required and placeholder values.
func (s *simpleSolver) createState(vertex Vertex, job *Job) *state {
	defaultCache := NewInMemoryCacheManager()

	st := &state{
		opts:         SolverOpt{DefaultCache: defaultCache, ResolveOpFunc: s.resolveOpFunc},
		parents:      map[digest.Digest]struct{}{},
		childVtx:     map[digest.Digest]struct{}{},
		allPw:        map[progress.Writer]struct{}{},
		mpw:          progress.NewMultiWriter(progress.WithMetadata("vertex", vertex.Digest())),
		mspan:        tracing.NewMultiSpan(),
		vtx:          vertex,
		clientVertex: initClientVertex(vertex),
		edges:        map[Index]*edge{},
		index:        s.solver.index,
		mainCache:    defaultCache,
		cache:        map[string]CacheManager{},
		solver:       s.solver,
		origDigest:   vertex.Digest(),
	}

	st.jobs = map[*Job]struct{}{
		job: {},
	}

	st.mpw.Add(job.pw)

	// Hack: this is used in combination with withAncestorCacheOpts to pass
	// necessary dependency information to a few caching components. We'll need
	// to expire these keys somehow. We should also move away from using the
	// actives map, but it's still being used by withAncestorCacheOpts for now.
	s.solver.actives[vertex.Digest()] = st

	op := newSharedOp(st.opts.ResolveOpFunc, st.opts.DefaultCache, st)

	// Required to access cache map results on state.
	st.op = op

	return st
}

func (s *simpleSolver) exploreVertices(e Edge) ([]digest.Digest, map[digest.Digest]Vertex) {

	digests := []digest.Digest{e.Vertex.Digest()}
	vertices := map[digest.Digest]Vertex{
		e.Vertex.Digest(): e.Vertex,
	}

	for _, edge := range e.Vertex.Inputs() {
		d, v := s.exploreVertices(edge)
		digests = append(d, digests...)
		for key, value := range v {
			vertices[key] = value
		}
	}

	ret := []digest.Digest{}
	m := map[digest.Digest]struct{}{}
	for _, d := range digests {
		if _, ok := m[d]; !ok {
			ret = append(ret, d)
			m[d] = struct{}{}
		}
	}

	return ret, vertices
}

func (s *simpleSolver) preprocessInputs(ctx context.Context, st *state, vertex Vertex, cm *CacheMap, job *Job) ([]Result, error) {
	// This struct is used to reconstruct a cache key from an LLB digest & all
	// parents using consistent digests that depend on the full dependency chain.
	scm := simpleCacheMap{
		digest: cm.Digest,
		deps:   make([]cacheMapDep, len(cm.Deps)),
		inputs: make([]digest.Digest, len(cm.Deps)),
	}

	// By default we generate a cache key that's not salted as the keys need to
	// persist across builds. However, when cache is disabled, we scope the keys
	// to the current session. This is because some jobs will be duplicated in a
	// given build & will need to be cached in a limited way.
	if vertex.Options().IgnoreCache {
		scm.salt = job.SessionID
	}

	var inputs []Result

	for i, in := range vertex.Inputs() {

		d := in.Vertex.Digest()

		// Compute a cache key given the LLB digest value.
		cacheKey, err := s.cacheKeyManager.cacheKey(ctx, d)
		if err != nil {
			return nil, err
		}

		// Lookup the result for that cache key.
		res, ok, err := s.resultSource.Load(ctx, cacheKey)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errors.Errorf("result not found for digest: %s", d)
		}

		dep := cm.Deps[i]

		// Unlazy the result.
		if dep.PreprocessFunc != nil {
			err = dep.PreprocessFunc(ctx, res, st)
			if err != nil {
				return nil, err
			}
		}

		// Add selectors (usually file references) to the struct.
		scm.deps[i] = cacheMapDep{
			selector: dep.Selector,
		}

		// ComputeDigestFunc will usually checksum files. This is then used as
		// part of the cache key to ensure it's consistent & distinct for this
		// operation.
		if dep.ComputeDigestFunc != nil {
			compDigest, err := dep.ComputeDigestFunc(ctx, res, st)
			if err != nil {
				bklog.G(ctx).Warnf("failed to compute digest: %v", err)
				return nil, err
			} else {
				scm.deps[i].computed = compDigest
			}
		}

		// The result can be released now that the preprocess & slow cache
		// digest functions have been run. This is crucial as failing to do so
		// will lead to full file copying from previously executed source
		// operations.
		err = res.Release(ctx)
		if err != nil {
			return nil, err
		}

		// Add input references to the struct as to link dependencies.
		scm.inputs[i] = in.Vertex.Digest()

		// Add the cached result to the input set. These inputs are used to
		// reconstruct dependencies (mounts, etc.) for a new container run.
		inputs = append(inputs, res)
	}

	s.cacheKeyManager.add(vertex.Digest(), &scm)

	return inputs, nil
}

type cacheKeyManager struct {
	cacheMaps map[digest.Digest]*simpleCacheMap
	mu        sync.Mutex
}

type cacheMapDep struct {
	selector digest.Digest
	computed digest.Digest
}

type simpleCacheMap struct {
	digest digest.Digest
	inputs []digest.Digest
	deps   []cacheMapDep
	salt   string
}

func newCacheKeyManager() *cacheKeyManager {
	return &cacheKeyManager{
		cacheMaps: map[digest.Digest]*simpleCacheMap{},
	}
}

func (m *cacheKeyManager) add(d digest.Digest, s *simpleCacheMap) {
	m.mu.Lock()
	m.cacheMaps[d] = s
	m.mu.Unlock()
}

// cacheKey recursively generates a cache key based on a sequence of ancestor
// operations & their cacheable values.
func (m *cacheKeyManager) cacheKey(ctx context.Context, d digest.Digest) (digest.Digest, error) {
	h := sha256.New()

	err := m.cacheKeyRecurse(ctx, d, h)
	if err != nil {
		return "", err
	}

	return newDigest(fmt.Sprintf("%x", h.Sum(nil))), nil
}

func (m *cacheKeyManager) cacheKeyRecurse(ctx context.Context, d digest.Digest, h hash.Hash) error {
	m.mu.Lock()
	c, ok := m.cacheMaps[d]
	m.mu.Unlock()
	if !ok {
		return errors.New("missing cache map key")
	}

	if c.salt != "" {
		io.WriteString(h, c.salt)
	}

	for _, in := range c.inputs {
		err := m.cacheKeyRecurse(ctx, in, h)
		if err != nil {
			return err
		}
	}

	io.WriteString(h, c.digest.String())
	for _, dep := range c.deps {
		if dep.selector != "" {
			io.WriteString(h, dep.selector.String())
		}
		if dep.computed != "" {
			io.WriteString(h, dep.computed.String())
		}
	}

	return nil
}

type parallelGuard struct {
	wait   time.Duration
	active map[digest.Digest]struct{}
	mu     sync.Mutex
}

func newParallelGuard(wait time.Duration) *parallelGuard {
	return &parallelGuard{wait: wait, active: map[digest.Digest]struct{}{}}
}

func (f *parallelGuard) acquire(ctx context.Context, d digest.Digest) (<-chan struct{}, func()) {

	ch := make(chan struct{})

	closer := func() {
		f.mu.Lock()
		delete(f.active, d)
		f.mu.Unlock()
	}

	go func() {
		tick := time.NewTicker(f.wait)
		defer tick.Stop()
		// A function is used here as the above ticker does not execute
		// immediately.
		check := func() bool {
			f.mu.Lock()
			if _, ok := f.active[d]; !ok {
				f.active[d] = struct{}{}
				close(ch)
				f.mu.Unlock()
				return true
			}
			f.mu.Unlock()
			return false
		}
		if check() {
			return
		}
		for {
			select {
			case <-tick.C:
				if check() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, closer
}

// RefIDStore uses a BoltDB database to store links from computed cache keys to
// worker ref IDs.
type RefIDStore struct {
	db         *bolt.DB
	bucketName string
	rootDir    string
}

// NewRefIDStore creates and returns a new store and initializes a BoltDB
// instance in the specified root directory.
func NewRefIDStore(rootDir string) (*RefIDStore, error) {
	r := &RefIDStore{
		bucketName: "ids",
		rootDir:    rootDir,
	}
	err := r.init()
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *RefIDStore) init() error {
	db, err := bolt.Open(filepath.Join(r.rootDir, "ids.db"), 0755, nil)
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("ids"))
		return err
	})
	if err != nil {
		return err
	}
	r.db = db
	return nil
}

// Set a cache key digest to the value of the worker ref ID.
func (r *RefIDStore) Set(ctx context.Context, key digest.Digest, id string) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(r.bucketName))
		return b.Put([]byte(key), []byte(id))
	})
}

// Get a worker ref ID given a cache key digest.
func (r *RefIDStore) Get(ctx context.Context, cacheKey digest.Digest) (string, bool, error) {
	var id string
	err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(r.bucketName))
		id = string(b.Get([]byte(cacheKey)))
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if id == "" {
		return "", false, nil
	}
	return id, true, nil
}

func (r *RefIDStore) delete(_ context.Context, key string) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(r.bucketName))
		return b.Delete([]byte(key))
	})
}

func newDigest(s string) digest.Digest {
	return digest.NewDigestFromEncoded(digest.SHA256, s)
}
