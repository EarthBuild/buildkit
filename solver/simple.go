package solver

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/docker/docker/errdefs"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/util/tracing"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// CommitRefFunc can be used to finalize a Result's ImmutableRef.
type CommitRefFunc func(ctx context.Context, result Result) error

type simpleSolver struct {
	resolveOpFunc   ResolveOpFunc
	commitRefFunc   CommitRefFunc
	solver          *Solver
	job             *Job
	parallelGuard   *parallelGuard
	resultCache     resultCache
	cacheKeyManager *cacheKeyManager
	mu              sync.Mutex
}

func newSimpleSolver(resolveOpFunc ResolveOpFunc, commitRefFunc CommitRefFunc, solver *Solver, cache resultCache) *simpleSolver {
	return &simpleSolver{
		cacheKeyManager: newCacheKeyManager(),
		resultCache:     cache,
		parallelGuard:   newParallelGuard(time.Millisecond * 100),
		resolveOpFunc:   resolveOpFunc,
		commitRefFunc:   commitRefFunc,
		solver:          solver,
	}
}

func (s *simpleSolver) build(ctx context.Context, job *Job, e Edge) (CachedResult, error) {

	// Ordered list of vertices to build.
	digests, vertices := s.exploreVertices(e)

	var ret Result

	for _, d := range digests {
		vertex, ok := vertices[d]
		if !ok {
			return nil, errors.Errorf("digest %s not found", d)
		}

		res, err := s.buildOne(ctx, d, vertex, job, e)
		if err != nil {
			return nil, err
		}

		ret = res
	}

	return NewCachedResult(ret, []ExportableCacheKey{}), nil
}

func (s *simpleSolver) buildOne(ctx context.Context, d digest.Digest, vertex Vertex, job *Job, e Edge) (Result, error) {
	// Ensure we don't have multiple threads working on the same digest.
	wait, done := s.parallelGuard.acquire(ctx, d.String())
	defer done()
	<-wait

	st := s.createState(vertex, job)

	op := newSharedOp(st.opts.ResolveOpFunc, st.opts.DefaultCache, st)

	// Required to access cache map results on state.
	st.op = op

	// Add cache opts to context as they will be accessed by cache retrieval.
	ctx = withAncestorCacheOpts(ctx, st)

	// CacheMap populates required fields in SourceOp.
	cm, err := op.CacheMap(ctx, int(e.Index))
	if err != nil {
		return nil, err
	}

	inputs, err := s.preprocessInputs(ctx, st, vertex, cm.CacheMap)
	if err != nil {
		return nil, err
	}

	cacheKey, err := s.cacheKeyManager.cacheKey(ctx, d.String())
	if err != nil {
		return nil, err
	}

	v, ok, err := s.resultCache.get(ctx, cacheKey)
	if err != nil {
		return nil, err
	}

	if ok && v != nil {
		ctx = progress.WithProgress(ctx, st.mpw)
		notifyCompleted := notifyStarted(ctx, &st.clientVertex, true)
		notifyCompleted(nil, true)
		return v, nil
	}

	results, _, err := op.Exec(ctx, inputs)
	if err != nil {
		return nil, err
	}

	// Ensure all results are finalized (committed to cache). It may be better
	// to background these calls at some point.
	for _, res := range results {
		err = s.commitRefFunc(ctx, res)
		if err != nil {
			return nil, err
		}
	}

	res := results[int(e.Index)]

	err = s.resultCache.set(ctx, cacheKey, res)
	if err != nil {
		return nil, err
	}

	return res, nil
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
	s.solver.mu.Lock()
	s.solver.actives[vertex.Digest()] = st
	s.solver.mu.Unlock()

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

func (s *simpleSolver) preprocessInputs(ctx context.Context, st *state, vertex Vertex, cm *CacheMap) ([]Result, error) {
	// This struct is used to reconstruct a cache key from an LLB digest & all
	// parents using consistent digests that depend on the full dependency chain.
	scm := simpleCacheMap{
		digest: cm.Digest.String(),
		deps:   make([]cacheMapDep, len(cm.Deps)),
		inputs: make([]string, len(cm.Deps)),
	}

	var inputs []Result

	for i, in := range vertex.Inputs() {
		// Compute a cache key given the LLB digest value.
		cacheKey, err := s.cacheKeyManager.cacheKey(ctx, in.Vertex.Digest().String())
		if err != nil {
			return nil, err
		}

		// Lookup the result for that cache key.
		res, ok, err := s.resultCache.get(ctx, cacheKey)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errors.Errorf("cache key not found: %s", cacheKey)
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
			selector: dep.Selector.String(),
		}

		// ComputeDigestFunc will usually checksum files. This is then used as
		// part of the cache key to ensure it's consistent & distinct for this
		// operation.
		if dep.ComputeDigestFunc != nil {
			compDigest, err := dep.ComputeDigestFunc(ctx, res, st)
			if err != nil {
				return nil, err
			}
			scm.deps[i].computed = compDigest.String()
		}

		// Add input references to the struct as to link dependencies.
		scm.inputs[i] = in.Vertex.Digest().String()

		// Add the cached result to the input set. These inputs are used to
		// reconstruct dependencies (mounts, etc.) for a new container run.
		inputs = append(inputs, res)
	}

	s.cacheKeyManager.add(vertex.Digest().String(), &scm)

	return inputs, nil
}

type cacheKeyManager struct {
	cacheMaps map[string]*simpleCacheMap
	mu        sync.Mutex
}

type cacheMapDep struct {
	selector string
	computed string
}

type simpleCacheMap struct {
	digest string
	inputs []string
	deps   []cacheMapDep
}

func newCacheKeyManager() *cacheKeyManager {
	return &cacheKeyManager{
		cacheMaps: map[string]*simpleCacheMap{},
	}
}

func (m *cacheKeyManager) add(key string, s *simpleCacheMap) {
	m.mu.Lock()
	m.cacheMaps[key] = s
	m.mu.Unlock()
}

// cacheKey recursively generates a cache key based on a sequence of ancestor
// operations & their cacheable values.
func (m *cacheKeyManager) cacheKey(ctx context.Context, d string) (string, error) {
	h := sha256.New()

	err := m.cacheKeyRecurse(ctx, d, h)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (m *cacheKeyManager) cacheKeyRecurse(ctx context.Context, d string, h hash.Hash) error {
	m.mu.Lock()
	c, ok := m.cacheMaps[d]
	m.mu.Unlock()
	if !ok {
		return errors.New("missing cache map key")
	}

	for _, in := range c.inputs {
		err := m.cacheKeyRecurse(ctx, in, h)
		if err != nil {
			return err
		}
	}

	io.WriteString(h, c.digest)
	for _, dep := range c.deps {
		if dep.selector != "" {
			io.WriteString(h, dep.selector)
		}
		if dep.computed != "" {
			io.WriteString(h, dep.computed)
		}
	}

	return nil
}

type parallelGuard struct {
	wait   time.Duration
	active map[string]struct{}
	mu     sync.Mutex
}

func newParallelGuard(wait time.Duration) *parallelGuard {
	return &parallelGuard{wait: wait, active: map[string]struct{}{}}
}

func (f *parallelGuard) acquire(ctx context.Context, d string) (<-chan struct{}, func()) {

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

type resultCache interface {
	set(ctx context.Context, key string, r Result) error
	get(ctx context.Context, key string) (Result, bool, error)
}

type inMemCache struct {
	cache map[string]Result
	mu    sync.Mutex
}

func newInMemCache() *inMemCache {
	return &inMemCache{cache: map[string]Result{}}
}

func (c *inMemCache) set(ctx context.Context, key string, r Result) error {
	c.mu.Lock()
	c.cache[key] = r
	c.mu.Unlock()
	return nil
}

func (c *inMemCache) get(ctx context.Context, key string) (Result, bool, error) {
	c.mu.Lock()
	r, ok := c.cache[key]
	c.mu.Unlock()
	return r, ok, nil
}

var _ resultCache = &inMemCache{}

type diskCache struct {
	resultGetter workerResultGetter
	db           *bolt.DB
	bucketName   string
}

type workerResultGetter interface {
	Get(ctx context.Context, id string) (Result, error)
}

func newDiskCache(resultGetter workerResultGetter) (*diskCache, error) {
	c := &diskCache{
		bucketName:   "ids",
		resultGetter: resultGetter,
	}
	err := c.init()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *diskCache) init() error {
	// TODO: pass in root config directory.
	db, err := bolt.Open("/tmp/earthly/buildkit/simple.db", 0600, nil)
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
	c.db = db
	return nil
}

func (c *diskCache) set(ctx context.Context, key string, r Result) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(c.bucketName))
		return b.Put([]byte(key), []byte(r.ID()))
	})
}

func (c *diskCache) get(ctx context.Context, key string) (Result, bool, error) {
	var id string
	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(c.bucketName))
		id = string(b.Get([]byte(key)))
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if id == "" {
		return nil, false, nil
	}
	res, err := c.resultGetter.Get(ctx, id)
	if err != nil {
		if errdefs.IsNotFound(err) {
			bklog.G(ctx).Warnf("failed to get cached result from worker: %v", err)
			return nil, false, nil
		}
		return nil, false, err
	}
	return res, true, nil
}

var _ resultCache = &diskCache{}
