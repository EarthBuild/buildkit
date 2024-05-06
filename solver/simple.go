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

	"github.com/davecgh/go-spew/spew"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/util/tracing"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

var ErrRefNotFound = errors.New("ref not found")

// CommitRefFunc can be used to finalize a Result's ImmutableRef.
type CommitRefFunc func(ctx context.Context, result Result) error

type simpleSolver struct {
	resolveOpFunc   ResolveOpFunc
	commitRefFunc   CommitRefFunc
	solver          *Solver
	parallelGuard   *parallelGuard
	resultCache     resultCache
	cacheManager    CacheManager
	cacheKeyManager *cacheKeyManager
	mu              sync.Mutex
}

var (
	depMu       = sync.Mutex{}
	printMu     = sync.Mutex{}
	depResults  = map[string]CachedResult{}
	debugDigest = "sha256:4eeaa0a0e2eb217e9e35404a781964714a63abadc4c275ef830117c01f3ab1f8"
)

func newSimpleSolver(resolveOpFunc ResolveOpFunc, commitRefFunc CommitRefFunc, solver *Solver, cache resultCache, cacheManager CacheManager) *simpleSolver {
	return &simpleSolver{
		cacheKeyManager: newCacheKeyManager(),
		cacheManager:    cacheManager,
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

	var ret CachedResult

	for _, d := range digests {
		vertex, ok := vertices[d]
		if !ok {
			return nil, errors.Errorf("digest %s not found", d)
		}

		res, err := s.buildLegacy(ctx, d, vertex, job, e)
		if err != nil {
			return nil, err
		}

		ret = res
	}

	return ret, nil
}

func (s *simpleSolver) buildLegacy(ctx context.Context, d digest.Digest, vtx Vertex, job *Job, e Edge) (CachedResult, error) {

	// Ensure we don't have multiple threads working on the same digest.
	wait, done := s.parallelGuard.acquire(ctx, d.String())
	defer done()
	<-wait

	st := s.state(vtx, job)

	// CacheMap populates required fields in SourceOp.
	cm, err := st.op.CacheMap(ctx, 0)
	if err != nil {
		return nil, err
	}

	cacheKey, inputs, err := s.preprocessInputsLegacy(ctx, st, cm.CacheMap, vtx)
	if err != nil {
		return nil, err
	}

	if cm.Digest.String() == debugDigest {
		printMu.Lock()
		fmt.Println("Checking cache")
		spew.Dump(cacheKey.TraceFields())
		printMu.Unlock()
		cacheKey.debug = true
	}

	records, err := st.op.Cache().Records(ctx, cacheKey)
	if err != nil {
		return nil, err
	}

	printMu.Lock()
	fmt.Println(vtx.Name())
	fmt.Println("Cache records", len(records))
	printMu.Unlock()

	if len(records) > 0 {
		rec := getBestResult(records)

		res, err := st.op.LoadCache(ctx, rec)
		if err != nil {
			return nil, err
		}

		r := NewCachedResult(res, []ExportableCacheKey{{CacheKey: rec.key, Exporter: &exporter{k: rec.key, record: rec}}})
		depMu.Lock()
		depResults[vtx.Digest().String()] = r
		depMu.Unlock()

		return r, nil
	}

	results, subExporters, err := st.op.Exec(ctx, toResultSlice(inputs))
	if err != nil {
		return nil, err
	}

	res := results[0]

	var exporters []CacheExporter

	ck, err := st.op.Cache().Save(cacheKey, res, time.Now())
	if err != nil {
		return nil, err
	}

	if _, ok := ck.Exporter.(*exporter); ok {
		// TODO: This is used in the cache exporter (secondary exporters)
		// exp.edge = e
	}

	exps := make([]CacheExporter, 0, len(subExporters))
	for _, exp := range subExporters {
		exps = append(exps, exp.Exporter)
	}

	exporters = append(exporters, ck.Exporter)
	exporters = append(exporters, exps...)

	ek := ExportableCacheKey{
		CacheKey: cacheKey,
		Exporter: &mergedExporter{exporters: exporters},
	}

	cachedResult := NewCachedResult(res, []ExportableCacheKey{ek})

	depMu.Lock()
	depResults[vtx.Digest().String()] = cachedResult
	depMu.Unlock()

	return cachedResult, nil
}

func (s *simpleSolver) preprocessInputsLegacy(ctx context.Context, st *state, cm *CacheMap, vtx Vertex) (*CacheKey, []CachedResult, error) {

	numInputs := len(vtx.Inputs())

	if numInputs == 0 {
		return NewCacheKey(cm.Digest, vtx.Digest(), 0), nil, nil
	}

	deps := make([][]CacheKeyWithSelector, numInputs)

	var results []CachedResult // This wasn't working with "make" & up-front length.
	for i, input := range vtx.Inputs() {
		depMu.Lock()
		result, ok := depResults[input.Vertex.Digest().String()]
		depMu.Unlock()
		if !ok {
			return nil, nil, errors.New("result not found")
		}

		results = append(results, result)

		for _, k := range result.CacheKeys() {
			deps[i] = append(deps[i], CacheKeyWithSelector{CacheKey: k, Selector: cm.Deps[i].Selector})
		}

		if cm.Deps[i].ComputeDigestFunc != nil || cm.Deps[i].PreprocessFunc != nil {
			fmt.Println("Calc slow cache key")
			v, err := st.op.CalcSlowCache(ctx, 0, cm.Deps[i].PreprocessFunc, cm.Deps[i].ComputeDigestFunc, result)
			if err != nil {
				return nil, nil, err
			}

			slowKey := NewCacheKey(v, "", -1)
			expCacheKey := &ExportableCacheKey{CacheKey: slowKey, Exporter: &exporter{k: slowKey}}

			st.op.Cache().Query([]CacheKeyWithSelector{{CacheKey: *expCacheKey}}, 0, cm.Digest, 0)
			st.op.Cache().Query(append(deps[i], CacheKeyWithSelector{CacheKey: *expCacheKey}), 0, cm.Digest, 0)

			deps[i] = append(deps[i], CacheKeyWithSelector{CacheKey: *expCacheKey})
		}
	}

	k := NewCacheKey(cm.Digest, vtx.Digest(), 0)
	k.deps = deps

	return k, results, nil
}

func (s *simpleSolver) buildOne(ctx context.Context, d digest.Digest, vertex Vertex, job *Job, e Edge) (Result, []ExportableCacheKey, error) {
	// Ensure we don't have multiple threads working on the same digest.
	wait, done := s.parallelGuard.acquire(ctx, d.String())
	defer done()
	<-wait

	st := s.state(vertex, job)

	// Add cache opts to context as they will be accessed by cache retrieval.
	ctx = withAncestorCacheOpts(ctx, st)

	// CacheMap populates required fields in SourceOp.
	cm, err := st.op.CacheMap(ctx, int(e.Index))
	if err != nil {
		return nil, nil, err
	}

	inputs, err := s.preprocessInputs(ctx, st, vertex, cm.CacheMap, job)
	if err != nil {
		notifyError(ctx, st, false, err)
		return nil, nil, err
	}

	cacheKey, err := s.cacheKeyManager.cacheKey(ctx, d.String())
	if err != nil {
		return nil, nil, err
	}

	v, ok, err := s.resultCache.get(ctx, cacheKey)
	if err != nil {
		return nil, nil, err
	}

	expCacheKeys := []ExportableCacheKey{
		{Exporter: &simpleExporter{cacheKey: cacheKey}},
	}

	if ok && v != nil {
		notifyError(ctx, st, true, nil)
		return v, expCacheKeys, nil
	}

	results, _, err := st.op.Exec(ctx, inputs)
	if err != nil {
		return nil, nil, err
	}

	// Ensure all results are finalized (committed to cache). It may be better
	// to background these calls at some point.
	for _, res := range results {
		err = s.commitRefFunc(ctx, res)
		if err != nil {
			return nil, nil, err
		}
	}

	res := results[int(e.Index)]

	err = s.resultCache.set(ctx, cacheKey, res)
	if err != nil {
		return nil, nil, err
	}

	return res, expCacheKeys, nil
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
	st := &state{
		opts:         SolverOpt{DefaultCache: s.cacheManager, ResolveOpFunc: s.resolveOpFunc},
		parents:      map[digest.Digest]struct{}{},
		childVtx:     map[digest.Digest]struct{}{},
		allPw:        map[progress.Writer]struct{}{},
		mpw:          progress.NewMultiWriter(progress.WithMetadata("vertex", vertex.Digest())),
		mspan:        tracing.NewMultiSpan(),
		vtx:          vertex,
		clientVertex: initClientVertex(vertex),
		edges:        map[Index]*edge{},
		index:        s.solver.index,
		mainCache:    s.cacheManager,
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
		digest: cm.Digest.String(),
		deps:   make([]cacheMapDep, len(cm.Deps)),
		inputs: make([]string, len(cm.Deps)),
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

		digest := in.Vertex.Digest().String()

		// Compute a cache key given the LLB digest value.
		cacheKey, err := s.cacheKeyManager.cacheKey(ctx, digest)
		if err != nil {
			return nil, err
		}

		// Lookup the result for that cache key.
		res, ok, err := s.resultCache.get(ctx, cacheKey)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errors.Errorf("result not found for digest: %s", digest)
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
				bklog.G(ctx).Warnf("failed to compute digest: %v", err)
				return nil, err
			} else {
				scm.deps[i].computed = compDigest.String()
			}
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
	salt   string
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
func (m *cacheKeyManager) cacheKey(ctx context.Context, digest string) (string, error) {
	h := sha256.New()

	err := m.cacheKeyRecurse(ctx, digest, h)
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

	if c.salt != "" {
		io.WriteString(h, c.salt)
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
	rootDir      string
}

type workerResultGetter interface {
	Get(ctx context.Context, id string) (Result, error)
}

func newDiskCache(resultGetter workerResultGetter, rootDir string) (*diskCache, error) {
	c := &diskCache{
		bucketName:   "ids",
		resultGetter: resultGetter,
		rootDir:      rootDir,
	}
	err := c.init()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *diskCache) init() error {
	db, err := bolt.Open(filepath.Join(c.rootDir, "ids.db"), 0755, nil)
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
		if errors.Is(err, ErrRefNotFound) {
			if err := c.delete(ctx, key); err != nil {
				bklog.G(ctx).Warnf("failed to delete cache key: %v", err)
			}
			return nil, false, nil
		}
		return nil, false, err
	}
	return res, true, nil
}

func (c *diskCache) delete(_ context.Context, key string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(c.bucketName))
		return b.Delete([]byte(key))
	})
}

var _ resultCache = &diskCache{}

type simpleExporter struct {
	cacheKey string
}

func (s *simpleExporter) ExportTo(ctx context.Context, t CacheExporterTarget, opt CacheExportOpt) ([]CacheExporterRecord, error) {
	return nil, nil
}
