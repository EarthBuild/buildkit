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
	resolveOpFunc      ResolveOpFunc
	commitRefFunc      CommitRefFunc
	solver             *Solver
	parallelGuard      *parallelGuard
	resultCache        resultCache
	cacheKeyManager    *cacheKeyManager
	cacheResultStorage CacheResultStorage
	defaultCache       CacheManager
	mu                 sync.Mutex
}

func newSimpleSolver(
	resolveOpFunc ResolveOpFunc,
	commitRefFunc CommitRefFunc,
	solver *Solver,
	defaultCache CacheManager,
	cacheResultStorage CacheResultStorage,
	cache resultCache,
) *simpleSolver {
	return &simpleSolver{
		cacheKeyManager:    newCacheKeyManager(),
		cacheResultStorage: cacheResultStorage,
		defaultCache:       defaultCache,
		resultCache:        cache,
		parallelGuard:      newParallelGuard(time.Millisecond * 100),
		resolveOpFunc:      resolveOpFunc,
		commitRefFunc:      commitRefFunc,
		solver:             solver,
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

		cachedResult, err := s.buildOne(ctx, d, vertex, job, e)
		if err != nil {
			return nil, err
		}

		ret = cachedResult
	}

	return ret, nil
}

// Temporary global maps used for new cache key code.
var (
	cacheKeyMu      = sync.Mutex{}
	cacheMapDigests = map[string][]digest.Digest{}
	slowCacheKeys   = map[string]*ExportableCacheKey{}
	inputResults    = map[string]CachedResult{}
)

func (s *simpleSolver) prepareInputs(vtx Vertex) []CachedResult {
	cacheKeyMu.Lock()
	defer cacheKeyMu.Unlock()

	vertexInputs := vtx.Inputs()

	if len(vertexInputs) == 0 {
		return nil
	}

	var results []CachedResult

	for _, input := range vertexInputs {
		k := fmt.Sprintf("%s:%d", input.Vertex.Digest(), 0)
		cachedResult := inputResults[k]
		results = append(results, cachedResult)
	}

	return results
}

func (s *simpleSolver) prepareCacheKeys(ctx context.Context, st *state, vtx Vertex, cm *CacheMap) ([]*CacheKey, error) {

	cacheKeyMu.Lock()
	defer cacheKeyMu.Unlock()

	vertexDigest := vtx.Digest()
	vertexInputs := vtx.Inputs()

	// TODO: figure out what this is.
	index := Index(0)

	if len(vertexInputs) == 0 {
		k := fmt.Sprintf("%s:%d", vertexDigest, index)
		cacheMapDigests := cacheMapDigests[k]
		var keys []*CacheKey
		for _, dgst := range cacheMapDigests {
			keys = append(keys, NewCacheKey(dgst, vertexDigest, index))
		}
		return keys, nil
	}

	k := NewCacheKey(cm.Digest, vertexDigest, index)

	inputs := make([][]CacheKeyWithSelector, len(vertexInputs))

	for i, input := range vertexInputs {
		k := fmt.Sprintf("%s:%d", input.Vertex.Digest(), 0)

		cachedResult := inputResults[k]
		for _, cacheKey := range cachedResult.CacheKeys() {
			inputs[i] = append(inputs[i], CacheKeyWithSelector{CacheKey: cacheKey, Selector: cm.Deps[i].Selector})
		}

		slowCacheKey, ok := slowCacheKeys[k]
		if !ok {
			slowCacheDigest, err := st.op.CalcSlowCache(ctx, index, cm.Deps[i].PreprocessFunc, cm.Deps[i].ComputeDigestFunc, cachedResult)
			if err != nil {
				return nil, err
			}
			ck := NewCacheKey(slowCacheDigest, "", -1)
			slowCacheKey = &ExportableCacheKey{CacheKey: ck, Exporter: &exporter{k: ck}}
			slowCacheKeys[k] = slowCacheKey
		}

		inputs[i] = append(inputs[i], CacheKeyWithSelector{CacheKey: *slowCacheKey})
	}

	k.deps = inputs

	spew.Dump("CREATE", k.TraceFields())

	return []*CacheKey{k}, nil
}

func (s *simpleSolver) execOp(ctx context.Context, st *state, cm *CacheMap, vtx Vertex) (CachedResult, error) {
	cacheKeys, err := s.prepareCacheKeys(ctx, st, vtx, cm)
	if err != nil {
		return nil, err
	}

	inputs := s.prepareInputs(vtx)

	results, subExporters, err := st.op.Exec(ctx, toResultSlice(inputs))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	index := 0

	if len(results) <= int(index) {
		return nil, errors.Errorf("invalid response from exec need %d index but %d results received", index, len(results))
	}

	res := results[int(index)]

	for i := range results {
		if i != int(index) {
			go results[i].Release(context.TODO())
		}
	}

	var exporters []CacheExporter

	for _, cacheKey := range cacheKeys {
		ck, err := st.op.Cache().Save(cacheKey, res, time.Now())
		if err != nil {
			return nil, err
		}

		if _, ok := ck.Exporter.(*exporter); ok {
			bklog.G(ctx).Warnf("secondary exporters likely needed")
			// TODO: handle secondary exporters if needed
			// exp.edge = e
		}

		exps := make([]CacheExporter, 0, len(subExporters))
		for _, exp := range subExporters {
			exps = append(exps, exp.Exporter)
		}

		exporters = append(exporters, ck.Exporter)
		exporters = append(exporters, exps...)
	}

	ek := make([]ExportableCacheKey, 0, len(cacheKeys))
	for _, ck := range cacheKeys {
		ek = append(ek, ExportableCacheKey{
			CacheKey: ck,
			Exporter: &mergedExporter{exporters: exporters},
		})
	}

	return NewCachedResult(res, ek), nil
}

func (s *simpleSolver) buildOne(ctx context.Context, d digest.Digest, vertex Vertex, job *Job, e Edge) (CachedResult, error) {
	// Ensure we don't have multiple threads working on the same digest.
	wait, done := s.parallelGuard.acquire(ctx, d.String())
	defer done()
	<-wait

	st := s.state(vertex, job)

	// Add cache opts to context as they will be accessed by cache retrieval.
	// ctx = withAncestorCacheOpts(ctx, st)

	// CacheMap populates required fields in SourceOp.
	cm, err := st.op.CacheMap(ctx, 0)
	if err != nil {
		return nil, err
	}

	tmpCacheKey := fmt.Sprintf("%s:%d", vertex.Digest(), 0)

	cacheKeyMu.Lock()
	cacheMapDigests[tmpCacheKey] = append(cacheMapDigests[tmpCacheKey], cm.CacheMap.Digest)
	cacheKeyMu.Unlock()

	// TODO: Add back cache check here.

	cacheRecords := map[string]*CacheRecord{}

	keys, err := st.op.Cache().Query(nil, 0, cm.Digest, 0)
	if err != nil {
		bklog.G(ctx).Warnf("failed to query cache keys: %v", err)
	} else {
		for _, key := range keys {
			spew.Dump("FOUND", key.TraceFields())

			key.vtx = vertex.Digest()
			records, err := st.op.Cache().Records(ctx, key)
			if err != nil {
				bklog.G(ctx).Warnf("failed to query cache records: %v", err)
				continue
			}
			for _, r := range records {
				cacheRecords[r.ID] = r
			}
		}
	}

	var cachedResult CachedResult

	if len(cacheRecords) > 0 {
		recs := make([]*CacheRecord, 0, len(cacheRecords))
		for _, r := range cacheRecords {
			recs = append(recs, r)
		}

		rec := getBestResult(recs)

		res, err := st.op.LoadCache(ctx, rec)
		if err != nil {
			return nil, err
		}

		fmt.Println("Successfully loaded cached result")

		// TODO: this also wanted an edge value for secondary exporters.
		cachedResult = NewCachedResult(res, []ExportableCacheKey{{CacheKey: rec.key, Exporter: &exporter{k: rec.key, record: rec}}})
	} else {
		fmt.Println("No cached result found. Exec-ing.")
		cachedResult, err = s.execOp(ctx, st, cm.CacheMap, vertex)
		if err != nil {
			return nil, err
		}
	}

	cacheKeyMu.Lock()
	inputResults[tmpCacheKey] = cachedResult
	cacheKeyMu.Unlock()

	return cachedResult, nil
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
		opts:         SolverOpt{DefaultCache: s.defaultCache, ResolveOpFunc: s.resolveOpFunc},
		parents:      map[digest.Digest]struct{}{},
		childVtx:     map[digest.Digest]struct{}{},
		allPw:        map[progress.Writer]struct{}{},
		mpw:          progress.NewMultiWriter(progress.WithMetadata("vertex", vertex.Digest())),
		mspan:        tracing.NewMultiSpan(),
		vtx:          vertex,
		clientVertex: initClientVertex(vertex),
		edges:        map[Index]*edge{},
		index:        s.solver.index,
		mainCache:    s.defaultCache,
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

	spew.Dump(scm)

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

// TODO: decouple key translation from result loading.
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
