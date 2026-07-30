package llbsolver

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/containerd/platforms"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/cache/remotecache"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/client/llb/sourceresolver"
	"github.com/moby/buildkit/executor"
	resourcestypes "github.com/moby/buildkit/executor/resources/types"
	"github.com/moby/buildkit/exporter"
	"github.com/moby/buildkit/frontend"
	gw "github.com/moby/buildkit/frontend/gateway/client"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/llbsolver/ops"
	"github.com/moby/buildkit/solver/pb"
	"github.com/moby/buildkit/sourcepolicy"
	spb "github.com/moby/buildkit/sourcepolicy/pb"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/entitlements"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/worker"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type llbBridge struct {
	builder                   solver.Builder
	frontends                 map[string]frontend.Frontend
	resolveWorker             func() (worker.Worker, error)
	eachWorker                func(func(worker.Worker) error) error
	resolveCacheImporterFuncs map[string]remotecache.ResolveCacheImporterFunc
	cms                       map[string]solver.CacheManager
	cmsMu                     sync.Mutex
	sm                        *session.Manager

	executorOnce sync.Once
	executorErr  error
	executor     executor.Executor
}

func (b *llbBridge) Warn(ctx context.Context, dgst digest.Digest, msg string, opts frontend.WarnOpts) error {
	return b.builder.InContext(ctx, func(ctx context.Context, _ solver.JobContext) error {
		pw, ok, _ := progress.NewFromContext(ctx, progress.WithMetadata("vertex", dgst))
		if !ok {
			return nil
		}
		level := opts.Level
		if level == 0 {
			level = 1
		}
		pw.Write(identity.NewID(), client.VertexWarning{
			Vertex:     dgst,
			Level:      level,
			Short:      []byte(msg),
			SourceInfo: opts.SourceInfo,
			Range:      opts.Range,
			Detail:     opts.Detail,
			URL:        opts.URL,
		})
		return pw.Close()
	})
}

func (b *llbBridge) loadResult(ctx context.Context, def *pb.Definition, cacheImports []gw.CacheOptionsEntry, pol []*spb.Policy) (solver.CachedResultWithProvenance, error) {
	w, err := b.resolveWorker()
	if err != nil {
		return nil, err
	}
	ent, err := loadEntitlements(b.builder)
	if err != nil {
		return nil, err
	}

	// TODO FIXME earthly-specific wait group is required to ensure the remotecache/registry's ResolveCacheImporterFunc can run
	// which requires the session to remain open in order to get dockerhub (or any other registry) credentials.
	// It seems like the cleaner approach is to bake this in somewhere into the edge or Load
	eg, _ := errgroup.WithContext(ctx)

	srcPol, err := loadSourcePolicy(b.builder)
	if err != nil {
		return nil, err
	}
	var polEngine *sourcepolicy.Engine
	if srcPol != nil || len(pol) > 0 {
		for _, p := range pol {
			if p == nil {
				return nil, errors.Errorf("invalid nil policy")
			}
			if err := validateSourcePolicy(p); err != nil {
				return nil, err
			}
		}
		if srcPol != nil {
			pol = append([]*spb.Policy{srcPol}, pol...)
		}
		polEngine = sourcepolicy.NewEngine(pol)
	}
	var cms []solver.CacheManager
	for _, im := range cacheImports {
		cmID, err := cmKey(im)
		if err != nil {
			return nil, err
		}
		b.cmsMu.Lock()
		var cm solver.CacheManager
		if prevCm, ok := b.cms[cmID]; !ok {
			func(cmID string, im gw.CacheOptionsEntry) {
				cm = newLazyCacheManager(cmID, func() (solver.CacheManager, error) {
					var cmNew solver.CacheManager
					if err := inBuilderContext(context.TODO(), b.builder, "importing cache manifest from "+cmID, "", func(ctx context.Context, jobCtx solver.JobContext) error {
						resolveCI, ok := b.resolveCacheImporterFuncs[im.Type]
						if !ok {
							return errors.Errorf("unknown cache importer: %s", im.Type)
						}
						var g session.Group
						if jobCtx != nil {
							g = jobCtx.Session()
						}
						ci, desc, err := resolveCI(ctx, g, im.Attrs)
						if err != nil {
							return errors.Wrapf(err, "failed to configure %v cache importer", im.Type)
						}
						cmNew, err = ci.Resolve(ctx, desc, cmID, w)
						return err
					}); err != nil {
						bklog.G(ctx).Debugf("error while importing cache manifest from cmId=%s: %v", cmID, err)
						return nil, err
					}
					return cmNew, nil
				})

				cmInst := cm
				eg.Go(func() error {
					if lcm, ok := cmInst.(*lazyCacheManager); ok {
						lcm.wait()
					}
					return nil
				})
			}(cmID, im)
			b.cms[cmID] = cm
		} else {
			cm = prevCm
		}
		cms = append(cms, cm)
		b.cmsMu.Unlock()
	}
	err = eg.Wait()
	if err != nil {
		return nil, err
	}
	dpc := &detectPrunedCacheID{}

	edge, err := Load(ctx, def, b.policy(polEngine), dpc.Load, ValidateEntitlements(ent, w.CDIManager()), WithCacheSources(cms), NormalizeRuntimePlatforms(), WithValidateCaps())
	if err != nil {
		return nil, errors.Wrap(err, "failed to load LLB")
	}

	if len(dpc.ids) > 0 {
		if err := b.eachWorker(func(w worker.Worker) error {
			return w.PruneCacheMounts(ctx, dpc.ids)
		}); err != nil {
			return nil, err
		}
	}

	res, err := b.builder.Build(ctx, edge)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// getExporter is earthly specific code which extracts the configured exporter
// from the job's metadata
func (b *llbBridge) getExporter(_ context.Context) (*ExporterRequest, error) {
	var exp *ExporterRequest
	numExporters := 0
	b.builder.EachValue(context.TODO(), keyEarthlyExporterInstance, func(v any) error {
		numExporters++
		exp = v.(*ExporterRequest)
		return nil
	})
	if numExporters != 1 {
		return nil, errors.Errorf("Export found %d exporters (should have been 1)", numExporters) // shouldn't happen
	}
	return exp, nil
}

func (b *llbBridge) Export(ctx context.Context, refs map[string]cache.ImmutableRef, metadata map[string][]byte) error {
	// generate an ID that's consistent for the refs
	refKeys := []string{}
	for k := range refs {
		refKeys = append(refKeys, k)
	}
	id := strings.Join(refKeys, "-")

	inp := &exporter.Source{
		Refs:     refs,
		Metadata: metadata,
	}

	exp, err := b.getExporter(ctx)
	if err != nil {
		return err
	}
	if len(exp.Exporters) == 0 {
		return errors.Errorf("Export had no exporter configured")
	}

	e := exp.Exporters[0]
	return inBuilderContext(ctx, b.builder, e.Name(), id, func(ctx context.Context, jobCtx solver.JobContext) error {
		sessionIDs := session.AllSessionIDs(jobCtx.Session())
		if len(sessionIDs) == 0 {
			return errors.Errorf("group has no session IDs") // shouldnt happen
		}
		sessionID := sessionIDs[0]
		_, _, _, err := e.Export(ctx, inp, exporter.ExportBuildInfo{SessionID: sessionID})
		return err
	})
}

func (b *llbBridge) policy(engine *sourcepolicy.Engine) SourcePolicyEvaluator {
	return &policyEvaluator{
		llbBridge: b,
		engine:    engine,
	}
}

func (b *llbBridge) validateEntitlements(p executor.ProcessInfo) error {
	ent, err := loadEntitlements(b.builder)
	if err != nil {
		return err
	}
	v := entitlements.Values{
		NetworkHost:      p.Meta.NetMode == pb.NetMode_HOST,
		SecurityInsecure: p.Meta.SecurityMode == pb.SecurityMode_INSECURE,
	}
	return ent.Check(v)
}

func (b *llbBridge) Run(ctx context.Context, id string, rootfs executor.Mount, mounts []executor.Mount, process executor.ProcessInfo, started chan<- struct{}) (resourcestypes.Recorder, error) {
	if err := b.validateEntitlements(process); err != nil {
		return nil, err
	}

	if err := b.loadExecutor(); err != nil {
		return nil, err
	}
	return b.executor.Run(ctx, id, rootfs, mounts, process, started)
}

func (b *llbBridge) Exec(ctx context.Context, id string, process executor.ProcessInfo) error {
	if err := b.validateEntitlements(process); err != nil {
		return err
	}

	if err := b.loadExecutor(); err != nil {
		return err
	}
	return b.executor.Exec(ctx, id, process)
}

func (b *llbBridge) loadExecutor() error {
	b.executorOnce.Do(func() {
		w, err := b.resolveWorker()
		if err != nil {
			b.executorErr = err
			return
		}
		b.executor = w.Executor()
	})
	return b.executorErr
}

func (b *llbBridge) ResolveImageConfig(ctx context.Context, ref string, opt sourceresolver.Opt) (string, digest.Digest, []byte, error) {
	fmt.Fprintf(os.Stderr, "REBUCK-SEAM: ResolveImageConfig ref=%s\n", ref)
	imr := sourceresolver.NewImageMetaResolver(b)
	local := func(ctx context.Context) (string, digest.Digest, []byte, error) {
		return imr.ResolveImageConfig(ctx, ref, opt)
	}

	// Agree ONE digest per reference across the fleet. Without this each machine
	// resolves independently, so a tag republished mid-run (which Docker Official
	// Images are, for CVE rebuilds) leaves part of the fleet on the old base and
	// part on the new -- a build no single machine would have produced.
	//
	// Everything that changes the answer has to reach the key, hence the platform
	// and variant folding. Coordination is best-effort throughout: with no
	// coordinator, an unreachable one, or an answer we cannot verify,
	// CoordinateResolve falls through to `local`.
	var (
		platform *ocispecs.Platform
		mode     string
		noConfig bool
		attChain bool
		att      []string
	)
	if opt.ImageOpt != nil {
		platform = opt.ImageOpt.Platform
		mode = opt.ImageOpt.ResolveMode
		noConfig = opt.ImageOpt.NoConfig
		attChain = opt.ImageOpt.AttestationChain
		att = opt.ImageOpt.ResolveAttestations
	}
	return ops.CoordinateResolve(ctx, ref,
		ops.ResolvePlatformKey(platform),
		ops.ResolveVariant(mode, noConfig, attChain, att),
		local)
}

func (b *llbBridge) ResolveSourceMetadata(ctx context.Context, op *pb.SourceOp, opt sourceresolver.Opt) (resp *sourceresolver.MetaResponse, err error) {
	return b.resolveSourceMetadata(ctx, op, opt, true)
}

func (b *llbBridge) resolveSourceMetadata(ctx context.Context, op *pb.SourceOp, opt sourceresolver.Opt, withPolicy bool) (resp *sourceresolver.MetaResponse, err error) {
	fmt.Fprintf(os.Stderr, "REBUCK-SEAM: resolveSourceMetadata id=%s\n", op.GetIdentifier())
	w, err := b.resolveWorker()
	if err != nil {
		return nil, err
	}
	if opt.LogName == "" {
		// TODO: better name
		opt.LogName = fmt.Sprintf("resolve image config for %s", op.Identifier)
	}
	id := op.Identifier

	var platform *ocispecs.Platform
	if opt.ImageOpt != nil && opt.ImageOpt.Platform != nil {
		platform = opt.ImageOpt.Platform
	} else if opt.OCILayoutOpt != nil && opt.OCILayoutOpt.Platform != nil {
		platform = opt.OCILayoutOpt.Platform
	}

	if platform != nil {
		id += platforms.FormatAll(*platform)
	} else {
		id += platforms.FormatAll(platforms.DefaultSpec())
	}
	pol, err := loadSourcePolicy(b.builder)
	if err != nil {
		return nil, err
	}
	if pol != nil {
		opt.SourcePolicies = append(opt.SourcePolicies, pol)
	}

	engine := sourcepolicy.NewEngine(opt.SourcePolicies)

	if !withPolicy {
		if _, err := engine.Evaluate(ctx, op); err != nil {
			return nil, errors.Wrap(err, "could not resolve image due to policy")
		}
	} else {
		var p *ocispecs.Platform
		if opt.ImageOpt != nil {
			p = opt.ImageOpt.Platform
		} else if opt.OCILayoutOpt != nil {
			p = opt.OCILayoutOpt.Platform
		}
		if _, err := b.policy(engine).Evaluate(ctx, &pb.Op{
			Op:       &pb.Op_Source{Source: op},
			Platform: toPBPlatform(p),
		}); err != nil {
			return nil, errors.Wrap(err, "could not resolve image due to policy")
		}
	}

	// policy is evaluated, so we can remove it from the options
	opt.SourcePolicies = nil

	err = inBuilderContext(ctx, b.builder, opt.LogName, id, func(ctx context.Context, jobCtx solver.JobContext) error {
		resp, err = w.ResolveSourceMetadata(ctx, op, opt, b.sm, jobCtx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type lazyCacheManager struct {
	id   string
	main solver.CacheManager

	waitCh chan struct{}
	err    error
}

func (lcm *lazyCacheManager) ID() string {
	return lcm.id
}

func (lcm *lazyCacheManager) Query(inp []solver.CacheKeyWithSelector, inputIndex solver.Index, dgst digest.Digest, outputIndex solver.Index) ([]*solver.CacheKey, error) {
	lcm.wait()
	if lcm.main == nil {
		return nil, nil
	}
	return lcm.main.Query(inp, inputIndex, dgst, outputIndex)
}

func (lcm *lazyCacheManager) Records(ctx context.Context, ck *solver.CacheKey) ([]*solver.CacheRecord, error) {
	lcm.wait()
	if lcm.main == nil {
		return nil, nil
	}
	return lcm.main.Records(ctx, ck)
}

func (lcm *lazyCacheManager) Load(ctx context.Context, rec *solver.CacheRecord) (solver.Result, error) {
	if err := lcm.wait(); err != nil {
		return nil, err
	}
	return lcm.main.Load(ctx, rec)
}

func (lcm *lazyCacheManager) Save(key *solver.CacheKey, s solver.Result, createdAt time.Time) (*solver.ExportableCacheKey, error) {
	if err := lcm.wait(); err != nil {
		return nil, err
	}
	return lcm.main.Save(key, s, createdAt)
}

func (lcm *lazyCacheManager) ReleaseUnreferenced(ctx context.Context) error {
	if err := lcm.wait(); err != nil {
		return err
	}
	return lcm.main.ReleaseUnreferenced(ctx)
}

func (lcm *lazyCacheManager) wait() error {
	<-lcm.waitCh
	return lcm.err
}

func newLazyCacheManager(id string, fn func() (solver.CacheManager, error)) solver.CacheManager {
	lcm := &lazyCacheManager{id: id, waitCh: make(chan struct{})}
	go func() {
		defer close(lcm.waitCh)
		cm, err := fn()
		if err != nil {
			lcm.err = err
			return
		}
		lcm.main = cm
	}()
	return lcm
}

func cmKey(im gw.CacheOptionsEntry) (string, error) {
	if im.Type == "registry" && im.Attrs["ref"] != "" {
		return im.Attrs["ref"], nil
	}
	i, err := hashstructure.Hash(im, hashstructure.FormatV2, nil)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", im.Type, i), nil
}
