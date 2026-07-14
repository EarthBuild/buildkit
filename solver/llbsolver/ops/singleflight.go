package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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
// lease is what a leader must quote to keep, publish, or give up its claim. The
// coordinator hands the id out with the grant: without it a ZOMBIE (a leader
// evicted for silence that finally finishes) could publish over its successor,
// and two writers would race one entry.
type lease struct {
	key    string
	holder string
	stop   chan struct{}
}

func (c *coordinator) claim(ctx context.Context, key string) (*publishedResult, *lease, bool) {
	url := fmt.Sprintf("%s/_rebuck/lease/claim/%s", c.base, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, nil, false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		// Coordinator unreachable: build it. A fleet that loses its coordinator
		// degrades to plain BuildKit, which is exactly right.
		return nil, nil, false
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if resp.Header.Get("X-Rebuck-Lease") == "leader" {
			l := &lease{key: key, holder: resp.Header.Get("X-Rebuck-Holder"), stop: make(chan struct{})}
			// A build can easily outrun the lease TTL. Without a heartbeat the
			// coordinator would presume us dead mid-flight, hand the job to a
			// follower, and every waiter would rebuild what we are already
			// building — the exact waste this feature exists to prevent.
			go c.beat(l)
			return nil, l, false
		}
		var pr publishedResult
		if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
			// The leader published something we cannot read. Rebuild rather than
			// guess at what it meant.
			return nil, nil, false
		}
		return &pr, nil, true
	default:
		// 409 (the leader died — re-claim), 5xx, anything else: build it. We do
		// not loop re-claiming; one wasted build beats a retry storm, and the
		// next vertex will coordinate normally.
		io.Copy(io.Discard, resp.Body)
		return nil, nil, false
	}
}

// beat keeps a leader's claim alive until it publishes or gives up.
func (c *coordinator) beat(l *lease) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			c.post(ctx, "heartbeat", l, nil)
			cancel()
		}
	}
}

func (c *coordinator) publish(ctx context.Context, l *lease, pr *publishedResult) {
	close(l.stop)
	if pr == nil {
		c.give_up(ctx, l)
		return
	}
	body, err := json.Marshal(pr)
	if err != nil {
		c.give_up(ctx, l)
		return
	}
	c.post(ctx, "release", l, body)
}

// pushBlobs uploads our layers to the coordinator's registry so followers can
// actually fetch them.
//
// This is not optional, and it is easy to miss: GetRemotes(createIfNeeded=true)
// only materializes the layer blob in the LEADER's local content store. Nothing
// has left the machine. Without this step a follower's FromRemote resolves
// descriptors it cannot fetch, fails open, and rebuilds — so single-flight would
// appear to work while silently doing nothing at all.
//
// Nor can we lean on `--export-cache type=registry` for it: that runs at the END
// of a build, and our followers are blocked in the MIDDLE of theirs.
//
// Returns false if any layer failed to land, in which case we publish nothing
// and the followers rebuild. Slower, never wrong.
func (c *coordinator) pushBlobs(ctx context.Context, provider content.Provider, descs []ocispecs.Descriptor) bool {
	for _, desc := range descs {
		// The fleet may already hold it — a shared base layer, an earlier build.
		// The CAS is content-addressed, so re-uploading is pure waste.
		if c.hasBlob(ctx, desc) {
			continue
		}
		if err := c.pushBlob(ctx, provider, desc); err != nil {
			bklog.G(ctx).Warnf("single-flight: pushing %s failed (%v); followers will rebuild", desc.Digest, err)
			return false
		}
	}
	return true
}

func (c *coordinator) hasBlob(ctx context.Context, desc ocispecs.Descriptor) bool {
	url := fmt.Sprintf("%s/v2/%s/blobs/%s", c.base, c.repo, desc.Digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (c *coordinator) pushBlob(ctx context.Context, provider content.Provider, desc ocispecs.Descriptor) error {
	ra, err := provider.ReaderAt(ctx, desc)
	if err != nil {
		return errors.Wrapf(err, "reading %s from the local content store", desc.Digest)
	}
	defer ra.Close()

	// Open an upload session.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/v2/%s/blobs/uploads/", c.base, c.repo), nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	loc := resp.Header.Get("Location")
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || loc == "" {
		return errors.Errorf("open upload: registry returned %s", resp.Status)
	}
	if !strings.HasPrefix(loc, "http") {
		loc = c.base + loc
	}

	// Stream it: a layer is hundreds of MB and this runs beside a compiler.
	sep := "?"
	if strings.Contains(loc, "?") {
		sep = "&"
	}
	put, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fmt.Sprintf("%s%sdigest=%s", loc, sep, desc.Digest),
		content.NewReader(ra))
	if err != nil {
		return err
	}
	put.ContentLength = desc.Size
	presp, err := c.client.Do(put)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, presp.Body)
	presp.Body.Close()
	if presp.StatusCode != http.StatusCreated {
		return errors.Errorf("upload %s: registry returned %s", desc.Digest, presp.Status)
	}
	return nil
}

// give_up frees our followers to rebuild. Called when we led and FAILED (or were
// cancelled): they must not wait out the lease TTL for a result that is never
// coming.
func (c *coordinator) give_up(ctx context.Context, l *lease) {
	select {
	case <-l.stop: // already closed by publish
	default:
		close(l.stop)
	}
	// The build context is likely already cancelled — this must still get out.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	c.post(ctx, "abandon", l, nil)
}

func (c *coordinator) post(ctx context.Context, op string, l *lease, body []byte) {
	url := fmt.Sprintf("%s/_rebuck/lease/%s/%s", c.base, op, l.key)
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, r)
	if err != nil {
		return
	}
	// Proves this is the CURRENT leader speaking, not a zombie.
	req.Header.Set("X-Rebuck-Holder", l.holder)
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
		rem := remotes[0]
		// Describing the layers is not the same as SENDING them: GetRemotes only
		// put them in our own content store. Push, or the followers are waiting
		// for bytes that never leave this machine.
		if !e.sf.pushBlobs(ctx, rem.Provider, rem.Descriptors) {
			return nil
		}
		pub.Outputs = append(pub.Outputs, rem.Descriptors)
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
