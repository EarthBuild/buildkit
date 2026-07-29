package solver

import (
	"testing"

	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func depKey(dgst string) CacheKeyWithSelector {
	k := NewCacheKey(digest.FromString(dgst), "", 0)
	return CacheKeyWithSelector{CacheKey: ExportableCacheKey{CacheKey: k}}
}

// THE safety property. An ExecOp's own digest says nothing about its inputs:
// `RUN cargo build` after `COPY src/` has the same op digest whatever the sources
// contain. If the lease key ignored the dependency chain, two machines building
// DIFFERENT source trees would claim the same key — and one would be handed the
// other's layer. Silently wrong output is the worst failure this feature could
// have, so it gets the first test.
func TestLeaseKeyDistinguishesInputContent(t *testing.T) {
	op := digest.FromString("exec: cargo build --release")

	a := NewCacheKey(op, "vtx", 0)
	a.deps = [][]CacheKeyWithSelector{{depKey("src content A")}}

	b := NewCacheKey(op, "vtx", 0)
	b.deps = [][]CacheKeyWithSelector{{depKey("src content B")}}

	require.Equal(t, a.Digest(), b.Digest(), "same op: the op digest CANNOT tell these apart")
	require.NotEqual(t, LeaseKey(a), LeaseKey(b),
		"lease key must distinguish different input content, or a follower gets the wrong layer")
}

// The other half: two machines building the SAME thing must agree, or the lease
// never coalesces and the whole feature is a no-op.
func TestLeaseKeyAgreesOnIdenticalWork(t *testing.T) {
	op := digest.FromString("exec: cargo build --release")

	mk := func() *CacheKey {
		k := NewCacheKey(op, "vtx", 0)
		k.deps = [][]CacheKeyWithSelector{{depKey("src content A")}, {depKey("deps lockfile")}}
		return k
	}
	require.Equal(t, LeaseKey(mk()), LeaseKey(mk()))
}

// Two machines may discover the same dependencies in a different ORDER (map
// iteration, scheduling). They must still agree, or coalescing becomes a
// coin-flip.
func TestLeaseKeyIsOrderIndependentWithinASlot(t *testing.T) {
	op := digest.FromString("exec: link")

	a := NewCacheKey(op, "vtx", 0)
	a.deps = [][]CacheKeyWithSelector{{depKey("x"), depKey("y")}}

	b := NewCacheKey(op, "vtx", 0)
	b.deps = [][]CacheKeyWithSelector{{depKey("y"), depKey("x")}}

	require.Equal(t, LeaseKey(a), LeaseKey(b))
}

// Dependency SLOTS are positional, though: swapping which input a dep arrived on
// is a genuinely different build.
func TestLeaseKeyRespectsDependencySlots(t *testing.T) {
	op := digest.FromString("exec: link")

	a := NewCacheKey(op, "vtx", 0)
	a.deps = [][]CacheKeyWithSelector{{depKey("x")}, {depKey("y")}}

	b := NewCacheKey(op, "vtx", 0)
	b.deps = [][]CacheKeyWithSelector{{depKey("y")}, {depKey("x")}}

	require.NotEqual(t, LeaseKey(a), LeaseKey(b))
}

// A different output index of the same op is a different artifact.
func TestLeaseKeyDistinguishesOutputIndex(t *testing.T) {
	op := digest.FromString("exec: multi-output")
	a := NewCacheKey(op, "vtx", 0)
	b := NewCacheKey(op, "vtx", 1)
	require.NotEqual(t, LeaseKey(a), LeaseKey(b))
}

// A diamond DAG must not be walked exponentially — memoization, not luck.
func TestLeaseKeyHandlesDiamondDAG(t *testing.T) {
	shared := depKey("shared base")
	mid := func(tag string) CacheKeyWithSelector {
		k := NewCacheKey(digest.FromString(tag), "", 0)
		k.deps = [][]CacheKeyWithSelector{{shared}}
		return CacheKeyWithSelector{CacheKey: ExportableCacheKey{CacheKey: k}}
	}
	top := NewCacheKey(digest.FromString("top"), "vtx", 0)
	top.deps = [][]CacheKeyWithSelector{{mid("left")}, {mid("right")}}

	require.NotEmpty(t, LeaseKey(top).String())
	require.Equal(t, LeaseKey(top), LeaseKey(top))
}

// ---------------------------------------------------------------------------
// A local source is NOT identified by its cache key.
//
// localSourceHandler.CacheKey returns "session:<name>:<hash(SessionID,...)>",
// and SourceOp.CacheMap then rewrites that digest with a "random:" prefix
// (solver/llbsolver/ops/source.go) — buildkit's marker for "never match this by
// identity". It does not care: it re-keys local sources by CONTENT via the slow
// cache. We must do the same, or every vertex downstream of a COPY gets a key
// that is random per run and single-flight silently never engages.
//
// Measured before this was fixed: two earthbuild instances building the same
// target produced four lease keys and zero merges.

func randomDepKey(tag string) CacheKeyWithSelector {
	// What SourceOp.CacheMap hands us for a local source: per-session, so per-run.
	d := digest.Digest("random:" + digest.FromString(tag).Encoded())
	return CacheKeyWithSelector{CacheKey: ExportableCacheKey{CacheKey: NewCacheKey(d, "", 0)}}
}

// THE bug. Two machines, identical source content, different sessions. The
// content key (slow cache) is identical; only the session key differs. They must
// agree — otherwise the lease never coalesces.
func TestLeaseKeyIgnoresRandomSessionKeys(t *testing.T) {
	op := digest.FromString("exec: go mod download")
	content := depKey("go.mod+go.sum content")

	mk := func(session string) *CacheKey {
		k := NewCacheKey(op, "vtx", 0)
		// Exactly what commitOptions assembles: the session-scoped fast key
		// alongside the content-addressed slow key, in one dep slot.
		k.deps = [][]CacheKeyWithSelector{{randomDepKey(session), content}}
		return k
	}

	require.Equal(t, LeaseKey(mk("session-A")), LeaseKey(mk("session-B")),
		"a random: session key must not reach the lease key — the content key identifies this dep")
}

// The safety half: dropping the random key must NOT make us blind to content.
// Different sources must still get different keys, or a follower adopts the
// wrong layer.
func TestLeaseKeyStillDistinguishesContentBehindRandomKeys(t *testing.T) {
	op := digest.FromString("exec: go build")
	a := NewCacheKey(op, "vtx", 0)
	a.deps = [][]CacheKeyWithSelector{{randomDepKey("session-A"), depKey("source A")}}
	b := NewCacheKey(op, "vtx", 0)
	b.deps = [][]CacheKeyWithSelector{{randomDepKey("session-B"), depKey("source B")}}

	require.NotEqual(t, LeaseKey(a), LeaseKey(b),
		"different content must still differ once the random key is dropped")
}

// No content key at all, only a random one: we have NO cross-machine identity
// for this dep. Refuse. Returning a key here would either be random (useless) or
// — worse — collide across genuinely different inputs. Fail open: build locally.
func TestLeaseKeyRefusesWithoutContentIdentity(t *testing.T) {
	op := digest.FromString("exec: something")
	k := NewCacheKey(op, "vtx", 0)
	k.deps = [][]CacheKeyWithSelector{{randomDepKey("session-A")}}

	require.Empty(t, LeaseKey(k).String(),
		"no content identity for a dep => no lease, rather than a key that cannot match")
}

// A random digest on the op ITSELF is equally unusable.
func TestLeaseKeyRefusesRandomOpDigest(t *testing.T) {
	k := NewCacheKey(digest.Digest("random:"+digest.FromString("x").Encoded()), "vtx", 0)
	require.Empty(t, LeaseKey(k).String())
}

// ---------------------------------------------------------------------------
// Invariants that must hold for ANY graph.
//
// Every test above builds a specific graph by hand, which only ever proves the
// case I thought of. These state the properties instead — the ones whose failure
// makes single-flight either useless (keys never match) or dangerous (keys match
// when they must not).

// THE property the whole feature rests on: a key must depend on nothing but the
// content of the graph. Same shape, same content, built twice => same key. If
// this can fail, single-flight silently does nothing, and no e2e that lacks a
// COPY will tell you.
func TestLeaseKeyIsAFunctionOfContentAlone(t *testing.T) {
	build := func() *CacheKey {
		leaf := depKey("leaf content")
		mid := NewCacheKey(digest.FromString("mid op"), "vtx-mid", 0)
		mid.deps = [][]CacheKeyWithSelector{{randomDepKey("a fresh session every time"), leaf}}
		top := NewCacheKey(digest.FromString("top op"), "vtx-top", 0)
		top.deps = [][]CacheKeyWithSelector{{CacheKeyWithSelector{CacheKey: ExportableCacheKey{CacheKey: mid}}}}
		return top
	}
	// Distinct pointer graphs, distinct sessions, identical content.
	require.Equal(t, LeaseKey(build()), LeaseKey(build()),
		"two machines building the same thing must agree, whatever their session ids")
}

// Divergence must be traceable. When two machines disagree, the digest is opaque
// and the pre-hash string is the only thing that says WHICH component differed —
// that is how the random: bug was found, and it should stay findable.
func TestLeaseKeyDebugStringExposesTheDivergence(t *testing.T) {
	a := NewCacheKey(digest.FromString("op"), "vtx", 0)
	a.deps = [][]CacheKeyWithSelector{{depKey("content A")}}
	b := NewCacheKey(digest.FromString("op"), "vtx", 0)
	b.deps = [][]CacheKeyWithSelector{{depKey("content B")}}

	require.NotEqual(t, LeaseKeyDebugString(a), LeaseKeyDebugString(b))
	require.Contains(t, LeaseKeyDebugString(a), digest.FromString("content A").String(),
		"the string must name the dep that differs, or divergence is undebuggable")
}

// A random: key anywhere in the chain — not just in a direct dep — must not
// leak into the key. Poison at depth is still poison, and real graphs are deep:
// the COPY that taints `go build` is several hops down.
func TestLeaseKeyRefusesRandomDeepInTheChain(t *testing.T) {
	buried := NewCacheKey(digest.FromString("mid"), "vtx", 0)
	buried.deps = [][]CacheKeyWithSelector{{randomDepKey("session")}} // no content key

	top := NewCacheKey(digest.FromString("top"), "vtx", 0)
	top.deps = [][]CacheKeyWithSelector{{CacheKeyWithSelector{CacheKey: ExportableCacheKey{CacheKey: buried}}}}

	require.Empty(t, LeaseKey(top).String(),
		"a dep with no content identity must refuse the lease however deep it sits")
}

// Selector is part of the identity: the same dep consumed under a different
// selector (COPY --from a different path) is different work.
func TestLeaseKeyDistinguishesSelector(t *testing.T) {
	mk := func(sel string) *CacheKey {
		k := NewCacheKey(digest.FromString("op"), "vtx", 0)
		d := depKey("same content")
		d.Selector = digest.FromString(sel)
		k.deps = [][]CacheKeyWithSelector{{d}}
		return k
	}
	require.NotEqual(t, LeaseKey(mk("/src")), LeaseKey(mk("/other")))
}

// ---------------------------------------------------------------------------
// The slow (content) key is a FALLBACK, not an extra ingredient.
//
// `RUN apt-get update` on a fixed base image is cached by buildkit on
// f(base image digest, command) -- it never hashes the OUTPUT, which is why the
// second run on one machine is a cache hit even though apt fetched different
// bytes. Two machines agree for exactly the same reason, so the layer is
// perfectly shippable.
//
// But commitOptions ALSO attaches a content hash of the input rootfs
// (ContentBasedHash), and if the lease key mixes that in, a vertex whose fast
// key already agrees is poisoned by bytes buildkit itself does not care about.
// Measured on +examples-1: three vertices differed by exactly ONE token, the
// @-1 (slow) one -- their fast keys already matched.
//
// So: use the fast key when it identifies the dep, and fall back to the content
// key only when the fast key is random: (a local source), where it is the only
// identity there is.

// slowKey is what commitOptions appends: no selector, output index -1.
func slowKey(tag string) CacheKeyWithSelector {
	return CacheKeyWithSelector{CacheKey: ExportableCacheKey{
		CacheKey: NewCacheKey(digest.FromString(tag), "", -1)}}
}

// THE bug this test exists for. Same base image, same command; each machine ran
// its own apt-get update so the rootfs content hashes differ. The fast key
// agrees, so the lease key must agree.
func TestLeaseKeyIgnoresContentHashWhenFastKeyIdentifiesTheDep(t *testing.T) {
	op := digest.FromString("exec: apt-get update && apt-get install -y cmake")
	mk := func(rootfsContent string) *CacheKey {
		k := NewCacheKey(op, "vtx", 0)
		// fast key = the ubuntu image: identical on both machines.
		// slow key = contenthash of the rootfs: differs, and buildkit does not
		// care -- it caches this vertex on the fast key alone.
		k.deps = [][]CacheKeyWithSelector{{depKey("ubuntu:24.04 image"), slowKey(rootfsContent)}}
		return k
	}
	require.Equal(t,
		LeaseKey(mk("apt lists fetched at 10:00")),
		LeaseKey(mk("apt lists fetched at 10:05")),
		"the fast key identifies this dep; a content hash buildkit ignores must not poison the lease")
}

// The fallback still works: no usable fast key (a local source, stamped
// random:) means the content key is the ONLY identity, and must be used.
func TestLeaseKeyFallsBackToContentKeyForLocalSources(t *testing.T) {
	op := digest.FromString("exec: go build")
	mk := func(session, content string) *CacheKey {
		k := NewCacheKey(op, "vtx", 0)
		k.deps = [][]CacheKeyWithSelector{{randomDepKey(session), slowKey(content)}}
		return k
	}
	// same content, different sessions => must agree
	require.Equal(t, LeaseKey(mk("s-A", "src v1")), LeaseKey(mk("s-B", "src v1")))
	// different content => must differ, or a follower adopts the wrong layer
	require.NotEqual(t, LeaseKey(mk("s-A", "src v1")), LeaseKey(mk("s-A", "src v2")))
}

// ---------------------------------------------------------------------------
// Host-side execution (earthly's LOCALLY) and why the solver cannot save you.
//
// A LOCALLY step runs on the HOST. Its effect is not a function of its inputs --
// it touches that machine's filesystem, its docker socket, its clock. Adopting
// one machine's LOCALLY result on another means the side effect never happens
// there: silent, and a direct violation of "the grid behaves as ONE machine".
//
// The solver has NO concept of this. A lease key is a function of content, and
// two hosts running an identical command over identical inputs are, to LeaseKey,
// the same work. The tests below pin that down rather than leaving it to be
// rediscovered.
//
// What actually protects earthbuild today is INCIDENTAL: converter.runCommand
// allocates the LOCALLY output file under os.MkdirTemp(os.TempDir(),
// "earthlyexproutput"), and that randomised path is shell-wrapped into the
// command string. So every LOCALLY op carries a per-run nonce in its args and
// can never match across machines.
//
// That is a property of where a temp file lives, not a safety rule. Nothing
// fails if it changes. These tests state the dependency so that a future
// "let's make the output path deterministic" tidy-up trips over it here rather
// than in a build that silently skipped someone's side effect.

// The uncomfortable truth, asserted so nobody assumes otherwise: identical
// content means identical key, side effects or not. Host-locality must be
// expressed IN the content, upstream, or it is not expressed at all.
func TestLeaseKeyHasNoConceptOfHostLocalExecution(t *testing.T) {
	// Two machines, same command, same (scratch) root. Nothing here says "this
	// runs on the host and must not be shared".
	mk := func() *CacheKey {
		k := NewCacheKey(digest.FromString("exec: ./scripts/tag-release.sh"), "vtx", 0)
		k.deps = [][]CacheKeyWithSelector{{depKey("scratch")}}
		return k
	}
	require.Equal(t, LeaseKey(mk()), LeaseKey(mk()),
		"LeaseKey is content-addressed and cannot see host locality; "+
			"a LOCALLY op made deterministic WOULD be adopted across machines")
}

// The mechanism earthbuild actually relies on. The per-run temp path reaches the
// op digest, so the two machines' ops differ and no lease can coalesce them.
// If this ever fails, LOCALLY steps have become cross-machine adoptable.
func TestLeaseKeyDistinguishesOpsCarryingAPerRunNonce(t *testing.T) {
	// What withShellAndEnvVarsOutput bakes in: .../earthlyexproutput<random>/output
	mk := func(tmpdir string) *CacheKey {
		k := NewCacheKey(
			digest.FromString("exec: sh -c './scripts/tag-release.sh > "+tmpdir+"/output'"),
			"vtx", 0)
		k.deps = [][]CacheKeyWithSelector{{depKey("scratch")}}
		return k
	}
	require.NotEqual(t,
		LeaseKey(mk("/tmp/earthlyexproutput2261401925")),
		LeaseKey(mk("/tmp/earthlyexproutput3355012844")),
		"the per-run temp path must reach the lease key -- it is the only thing "+
			"keeping a host-side LOCALLY step from being adopted by another machine")
}

// ---------------------------------------------------------------------------
// The key must name the BUILDER, not just the build.
//
// LeaseKey is a function of graph content alone, so two daemons at different
// commits compute the SAME key for the same vertex and adopt each other's
// results. Fine while a fleet is homogeneous; wrong the moment it is not, and
// silent when it is wrong -- a layer produced under one daemon's semantics is
// handed to another that does not share them.
//
// earthbuild is homogeneous only by accident: earthly-entrypoint.sh starts a
// buildkitd inside every test container (~480 per CI run) whose image is built
// from ./buildkitd+buildkitd, so inner and outer match today. Pin a released
// buildkitd for the inner daemon -- the obvious thing to do -- and two versions
// share one keyspace.
//
// Partitioning by cohort is the cheap, safe half: versions never merge across.
// The ambitious half (adoption decides compatibility from version metadata)
// needs a policy for what "compatible" means when LLB semantics change, and is
// unsound without this in place first.

// Same graph, different builder cohort => different key. Otherwise a v0.8 daemon
// adopts a v0.9 daemon's layer and nothing anywhere says so.
func TestLeaseKeyPartitionsByBuilderCohort(t *testing.T) {
	mk := func() *CacheKey {
		k := NewCacheKey(digest.FromString("exec: go build"), "vtx", 0)
		k.deps = [][]CacheKeyWithSelector{{depKey("src content")}}
		return k
	}

	defer func(old string) { leaseKeyCohort = old }(leaseKeyCohort)

	leaseKeyCohort = "buildkit-aaaaaaa"
	a := LeaseKey(mk())
	leaseKeyCohort = "buildkit-bbbbbbb"
	b := LeaseKey(mk())

	require.NotEqual(t, a, b,
		"two builder cohorts must not share a keyspace -- adoption across them is silent and unsound")
}

// The other half: within one cohort nothing changes, or we have broken every
// merge to fix a hypothetical.
func TestLeaseKeyAgreesWithinACohort(t *testing.T) {
	mk := func() *CacheKey {
		k := NewCacheKey(digest.FromString("exec: go build"), "vtx", 0)
		k.deps = [][]CacheKeyWithSelector{{depKey("src content")}}
		return k
	}

	defer func(old string) { leaseKeyCohort = old }(leaseKeyCohort)
	leaseKeyCohort = "buildkit-aaaaaaa"

	require.Equal(t, LeaseKey(mk()), LeaseKey(mk()))
}

// A version partition must be DIAGNOSABLE. A fleet that merges nothing because
// two daemons disagree looks exactly like a fleet whose coordinator is
// unreachable -- and that ambiguity has already cost ten days once. The cohort
// belongs in the pre-hash string, which is the only thing that names which
// component diverged.
func TestLeaseKeyDebugStringNamesTheCohort(t *testing.T) {
	defer func(old string) { leaseKeyCohort = old }(leaseKeyCohort)
	leaseKeyCohort = "buildkit-deadbee"

	k := NewCacheKey(digest.FromString("exec: x"), "vtx", 0)
	k.deps = [][]CacheKeyWithSelector{{depKey("c")}}

	require.Contains(t, LeaseKeyDebugString(k), "buildkit-deadbee",
		"the cohort must be visible in the debug string, or a version split is undebuggable")
}
