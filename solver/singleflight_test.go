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
