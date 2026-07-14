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
