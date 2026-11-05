package cacheimport

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/moby/buildkit/solver"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestSimpleMarshal(t *testing.T) {
	cc := NewCacheChains()

	addRecords := func() {
		foo := cc.Add(outputKey(dgst("foo"), 0))
		bar := cc.Add(outputKey(dgst("bar"), 1))
		baz := cc.Add(outputKey(dgst("baz"), 0))

		baz.LinkFrom(foo, 0, "")
		baz.LinkFrom(bar, 1, "sel0")
		r0 := &solver.Remote{
			Descriptors: []ocispecs.Descriptor{{
				Digest: dgst("d0"),
			}, {
				Digest: dgst("d1"),
			}},
		}
		baz.AddResult("", 0, time.Now(), r0)
	}

	addRecords()

	cfg, _, err := cc.Marshal(context.TODO())
	require.NoError(t, err)

	require.Equal(t, len(cfg.Layers), 2)
	require.Equal(t, len(cfg.Records), 3)

	require.Equal(t, cfg.Layers[0].Blob, dgst("d0"))
	require.Equal(t, cfg.Layers[0].ParentIndex, -1)
	require.Equal(t, cfg.Layers[1].Blob, dgst("d1"))
	require.Equal(t, cfg.Layers[1].ParentIndex, 0)

	require.Equal(t, cfg.Records[0].Digest, outputKey(dgst("baz"), 0))
	require.Equal(t, len(cfg.Records[0].Inputs), 2)
	require.Equal(t, len(cfg.Records[0].Results), 1)

	require.Equal(t, cfg.Records[1].Digest, outputKey(dgst("foo"), 0))
	require.Equal(t, len(cfg.Records[1].Inputs), 0)
	require.Equal(t, len(cfg.Records[1].Results), 0)

	require.Equal(t, cfg.Records[2].Digest, outputKey(dgst("bar"), 1))
	require.Equal(t, len(cfg.Records[2].Inputs), 0)
	require.Equal(t, len(cfg.Records[2].Results), 0)

	require.Equal(t, cfg.Records[0].Results[0].LayerIndex, 1)
	require.Equal(t, cfg.Records[0].Inputs[0][0].Selector, "")
	require.Equal(t, cfg.Records[0].Inputs[0][0].LinkIndex, 1)
	require.Equal(t, cfg.Records[0].Inputs[1][0].Selector, "sel0")
	require.Equal(t, cfg.Records[0].Inputs[1][0].LinkIndex, 2)

	// adding same info again doesn't produce anything extra
	addRecords()

	cfg2, descPairs, err := cc.Marshal(context.TODO())
	require.NoError(t, err)

	require.EqualValues(t, cfg, cfg2)

	// marshal roundtrip
	dt, err := json.Marshal(cfg)
	require.NoError(t, err)

	newChains := NewCacheChains()
	err = Parse(dt, descPairs, newChains)
	require.NoError(t, err)

	cfg3, _, err := cc.Marshal(context.TODO())
	require.NoError(t, err)
	require.EqualValues(t, cfg, cfg3)

	// add extra item
	cc.Add(outputKey(dgst("bay"), 0))
	cfg, _, err = cc.Marshal(context.TODO())
	require.NoError(t, err)

	require.Equal(t, len(cfg.Layers), 2)
	require.Equal(t, len(cfg.Records), 4)
}

func dgst(s string) digest.Digest {
	return digest.FromBytes([]byte(s))
}

// addDiamondChain adds a chain of n fan-in "diamonds" (a_i -> {b_i, c_i} -> a_i+1)
// to cc. The graph has 3n+1 items but 2^n distinct root-to-leaf paths.
func addDiamondChain(cc *CacheChains, n int) {
	prev := cc.Add(outputKey(dgst("a0"), 0))
	prev.AddResult("", 0, time.Now(), &solver.Remote{})
	for i := 0; i < n; i++ {
		b := cc.Add(outputKey(dgst(fmt.Sprintf("b%d", i)), 0))
		b.LinkFrom(prev, 0, "")
		c := cc.Add(outputKey(dgst(fmt.Sprintf("c%d", i)), 0))
		c.LinkFrom(prev, 0, "")
		a := cc.Add(outputKey(dgst(fmt.Sprintf("a%d", i+1)), 0))
		a.LinkFrom(b, 0, "")
		a.LinkFrom(c, 1, "")
		prev = a
	}
}

// TestMarshalDiamondChain guards against loop detection enumerating every
// path through the cache graph, which is exponential in the number of
// diamonds and made cache export hang on large build graphs.
func TestMarshalDiamondChain(t *testing.T) {
	const n = 1000
	cc := NewCacheChains()
	addDiamondChain(cc, n)

	done := make(chan error, 1)
	var cfg *CacheConfig
	go func() {
		var err error
		cfg, _, err = cc.Marshal(context.TODO())
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatalf("marshaling a chain of %d diamonds did not finish in time", n)
	}
	require.Equal(t, 3*n+1, len(cfg.Records))
}

func TestMarshalCanceled(t *testing.T) {
	cc := NewCacheChains()
	addDiamondChain(cc, 10)

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	_, _, err := cc.Marshal(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// TestMarshalLoop checks that loops created by deduplication during
// normalization are still broken, including when the loop is reachable
// through several paths (so its nodes are already fully explored when the
// second path reaches them).
func TestMarshalLoop(t *testing.T) {
	cc := NewCacheChains()

	root := cc.Add(outputKey(dgst("root"), 0))
	root.AddResult("", 0, time.Now(), &solver.Remote{})
	// diamond in front of the loop: root -> {p, q} -> x
	p := cc.Add(outputKey(dgst("p"), 0))
	p.LinkFrom(root, 0, "")
	q := cc.Add(outputKey(dgst("q"), 0))
	q.LinkFrom(root, 0, "")
	x1 := cc.Add(outputKey(dgst("x"), 0))
	x1.LinkFrom(p, 0, "")
	x1.LinkFrom(q, 0, "")
	y := cc.Add(outputKey(dgst("y"), 0))
	y.LinkFrom(x1, 0, "")
	// x2 has the same digest as x1 and one input alternative in common, so
	// normalization merges it into x1, which creates the loop x -> y -> x.
	x2 := cc.Add(outputKey(dgst("x"), 0))
	x2.LinkFrom(p, 0, "")
	x2.LinkFrom(y, 0, "")

	cfg, _, err := cc.Marshal(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 5, len(cfg.Records))

	// the exported records must form a DAG
	state := map[int]int{}
	var visit func(i int)
	visit = func(i int) {
		require.NotEqual(t, 1, state[i], "loop in exported cache records")
		if state[i] == 2 {
			return
		}
		state[i] = 1
		for _, inputs := range cfg.Records[i].Inputs {
			for _, in := range inputs {
				visit(in.LinkIndex)
			}
		}
		state[i] = 2
	}
	for i := range cfg.Records {
		visit(i)
	}
}
