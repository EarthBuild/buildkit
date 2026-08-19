package solver

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSchedulerMergeCancelInconsistentGraphState reproduces the long-standing
// "inconsistent graph state" scheduler failure (moby/buildkit#4733, #2303;
// EarthBuild/earthbuild#768).
//
// Two jobs build distinct vertices that share cache-key seeds, so their edges
// MUST edge-merge. A shared cache-map barrier forces their key computation to
// overlap (guaranteeing the merge races job teardown). One job is then
// cancelled at the merge point; its edge state is deleted from the actives map
// while the surviving job's merged edge still references it, so the surviving
// job requests a deleted edge -> getEdge()==nil -> "inconsistent graph state".
//
// Job lifecycle matches llbsolver.Solve: Discard is deferred and therefore runs
// only after Build returns (the realistic sequencing).
//
// Skipped by default because on current code it FAILS (that is the point): it
// documents an open bug. Set BUILDKIT_TEST_SCHEDULER_MERGE_CANCEL=1 to run.
// Reproduces in roughly 2 of 3 runs within the iteration budget below.
func TestSchedulerMergeCancelInconsistentGraphState(t *testing.T) {
	if os.Getenv("BUILDKIT_TEST_SCHEDULER_MERGE_CANCEL") == "" {
		t.Skip("reproduces moby/buildkit#4733; set BUILDKIT_TEST_SCHEDULER_MERGE_CANCEL=1 to run")
	}

	const iters = 2000
	for i := 0; i < iters; i++ {
		if msg := runMergeCancelIter(); msg != "" {
			t.Fatalf("iter %d: surviving job observed scheduler corruption: %s", i, msg)
		}
	}
}

func runMergeCancelIter() string {
	s := NewSolver(SolverOpt{ResolveOpFunc: testOpResolver})
	defer s.Close()

	// Barrier: both roots block in CacheMap until both arrive, so their key
	// computation (and therefore the edge merge) overlaps.
	var arrived int32
	gate := make(chan struct{})
	barrier := func(ctx context.Context) error {
		if atomic.AddInt32(&arrived, 1) == 2 {
			close(gate)
		}
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	mkGraph := func(tag string) Edge {
		dep := vtxConst(1, vtxOpt{name: "dep-" + tag, cacheKeySeed: "shared-dep"})
		root := vtxAdd(2, vtxOpt{
			name:         "root-" + tag,
			cacheKeySeed: "shared-root",
			cachePreFunc: barrier,
			execDelay:    2 * time.Millisecond, // keep the surviving edge active during the cancel
			inputs:       []Edge{{Vertex: dep}},
		})
		return Edge{Vertex: root}
	}

	jA, err := s.NewJob("A")
	if err != nil {
		return ""
	}
	jB, err := s.NewJob("B")
	if err != nil {
		return ""
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelB()

	var got atomic.Value
	record := func(e error) {
		if e == nil {
			return
		}
		m := e.Error()
		if strings.Contains(m, "inconsistent graph state") ||
			strings.Contains(m, "return leaving outgoing open") ||
			strings.Contains(m, "return leaving incoming open") {
			got.Store(m)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer func() { _ = jA.Discard() }()
		_, e := jA.Build(ctxA, mkGraph("A"))
		record(e)
	}()
	go func() {
		defer wg.Done()
		defer func() { _ = jB.Discard() }()
		_, e := jB.Build(ctxB, mkGraph("B"))
		record(e)
	}()

	// Cancel A the moment both edges have reached the merge point; A's deferred
	// Discard then runs after its Build unwinds.
	go func() {
		select {
		case <-gate:
		case <-time.After(3 * time.Second):
		}
		cancelA()
	}()

	wg.Wait()
	if v := got.Load(); v != nil {
		return v.(string)
	}
	return ""
}
