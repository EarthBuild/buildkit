package solver

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These extend TestSchedulerMergeCancelInconsistentGraphState to the part of
// the failure seen in production: after the first "inconsistent graph state",
// every later build that reuses the lost vertex fails too, until nothing that
// references it is left and the state can be dropped.
//
// Both reuse the merge/cancel race to poison the solver, keep the surviving
// job (B) open the way an earth session stays open, and then build B's graph
// again with fresh, uncancelled jobs. Skipped by default for the same reason
// as the base repro; set BUILDKIT_TEST_SCHEDULER_MERGE_CANCEL=1 to run.

func isLostEdge(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "inconsistent graph state") ||
		strings.Contains(m, "return leaving outgoing open") ||
		strings.Contains(m, "return leaving incoming open")
}

func mergeCancelGraph(tag string, barrier func(context.Context) error) Edge {
	dep := vtxConst(1, vtxOpt{name: "dep-" + tag, cacheKeySeed: "shared-dep"})
	root := vtxAdd(2, vtxOpt{
		name:         "root-" + tag,
		cacheKeySeed: "shared-root",
		cachePreFunc: barrier,
		execDelay:    2 * time.Millisecond,
		inputs:       []Edge{{Vertex: dep}},
	})
	return Edge{Vertex: root}
}

// poisonSolver runs the merge/cancel race once. When the surviving job saw the
// failure it returns that job still open, and the caller owns its Discard.
func poisonSolver(s *Solver, i int) (*Job, string) {
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
	jA, err := s.NewJob(fmt.Sprintf("A%d", i))
	if err != nil {
		return nil, ""
	}
	jB, err := s.NewJob(fmt.Sprintf("B%d", i))
	if err != nil {
		return nil, ""
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelB()

	var got atomic.Value
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer func() { _ = jA.Discard() }()
		if _, e := jA.Build(ctxA, mergeCancelGraph("A", barrier)); isLostEdge(e) {
			got.Store(e.Error())
		}
	}()
	go func() {
		defer wg.Done()
		// B is the surviving session: it stays open after Build returns.
		if _, e := jB.Build(ctxB, mergeCancelGraph("B", barrier)); isLostEdge(e) {
			got.Store(e.Error())
		}
	}()
	go func() {
		select {
		case <-gate:
		case <-time.After(3 * time.Second):
		}
		cancelA()
	}()
	wg.Wait()
	if v := got.Load(); v != nil {
		return jB, v.(string)
	}
	_ = jB.Discard()
	return nil, ""
}

func rebuild(s *Solver, id, tag string) error {
	j, err := s.NewJob(id)
	if err != nil {
		return err
	}
	defer func() { _ = j.Discard() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = j.Build(ctx, mergeCancelGraph(tag, func(context.Context) error { return nil }))
	return err
}

// While the surviving job is open, rebuilding its graph fails every time;
// once it is discarded, the same rebuild passes.
func TestSchedulerLostEdgePersistsWhileReferenced(t *testing.T) {
	if os.Getenv("BUILDKIT_TEST_SCHEDULER_MERGE_CANCEL") == "" {
		t.Skip("reproduces moby/buildkit#4733; set BUILDKIT_TEST_SCHEDULER_MERGE_CANCEL=1 to run")
	}
	for i := 0; i < 3000; i++ {
		s := NewSolver(SolverOpt{ResolveOpFunc: testOpResolver})
		holder, first := poisonSolver(s, i)
		if holder == nil {
			s.Close()
			continue
		}
		held := 0
		for r := 0; r < 3; r++ {
			if isLostEdge(rebuild(s, fmt.Sprintf("held-%d-%d", i, r), "B")) {
				held++
			}
		}
		_ = holder.Discard()
		freed := 0
		for r := 0; r < 3; r++ {
			if isLostEdge(rebuild(s, fmt.Sprintf("freed-%d-%d", i, r), "B")) {
				freed++
			}
		}
		s.Close()
		t.Logf("iter %d: first failure %q; rebuilds while B open: %d/3 failed; after B discarded: %d/3 failed", i, first, held, freed)
		if held > 0 {
			t.Fatalf("lost edge persisted: %d/3 rebuilds failed while the surviving job was open", held)
		}
		return
	}
	t.Skip("race not hit in 3000 iterations")
}

// Overlapping rebuilds (each job still open when the next one loads, like
// concurrent CI jobs sharing a daemon) keep the lost vertex referenced, so it
// is never dropped and no rebuild passes.
func TestSchedulerLostEdgePersistsAcrossOverlappingJobs(t *testing.T) {
	if os.Getenv("BUILDKIT_TEST_SCHEDULER_MERGE_CANCEL") == "" {
		t.Skip("reproduces moby/buildkit#4733; set BUILDKIT_TEST_SCHEDULER_MERGE_CANCEL=1 to run")
	}
	for i := 0; i < 3000; i++ {
		s := NewSolver(SolverOpt{ResolveOpFunc: testOpResolver})
		prev, first := poisonSolver(s, i)
		if prev == nil {
			s.Close()
			continue
		}
		res := ""
		for r := 0; r < 10; r++ {
			j, err := s.NewJob(fmt.Sprintf("chain-%d-%d", i, r))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, e := j.Build(ctx, mergeCancelGraph("B", func(context.Context) error { return nil }))
			cancel()
			_ = prev.Discard() // the previous job leaves only after this one has loaded
			prev = j
			if isLostEdge(e) {
				res += "F"
			} else {
				res += "."
			}
		}
		_ = prev.Discard()
		after := rebuild(s, fmt.Sprintf("after-%d", i), "B")
		s.Close()
		t.Logf("iter %d: first failure %q; 10 overlapping rebuilds [%s] (F = lost edge); once nothing references it: err=%v", i, first, res, after)
		if strings.Contains(res, "F") {
			t.Fatalf("lost edge persisted across overlapping jobs: [%s]", res)
		}
		return
	}
	t.Skip("race not hit in 3000 iterations")
}
