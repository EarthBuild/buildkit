package solver

import (
	"context"
	"sync"
	"testing"

	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/progress"
	"github.com/stretchr/testify/require"
)

// earthly-specific: these tests characterise how a vertex's log output is
// fanned out to concurrent jobs (separate solve requests) that end up sharing
// work in the scheduler. EarthBuild prints every log line it receives from
// every Status stream, so a log line that is delivered to more than one job is
// printed more than once.

// runFanoutJobs builds e0 in job j0, then (with j0 still alive) builds e1 in
// job j1, and returns how many times logLine was delivered to each job's
// Status stream.
func runFanoutJobs(t *testing.T, e0, e1 Edge, logLine string) (j0Count, j1Count int) {
	t.Helper()
	ctx := context.TODO()

	l := NewSolver(SolverOpt{
		ResolveOpFunc: testOpResolver,
		DefaultCache:  NewInMemoryCacheManager(),
	})
	defer l.Close()

	j0, err := l.NewJob("j0")
	require.NoError(t, err)
	j1, err := l.NewJob("j1")
	require.NoError(t, err)

	count := func(j *Job, out *int, wg *sync.WaitGroup) {
		defer wg.Done()
		ch := make(chan *client.SolveStatus)
		go func() {
			_ = j.Status(ctx, false, ch)
		}()
		for ss := range ch {
			for _, lg := range ss.Logs {
				if string(lg.Data) == logLine {
					*out++
				}
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go count(j0, &j0Count, &wg)
	go count(j1, &j1Count, &wg)

	_, err = j0.Build(ctx, e0)
	require.NoError(t, err)

	// j0 is intentionally not discarded yet, so its active state (and edge
	// index entry) is still live while j1 solves.
	_, err = j1.Build(ctx, e1)
	require.NoError(t, err)

	j0.CloseProgress()
	j1.CloseProgress()
	wg.Wait()

	require.NoError(t, j0.Discard())
	require.NoError(t, j1.Discard())
	return j0Count, j1Count
}

// logOnExec returns an execPreFunc that writes logLine to the vertex's
// progress stream, the same way an ExecOp writes RUN output.
func logOnExec(logLine string) func(context.Context) error {
	return func(ctx context.Context) error {
		pw, _, _ := progress.NewFromContext(ctx)
		defer pw.Close()
		return pw.Write(identity.NewID(), client.VertexLog{Stream: 1, Data: []byte(logLine)})
	}
}

// TestEarthlyMergedEdgeLogsReachMergedJob covers two solves whose vertices
// have different digests but the same cache key, so the scheduler merges j1's
// edge into j0's active edge and the operation runs once.
//
// This is a characterisation test of upstream behaviour, not of
// earthly-specific code. On old EarthBuild main (BuildKit v0.12 base) the log
// line of the merged operation was delivered only to the job that ran it.
// Since upstream commit e1da8b7f ("solver: fix printing progress messages
// after merged edges", first released in v0.13), state.setEdge adds the merge
// source's progress writer to the target, and MultiWriter.Add replays the
// target's history, so j1 also receives the log line of j0's vertex.
//
// BuildKit is deliberately not patched to suppress this (see AGENTS.md):
// streaming vertex output to every attached caller is upstream's contract.
// EarthBuild relies on that contract and must dedupe on its side, by not
// issuing redundant concurrent solves for the same work, or by
// deduplicating log output across the Status streams of concurrent solves
// (earthbuild#1004). If this test starts failing after an upstream bump, the
// fan-out contract has changed and EarthBuild's dedupe must be revisited.
func TestEarthlyMergedEdgeLogsReachMergedJob(t *testing.T) {
	t.Parallel()

	const logLine = "merged-edge-log\n"
	e0 := Edge{Vertex: vtx(vtxOpt{
		name:         "v0",
		cacheKeySeed: "same-cache-key",
		value:        "result",
		execPreFunc:  logOnExec(logLine),
	})}
	e1 := Edge{Vertex: vtx(vtxOpt{
		name:         "v1", // different digest from v0
		cacheKeySeed: "same-cache-key",
		value:        "result",
		execPreFunc:  logOnExec(logLine),
	})}

	j0Count, j1Count := runFanoutJobs(t, e0, e1, logLine)

	require.Equal(t, 1, j0Count, "job that ran the operation should see its log once")
	require.Equal(t, 1, j1Count, "job whose edge was merged into another job's edge should have that job's log replayed (upstream e1da8b7f)")
}

// TestEarthlySharedVertexLogsDeliveredToEveryJob covers two solves that load
// a vertex with the identical digest, so both jobs attach to the same active
// state. This is a control case that passes on both old EarthBuild main and
// this merge: loadUnlocked has always added every job's progress writer to
// the shared state's MultiWriter, and MultiWriter.Add replays history, so
// every attached job receives the log line. This fan-out is longstanding
// upstream behaviour, not a regression from the v0.14.1 merge.
func TestEarthlySharedVertexLogsDeliveredToEveryJob(t *testing.T) {
	t.Parallel()

	const logLine = "shared-vertex-log\n"
	v := vtx(vtxOpt{
		name:         "shared",
		cacheKeySeed: "shared-cache-key",
		value:        "result",
		execPreFunc:  logOnExec(logLine),
	})

	j0Count, j1Count := runFanoutJobs(t, Edge{Vertex: v}, Edge{Vertex: v}, logLine)

	require.Equal(t, 1, j0Count, "job that ran the operation should see its log once")
	require.Equal(t, 1, j1Count, "job attached to an already-executed shared vertex should have its log replayed")
}
