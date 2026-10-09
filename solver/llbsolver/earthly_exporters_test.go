package llbsolver

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/exporter"
	"github.com/moby/buildkit/exporter/containerimage/exptypes"
	"github.com/moby/buildkit/exporter/verifier"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/result"
	"github.com/stretchr/testify/require"
)

// fakeExporterInstance is a minimal exporter.ExporterInstance that records
// whether Export was called.
type fakeExporterInstance struct {
	typ      string
	exported atomic.Bool
}

func (e *fakeExporterInstance) ID() int                  { return 0 }
func (e *fakeExporterInstance) Name() string             { return "fake " + e.typ }
func (e *fakeExporterInstance) Config() *exporter.Config { return exporter.NewConfig() }
func (e *fakeExporterInstance) Type() string             { return e.typ }
func (e *fakeExporterInstance) Attrs() map[string]string { return nil }
func (e *fakeExporterInstance) Export(_ context.Context, _ *exporter.Source, _ exptypes.InlineCache, _ string) (map[string]string, exporter.DescriptorReference, error) {
	e.exported.Store(true)
	return nil, nil, nil
}

// earthly-specific: runExporters skips verifier.CheckInvalidPlatforms for
// the whole solve as soon as any exporter is the EarthBuild exporter, so a
// non-EarthBuild exporter in the same solve loses the upstream v0.14 result
// validation and is handed a multi-ref result without a platforms mapping.
func TestRunExportersValidatesNonEarthlyExporterAlongsideEarthly(t *testing.T) {
	t.Parallel()
	ctx := context.TODO()

	jl := solver.NewSolver(solver.SolverOpt{})
	defer jl.Close()
	job, err := jl.NewJob("j0")
	require.NoError(t, err)
	defer job.Discard()

	// EarthBuild-shaped result: several named refs, no platforms mapping.
	inp := &result.Result[cache.ImmutableRef]{
		Refs: map[string]cache.ImmutableRef{
			"output-0": nil,
			"output-1": nil,
		},
	}
	// Solve always records the frontend request options before exporting.
	require.NoError(t, verifier.CaptureFrontendOpts(map[string]string{}, inp))

	s := &Solver{}

	// Control: with only a non-EarthBuild exporter the upstream check runs.
	imageOnly := &fakeExporterInstance{typ: client.ExporterImage}
	_, _, err = s.runExporters(ctx, []exporter.ExporterInstance{imageOnly}, nil, job, nil, inp)
	require.ErrorContains(t, err, "build result contains multiple refs without platforms mapping")
	require.False(t, imageOnly.exported.Load())

	// Mixed solve: the image exporter must still be protected by the check.
	earthly := &fakeExporterInstance{typ: client.ExporterEarthly}
	image := &fakeExporterInstance{typ: client.ExporterImage}
	_, _, err = s.runExporters(ctx, []exporter.ExporterInstance{earthly, image}, nil, job, nil, inp)
	require.ErrorContains(t, err, "build result contains multiple refs without platforms mapping",
		"platform validation was skipped for the image exporter because an EarthBuild exporter was also present")
	require.False(t, image.exported.Load(), "image exporter ran on a result that failed platform validation")
}
