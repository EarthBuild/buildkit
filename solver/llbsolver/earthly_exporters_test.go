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
	"github.com/stretchr/testify/assert"
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

// earthly-specific: runExporters skips verifier.CheckInvalidPlatforms only
// when every exporter is the EarthBuild exporter. A non-EarthBuild exporter in
// the same solve must keep the upstream v0.14 result validation rather than
// be handed a multi-ref result without a platforms mapping.
func TestRunExportersValidatesNonEarthlyExporterAlongsideEarthly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		exporterTypes []string
		wantErr       string
		wantExported  bool
	}{
		{
			name:          "no exporters",
			exporterTypes: nil,
			wantErr:       "build result contains multiple refs without platforms mapping",
			wantExported:  false,
		},
		{
			name:          "non-earthly exporter only",
			exporterTypes: []string{client.ExporterImage},
			wantErr:       "build result contains multiple refs without platforms mapping",
			wantExported:  false,
		},
		{
			name:          "earthly exporter only",
			exporterTypes: []string{client.ExporterEarthly},
			wantErr:       "",
			wantExported:  true,
		},
		{
			name:          "mixed earthly and non-earthly exporters",
			exporterTypes: []string{client.ExporterEarthly, client.ExporterImage},
			wantErr:       "build result contains multiple refs without platforms mapping",
			wantExported:  false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

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
			require.NoError(t, verifier.CaptureFrontendOpts(map[string]string{}, inp))

			exporters := make([]exporter.ExporterInstance, 0, len(tc.exporterTypes))
			fakes := make([]*fakeExporterInstance, 0, len(tc.exporterTypes))
			for _, typ := range tc.exporterTypes {
				f := &fakeExporterInstance{typ: typ}
				exporters = append(exporters, f)
				fakes = append(fakes, f)
			}

			s := &Solver{}
			_, _, err = s.runExporters(ctx, exporters, nil, job, nil, inp)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			for _, f := range fakes {
				assert.Equal(t, tc.wantExported, f.exported.Load(), "unexpected exported state for exporter %q", f.typ)
			}
		})
	}
}
