package solver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/moby/buildkit/client"
	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestRootCauseRecorderStoresFirstUsefulNonCancellationError(t *testing.T) {
	t.Parallel()

	s := NewSolver(SolverOpt{})
	defer s.Close()
	j, err := s.NewJob("solve-ref")
	require.NoError(t, err)
	j.SessionID = "session-id"

	j.RecordRootCause(t.Context(), RootCause{Source: RootCauseSourceExec, Err: context.Canceled})
	_, ok := j.RootCause()
	require.False(t, ok)

	first := errors.New("first useful failure")
	j.RecordRootCause(t.Context(), RootCause{Source: RootCauseSourceExec, Err: first})
	j.RecordRootCause(t.Context(), RootCause{Source: RootCauseSourceExec, Err: errors.New("later useful failure")})
	j.RecordRootCause(t.Context(), RootCause{Source: RootCauseSourceExec, Err: context.Canceled})

	rc, ok := j.RootCause()
	require.True(t, ok)
	require.ErrorIs(t, rc, first)
	require.Equal(t, "solve-ref", rc.SolveRef)
	require.Equal(t, "session-id", rc.SessionID)
	require.Contains(t, rc.Error(), "first useful failure")
	require.NotContains(t, rc.Error(), "later useful failure")
}

func TestRootCauseRecorderReplacesSessionContextWithUsefulCause(t *testing.T) {
	t.Parallel()

	s := NewSolver(SolverOpt{})
	defer s.Close()
	j, err := s.NewJob("solve-ref")
	require.NoError(t, err)

	j.RecordRootCause(t.Context(), RootCause{
		Source: RootCauseSourceLocalSource,
		Err:    errors.New("could not access local files without session"),
	})
	rc, ok := j.RootCause()
	require.True(t, ok)
	require.Equal(t, RootCauseKindSession, rc.Kind)

	useful := errors.New("process failed with exit code: 42")
	j.RecordRootCause(t.Context(), RootCause{Source: RootCauseSourceExec, Err: useful})

	rc, ok = j.RootCause()
	require.True(t, ok)
	require.ErrorIs(t, rc, useful)
	require.Equal(t, RootCauseKindError, rc.Kind)
}

func TestRootCauseRecorderUsesSpecificContextCause(t *testing.T) {
	t.Parallel()

	s := NewSolver(SolverOpt{})
	defer s.Close()
	j, err := s.NewJob("solve-ref")
	require.NoError(t, err)

	specific := errors.New("gateway callback failed")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(specific)

	j.RecordRootCause(ctx, RootCause{Source: RootCauseSourceGateway, Err: context.Canceled})

	rc, ok := j.RootCause()
	require.True(t, ok)
	require.ErrorIs(t, rc, specific)
	require.Contains(t, rc.Error(), "gateway callback failed")
}

func TestEncoreAddsRootCauseToCanceledVertex(t *testing.T) {
	t.Parallel()

	started := time.Now()
	dgst := digest.FromString("vertex")
	vertex := &client.Vertex{
		Digest:  dgst,
		Name:    "RUN failing-command",
		Started: &started,
	}
	vs := &vertexStream{
		cache:     map[digest.Digest]*client.Vertex{dgst: vertex},
		wasCached: map[digest.Digest]struct{}{},
	}

	out := vs.encore(RootCause{
		VertexDigest: dgst,
		VertexName:   "RUN failing-command",
		Source:       RootCauseSourceExec,
		Kind:         RootCauseKindError,
		Err:          errors.New("inner failure"),
		RecordedAt:   started,
	}, true)

	require.Len(t, out, 1)
	require.Contains(t, out[0].Error, "inner failure")
	require.NotEqual(t, context.Canceled.Error(), out[0].Error)
}
