package session

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()

	sm, err := NewManager(&ManagerOpt{
		HealthFrequency:       time.Second,
		HealthTimeout:         time.Second,
		HealthAllowedFailures: 1,
		ShutdownCh:            make(chan struct{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return sm
}

func TestManagerGetReturnsActiveClosedSessionCause(t *testing.T) {
	t.Parallel()

	sm := newTestManager(t)
	cause := errors.New("session healthcheck failed too many times after 3 consecutive failures")
	sessionCtx, closeSession := context.WithCancelCause(context.Background())
	closeSession(cause)

	sm.mu.Lock()
	sm.sessions["session-id"] = &client{
		Session: Session{
			id:  "session-id",
			ctx: sessionCtx,
		},
	}
	sm.mu.Unlock()

	getCtx, cancelGet := context.WithCancel(context.Background())
	defer cancelGet()

	c, err := sm.Get(getCtx, "session-id", false)
	if c != nil {
		t.Fatalf("expected no caller for a closed session")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("expected closed session cause %v, got %v", cause, err)
	}
	if !strings.Contains(err.Error(), "session closed for session-id") {
		t.Fatalf("expected session id in error, got %v", err)
	}
}

func TestManagerGetReturnsRecentlyClosedSessionCause(t *testing.T) {
	t.Parallel()

	sm := newTestManager(t)
	cause := errors.New("session healthcheck failed too many times after 3 consecutive failures")

	sm.mu.Lock()
	sm.recordClosedSessionLocked("session-id", cause)
	sm.mu.Unlock()

	getCtx, cancelGet := context.WithCancel(context.Background())
	defer cancelGet()

	c, err := sm.Get(getCtx, "session-id", false)
	if c != nil {
		t.Fatalf("expected no caller for a recently closed session")
	}
	if err == nil {
		t.Fatalf("expected recently closed session error")
	}
	if !strings.Contains(err.Error(), "session closed for session-id") {
		t.Fatalf("expected session id in error, got %v", err)
	}
	if !strings.Contains(err.Error(), cause.Error()) {
		t.Fatalf("expected closed session cause in error, got %v", err)
	}
}

func TestManagerGetNoWaitStillReturnsNilForMissingSession(t *testing.T) {
	t.Parallel()

	sm := newTestManager(t)
	sm.mu.Lock()
	sm.recordClosedSessionLocked("session-id", errors.New("session closed"))
	sm.mu.Unlock()

	getCtx, cancelGet := context.WithCancel(context.Background())
	defer cancelGet()

	c, err := sm.Get(getCtx, "session-id", true)
	if err != nil {
		t.Fatalf("expected noWait lookup to ignore missing closed session, got %v", err)
	}
	if c != nil {
		t.Fatalf("expected no caller for missing session")
	}
}
