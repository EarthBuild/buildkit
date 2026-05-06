package solver

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/moby/buildkit/solver/errdefs"
	"github.com/moby/buildkit/util/grpcerrors"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	RootCauseSourceExec        = "exec"
	RootCauseSourceCacheMap    = "cache map"
	RootCauseSourceSlowCache   = "slow cache"
	RootCauseSourceLocalSource = "local source"
	RootCauseSourceGateway     = "gateway"
	RootCauseSourceExporter    = "exporter"
	RootCauseSourceFinalizer   = "exporter finalizer"
	RootCauseSourceSession     = "session"
)

type RootCauseKind string

const (
	RootCauseKindError        RootCauseKind = "error"
	RootCauseKindCancellation RootCauseKind = "cancellation"
	RootCauseKindSession      RootCauseKind = "session"
	RootCauseKindShutdown     RootCauseKind = "shutdown"
	RootCauseKindResourceKill RootCauseKind = "resource kill"
)

// RootCause is the first useful solve failure observed before cancellation
// propagation has a chance to collapse it into a generic context cancellation.
type RootCause struct {
	SolveRef      string
	SessionID     string
	VertexDigest  digest.Digest
	VertexName    string
	OpDescription map[string]string
	Source        string
	Kind          RootCauseKind
	Err           error
	RecordedAt    time.Time
}

func (rc RootCause) Error() string {
	if rc.Err == nil {
		return "buildkit solve failed"
	}

	subject := rc.VertexName
	if subject == "" && rc.VertexDigest != "" {
		subject = rc.VertexDigest.String()
	}

	prefix := "BuildKit"
	switch rc.Kind {
	case RootCauseKindCancellation:
		prefix = "BuildKit canceled execution"
	case RootCauseKindSession:
		prefix = "BuildKit lost the solve session"
	case RootCauseKindShutdown:
		prefix = "BuildKit shut down or closed the solve connection"
	case RootCauseKindResourceKill:
		prefix = "BuildKit resource failure"
	default:
		if rc.Source != "" {
			prefix = fmt.Sprintf("BuildKit %s failed", rc.Source)
		} else {
			prefix = "BuildKit failed"
		}
	}

	if subject != "" {
		return fmt.Sprintf("%s while running %q: %v", prefix, subject, rc.Err)
	}
	if rc.SolveRef != "" || rc.SessionID != "" {
		return fmt.Sprintf("%s for solve ref=%q session=%q: %v", prefix, rc.SolveRef, rc.SessionID, rc.Err)
	}
	return fmt.Sprintf("%s: %v", prefix, rc.Err)
}

func (rc RootCause) Unwrap() error {
	return rc.Err
}

func (rc RootCause) Is(target error) bool {
	return errors.Is(rc.Err, target)
}

func (rc RootCause) GRPCStatus() *status.Status {
	if rc.Kind == RootCauseKindSession {
		return status.New(codes.Canceled, rc.Error())
	}
	return status.New(codes.Unknown, rc.Error())
}

// ActiveVertex is a compact summary of a vertex that was still running when a
// solve cancellation was observed.
type ActiveVertex struct {
	Digest  digest.Digest
	Name    string
	Started time.Time
}

func (v ActiveVertex) String() string {
	if v.Name != "" {
		return v.Name
	}
	return v.Digest.String()
}

// SolveCancellation describes a canceled solve when no first non-cancellation
// root cause was observed.
type SolveCancellation struct {
	SolveRef  string
	SessionID string
	Active    []ActiveVertex
	Err       error
}

func (sc SolveCancellation) Error() string {
	base := "BuildKit solve was canceled"
	if sc.SolveRef != "" || sc.SessionID != "" {
		base = fmt.Sprintf("%s: ref=%q session=%q", base, sc.SolveRef, sc.SessionID)
	}
	if len(sc.Active) == 0 {
		if sc.Err != nil {
			return fmt.Sprintf("%s: %v", base, sc.Err)
		}
		return base
	}

	var b strings.Builder
	b.WriteString(base)
	b.WriteString("; last active operations:")
	for _, v := range sc.Active {
		b.WriteString("\n  - ")
		b.WriteString(v.String())
	}
	if sc.Err != nil {
		b.WriteString("\nOriginal BuildKit error: ")
		b.WriteString(sc.Err.Error())
	}
	return b.String()
}

func (sc SolveCancellation) Unwrap() error {
	return sc.Err
}

func (sc SolveCancellation) Is(target error) bool {
	return errors.Is(sc.Err, target)
}

func (sc SolveCancellation) GRPCStatus() *status.Status {
	return status.New(codes.Canceled, sc.Error())
}

// RootCauseRecorder can be implemented by job contexts that can retain solve
// root-cause context for later cancellation handling.
type RootCauseRecorder interface {
	RecordRootCause(context.Context, RootCause)
}

func (j *Job) RecordRootCause(ctx context.Context, cause RootCause) {
	cause.Err = bestRootCauseError(ctx, cause.Err)
	if cause.Err == nil {
		return
	}

	priority, kind := rootCausePriority(cause.Err)
	if priority == rootCausePriorityIgnore {
		return
	}

	if cause.SolveRef == "" {
		cause.SolveRef = j.id
	}
	if cause.SessionID == "" {
		cause.SessionID = j.SessionID
	}
	cause.Kind = kind
	cause.RecordedAt = time.Now()

	j.mu.Lock()
	defer j.mu.Unlock()

	if j.rootCause != nil {
		existingPriority, _ := rootCausePriority(j.rootCause.Err)
		if existingPriority >= priority {
			return
		}
	}

	copied := cause
	copied.OpDescription = maps.Clone(cause.OpDescription)
	j.rootCause = &copied
}

func (j *Job) RootCause() (RootCause, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.rootCause == nil {
		return RootCause{}, false
	}
	return *j.rootCause, true
}

func (j *Job) SnapshotCancellation(ctx context.Context, err error) {
	j.mu.Lock()
	if j.cancelSummary != nil {
		j.mu.Unlock()
		return
	}
	j.mu.Unlock()

	summary := SolveCancellation{
		SolveRef:  j.id,
		SessionID: j.SessionID,
		Active:    j.activeVertices(5),
		Err:       bestRootCauseError(ctx, err),
	}

	j.mu.Lock()
	if j.cancelSummary == nil {
		j.cancelSummary = &summary
	}
	j.mu.Unlock()
}

func (j *Job) Cancellation() (SolveCancellation, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.cancelSummary == nil {
		return SolveCancellation{}, false
	}
	return *j.cancelSummary, true
}

func (jl *Solver) RootCause(id string) (RootCause, bool) {
	jl.mu.RLock()
	j := jl.jobs[id]
	jl.mu.RUnlock()
	if j == nil {
		return RootCause{}, false
	}
	return j.RootCause()
}

func (jl *Solver) Cancellation(id string) (SolveCancellation, bool) {
	jl.mu.RLock()
	j := jl.jobs[id]
	jl.mu.RUnlock()
	if j == nil {
		return SolveCancellation{}, false
	}
	return j.Cancellation()
}

func (s *state) RecordRootCause(ctx context.Context, cause RootCause) {
	if cause.VertexDigest == "" {
		cause.VertexDigest = s.origDigest
		if cause.VertexDigest == "" {
			cause.VertexDigest = s.vtx.Digest()
		}
	}
	if cause.VertexName == "" {
		cause.VertexName = s.vtx.Name()
	}
	if cause.OpDescription == nil {
		cause.OpDescription = maps.Clone(s.vtx.Options().Description)
	}

	s.mu.Lock()
	jobs := make([]*Job, 0, len(s.jobs))
	for j := range s.jobs {
		jobs = append(jobs, j)
	}
	s.mu.Unlock()

	for _, j := range jobs {
		j.RecordRootCause(ctx, cause)
	}
}

func (j *Job) activeVertices(limit int) []ActiveVertex {
	j.list.mu.RLock()
	defer j.list.mu.RUnlock()

	active := make([]ActiveVertex, 0, limit)
	for _, st := range j.list.actives {
		st.mu.Lock()
		_, hasJob := st.jobs[j]
		v := st.clientVertex
		st.mu.Unlock()
		if !hasJob || v.Started == nil || v.Completed != nil {
			continue
		}
		active = append(active, ActiveVertex{
			Digest:  v.Digest,
			Name:    v.Name,
			Started: *v.Started,
		})
		if len(active) == limit {
			break
		}
	}
	return active
}

func bestRootCauseError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, err) {
		return err
	}
	if errdefs.IsCanceled(ctx, err) || isGenericCancellation(err) {
		return cause
	}
	return err
}

func IsSpecificRootCause(err error) bool {
	priority, _ := rootCausePriority(err)
	return priority != rootCausePriorityIgnore
}

func IsCanceledError(ctx context.Context, err error) bool {
	return errdefs.IsCanceled(ctx, err) || isGenericCancellation(err) || isCancellationCleanup(err)
}

const (
	rootCausePriorityIgnore = iota
	rootCausePriorityCancellationContext
	rootCausePriorityUseful
)

func rootCausePriority(err error) (int, RootCauseKind) {
	if err == nil {
		return rootCausePriorityIgnore, ""
	}

	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "session healthcheck failed"):
		return rootCausePriorityUseful, RootCauseKindSession
	case strings.Contains(msg, "no active sessions"),
		strings.Contains(msg, "without session"),
		strings.Contains(msg, "session not found"),
		strings.Contains(msg, "session closed"):
		return rootCausePriorityCancellationContext, RootCauseKindSession
	case strings.Contains(msg, "transport is closing"),
		strings.Contains(msg, "server closed"),
		strings.Contains(msg, "buildkitd is shutting down"),
		strings.Contains(msg, "daemon is shutting down"):
		return rootCausePriorityUseful, RootCauseKindShutdown
	case strings.Contains(msg, "out of memory"),
		strings.Contains(msg, "oom"),
		(isKilledProcess(msg) && !strings.Contains(msg, "context canceled")):
		return rootCausePriorityUseful, RootCauseKindResourceKill
	case isKilledProcess(msg) && strings.Contains(msg, "context canceled"):
		return rootCausePriorityCancellationContext, RootCauseKindCancellation
	case isGenericCancellation(err):
		return rootCausePriorityIgnore, ""
	default:
		return rootCausePriorityUseful, RootCauseKindError
	}
}

func isGenericCancellation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || grpcerrors.Code(err) == codes.Canceled {
		return true
	}
	msg := strings.TrimSpace(strings.ToLower(err.Error()))
	return msg == context.Canceled.Error() ||
		msg == "rpc error: code = canceled desc = context canceled"
}

func isCancellationCleanup(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return isKilledProcess(msg) && strings.Contains(msg, "context canceled")
}

func isKilledProcess(msg string) bool {
	return strings.Contains(msg, "signal: killed") || strings.Contains(msg, "exit code: 137")
}
