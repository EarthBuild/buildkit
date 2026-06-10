package solver

import (
	"context"
	"fmt"
	"sync"

	"github.com/moby/buildkit/errdefs"
	"github.com/moby/buildkit/solver/internal/pipe"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/cond"
	"github.com/pkg/errors"
)

func newScheduler(ef edgeFactory) *scheduler {
	s := &scheduler{
		waitq:    map[*edge]struct{}{},
		incoming: map[*edge][]*edgePipe{},
		outgoing: map[*edge][]*edgePipe{},

		stopped: make(chan struct{}),
		closed:  make(chan struct{}),

		ef: ef,
	}
	s.cond = cond.NewStatefulCond(&s.mu)

	go s.loop()

	return s
}

type dispatcher struct {
	next *dispatcher
	e    *edge
}

type scheduler struct {
	cond *cond.StatefulCond
	mu   sync.Mutex
	muQ  sync.Mutex

	ef edgeFactory

	waitq       map[*edge]struct{}
	next        *dispatcher
	last        *dispatcher
	stopped     chan struct{}
	stoppedOnce sync.Once
	closed      chan struct{}

	incoming map[*edge][]*edgePipe
	outgoing map[*edge][]*edgePipe
}

func (s *scheduler) Stop() {
	s.stoppedOnce.Do(func() {
		close(s.stopped)
	})
	<-s.closed
}

func (s *scheduler) loop() {
	defer func() {
		close(s.closed)
	}()

	go func() {
		<-s.stopped
		s.mu.Lock()
		s.cond.Signal()
		s.mu.Unlock()
	}()

	s.mu.Lock()
	for {
		select {
		case <-s.stopped:
			s.mu.Unlock()
			return
		default:
		}
		s.muQ.Lock()
		l := s.next
		if l != nil {
			if l == s.last {
				s.last = nil
			}
			s.next = l.next
			delete(s.waitq, l.e)
		}
		s.muQ.Unlock()
		if l == nil {
			s.cond.Wait()
			continue
		}
		s.dispatch(l.e)
	}
}

// dispatch schedules an edge to be processed
func (s *scheduler) dispatch(e *edge) {
	// Earthbuild: breadcrumbs for the scheduler-error branches at the end of
	// this function. This loop is the scheduler hot path and runs while other
	// goroutines mutate edge/state under their own locks, so only cheap,
	// race-free values may be recorded: NO reflective formatting (%+v) of
	// edge or state structs — that races with their owners and a reflective
	// read of a map being written is a runtime fatal that takes down the
	// whole daemon (observed in CI as nested solve sessions vanishing
	// without a root cause).
	trace := dispatchTrace{
		vertexName:    e.edge.Vertex.Name(),
		incomingTotal: len(s.incoming),
		outgoingTotal: len(s.outgoing),
		incomingEdge:  len(s.incoming[e]),
		outgoingEdge:  len(s.outgoing[e]),
	}

	inc := make([]pipeSender, len(s.incoming[e]))
	for i, p := range s.incoming[e] {
		inc[i] = p.Sender
	}
	out := make([]pipeReceiver, len(s.outgoing[e]))
	for i, p := range s.outgoing[e] {
		out[i] = p.Receiver
	}

	e.hasActiveOutgoing = false
	updates := []pipeReceiver{}
	for _, p := range out {
		if ok := p.Receive(); ok {
			updates = append(updates, p)
		}
		if !p.Status().Completed {
			e.hasActiveOutgoing = true
		}
	}
	trace.hasActiveOutgoing = e.hasActiveOutgoing

	pf := &pipeFactory{s: s, e: e}

	// unpark the edge
	debugSchedulerPreUnpark(e, inc, updates, out)
	e.unpark(inc, updates, out, pf)
	debugSchedulerPostUnpark(e, inc)

	// set up new requests that didn't complete/were added by this run
	openIncoming := make([]*edgePipe, 0, len(inc))
	for _, r := range s.incoming[e] {
		status := r.Sender.Status()
		trace.incomingStatus = append(trace.incomingStatus, pipeStatusBits(status.Completed, status.Canceled))
		if !status.Completed {
			openIncoming = append(openIncoming, r)
		}
	}
	if len(openIncoming) > 0 {
		s.incoming[e] = openIncoming
	} else {
		delete(s.incoming, e)
	}

	openOutgoing := make([]*edgePipe, 0, len(out))
	for _, r := range s.outgoing[e] {
		status := r.Receiver.Status()
		trace.outgoingStatus = append(trace.outgoingStatus, pipeStatusBits(status.Completed, status.Canceled))
		if !status.Completed {
			openOutgoing = append(openOutgoing, r)
		}
	}
	if len(openOutgoing) > 0 {
		s.outgoing[e] = openOutgoing
	} else {
		delete(s.outgoing, e)
	}

	// if keys changed there might be possiblity for merge with other edge
	if e.keysDidChange {
		if k := e.currentIndexKey(); k != nil {
			// skip this if not at least 1 key per dep
			origEdge := e.index.LoadOrStore(k, e)
			if origEdge != nil {
				if e.isDep(origEdge) || origEdge.isDep(e) {
					debugSchedulerSkipMergeDueToDependency(e, origEdge)
				} else {
					dest, src := origEdge, e
					if s.ef.hasOwner(origEdge.edge, e.edge) {
						debugSchedulerSwapMergeDueToOwner(e, origEdge)
						dest, src = src, dest
					}

					bklog.G(context.TODO()).Debugf("merging edge %s[%d] to %s[%d]\n", src.edge.Vertex.Name(), src.edge.Index, dest.edge.Vertex.Name(), dest.edge.Index)
					debugSchedulerMergingEdges(src, dest)
					if s.mergeTo(dest, src) {
						s.ef.setEdge(src.edge, dest)
					} else {
						debugSchedulerMergingEdgesSkipped(src, dest)
					}
				}
			}
		}
		e.keysDidChange = false
	}

	trace.openIncoming = len(openIncoming)
	trace.openOutgoing = len(openOutgoing)

	// validation to avoid deadlocks/resource leaks:
	// TODO: if these start showing up in error reports they can be changed
	// to error the edge instead. They can only appear from algorithm bugs in
	// unpark(), not for any external input.
	if len(openIncoming) > 0 && len(openOutgoing) == 0 {
		bklog.G(context.TODO()).Errorf("return leaving incoming open: %s\n", trace.String())
		e.markFailed(pf, errors.New("buildkit scheduler error: return leaving incoming open. Please report this with BUILDKIT_SCHEDULER_DEBUG=1"))
	}
	if len(openIncoming) == 0 && len(openOutgoing) > 0 {
		bklog.G(context.TODO()).Errorf("return leaving outgoing open: %s\n", trace.String())
		e.markFailed(pf, errors.New("buildkit scheduler error: return leaving outgoing open. Please report this with BUILDKIT_SCHEDULER_DEBUG=1"))
	}
}

// dispatchTrace carries race-free breadcrumbs from a dispatch run for the
// scheduler-error logs above. Formatting happens only on the (rare) error
// path. Earthbuild: replaces the previous per-dispatch %+v dumps of edge
// state, which raced with other goroutines and could fatal the daemon.
type dispatchTrace struct {
	vertexName        string
	incomingTotal     int
	outgoingTotal     int
	incomingEdge      int
	outgoingEdge      int
	hasActiveOutgoing bool
	incomingStatus    []byte
	outgoingStatus    []byte
	openIncoming      int
	openOutgoing      int
}

// pipeStatusBits encodes a pipe status as 'c' (completed), 'x' (canceled),
// 'o' (open) for compact, allocation-light tracing.
func pipeStatusBits(completed, canceled bool) byte {
	switch {
	case canceled:
		return 'x'
	case completed:
		return 'c'
	default:
		return 'o'
	}
}

func (d dispatchTrace) String() string {
	return fmt.Sprintf("v2 %s: pipes total in/out %d/%d edge in/out %d/%d activeOut=%v statuses in=%q out=%q open in/out %d/%d",
		d.vertexName, d.incomingTotal, d.outgoingTotal, d.incomingEdge, d.outgoingEdge,
		d.hasActiveOutgoing, d.incomingStatus, d.outgoingStatus, d.openIncoming, d.openOutgoing)
}

// signal notifies that an edge needs to be processed again
func (s *scheduler) signal(e *edge) {
	s.muQ.Lock()
	if _, ok := s.waitq[e]; !ok {
		d := &dispatcher{e: e}
		if s.last == nil {
			s.next = d
		} else {
			s.last.next = d
		}
		s.last = d
		s.waitq[e] = struct{}{}
		s.cond.Signal()
	}
	s.muQ.Unlock()
}

// build evaluates edge into a result
func (s *scheduler) build(ctx context.Context, edge Edge) (CachedResult, error) {
	s.mu.Lock()
	e := s.ef.getEdge(edge)
	if e == nil {
		s.mu.Unlock()
		return nil, errors.Errorf("invalid request %v for build", edge)
	}

	wait := make(chan struct{})

	p := s.newPipe(e, nil, pipeRequest{Payload: &edgeRequest{desiredState: edgeStatusComplete}})
	p.OnSendCompletion = func() {
		p.Receiver.Receive()
		if p.Receiver.Status().Completed {
			close(wait)
		}
	}
	s.mu.Unlock()

	ctx, cancel := context.WithCancelCause(ctx)
	defer func() { cancel(errors.WithStack(context.Canceled)) }()

	go func() {
		<-ctx.Done()
		p.Receiver.Cancel()
	}()

	<-wait

	if err := p.Receiver.Status().Err; err != nil {
		return nil, err
	}
	return p.Receiver.Status().Value.(*edgeState).result.CloneCachedResult(), nil
}

// newPipe creates a new request pipe between two edges
func (s *scheduler) newPipe(target, from *edge, req pipeRequest) *pipe.Pipe[*edgeRequest, any] {
	p := &edgePipe{
		Pipe:   newPipe(req),
		Target: target,
		From:   from,
	}

	s.signal(target)
	if from != nil {
		p.OnSendCompletion = func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			s.signal(p.From)
		}
		s.outgoing[from] = append(s.outgoing[from], p)
	}
	s.incoming[target] = append(s.incoming[target], p)
	p.OnReceiveCompletion = func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		s.signal(p.Target)
	}
	return p.Pipe
}

// newRequestWithFunc creates a new request pipe that invokes a async function
func (s *scheduler) newRequestWithFunc(e *edge, f func(context.Context) (any, error)) pipeReceiver {
	pp, start := pipe.NewWithFunction[*edgeRequest](f)
	p := &edgePipe{
		Pipe: pp,
		From: e,
	}
	p.OnSendCompletion = func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		s.signal(p.From)
	}
	s.outgoing[e] = append(s.outgoing[e], p)
	go start()
	return p.Receiver
}

// mergeTo merges the state from one edge to another. source edge is discarded.
func (s *scheduler) mergeTo(target, src *edge) bool {
	if !target.edge.Vertex.Options().IgnoreCache && src.edge.Vertex.Options().IgnoreCache {
		return false
	}
	for _, inc := range s.incoming[src] {
		inc.mu.Lock()
		inc.Target = target
		s.incoming[target] = append(s.incoming[target], inc)
		inc.mu.Unlock()
	}

	for _, out := range s.outgoing[src] {
		out.mu.Lock()
		out.From = target
		s.outgoing[target] = append(s.outgoing[target], out)
		out.mu.Unlock()
		out.Receiver.Cancel()
	}

	delete(s.incoming, src)
	delete(s.outgoing, src)
	s.signal(target)

	for i, d := range src.deps {
		for _, k := range d.keys {
			target.secondaryExporters = append(target.secondaryExporters, expDep{i, CacheKeyWithSelector{CacheKey: k, Selector: src.cacheMap.Deps[i].Selector}})
		}
		if d.slowCacheKey != nil {
			target.secondaryExporters = append(target.secondaryExporters, expDep{i, CacheKeyWithSelector{CacheKey: *d.slowCacheKey}})
		}
		if d.result != nil {
			for _, dk := range d.result.CacheKeys() {
				target.secondaryExporters = append(target.secondaryExporters, expDep{i, CacheKeyWithSelector{CacheKey: dk, Selector: src.cacheMap.Deps[i].Selector}})
			}
		}
	}

	// TODO(tonistiigi): merge cache providers

	return true
}

// edgeFactory allows access to the edges from a shared graph
type edgeFactory interface {
	getEdge(Edge) *edge
	setEdge(Edge, *edge)
	hasOwner(Edge, Edge) bool
}

type pipeFactory struct {
	e *edge
	s *scheduler
}

func (pf *pipeFactory) NewInputRequest(ee Edge, req *edgeRequest) pipeReceiver {
	target := pf.s.ef.getEdge(ee)
	if target == nil {
		detail := inconsistentGraphStateEdgeDetail(ee, req.desiredState)
		bklog.G(context.TODO()).Errorf("failed to get edge: inconsistent graph state (%s); actives history: %s", detail, dgstTrackerInst.String()) // earthly-specific
		debugSchedulerInconsistentGraphState(ee, req.desiredState)
		return pf.NewFuncRequest(func(_ context.Context) (any, error) {
			return nil, errdefs.Internal(errors.Errorf("failed to get edge: inconsistent graph state (%s)", detail))
		})
	}
	p := pf.s.newPipe(target, pf.e, pipeRequest{Payload: req})
	debugSchedulerNewPipe(pf.e, p, req)
	return p.Receiver
}

func inconsistentGraphStateEdgeDetail(ee Edge, desiredState edgeStatusType) string {
	return fmt.Sprintf("missing active state for edge vertex=%q digest=%s index=%d desired_state=%s", ee.Vertex.Name(), ee.Vertex.Digest(), ee.Index, desiredState)
}

func (pf *pipeFactory) NewFuncRequest(f func(context.Context) (any, error)) pipeReceiver {
	p := pf.s.newRequestWithFunc(pf.e, f)
	debugSchedulerNewFunc(pf.e, p)
	return p
}
