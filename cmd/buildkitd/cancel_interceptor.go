package main

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/moby/buildkit/session"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func unaryCancelInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, cancelFn := context.WithCancelCause(ctx)
		id := uuid.NewString()
		ctx = context.WithValue(ctx, session.SessionContextKey, id)
		cancelCh := make(chan bool)
		session.GrpcSessions[id] = cancelCh
		defer cancelFn(nil)
		done := make(chan bool)
		defer close(done)
		go handleSessionCancel(done, cancelCh, cancelFn)
		resp, err := handler(ctx, req)
		if errors.Is(err, context.Canceled) && context.Cause(ctx) == errSessionTimeout {
			return resp, errors.Errorf("build exceeded max duration of %s", sessionTimeout.String())
		}
		return resp, err
	}
}

func streamCancelInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, cancelFn := context.WithCancelCause(stream.Context())
		id := uuid.NewString()
		ctx = context.WithValue(ctx, session.SessionContextKey, id)
		cancelCh := make(chan bool)
		session.GrpcSessions[id] = cancelCh
		defer cancelFn(nil)
		done := make(chan bool)
		defer close(done)
		go handleSessionCancel(done, cancelCh, cancelFn)
		err := handler(srv, newWrappedStream(stream, ctx))
		if errors.Is(err, context.Canceled) && context.Cause(ctx) == errSessionTimeout {
			return errors.Errorf("build exceeded max duration of %s", sessionTimeout.String())
		}
		return err
	}
}

func handleSessionCancel(doneCh, cancelCh chan bool, cancelFn func(error)) {
	sessionTimer := time.NewTimer(sessionTimeout)
	defer sessionTimer.Stop()
	select {
	case <-doneCh:
		return
	case <-sessionTimer.C:
		cancelFn(errSessionTimeout)
	}
}

// type wrappedStream struct {
// 	s   grpc.ServerStream
// 	ctx context.Context
// }

// func (w *wrappedStream) RecvMsg(m interface{}) error {
// 	return w.s.RecvMsg(m)
// }

// func (w *wrappedStream) SendMsg(m interface{}) error {
// 	return w.s.SendMsg(m)
// }

// func (w *wrappedStream) Context() context.Context {
// 	return w.ctx
// }

// func (w *wrappedStream) SetHeader(m metadata.MD) error {
// 	return w.s.SetHeader(m)
// }

// func (w *wrappedStream) SendHeader(m metadata.MD) error {
// 	return w.s.SendHeader(m)
// }

// func (w *wrappedStream) SetTrailer(m metadata.MD) {
// 	w.s.SetTrailer(m)
// }

// func newWrappedStream(s grpc.ServerStream, ctx context.Context) grpc.ServerStream {
// 	return &wrappedStream{s: s, ctx: ctx}
// }
