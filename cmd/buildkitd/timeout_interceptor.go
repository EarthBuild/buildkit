package main

// The sessionTimeout interceptors in this file are earthly-specific.
// They are used to automatically cancel builds in CI or Satellites that have run for too long.

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var sessionTimeout time.Duration

var errSessionTimeout = errors.New("session timeout")

func init() {
	env, ok := os.LookupEnv("BUILDKIT_SESSION_TIMEOUT")
	if !ok {
		return
	}
	var err error
	sessionTimeout, err = time.ParseDuration(env)
	if err != nil {
		panic(fmt.Sprintf("invalid value for 'BUILDKIT_SESSION_TIMEOUT': %s", env))
	}
}

func unaryTimeoutInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if sessionTimeout > 0 {
			// TODO we should replace the following code with context.WithTimeoutCause
			//   when it is is released in a future version of Go
			//   https://github.com/golang/go/blob/master/src/context/context.go#L688-L693
			ctx, cancel := context.WithCancelCause(ctx)
			defer cancel(nil)
			done := make(chan bool)
			defer close(done)
			go handleTimeout(done, cancel)
			// End of TODO
			resp, err := handler(ctx, req)
			if errors.Is(err, context.Canceled) && context.Cause(ctx) == errSessionTimeout {
				return resp, errors.Errorf("build exceeded max duration of %s", sessionTimeout.String())
			}
			return resp, err
		}
		return handler(ctx, req)
	}
}

func streamTimeoutInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if sessionTimeout > 0 {
			// TODO we should replace the following code with context.WithTimeoutCause
			//   when it is is released in a future version of Go
			//   https://github.com/golang/go/blob/master/src/context/context.go#L688-L693
			ctx, cancel := context.WithCancelCause(stream.Context())
			defer cancel(nil)
			done := make(chan bool)
			defer close(done)
			go handleTimeout(done, cancel)
			// End of TODO
			err := handler(srv, newWrappedStream(ctx, stream))
			if errors.Is(err, context.Canceled) && context.Cause(ctx) == errSessionTimeout {
				return errors.Errorf("build exceeded max duration of %s", sessionTimeout.String())
			}
			return err
		}
		return handler(srv, stream)
	}
}

func handleTimeout(doneCh chan bool, cancelFn func(error)) {
	sessionTimer := time.NewTimer(sessionTimeout)
	defer sessionTimer.Stop()
	select {
	case <-doneCh:
		return
	case <-sessionTimer.C:
		cancelFn(errSessionTimeout)
	}
}

type wrappedStream struct {
	s   grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	return w.s.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	return w.s.SendMsg(m)
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func (w *wrappedStream) SetHeader(m metadata.MD) error {
	return w.s.SetHeader(m)
}

func (w *wrappedStream) SendHeader(m metadata.MD) error {
	return w.s.SendHeader(m)
}

func (w *wrappedStream) SetTrailer(m metadata.MD) {
	w.s.SetTrailer(m)
}

func newWrappedStream(ctx context.Context, s grpc.ServerStream) grpc.ServerStream {
	return &wrappedStream{s: s, ctx: ctx}
}
