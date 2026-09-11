package earthly_registry_v1 //nolint:revive

import (
	"context"
	"errors"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"
)

// copyBufferSize is the size of the buffer each direction reads into. 32KiB is
// io.Copy's own default, and what session/sshforward.Copy and
// session/socketforward use for the same job; it is comfortably under gRPC's
// 4MiB default maximum message size, so a full buffer never needs splitting
// across messages. Nothing depends on the two ends of the tunnel choosing the
// same value -- that they happened to agree is what kept StreamRW's leftover
// handling from ever being exercised -- so this is a throughput knob and
// nothing more.
const copyBufferSize = 32 * 1024

// Stream is the part of a gRPC bidirectional stream the tunnel needs. Both
// ends of Registry.Proxy satisfy it, so the same copy runs on the daemon and
// on the client.
type Stream interface {
	SendMsg(m any) error
	RecvMsg(m any) error
}

// Copy joins a connection to a gRPC stream in both directions and returns once
// both are done. The bytes are opaque: nothing here knows where one HTTP
// request or response ends, because nothing needs to. Each direction ends when
// its source says so -- io.EOF from the connection, or a peer that closed its
// send direction -- and that end is passed on as a half-close, so the other
// side can finish what it still owes before the whole conversation is torn
// down.
//
// This mirrors session/sshforward.Copy, which has carried forwarded agent
// sockets for years; see that file for the same shape with commentary.
func Copy(ctx context.Context, conn io.ReadWriteCloser, stream Stream, closeStream func() error) error {
	defer conn.Close()

	eg, ctx := errgroup.WithContext(ctx)

	// Peer to connection.
	eg.Go(func() error {
		msg := &ByteMessage{}
		for {
			if err := stream.RecvMsg(msg); err != nil {
				if errors.Is(err, io.EOF) {
					// The peer has finished sending. It is still reading, so
					// close only this direction and leave the response to
					// come back.
					if closeWriter, ok := conn.(interface{ CloseWrite() error }); ok {
						// Best effort: the read half stays open either way.
						closeWriter.CloseWrite()
					} else {
						conn.Close()
					}
					return nil
				}
				conn.Close()
				return fmt.Errorf("receive from stream: %w", err)
			}

			select {
			case <-ctx.Done():
				conn.Close()
				return context.Cause(ctx)
			default:
			}

			if _, err := conn.Write(msg.GetData()); err != nil {
				conn.Close()
				return fmt.Errorf("write to connection: %w", err)
			}

			msg.Data = msg.Data[:0]
		}
	})

	// Connection to peer.
	eg.Go(func() error {
		buf := make([]byte, copyBufferSize)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if err := stream.SendMsg(&ByteMessage{Data: buf[:n]}); err != nil {
					return fmt.Errorf("send to stream: %w", err)
				}
			}

			if err != nil {
				if errors.Is(err, io.EOF) {
					// Everything the connection had to say has been said.
					if closeStream != nil {
						if err := closeStream(); err != nil {
							return fmt.Errorf("close stream: %w", err)
						}
					}
					return nil
				}
				return fmt.Errorf("read from connection: %w", err)
			}

			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
			}
		}
	})

	return eg.Wait()
}
