package earthly_registry_v1 //nolint:revive

import (
	"net"
	"strings"

	"github.com/pkg/errors"
)

// NewServer creates and returns a new proxy server with a given host and client.
func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
	}
}

// Server connects incoming gRPC data streams to a backing HTTP service.
type Server struct {
	addr string
	UnimplementedRegistryServer
}

type streamSource interface {
	Send(*ByteMessage) error
	Recv() (*ByteMessage, error)
}

// NewStreamRW creates and returns a gRPC stream reader-writer that implements
// io.Reader & io.Writer as to utilize the gRPC stream with standard methods.
func NewStreamRW(stream streamSource) *StreamRW {
	return &StreamRW{stream: stream}
}

type StreamRW struct {
	stream streamSource
	last   []byte
}

// Write implements io.Writer.
func (s *StreamRW) Write(p []byte) (int, error) {
	err := s.stream.Send(&ByteMessage{
		Data: p,
	})
	if err != nil {
		return 0, errors.Wrap(err, "failed to write data to client")
	}
	return len(p), nil
}

// Read implements io.Reader.
func (s *StreamRW) Read(p []byte) (int, error) {
	l := 0
	if len(s.last) > 0 {
		l = copy(p, s.last)
	}

	msg, err := s.stream.Recv()
	if err != nil {
		return 0, err
	}

	s.last = msg.GetData()
	n := copy(p, s.last)
	s.last = s.last[n:]

	return n + l, nil
}

// Proxy requests sent via gRPC data stream to the embedded Docker registry and
// pipe them back out through the stream again. This allows us to send HTTP
// requests to the embedded registry without having to connect via some other
// exposed server or port.
//
// One stream carries one connection from the client's local listener, for as
// long as the client keeps it: docker reuses a connection across the manifest
// and blob requests of a single pull, and each of those requests is answered
// on the stream that carried it.
func (s *Server) Proxy(stream Registry_ProxyServer) error {
	addr := strings.ReplaceAll(s.addr, "0.0.0.0", "127.0.0.1")

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return errors.Wrapf(err, "failed to dial the embedded registry at %s", addr)
	}

	// The stream is closed by returning from this handler, so there is no send
	// direction for the copy to close on its own.
	return Copy(stream.Context(), conn, stream, nil)
}
