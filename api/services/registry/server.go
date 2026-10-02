package earthly_registry_v1 //nolint:revive

import (
	"fmt"
	"net"
	"strings"
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
		return fmt.Errorf("dial embedded registry at %s: %w", addr, err)
	}

	// The stream is closed by returning from this handler, so there is no send
	// direction for the copy to close on its own.
	return Copy(stream.Context(), conn, stream, nil)
}
