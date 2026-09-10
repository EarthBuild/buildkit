package earthly_registry_v1 //nolint:revive

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var errUnexpectedMessage = errors.New("stream carried a message that was not a ByteMessage")

// The proxy carries one HTTP conversation between a local docker daemon and
// buildkitd's embedded registry over a gRPC stream, as opaque bytes. These
// tests stand a real HTTP server in for the embedded registry, drive it with a
// real net/http client, and put the production Server.Proxy between the two.
// Nothing here parses HTTP: whether the bytes arrive intact is the whole
// question, so both ends are left to net/http to frame.

// tunnel returns an http.Client whose connections are proxied to reg through
// Server.Proxy, and a count of how many connections it dialled -- reuse of a
// single connection is a property worth asserting, not assuming.
func tunnel(t *testing.T, reg *httptest.Server) (*http.Client, func() int) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	srv := NewServer(reg.Listener.Addr().String())

	var (
		mu    sync.Mutex
		dials int
	)

	tr := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			mu.Lock()
			dials++
			mu.Unlock()

			near, far, err := tcpPair(t)
			if err != nil {
				return nil, err
			}

			clientStream, serverStream := newStreamPair(ctx)

			go func() {
				defer serverStream.closeSend()
				_ = srv.Proxy(serverStream)
			}()

			go func() {
				defer far.Close()
				_ = pump(far, clientStream)
			}()

			return near, nil
		},
	}
	t.Cleanup(tr.CloseIdleConnections)

	// A tunnel that mishandles termination strands a request rather than
	// failing it, so bound every request: a hung pull is a failure too.
	return &http.Client{Transport: tr, Timeout: 20 * time.Second}, func() int {
		mu.Lock()
		defer mu.Unlock()
		return dials
	}
}

// pump is the client half of the tunnel: the daemon-side counterpart to
// Server.Proxy, written out here rather than borrowed from production code so
// that the tests exercise the server against an independently correct peer.
// Termination is by half-close in both directions, never by a timer.
func pump(conn *net.TCPConn, stream *clientStream) error {
	errs := make(chan error, 2)

	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if serr := stream.SendMsg(&ByteMessage{Data: buf[:n]}); serr != nil {
					errs <- serr
					return
				}
			}
			if err != nil {
				// The daemon has finished its request. Tell the far side, but
				// keep reading the response.
				stream.closeSend()
				if err == io.EOF {
					err = nil
				}
				errs <- err
				return
			}
		}
	}()

	go func() {
		for {
			msg := &ByteMessage{}
			err := stream.RecvMsg(msg)
			if err != nil {
				if err == io.EOF {
					// The response is complete: signal end-of-body downstream
					// without discarding anything the daemon still owes us.
					errs <- conn.CloseWrite()
					return
				}
				errs <- err
				return
			}
			if _, err := conn.Write(msg.GetData()); err != nil {
				errs <- err
				return
			}
		}
	}()

	if err := <-errs; err != nil {
		return err
	}
	return <-errs
}

// tcpPair returns the two ends of a real TCP connection. net.Pipe would be
// simpler but has no CloseWrite, and half-close is precisely what is under
// test.
func tcpPair(t *testing.T) (near, far *net.TCPConn, err error) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	defer ln.Close()

	type accepted struct {
		conn net.Conn
		err  error
	}
	ch := make(chan accepted, 1)
	go func() {
		conn, err := ln.Accept()
		ch <- accepted{conn: conn, err: err}
	}()

	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		return nil, nil, err
	}

	got := <-ch
	if got.err != nil {
		dialed.Close()
		return nil, nil, got.err
	}

	t.Cleanup(func() {
		dialed.Close()
		got.conn.Close()
	})

	return dialed.(*net.TCPConn), got.conn.(*net.TCPConn), nil
}

// newStreamPair returns the two ends of an in-memory bidirectional stream with
// gRPC's semantics: a receive returns io.EOF once the peer has closed its send
// direction and every message already sent has been delivered, and message
// bytes are copied on send, as marshalling would copy them.
func newStreamPair(ctx context.Context) (*clientStream, *serverStream) {
	var (
		toServer = make(chan []byte, 16)
		toClient = make(chan []byte, 16)
	)

	c := &clientStream{halfStream: halfStream{ctx: ctx, send: toServer, recv: toClient}}
	s := &serverStream{halfStream: halfStream{ctx: ctx, send: toClient, recv: toServer}}

	return c, s
}

type halfStream struct {
	ctx  context.Context
	send chan []byte
	recv chan []byte
	once sync.Once
}

func (h *halfStream) SendMsg(m any) error {
	msg, ok := m.(*ByteMessage)
	if !ok {
		return errUnexpectedMessage
	}

	data := make([]byte, len(msg.GetData()))
	copy(data, msg.GetData())

	select {
	case h.send <- data:
		return nil
	case <-h.ctx.Done():
		return h.ctx.Err()
	}
}

func (h *halfStream) RecvMsg(m any) error {
	msg, ok := m.(*ByteMessage)
	if !ok {
		return errUnexpectedMessage
	}

	select {
	case data, open := <-h.recv:
		if !open {
			return io.EOF
		}
		msg.Data = data
		return nil
	case <-h.ctx.Done():
		return h.ctx.Err()
	}
}

func (h *halfStream) closeSend() {
	h.once.Do(func() { close(h.send) })
}

func (h *halfStream) Context() context.Context { return h.ctx }

type clientStream struct {
	halfStream
}

func (c *clientStream) Send(m *ByteMessage) error { return c.SendMsg(m) }

func (c *clientStream) Recv() (*ByteMessage, error) {
	msg := &ByteMessage{}
	if err := c.RecvMsg(msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// serverStream stands in for the generated Registry_ProxyServer. The embedded
// grpc.ServerStream covers the header and trailer methods the proxy never
// calls; the ones it does call are implemented above.
type serverStream struct {
	halfStream
	grpc.ServerStream
}

func (s *serverStream) Send(m *ByteMessage) error { return s.halfStream.SendMsg(m) }

func (s *serverStream) Recv() (*ByteMessage, error) {
	msg := &ByteMessage{}
	if err := s.halfStream.RecvMsg(msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (s *serverStream) SendMsg(m any) error { return s.halfStream.SendMsg(m) }

func (s *serverStream) RecvMsg(m any) error { return s.halfStream.RecvMsg(m) }

func (s *serverStream) Context() context.Context { return s.halfStream.Context() }

var _ Registry_ProxyServer = (*serverStream)(nil)

// blob is a stand-in for a layer: larger than the 32KiB copy buffers on both
// sides, so it crosses the stream as many messages.
func blob(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestProxyServesAResponse(t *testing.T) {
	body := blob(128 * 1024)

	reg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(reg.Close)

	client, _ := tunnel(t, reg)

	resp, err := client.Get("http://registry.invalid/v2/img/blobs/sha256:0")
	if err != nil {
		t.Fatalf("GET through the proxy: %v", err)
	}
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the proxied body: %v", err)
	}

	if len(got) != len(body) {
		t.Fatalf("body length: got %d bytes, want %d", len(got), len(body))
	}
	if string(got) != string(body) {
		t.Error("body differs from what the registry served")
	}
}

func TestProxyDoesNotTruncateAResponseThatStallsMidBody(t *testing.T) {
	body := blob(128 * 1024)

	reg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		half := len(body) / 2

		_, _ = w.Write(body[:half])
		w.(http.Flusher).Flush()

		// A loaded runner -- the -race integration suite, say -- can leave a
		// registry this long between chunks. Silence on the socket is not the
		// end of a response.
		time.Sleep(300 * time.Millisecond)

		_, _ = w.Write(body[half:])
	}))
	t.Cleanup(reg.Close)

	client, _ := tunnel(t, reg)

	resp, err := client.Get("http://registry.invalid/v2/img/blobs/sha256:0")
	if err != nil {
		t.Fatalf("GET through the proxy: %v", err)
	}
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading a body the registry paused mid-way: %v (got %d of %d bytes)", err, len(got), len(body))
	}
	if string(got) != string(body) {
		t.Errorf("body differs from what the registry served: got %d bytes, want %d", len(got), len(body))
	}
}

func TestProxyReusesOneConnectionForTwoRequests(t *testing.T) {
	body := blob(4096)

	reg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(reg.Close)

	client, dials := tunnel(t, reg)

	for i := 0; i < 2; i++ {
		if i > 0 {
			// docker does its own work between a manifest fetch and the blob
			// fetches that follow. A connection that only survives while
			// requests arrive back-to-back is not a keep-alive connection.
			time.Sleep(200 * time.Millisecond)
		}

		resp, err := client.Get("http://registry.invalid/v2/img/manifests/latest")
		if err != nil {
			t.Fatalf("request %d through the proxy: %v", i+1, err)
		}

		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("reading response %d: %v", i+1, err)
		}
		if string(got) != string(body) {
			t.Fatalf("response %d differs from what the registry served", i+1)
		}
	}

	// docker holds keep-alive connections open across the many manifest and
	// blob requests of one pull. Tearing the tunnel down after the first
	// response makes every later request pay for a new stream, and strands any
	// request already in flight on the old one.
	if n := dials(); n != 1 {
		t.Errorf("connections dialled: got %d, want 1 -- the tunnel did not survive the first response", n)
	}
}

func TestProxyPassesALargeRequestBodyThrough(t *testing.T) {
	sent := blob(256 * 1024)

	var (
		mu       sync.Mutex
		received []byte
	)

	reg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		mu.Lock()
		received = b
		mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(reg.Close)

	client, _ := tunnel(t, reg)

	resp, err := client.Post("http://registry.invalid/v2/img/blobs/uploads/", "application/octet-stream", bytes.NewReader(sent))
	if err != nil {
		t.Fatalf("POST through the proxy: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != len(sent) {
		t.Fatalf("request body length: got %d bytes, want %d", len(received), len(sent))
	}
	if string(received) != string(sent) {
		t.Error("request body differs from what was sent")
	}
}
