//go:build linux
// +build linux

package runcexecutor

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"

	runc "github.com/containerd/go-runc"
)

type recordWriter struct {
	writes [][]byte
}

func (rw *recordWriter) Write(p []byte) (n int, err error) {
	cp := make([]byte, len(p))
	copy(cp, p)
	rw.writes = append(rw.writes, cp)
	return len(p), nil
}

type errWriter struct {
	err error
}

func (ew *errWriter) Write(p []byte) (n int, err error) {
	return 0, ew.err
}

func TestWriteStatsToStream_AtomicSingleWrite(t *testing.T) {
	t.Parallel()

	rw := &recordWriter{}
	stats := &runc.Stats{
		Cpu: runc.Cpu{
			Usage: runc.CpuUsage{
				Total: 123456,
			},
		},
	}

	err := writeStatsToStream(rw, stats)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must be a single atomic write so that downstream progress log stream
	// does not fragment the frame across multiple client.VertexLog events.
	if got := len(rw.writes); got != 1 {
		t.Fatalf("got %d writes, want 1 atomic write", got)
	}

	payload := rw.writes[0]
	if len(payload) < 5 {
		t.Fatalf("payload length %d too short, want at least 5 bytes", len(payload))
	}

	// Protocol version byte
	if got, want := payload[0], uint8(1); got != want {
		t.Errorf("version = %d, want %d", got, want)
	}

	// 4-byte LittleEndian payload length
	n := binary.LittleEndian.Uint32(payload[1:5])
	jsonBytes := payload[5:]
	if got, want := uint32(len(jsonBytes)), n; got != want {
		t.Errorf("json length = %d, want %d (from prefix)", got, want)
	}

	// JSON payload matches stats
	var decoded runc.Stats
	err = json.Unmarshal(jsonBytes, &decoded)
	if err != nil {
		t.Fatalf("failed to unmarshal json payload: %v", err)
	}

	if decoded.Cpu.Usage.Total != 123456 {
		t.Errorf("decoded stats Total = %d, want 123456", decoded.Cpu.Usage.Total)
	}
}

func TestWriteStatsToStream_WriterError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("simulated pipe write failure")
	ew := &errWriter{err: wantErr}
	stats := &runc.Stats{}

	err := writeStatsToStream(ew, stats)
	if !errors.Is(err, wantErr) {
		t.Fatalf("writeStatsToStream returned error %v, want %v", err, wantErr)
	}
}

func TestWriteStatsToStream_MultipleMetrics(t *testing.T) {
	t.Parallel()

	rw := &recordWriter{}
	stats := &runc.Stats{
		Cpu: runc.Cpu{
			Usage: runc.CpuUsage{
				Total: 987654,
			},
		},
		Memory: runc.Memory{
			Usage: runc.MemoryEntry{
				Limit: 2048,
				Usage: 1024,
			},
		},
		Pids: runc.Pids{
			Current: 42,
			Limit:   100,
		},
	}

	err := writeStatsToStream(rw, stats)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := len(rw.writes); got != 1 {
		t.Fatalf("got %d writes, want 1 atomic write", got)
	}

	payload := rw.writes[0]
	n := binary.LittleEndian.Uint32(payload[1:5])
	jsonBytes := payload[5:]
	if got, want := uint32(len(jsonBytes)), n; got != want {
		t.Errorf("json length = %d, want %d (from prefix)", got, want)
	}

	var decoded runc.Stats
	err = json.Unmarshal(jsonBytes, &decoded)
	if err != nil {
		t.Fatalf("failed to unmarshal json payload: %v", err)
	}

	if decoded.Memory.Usage.Usage != 1024 {
		t.Errorf("decoded memory usage = %d, want 1024", decoded.Memory.Usage.Usage)
	}
	if decoded.Pids.Current != 42 {
		t.Errorf("decoded pids current = %d, want 42", decoded.Pids.Current)
	}
}
