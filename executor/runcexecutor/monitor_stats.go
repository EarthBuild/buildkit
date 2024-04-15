package runcexecutor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"time"

	runc "github.com/containerd/go-runc"
	"github.com/moby/buildkit/util/bklog"
	"github.com/pkg/errors"
)

// earthly-specific: This entire file is earthly-specific, it is used to collect runc Stats and sends them via a stream to the client
//
// the stats protocol is:
// loop of stats events:
//   <uint8> version (currently 1)
//   <uint32> length of payload (stored as n)
//   <bytes> n bytes (a json-encoded string of the go-runc Stats structure)

// writeUint32PrefixedBytes writes a uint32 representing the length of the b byte array, followed by the actual
// byte array.
func writeUint32PrefixedBytes(w io.Writer, b []byte) error {
	n := len(b)
	err := binary.Write(w, binary.LittleEndian, uint32(n))
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func writeStatsToStream(w io.Writer, stats *runc.Stats) error {
	statsJSON, err := json.Marshal(stats)
	if err != nil {
		return errors.Wrap(err, "failed to encode runc stats")
	}
	err = binary.Write(w, binary.LittleEndian, uint8(1)) // earthly stats stream protocol v1
	if err != nil {
		return err
	}
	return writeUint32PrefixedBytes(w, statsJSON)
}

func (w *runcExecutor) monitorContainerStats(ctx context.Context, id string, sampleFrequency time.Duration, statsWriter io.WriteCloser) {
	numFailuresAllowed := 10

	timer := time.NewTimer(sampleFrequency)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			bklog.G(ctx).Infof("stats collection context done: %v", ctx.Err())
			return
		case <-timer.C:
			stats, err := w.runc.Stats(ctx, id)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if numFailuresAllowed > 0 {
					// allow the initial calls to runc.Stats to fail, for cases where the program didn't start within the initial
					// sampleFrequency; this should only occur under heavy workloads
					bklog.G(ctx).Warnf("ignoring runc stats collection error: %s", err)
					numFailuresAllowed--
					continue
				}
				bklog.G(ctx).Errorf("runc stats collection error: %s", err)
				return
			}

			// once runc.Stats has succeeded, don't ignore future errors
			numFailuresAllowed = 0

			err = writeStatsToStream(statsWriter, stats)
			if err != nil {
				bklog.G(ctx).Errorf("failed to send runc stats to client-stream: %s", err)
				return
			}
		}
	}
}
