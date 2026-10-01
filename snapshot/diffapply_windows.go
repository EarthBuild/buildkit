//go:build windows
// +build windows

package snapshot

import (
	"context"

	"github.com/containerd/containerd/snapshots"
	"github.com/pkg/errors"
)

func (sn *mergeSnapshotter) diffApply(ctx context.Context, dest Mountable, diffs ...Diff) (_ snapshots.Usage, rerr error) {
	return snapshots.Usage{}, errors.New("diffApply not yet supported on windows")
}
