//go:build !linux
// +build !linux

package snapshot

import (
	"context"

	"github.com/containerd/containerd/leases"
)

func needsUserXAttr(ctx context.Context, sn Snapshotter, lm leases.Manager) (bool, error) {
	return false, nil
}
