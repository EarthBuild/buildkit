//go:build linux
// +build linux

package snapshot

import (
	"context"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots/overlay/overlayutils"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/pkg/errors"
)

func needsUserXAttr(ctx context.Context, sn Snapshotter, lm leases.Manager) (bool, error) {
	key := identity.NewID()

	ctx, done, err := leaseutil.WithLease(ctx, lm, leaseutil.MakeTemporary)
	if err != nil {
		return false, errors.Wrap(err, "failed to create lease for checking user xattr")
	}
	defer done(context.TODO())

	err = sn.Prepare(ctx, key, "")
	if err != nil {
		return false, err
	}
	mntable, err := sn.Mounts(ctx, key)
	if err != nil {
		return false, err
	}
	mnts, unmount, err := mntable.Mount()
	if err != nil {
		return false, err
	}
	defer unmount()

	var userxattr bool
	if err := mount.WithTempMount(ctx, mnts, func(root string) error {
		var err error
		userxattr, err = overlayutils.NeedsUserXAttr(root)
		return err
	}); err != nil {
		return false, err
	}
	return userxattr, nil
}
