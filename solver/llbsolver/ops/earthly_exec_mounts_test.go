package ops

import (
	"context"
	"testing"

	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver/pb"
	"github.com/stretchr/testify/require"
)

// earthly-specific
//
// TestExecOpEarthlySpecialMountsNotContentHashed pins the mount shape that
// EarthBuild sends for every RUN (decoded from a real Solve request): the
// rootfs, a host-bind debugger binary, a secret mount and two socket mounts
// (earthly_interactive / earthly_save_file). The secret and socket mounts
// carry Input:0 and no selector even though they do not consume the rootfs.
//
// If getMountDeps treats them as real inputs, the "whole source" branch
// marks input 0 as content-hashed and, being last-mount-wins, overrides the
// rootfs mount's own "no content cache on /" decision. Every RUN then gets a
// content-based cache key for its rootfs, so a RUN following a
// `RUN --no-cache` that did not change the filesystem is wrongly reported as
// *cached*.
func TestExecOpEarthlySpecialMountsNotContentHashed(t *testing.T) {
	op := &ExecOp{numInputs: 1, op: &pb.ExecOp{
		Meta: &pb.Meta{Args: []string{"/bin/sh", "-c", "echo hi"}},
		Mounts: []*pb.Mount{
			{Input: 0, Dest: "/", Output: 0},
			{Input: pb.Empty, Selector: "/usr/bin/earth_debugger", Dest: "/usr/bin/earth_debugger", Output: 1, MountType: pb.MountType_HOST_BIND},
			{Input: 0, Dest: "/run/secrets/earthly_debugger_settings", Output: 0, MountType: pb.MountType_SECRET, SecretOpt: &pb.SecretOpt{ID: "earthly_debugger_settings"}},
			{Input: 0, Dest: "/var/run/earthly_interactive", Output: 0, MountType: pb.MountType_SOCKET, SockOpt: &pb.SockOpt{ID: "earthly_interactive"}},
			{Input: 0, Dest: "/var/run/earthly_save", Output: 0, MountType: pb.MountType_SOCKET, SockOpt: &pb.SockOpt{ID: "earthly_save_file"}},
		},
	}}

	deps, err := op.getMountDeps()
	require.NoError(t, err)
	require.Len(t, deps, 1)
	require.False(t, deps[0].ContentBasedHash, "rootfs input must not be content-hashed")
	require.Equal(t, []string{"/"}, deps[0].Selectors, "only the rootfs mount should select from input 0")

	m, ok, err := op.CacheMap(context.Background(), session.NewGroup(t.Name()), 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, m.Deps, 1)
	require.Nil(t, m.Deps[0].ComputeDigestFunc, "rootfs dep must not have a content-based (slow) cache key")
}
