package ops

import (
	"context"
	"testing"

	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// earthly-specific: verify that special non-input mounts (secret, ssh, tmpfs,
// socket, and host-bind) do not register selectors or enable content-based
// hashing on the rootfs input even when their proto Input field defaults to 0.
func TestExecOpEarthlySpecialMountsNotContentHashed(t *testing.T) {
	t.Parallel()

	op := &ExecOp{
		numInputs: 1,
		op: &pb.ExecOp{
			Meta: &pb.Meta{Args: []string{"/bin/sh", "-c", "echo hi"}},
			Mounts: []*pb.Mount{
				{Input: 0, Dest: "/", Output: 0},
				{Input: pb.Empty, Selector: "/usr/bin/earth_debugger", Dest: "/usr/bin/earth_debugger", Output: 1, MountType: pb.MountType_HOST_BIND},
				{Input: 0, Selector: "/usr/bin/earth_debugger_ro", Dest: "/usr/bin/earth_debugger_ro", Output: pb.SkipOutput, MountType: pb.MountType_HOST_BIND},
				{Input: 0, Dest: "/run/secrets/earthly_debugger_settings", Output: 0, MountType: pb.MountType_SECRET, SecretOpt: &pb.SecretOpt{ID: "earthly_debugger_settings"}},
				{Input: 0, Dest: "/run/buildkit/ssh_agent.0", Output: 0, MountType: pb.MountType_SSH, SSHOpt: &pb.SSHOpt{ID: "default"}},
				{Input: 0, Dest: "/tmp/work", Output: pb.SkipOutput, MountType: pb.MountType_TMPFS, TmpfsOpt: &pb.TmpfsOpt{Size_: 1024}},
				{Input: 0, Dest: "/var/run/earthly_interactive", Output: 0, MountType: pb.MountType_SOCKET, SockOpt: &pb.SockOpt{ID: "earthly_interactive"}},
				{Input: 0, Dest: "/var/run/earthly_save", Output: 0, MountType: pb.MountType_SOCKET, SockOpt: &pb.SockOpt{ID: "earthly_save_file"}},
			},
		},
	}

	deps, err := op.getMountDeps()
	require.NoError(t, err)
	require.Len(t, deps, 1)
	assert.False(t, deps[0].ContentBasedHash, "rootfs input must not be content-hashed")
	assert.Equal(t, []string{"/"}, deps[0].Selectors, "only the rootfs mount should select from input 0")

	m, ok, err := op.CacheMap(context.Background(), session.NewGroup(t.Name()), 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, m.Deps, 1)
	assert.Nil(t, m.Deps[0].ComputeDigestFunc, "rootfs dep must not have a content-based (slow) cache key")
}
