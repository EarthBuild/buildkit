package gitutil

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetGitSSHCommandUsesConfigPath(t *testing.T) {
	cmd := getGitSSHCommand("", GitLogLevelDefault, "")
	require.Equal(t, "ssh -F "+os.DevNull+" -o StrictHostKeyChecking=no", cmd)

	cmd = getGitSSHCommand("/known-hosts", GitLogLevelDefault, "")
	require.Equal(t, "ssh -F "+os.DevNull+" -o UserKnownHostsFile=/known-hosts", cmd)
}

func TestGitCLIConfigEnv(t *testing.T) {
	t.Setenv("HOME", "/tmp/home")
	t.Setenv("XDG_CONFIG_HOME", "/tmp/xdg")
	t.Setenv("USERPROFILE", `C:\Users\tester`)
	t.Setenv("HOMEDRIVE", "C:")
	t.Setenv("HOMEPATH", `\Users\tester`)
	t.Setenv("GIT_CONFIG_GLOBAL", "/tmp/global-gitconfig")
	t.Setenv("GIT_CONFIG_SYSTEM", "/tmp/system-gitconfig")

	// earthly-specific: earthly's fork does NOT isolate git config by default.
	// HOME is always passed through so git can read /root/.gitconfig,
	// and GIT_CONFIG_NOSYSTEM is not set.
	t.Run("isolated by default", func(t *testing.T) {
		var got []string
		cli := NewGitCLI(WithExec(func(ctx context.Context, cmd *exec.Cmd) error {
			got = append([]string(nil), cmd.Env...)
			return nil
		}))
		_, err := cli.Run(context.Background(), "status")
		require.NoError(t, err)
		// earthly-specific: no isolation — HOME is real, no GIT_CONFIG_NOSYSTEM
		require.NotContains(t, got, "GIT_CONFIG_NOSYSTEM=1")
		require.Contains(t, got, "HOME=/tmp/home")
	})

	t.Run("host git config opt-in", func(t *testing.T) {
		var got []string
		cli := NewGitCLI(
			WithHostGitConfig(),
			WithExec(func(ctx context.Context, cmd *exec.Cmd) error {
				got = append([]string(nil), cmd.Env...)
				return nil
			}),
		)
		_, err := cli.Run(context.Background(), "status")
		require.NoError(t, err)
		// earthly-specific: same as default — HOME is always passed through
		require.NotContains(t, got, "GIT_CONFIG_NOSYSTEM=1")
		require.Contains(t, got, "HOME=/tmp/home")
	})
}
