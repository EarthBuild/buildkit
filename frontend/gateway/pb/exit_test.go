package moby_buildkit_v1_frontend

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestExitErrorExplainsExitCode126(t *testing.T) {
	t.Parallel()

	err := &ExitError{ExitCode: 126}

	require.Contains(t, err.Error(), "exit code: 126")
	require.Contains(t, err.Error(), "command was found but could not be executed")
	require.Contains(t, err.Error(), "executable permissions")
	require.Contains(t, err.Error(), "shebang/interpreter")
}

func TestExitErrorExplainsExitCode126WithWrappedError(t *testing.T) {
	t.Parallel()

	err := &ExitError{ExitCode: 126, Err: errors.New("exit code: 126")}

	require.Contains(t, err.Error(), "exit code: 126")
	require.Contains(t, err.Error(), "command was found but could not be executed")
}

func TestExitErrorDoesNotDuplicateExitCodeDetail(t *testing.T) {
	t.Parallel()

	err := &ExitError{
		ExitCode: 126,
		Err: errors.New(
			"exit code: 126 (exit code 126 conventionally means the command was found but could not be executed; " +
				"check executable permissions, the shebang/interpreter, CPU architecture, noexec mounts, " +
				"and container runtime or security restrictions)"),
	}

	require.Equal(t, err.Err.Error(), err.Error())
}

func TestExitErrorLeavesOrdinaryExitCodeUnchanged(t *testing.T) {
	t.Parallel()

	err := &ExitError{ExitCode: 1}

	require.Equal(t, "exit code: 1", err.Error())
}
