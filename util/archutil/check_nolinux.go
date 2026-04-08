//go:build !linux && !windows

package archutil

import (
	"os/exec"
	"syscall"
)

func withChroot(cmd *exec.Cmd, dir string) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Chroot: dir,
	}
}

func check(_, _ string) (string, error) {
	return "", nil
}
