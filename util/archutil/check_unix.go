//go:build linux

package archutil

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

func withChroot(cmd *exec.Cmd, dir string) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Chroot: dir,
	}
}

// Earthly-specific.
func mountProc(target string) (func() error, error) {
	err := unix.Mount("proc", target, "proc", 0, "")
	if err != nil {
		return nil, err
	}
	return func() error {
		return unix.Unmount(target, 0)
	}, nil
}

func check(arch, bin string) (string, error) {
	tmpdir, err := os.MkdirTemp("", "qemu-check")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpdir)
	pp := filepath.Join(tmpdir, "check")

	r, err := gzip.NewReader(bytes.NewReader([]byte(bin)))
	if err != nil {
		return "", err
	}
	defer r.Close()

	f, err := os.OpenFile(pp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		return "", err
	}

	//nolint:gosec // inputs should be static strings
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return "", err
	}
	f.Close()

	cmd := exec.CommandContext(context.TODO(), "/check")
	withChroot(cmd, tmpdir)

	// Earthly-specific.
	// In case rosetta is used, /proc needs to be mounted since rosetta tries to access /proc/self/exe (unavailable in a chroot environment)
	tmpProcDir := filepath.Join(tmpdir, "proc")
	err = os.Mkdir(tmpProcDir, 0700)
	if err != nil {
		return "", err
	}
	umount, err := mountProc(tmpProcDir)
	if err != nil {
		return "", err
	}
	defer umount()

	err = cmd.Run()
	if arch != "amd64" {
		return "", err
	}

	// special handling for amd64. Exit code is 64 + amd64 variant
	if err == nil {
		return "", errors.Errorf("invalid zero exit code")
	}
	exitError := &exec.ExitError{}
	if errors.As(err, &exitError) {
		switch exitError.ExitCode() {
		case 65:
			return "v1", nil
		case 66:
			return "v2", nil
		}
	}
	return "", err
}
