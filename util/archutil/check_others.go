//go:build !linux && !windows && !freebsd
// +build !linux,!windows,!freebsd

package archutil

import (
	"errors"
)

func check(arch, bin string) (string, error) {
	return "", errors.New("arch checking is not supported on this platform")
}
