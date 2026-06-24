//go:build !linux && !freebsd && !windows
// +build !linux,!freebsd,!windows

package snapshot

import (
	"github.com/pkg/errors"
)

func (lm *localMounter) Mount() (string, error) {
	return "", errors.New("local mounting is not supported on this platform")
}

func (lm *localMounter) Unmount() error {
	return nil
}
