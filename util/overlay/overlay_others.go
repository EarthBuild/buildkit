//go:build !linux
// +build !linux

package overlay

import (
	"context"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/continuity/fs"
	"github.com/pkg/errors"
)

func GetOverlayLayers(mnt mount.Mount) ([]string, error) {
	return nil, errors.New("overlay layers not supported on this platform")
}

func GetUpperdir(lower, upper []mount.Mount) (string, error) {
	return "", errors.New("upperdir not supported on this platform")
}

func Changes(ctx context.Context, changeFn fs.ChangeFunc, upperdir, upperdirView, base string) error {
	return errors.New("overlay changes not supported on this platform")
}

