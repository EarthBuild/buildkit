//go:build !linux && !freebsd && !windows
// +build !linux,!freebsd,!windows

package oci

import (
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/oci"
	"github.com/containerd/continuity/fs"
	"github.com/docker/docker/pkg/idtools"
	"github.com/moby/buildkit/solver/pb"
	specs "github.com/opencontainers/runtime-spec/specs-go"

)

func withProcessArgs(args ...string) oci.SpecOpts {
	return oci.WithProcessArgs(args...)
}

func generateMountOpts(resolvConf, hostsFile string) ([]oci.SpecOpts, error) {
	return nil, nil
}

func generateSecurityOpts(mode pb.SecurityMode, apparmorProfile string, selinuxB bool) ([]oci.SpecOpts, error) {
	return nil, nil
}

func generateProcessModeOpts(mode ProcessMode) ([]oci.SpecOpts, error) {
	return nil, nil
}

func generateIDmapOpts(idmap *idtools.IdentityMapping) ([]oci.SpecOpts, error) {
	return nil, nil
}

func generateRlimitOpts(ulimits []*pb.Ulimit) ([]oci.SpecOpts, error) {
	return nil, nil
}

func getTracingSocketMount(socket string) *specs.Mount {
	return nil
}

func getTracingSocket() string {
	return ""
}

func cgroupV2NamespaceSupported() bool {
	return false
}

func sub(m mount.Mount, subPath string) (mount.Mount, func() error, error) {
	src, err := fs.RootPath(m.Source, subPath)
	if err != nil {
		return mount.Mount{}, nil, err
	}
	m.Source = src
	return m, func() error { return nil }, nil
}
