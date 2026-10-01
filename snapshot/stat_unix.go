//go:build linux || freebsd
// +build linux freebsd

package snapshot

import (
	"syscall"
	"golang.org/x/sys/unix"
)

func statAtime(st *syscall.Stat_t) unix.Timespec {
	return unix.Timespec{Sec: st.Atim.Sec, Nsec: st.Atim.Nsec}
}

func statMtime(st *syscall.Stat_t) unix.Timespec {
	return unix.Timespec{Sec: st.Mtim.Sec, Nsec: st.Mtim.Nsec}
}

func statMode(st *syscall.Stat_t) uint32 {
	return uint32(st.Mode)
}

const utimeOmit = unix.UTIME_OMIT
