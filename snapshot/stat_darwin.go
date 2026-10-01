//go:build darwin
// +build darwin

package snapshot

import (
	"syscall"
	"golang.org/x/sys/unix"
)

func statAtime(st *syscall.Stat_t) unix.Timespec {
	return unix.Timespec{Sec: st.Atimespec.Sec, Nsec: st.Atimespec.Nsec}
}

func statMtime(st *syscall.Stat_t) unix.Timespec {
	return unix.Timespec{Sec: st.Mtimespec.Sec, Nsec: st.Mtimespec.Nsec}
}

func statMode(st *syscall.Stat_t) uint32 {
	return uint32(st.Mode)
}

const utimeOmit = -2 // UTIME_OMIT on most systems, missing in Darwin's unix package but supported by kernel
