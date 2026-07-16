//go:build !linux && !windows

package archutil

func check(_, _ string) (string, error) {
	return "", nil
}
