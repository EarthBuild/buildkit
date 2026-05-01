package moby_buildkit_v1_frontend //nolint:staticcheck

import (
	"fmt"
	"strings"

	"github.com/containerd/typeurl/v2"
	"github.com/moby/buildkit/util/grpcerrors"
)

const (
	// UnknownExitStatus might be returned in (*ExitError).ExitCode via
	// ContainerProcess.Wait.  This can happen if the process never starts
	// or if an error was encountered when obtaining the exit status, it is set to 255.
	//
	// This const is defined here to prevent importing github.com/containerd/containerd
	// and corresponds with https://github.com/containerd/containerd/blob/40b22ef0741028917761d8c5d5d29e0d19038836/task.go#L52-L55
	UnknownExitStatus = 255
)

func init() {
	typeurl.Register((*ExitMessage)(nil), "github.com/moby/buildkit", "gatewayapi.ExitMessage+json")
}

// ExitError will be returned when the container process exits with a non-zero
// exit code.
type ExitError struct {
	ExitCode uint32
	Err      error
}

func (err *ExitError) ToProto() grpcerrors.TypedErrorProto {
	return &ExitMessage{
		Code: err.ExitCode,
	}
}

func (err *ExitError) Error() string {
	var msg string
	if err.Err != nil {
		msg = err.Err.Error()
	} else {
		msg = fmt.Sprintf("exit code: %d", err.ExitCode)
	}

	if detail := exitCodeDetail(err.ExitCode); detail != "" && !strings.Contains(msg, detail) {
		return fmt.Sprintf("%s (%s)", msg, detail)
	}

	return msg
}

func (err *ExitError) Unwrap() error {
	return err.Err
}

func (e *ExitMessage) WrapError(err error) error {
	return &ExitError{
		Err:      err,
		ExitCode: e.Code,
	}
}

func exitCodeDetail(code uint32) string {
	switch code {
	case 126:
		return "exit code 126 conventionally means the command was found but could not be executed; " +
			"check executable permissions, the shebang/interpreter, CPU architecture, noexec mounts, " +
			"and container runtime or security restrictions"
	default:
		return ""
	}
}
