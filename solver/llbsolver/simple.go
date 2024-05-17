package llbsolver

import (
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/llbsolver/ops"
)

// isRunOnce returns a function that can be called to determine if a Vertex
// contains an operation that must be run at least once per build.
func (s *Solver) isRunOnceOp() solver.IsRunOnceFunc {
	return func(v solver.Vertex, b solver.Builder) (bool, error) {
		w, err := s.resolveWorker()
		if err != nil {
			return false, err
		}

		op, err := w.ResolveOp(v, s.Bridge(b), s.sm)
		if err != nil {
			return false, err
		}

		switch op.(type) {
		case *ops.SourceOp:
			return true, nil
		default:
			return false, nil
		}
	}
}
