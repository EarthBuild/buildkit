package ops

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Coordinating image RESOLUTION, not execution.
//
// SingleFlightKey is consumed in exactly one place -- ExecOp.Exec -- so source
// ops are never coordinated: every machine resolves and pulls every base image
// independently. That is where earthbuild's duplicated I/O actually lives (~18
// registry pulls per CI job, and ~480 inner buildkitds each pulling on their
// own -- earthbuild already pays for the workaround in Docker Hub mirror creds).
//
// It is also a CORRECTNESS gap. Docker Official Images are republished under the
// same tag for CVE rebuilds, so a tag that moves mid-run leaves half a fleet on
// the old base and half on the new: a build no single machine would produce,
// which is principle 1 violated. Worse, the differing digests then produce
// differing lease keys, so the merge rate collapses to zero and looks exactly
// like a broken coordinator.

// Two machines resolving the same reference must agree, or there is nothing to
// coordinate.
func TestResolveLeaseKeyAgreesOnTheSameReference(t *testing.T) {
	a := ResolveLeaseKey("docker.io/library/alpine:3.24.1", "linux/amd64", "default")
	b := ResolveLeaseKey("docker.io/library/alpine:3.24.1", "linux/amd64", "default")
	require.Equal(t, a, b)
	require.NotEmpty(t, a)
}

// Different references are different work.
func TestResolveLeaseKeyDistinguishesReferences(t *testing.T) {
	require.NotEqual(t,
		ResolveLeaseKey("docker.io/library/alpine:3.24.1", "linux/amd64", "default"),
		ResolveLeaseKey("docker.io/library/alpine:3.24", "linux/amd64", "default"))
}

// THE one that would be silently wrong. A multi-arch tag resolves to a DIFFERENT
// digest per platform, so an arm64 machine adopting an amd64 machine's answer
// gets an image it cannot run -- and the failure would surface far downstream as
// an exec error, not as a resolution bug.
func TestResolveLeaseKeyDistinguishesPlatform(t *testing.T) {
	require.NotEqual(t,
		ResolveLeaseKey("docker.io/library/alpine:3.24.1", "linux/amd64", "default"),
		ResolveLeaseKey("docker.io/library/alpine:3.24.1", "linux/arm64", "default"))
}

// Resolve mode changes the answer (pull vs prefer-local), so it changes the key.
func TestResolveLeaseKeyDistinguishesResolveMode(t *testing.T) {
	require.NotEqual(t,
		ResolveLeaseKey("docker.io/library/alpine:3.24.1", "linux/amd64", "default"),
		ResolveLeaseKey("docker.io/library/alpine:3.24.1", "linux/amd64", "forcepull"))
}

// Resolution leases share a coordinator with execution leases, so they must live
// in a distinct namespace. A collision would hand an ExecOp waiter a resolution
// result, or vice versa -- both nonsense, both silent.
func TestResolveLeaseKeyIsNamespacedAwayFromExecutionLeases(t *testing.T) {
	k := ResolveLeaseKey("docker.io/library/alpine:3.24.1", "linux/amd64", "default")
	require.Contains(t, k, "resolve-",
		"resolution keys must be distinguishable from execution keys in the same table")
}

// Deliberately NOT partitioned by buildkit version, unlike execution leases.
// Which digest a tag points at is a fact about the registry, not about the
// daemon that asked -- two buildkit versions resolving the same tag SHOULD agree,
// and partitioning here would forfeit sharing for no safety gain.
func TestResolveLeaseKeyIsIndependentOfBuilderVersion(t *testing.T) {
	require.NotContains(t, ResolveLeaseKey("alpine:3.24.1", "linux/amd64", "default"), "buildkit:",
		"a tag's digest is a registry fact, not a builder fact")
}
