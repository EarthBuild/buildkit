package ops

import (
	"fmt"

	digest "github.com/opencontainers/go-digest"
)

// Cross-machine coordination of image RESOLUTION.
//
// Execution leases (singleflight.go) make the fleet build a vertex once. They
// say nothing about what it built ON: SingleFlightKey is consumed only in
// ExecOp.Exec, so every machine resolves and pulls every base image on its own.
//
// That is a consistency hole before it is a performance one. Docker Official
// Images are republished under the SAME tag for CVE rebuilds, so a tag that
// moves mid-run leaves part of the fleet on the old base and part on the new --
// a build no single machine would have produced, which is exactly what
// dist-buildkit-principles.md section 1 forbids. It degrades badly too: the
// differing digests yield differing execution lease keys, so the merge rate
// collapses to zero and is indistinguishable from a broken coordinator.
//
// Leasing the resolution makes the fleet single-valued about its bases: the
// first machine to resolve a reference publishes the digest and the rest adopt
// it, whether or not the tag moves underneath them.
//
// Reuses the execution lease protocol deliberately -- claim/publish is a plain
// key/value exchange and needs no coordinator change. Unlike an execution lease
// there are no blobs to push: the payload is one digest.

// resolveKeyPrefix namespaces resolution leases away from execution leases,
// which share the coordinator's table. A collision would hand an ExecOp waiter a
// resolution answer, or the reverse -- both nonsense, and both silent.
const resolveKeyPrefix = "resolve-"

// ResolveLeaseKey derives the coordination key for resolving one image
// reference.
//
// Platform is part of the identity and must not be dropped: a multi-arch tag
// resolves to a different digest per platform, so an arm64 machine adopting an
// amd64 answer would get an image it cannot run, and the failure would surface
// far downstream as an exec error rather than as a resolution bug.
//
// Deliberately NOT partitioned by buildkit version, unlike LeaseKey. Which
// digest a tag points at is a fact about the registry, not about the daemon
// doing the asking, so two versions should agree; partitioning would forfeit
// sharing for no safety gain.
func ResolveLeaseKey(ref, platform, resolveMode string) string {
	// Hash the components rather than concatenating them: a reference can carry
	// slashes and colons, and the key travels in a URL path.
	d := digest.FromString(fmt.Sprintf("ref=%s|platform=%s|mode=%s", ref, platform, resolveMode))
	return resolveKeyPrefix + d.Encoded()
}
