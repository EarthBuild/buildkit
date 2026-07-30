package ops

import (
	"context"
	"fmt"
	"sort"
	"strings"

	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
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

// A resolution rides the execution lease protocol, so it has to fit
// publishedResult's shape: one descriptor, carrying the answer.
//
//	Digest      -> the resolved manifest digest (the thing we are agreeing on)
//	Data        -> the image config bytes, so a follower need not fetch them
//	Annotations -> the resolved reference, and a marker identifying this as a
//	               resolution rather than an execution result
//
// The marker is not decoration. Executions and resolutions share one
// coordinator table, and adopting an execution result as a resolution would
// hand the build a layer digest where a manifest digest belongs -- picking the
// wrong base image for everything downstream, silently. Refuse instead.
const (
	resolveAnnotationKind = "rebuck.resolve/kind"
	resolveAnnotationRef  = "rebuck.resolve/ref"
	resolveKindValue      = "image-resolution"
)

func encodeResolution(ref string, dgst digest.Digest, config []byte) *publishedResult {
	return &publishedResult{Outputs: [][]ocispecs.Descriptor{{{
		MediaType: ocispecs.MediaTypeImageManifest,
		Digest:    dgst,
		Size:      int64(len(config)),
		Data:      config,
		Annotations: map[string]string{
			resolveAnnotationKind: resolveKindValue,
			resolveAnnotationRef:  ref,
		},
	}}}}
}

// decodeResolution returns ok=false for anything that is not unmistakably a
// resolution we wrote. Fail-closed: the caller then resolves locally, which is
// slower and correct, rather than adopting an answer it cannot verify.
func decodeResolution(pr *publishedResult) (string, digest.Digest, []byte, bool) {
	if pr == nil || len(pr.Outputs) == 0 || len(pr.Outputs[0]) == 0 {
		return "", "", nil, false
	}
	d := pr.Outputs[0][0]
	if d.Annotations[resolveAnnotationKind] != resolveKindValue {
		return "", "", nil, false
	}
	ref := d.Annotations[resolveAnnotationRef]
	if ref == "" || d.Digest == "" {
		return "", "", nil, false
	}
	return ref, d.Digest, d.Data, true
}

// ResolveFunc is the local resolution CoordinateResolve wraps: ref, manifest
// digest, image config, error -- the shape llbBridge.ResolveImageConfig returns.
type ResolveFunc func(context.Context) (string, digest.Digest, []byte, error)

// CoordinateResolve agrees one answer across the fleet for a single image
// reference: the first machine to ask resolves and publishes, the rest adopt.
//
// Every failure degrades to a local resolve. No coordinator configured, one that
// cannot be reached, a published answer we cannot verify -- all fall through to
// `resolve`, which is exactly what unmodified BuildKit does. That is not
// politeness: a fleet whose builds die when the coordinator blinks is worse than
// one that never coordinated, and single-flight has already been caught making
// coordinator loss fatal once.
func CoordinateResolve(ctx context.Context, ref, platform, resolveMode string, resolve ResolveFunc) (string, digest.Digest, []byte, error) {
	c := coordinatorFromEnv()
	if c == nil {
		return resolve(ctx)
	}

	pr, l, adopted := c.claim(ctx, ResolveLeaseKey(ref, platform, resolveMode))
	if adopted {
		if gotRef, dgst, cfg, ok := decodeResolution(pr); ok {
			return gotRef, dgst, cfg, nil
		}
		// Published, but not something we recognise. Resolve it ourselves rather
		// than adopt an answer we cannot verify -- see decodeResolution.
		return resolve(ctx)
	}
	if l == nil {
		return resolve(ctx)
	}

	// We hold the lease, so followers are waiting on us. Publish either way:
	// publish(nil) gives the lease up, which releases them to resolve for
	// themselves. Dropping it silently would strand them until the TTL expires.
	gotRef, dgst, cfg, err := resolve(ctx)
	if err != nil {
		c.publish(ctx, l, nil)
		return "", "", nil, err
	}
	c.publish(ctx, l, encodeResolution(gotRef, dgst, cfg))
	return gotRef, dgst, cfg, nil
}

// ResolvePlatformKey renders a platform for the lease key.
//
// nil is NOT the same as "the default platform": ResolveImageConfig is called
// with no ImageOpt on some paths, and if that collapsed onto linux/amd64 an
// arm64 machine could adopt an amd64 digest. Give the unspecified case its own
// marker so it only ever coordinates with other unspecified callers.
func ResolvePlatformKey(p *ocispecs.Platform) string {
	if p == nil {
		return "platform-unspecified"
	}
	// Variant matters (v7 vs v8 arm), so it is included; the remaining fields
	// (OSVersion, OSFeatures) do not affect which manifest a tag selects.
	return strings.Join([]string{p.OS, p.Architecture, p.Variant}, "/")
}

// ResolveVariant folds every option that changes the ANSWER into the key.
//
// NoConfig is the subtle one: it omits the image config from the response, so a
// follower that wanted config must never adopt an answer published without it --
// it would get empty config bytes and fail somewhere far downstream. The
// attestation options reshape the response for the same reason.
func ResolveVariant(mode string, noConfig, attestationChain bool, attestations []string) string {
	if mode == "" {
		mode = "default"
	}
	// Sorted: two machines may list attestations in a different order and are
	// still asking for the same thing.
	att := append([]string(nil), attestations...)
	sort.Strings(att)
	return fmt.Sprintf("mode=%s|noconfig=%t|attchain=%t|att=%s",
		mode, noConfig, attestationChain, strings.Join(att, ","))
}

// dockerImageScheme is the SourceOp identifier prefix for an image reference.
// Only image sources resolve to a manifest digest; git, http and local sources
// have their own identity and must be left alone.
const dockerImageScheme = "docker-image://"

// PinnedRef rewrites a SourceOp identifier to name the digest the fleet agreed
// on, so a follower can adopt a resolution WITHOUT anyone serialising the
// protobuf response.
//
// resolveSourceMetadata returns a MetaResponse carrying a mutated Op alongside
// the image answer, and rebuilding that Op on a follower would be guesswork.
// It does not need rebuilding: a follower that learns alpine:3.19 -> sha256:X
// resolves "alpine:3.19@sha256:X" down the ORDINARY path. Correctness comes from
// the code that already works; consistency comes from the shared digest.
//
// The follower still contacts the registry, so this buys agreement rather than
// saved bandwidth -- leasing the PULL is the separate, later half of P4.
//
// Returns ok=false whenever there is nothing to agree: a non-image source, an
// identifier already pinned by digest, or an empty digest.
func PinnedRef(identifier string, dgst digest.Digest) (string, bool) {
	if dgst == "" || !strings.HasPrefix(identifier, dockerImageScheme) {
		return "", false
	}
	// Already pinned: the reference carries its own identity and coordinating it
	// would only risk producing a double-digest that parses as neither.
	if strings.Contains(identifier[len(dockerImageScheme):], "@") {
		return "", false
	}
	return identifier + "@" + dgst.String(), true
}

// ResolveAgreement is the seam resolveSourceMetadata uses to stop every machine
// resolving every tag for itself.
//
// Returns (agreed, publish, coordinated):
//
//	coordinated=false        nobody is coordinating -- resolve exactly as before
//	agreed != ""             the fleet already decided; resolve the PINNED form
//	agreed == "", publish!=nil  we hold the lease: resolve, then publish the
//	                            digest, or publish "" to release the waiters
//
// publish must be called exactly once by a leader, including on failure --
// dropping it silently strands every follower until the lease TTL expires.
//
// Skips anything with nothing to agree about: non-image sources, and references
// already pinned by digest (no round trip spent on a settled question).
func ResolveAgreement(ctx context.Context, identifier, platformKey, variant string) (digest.Digest, func(digest.Digest), bool) {
	// PinnedRef with a throwaway digest answers "is this an unpinned image ref?"
	// without duplicating the parsing rules.
	if _, ok := PinnedRef(identifier, digest.FromString("probe")); !ok {
		return "", nil, false
	}
	c := coordinatorFromEnv()
	if c == nil {
		return "", nil, false
	}

	pr, l, adopted := c.claim(ctx, ResolveLeaseKey(identifier, platformKey, variant))
	if adopted {
		if _, dgst, _, ok := decodeResolution(pr); ok && dgst != "" {
			return dgst, nil, true
		}
		// Published, but not something we can verify. Resolve it ourselves.
		return "", nil, false
	}
	if l == nil {
		return "", nil, false
	}
	return "", func(d digest.Digest) {
		if d == "" {
			c.publish(ctx, l, nil)
			return
		}
		c.publish(ctx, l, encodeResolution(identifier, d, nil))
	}, true
}
