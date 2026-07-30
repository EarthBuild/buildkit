package ops

import (
	"context"
	"errors"
	"testing"

	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
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

// ---------------------------------------------------------------------------
// A resolution rides the execution lease protocol, so it must survive the round
// trip through publishedResult -- and, more importantly, must REFUSE anything
// that is not a resolution. The coordinator's table holds both kinds; adopting
// an execution result as a resolution would hand the build a garbage digest.

func TestResolutionSurvivesTheRoundTrip(t *testing.T) {
	cfg := []byte(`{"architecture":"amd64","os":"linux"}`)
	dgst := digest.FromString("the resolved manifest")

	ref, got, gotCfg, ok := decodeResolution(encodeResolution("docker.io/library/alpine:3.24.1@sha256:abc", dgst, cfg))

	require.True(t, ok)
	require.Equal(t, "docker.io/library/alpine:3.24.1@sha256:abc", ref)
	require.Equal(t, dgst, got)
	require.Equal(t, cfg, gotCfg)
}

// An execution result has descriptors but none of the resolution annotations.
// Refuse rather than improvise: a wrong digest here picks the wrong base image
// for the whole build, silently.
func TestDecodeResolutionRefusesAnExecutionResult(t *testing.T) {
	exec := &publishedResult{Outputs: [][]ocispecs.Descriptor{{{
		MediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
		Digest:    digest.FromString("a layer"),
		Size:      1234,
	}}}}
	_, _, _, ok := decodeResolution(exec)
	require.False(t, ok, "an execution result must never be adopted as a resolution")
}

// Defensive: malformed shapes must not panic or yield a half-answer.
func TestDecodeResolutionRefusesMalformed(t *testing.T) {
	for name, pr := range map[string]*publishedResult{
		"nil":          nil,
		"no outputs":   {Outputs: nil},
		"empty output": {Outputs: [][]ocispecs.Descriptor{{}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, ok := decodeResolution(pr)
			require.False(t, ok)
		})
	}
}

// ---------------------------------------------------------------------------
// Fail-open is the whole safety story. Coordination is best-effort: with no
// coordinator configured, or one that cannot be reached, resolution must behave
// exactly as unmodified BuildKit does. Get this wrong and every build in the
// fleet dies the moment the driver blinks -- which has already happened once for
// execution leases (see the nits file: losing the coordinator mid-build is
// currently fatal rather than degrading).

func TestCoordinateResolveFallsOpenWithoutCoordinator(t *testing.T) {
	t.Setenv("BUILDKIT_SINGLEFLIGHT_URL", "")

	calls := 0
	ref, dgst, cfg, err := CoordinateResolve(context.Background(),
		"alpine:3.24.1", "linux/amd64", "default",
		func(context.Context) (string, digest.Digest, []byte, error) {
			calls++
			return "alpine:3.24.1@sha256:x", digest.FromString("m"), []byte("cfg"), nil
		})

	require.NoError(t, err)
	require.Equal(t, 1, calls, "with no coordinator the local resolve runs exactly once")
	require.Equal(t, "alpine:3.24.1@sha256:x", ref)
	require.Equal(t, digest.FromString("m"), dgst)
	require.Equal(t, []byte("cfg"), cfg)
}

// A resolve failure is the caller's to handle; coordination must not swallow or
// disguise it.
func TestCoordinateResolvePropagatesFailure(t *testing.T) {
	t.Setenv("BUILDKIT_SINGLEFLIGHT_URL", "")

	want := errors.New("manifest unknown")
	_, _, _, err := CoordinateResolve(context.Background(),
		"alpine:nope", "linux/amd64", "default",
		func(context.Context) (string, digest.Digest, []byte, error) {
			return "", "", nil, want
		})
	require.ErrorIs(t, err, want)
}

// An unreachable coordinator must degrade, not fail. Same contract as claim's
// own error path: build it, exactly as plain BuildKit would.
func TestCoordinateResolveFallsOpenWhenCoordinatorUnreachable(t *testing.T) {
	// Port 1 is reserved and never listening.
	t.Setenv("BUILDKIT_SINGLEFLIGHT_URL", "http://127.0.0.1:1")

	calls := 0
	_, _, _, err := CoordinateResolve(context.Background(),
		"alpine:3.24.1", "linux/amd64", "default",
		func(context.Context) (string, digest.Digest, []byte, error) {
			calls++
			return "alpine@sha256:y", digest.FromString("m"), nil, nil
		})

	require.NoError(t, err)
	require.Equal(t, 1, calls, "an unreachable coordinator must not stop the build")
}

// ---------------------------------------------------------------------------
// Everything that changes the ANSWER must reach the key. Two hazards here, both
// nil-shaped or invisible, both silently wrong if dropped.

// A nil platform must not collapse into a concrete one. ResolveImageConfig is
// called with opt.ImageOpt == nil on some paths; if that produced the same key
// as linux/amd64, an arm64 machine could adopt an amd64 digest.
func TestResolvePlatformKeyDistinguishesNilFromConcrete(t *testing.T) {
	amd64 := &ocispecs.Platform{OS: "linux", Architecture: "amd64"}
	arm64 := &ocispecs.Platform{OS: "linux", Architecture: "arm64"}

	require.NotEqual(t, ResolvePlatformKey(nil), ResolvePlatformKey(amd64))
	require.NotEqual(t, ResolvePlatformKey(amd64), ResolvePlatformKey(arm64))
	require.Equal(t, ResolvePlatformKey(nil), ResolvePlatformKey(nil))
}

// Variant is the same platform expressed two ways; it must not fork the key or
// two machines describing one platform differently would never coordinate.
func TestResolvePlatformKeyIsStableForTheSamePlatform(t *testing.T) {
	a := &ocispecs.Platform{OS: "linux", Architecture: "amd64"}
	b := &ocispecs.Platform{OS: "linux", Architecture: "amd64"}
	require.Equal(t, ResolvePlatformKey(a), ResolvePlatformKey(b))
}

// NoConfig changes what the resolve RETURNS -- the config bytes are omitted. A
// follower that wanted config must not adopt an answer published without it, or
// it gets an empty image config and fails somewhere far away.
func TestResolveVariantSeparatesNoConfig(t *testing.T) {
	require.NotEqual(t,
		ResolveVariant("default", false, false, nil),
		ResolveVariant("default", true, false, nil))
}

// Attestations likewise change the response shape.
func TestResolveVariantSeparatesAttestations(t *testing.T) {
	require.NotEqual(t,
		ResolveVariant("default", false, false, nil),
		ResolveVariant("default", false, true, nil))
	require.NotEqual(t,
		ResolveVariant("default", false, false, nil),
		ResolveVariant("default", false, false, []string{"sbom"}))
}

// Same options, same variant -- or nothing ever coordinates.
func TestResolveVariantIsStable(t *testing.T) {
	require.Equal(t,
		ResolveVariant("forcepull", true, true, []string{"sbom", "provenance"}),
		ResolveVariant("forcepull", true, true, []string{"sbom", "provenance"}))
}

// ---------------------------------------------------------------------------
// Adopting a resolution without serialising the response.
//
// resolveSourceMetadata returns a MetaResponse carrying a mutated protobuf Op
// as well as the image answer, and reconstructing that Op for a follower would
// be guesswork. It does not need reconstructing: a follower that learns
// alpine:3.19 -> sha256:X can simply resolve "alpine:3.19@sha256:X" through the
// ORDINARY local path. Correctness then comes from the code that already works,
// and consistency from the shared digest.

func TestPinnedRefAppendsTheAgreedDigest(t *testing.T) {
	d := digest.FromString("agreed manifest")
	got, ok := PinnedRef("docker-image://docker.io/library/alpine:3.19", d)
	require.True(t, ok)
	require.Equal(t, "docker-image://docker.io/library/alpine:3.19@"+d.String(), got)
}

// An identifier already pinned by digest has nothing to agree about, and must
// not be double-pinned into nonsense.
func TestPinnedRefRefusesAnAlreadyPinnedRef(t *testing.T) {
	d := digest.FromString("x")
	_, ok := PinnedRef("docker-image://docker.io/library/alpine@sha256:"+digest.FromString("y").Encoded(), d)
	require.False(t, ok, "an already-pinned reference needs no coordination")
}

// Only image sources resolve to a manifest digest. git://, http://, local://
// and friends must be left alone.
func TestPinnedRefRefusesNonImageSources(t *testing.T) {
	d := digest.FromString("x")
	for _, id := range []string{
		"git://github.com/foo/bar#main",
		"https://example.com/file.tar",
		"local://context",
		"",
	} {
		_, ok := PinnedRef(id, d)
		require.False(t, ok, "non-image source must not be pinned: %q", id)
	}
}

// An empty digest is not an agreement.
func TestPinnedRefRefusesEmptyDigest(t *testing.T) {
	_, ok := PinnedRef("docker-image://docker.io/library/alpine:3.19", "")
	require.False(t, ok)
}
