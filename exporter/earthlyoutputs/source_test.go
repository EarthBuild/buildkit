package earthlyoutputs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/remotes"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var errNoNetwork = errors.New("network access not allowed in this test")

// offlineResolver fails any attempt to reach a registry.
type offlineResolver struct{}

func (offlineResolver) Resolve(context.Context, string) (string, ocispecs.Descriptor, error) {
	return "", ocispecs.Descriptor{}, errNoNetwork
}

func (offlineResolver) Fetcher(context.Context, string) (remotes.Fetcher, error) {
	return remotes.FetcherFunc(func(context.Context, ocispecs.Descriptor) (io.ReadCloser, error) {
		return nil, errNoNetwork
	}), nil
}

func (offlineResolver) Pusher(context.Context, string) (remotes.Pusher, error) {
	return nil, errNoNetwork
}

func writeJSON(t *testing.T, cs content.Store, mediaType string, v interface{}) ocispecs.Descriptor {
	t.Helper()
	dt, err := json.Marshal(v)
	require.NoError(t, err)
	desc := ocispecs.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(dt), Size: int64(len(dt))}
	require.NoError(t, content.WriteBlob(context.Background(), cs, desc.Digest.String(), bytes.NewReader(dt), desc))
	return desc
}

// writeImage writes a two-platform image index to cs, with only the amd64
// manifest and config present, as BuildKit leaves it after an amd64 pull.
func writeImage(t *testing.T, cs content.Store) (index, amd64Mfst, amd64Cfg ocispecs.Descriptor) {
	t.Helper()
	layer := ocispecs.Descriptor{MediaType: ocispecs.MediaTypeImageLayerGzip, Digest: digest.FromString("layer"), Size: 5}

	amd64 := ocispecs.Platform{OS: "linux", Architecture: "amd64"}
	amd64Cfg = writeJSON(t, cs, ocispecs.MediaTypeImageConfig, ocispecs.Image{Platform: amd64})
	amd64Mfst = writeJSON(t, cs, ocispecs.MediaTypeImageManifest, ocispecs.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispecs.MediaTypeImageManifest,
		Config:    amd64Cfg,
		Layers:    []ocispecs.Descriptor{layer},
	})
	amd64Mfst.Platform = &amd64

	// Never written to cs: fetching it would need the network.
	arm64Mfst := ocispecs.Descriptor{
		MediaType: ocispecs.MediaTypeImageManifest,
		Digest:    digest.FromString("arm64 manifest"),
		Size:      14,
		Platform:  &ocispecs.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"},
	}

	index = writeJSON(t, cs, ocispecs.MediaTypeImageIndex, ocispecs.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispecs.MediaTypeImageIndex,
		Manifests: []ocispecs.Descriptor{arm64Mfst, amd64Mfst},
	})
	return index, amd64Mfst, amd64Cfg
}

func TestSourceContentFromContentStore(t *testing.T) {
	ctx := context.Background()
	cs, err := local.NewStore(t.TempDir())
	require.NoError(t, err)
	index, mfst, cfg := writeImage(t, cs)

	ref, err := parseSourceRef("alpine@" + index.Digest.String())
	require.NoError(t, err)

	descs, err := sourceContent(ctx, cs, offlineResolver{}, ref, ocispecs.Platform{OS: "linux", Architecture: "amd64"})
	require.NoError(t, err)

	var got []digest.Digest
	for _, desc := range descs {
		got = append(got, desc.Digest)
	}
	require.ElementsMatch(t, []digest.Digest{index.Digest, mfst.Digest, cfg.Digest}, got)
}

func TestSourceContentMissingRootResolves(t *testing.T) {
	ctx := context.Background()
	cs, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	ref, err := parseSourceRef("alpine@" + digest.FromString("not in the store").String())
	require.NoError(t, err)

	_, err = sourceContent(ctx, cs, offlineResolver{}, ref, ocispecs.Platform{OS: "linux", Architecture: "amd64"})
	require.ErrorIs(t, err, errNoNetwork)
}

func TestImagePlatform(t *testing.T) {
	ctx := context.Background()
	cs, err := local.NewStore(t.TempDir())
	require.NoError(t, err)
	_, mfst, _ := writeImage(t, cs)

	got, err := imagePlatform(ctx, cs, mfst)
	require.NoError(t, err)
	require.Equal(t, ocispecs.Platform{OS: "linux", Architecture: "amd64"}, got)
}

func TestParseSourceRef(t *testing.T) {
	dgst := digest.FromString("x").String()

	ref, err := parseSourceRef("alpine:3.24.2@" + dgst)
	require.NoError(t, err)
	require.Equal(t, "docker.io/library/alpine:3.24.2@"+dgst, ref.String())

	_, err = parseSourceRef("alpine:3.24.2")
	require.ErrorContains(t, err, "not pinned by digest")
}
