package earthlyoutputs

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/docker/distribution/reference"
	"github.com/moby/buildkit/util/imageutil"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// keyLocalRegistrySource is the per-ref metadata key naming the image a local
// registry export was pulled from, as a digest-pinned reference. When set, the
// original index, manifest and config of that image are served from the
// embedded registry alongside the exported image, so that a client can pull
// the pinned reference itself (e.g. via a registry mirror) and have its digest
// verify. Layers are not added: unmodified layers keep their original digests
// and are already served as part of the exported image.
const keyLocalRegistrySource = "export-image-local-registry-source"

// parseSourceRef parses the value of keyLocalRegistrySource.
func parseSourceRef(s string) (reference.Canonical, error) {
	named, err := reference.ParseNormalizedNamed(s)
	if err != nil {
		return nil, errors.Wrapf(err, "parse %s %q", keyLocalRegistrySource, s)
	}
	canonical, ok := named.(reference.Canonical)
	if !ok {
		return nil, errors.Errorf("%s %q is not pinned by digest", keyLocalRegistrySource, s)
	}
	return canonical, nil
}

// sourceContent makes sure the non-layer content (index, the manifest matching
// platform, and its config) of the image ref is in cs, and returns its
// descriptors. Content already in cs is used as is: images pulled by BuildKit
// keep this content leased to their cache records, so the common case needs no
// network access. Anything missing is fetched with resolver.
func sourceContent(ctx context.Context, cs content.Store, resolver remotes.Resolver, ref reference.Canonical, platform ocispecs.Platform) ([]ocispecs.Descriptor, error) {
	root, err := rootDescriptor(ctx, cs, resolver, ref)
	if err != nil {
		return nil, err
	}

	fetcher, err := resolver.Fetcher(ctx, ref.String())
	if err != nil {
		return nil, errors.Wrapf(err, "fetcher for %s", ref)
	}

	matcher := platforms.Only(platform)
	children := images.LimitManifests(images.FilterPlatforms(images.ChildrenHandler(cs), matcher), matcher, 1)

	var (
		mu    sync.Mutex
		descs []ocispecs.Descriptor
	)
	collect := images.HandlerFunc(func(ctx context.Context, desc ocispecs.Descriptor) ([]ocispecs.Descriptor, error) {
		if images.IsLayerType(desc.MediaType) {
			return nil, images.ErrSkipDesc
		}
		mu.Lock()
		descs = append(descs, desc)
		mu.Unlock()
		return nil, nil
	})

	// FetchHandler is a no-op for content that is already in cs.
	if err := images.Dispatch(ctx, images.Handlers(collect, remotes.FetchHandler(cs, fetcher), children), nil, root); err != nil {
		return nil, errors.Wrapf(err, "fetch %s", ref)
	}
	return descs, nil
}

// rootDescriptor returns the descriptor ref's digest points to, from cs if
// present there, otherwise by resolving ref.
func rootDescriptor(ctx context.Context, cs content.Store, resolver remotes.Resolver, ref reference.Canonical) (ocispecs.Descriptor, error) {
	info, err := cs.Info(ctx, ref.Digest())
	switch {
	case err == nil:
		desc := ocispecs.Descriptor{Digest: info.Digest, Size: info.Size}
		dt, err := content.ReadBlob(ctx, cs, desc)
		if err != nil {
			return ocispecs.Descriptor{}, errors.Wrapf(err, "read %s", ref)
		}
		desc.MediaType, err = imageutil.DetectManifestBlobMediaType(dt)
		if err != nil {
			return ocispecs.Descriptor{}, errors.Wrapf(err, "detect media type of %s", ref)
		}
		return desc, nil
	case cerrdefs.IsNotFound(err):
		_, desc, err := resolver.Resolve(ctx, ref.String())
		if err != nil {
			return ocispecs.Descriptor{}, errors.Wrapf(err, "resolve %s", ref)
		}
		return desc, nil
	default:
		return ocispecs.Descriptor{}, errors.Wrapf(err, "stat %s", ref)
	}
}

// imagePlatform returns the platform of the image described by the manifest
// desc, as recorded in its config.
func imagePlatform(ctx context.Context, provider content.Provider, desc ocispecs.Descriptor) (ocispecs.Platform, error) {
	mfst, err := images.Manifest(ctx, provider, desc, platforms.All)
	if err != nil {
		return ocispecs.Platform{}, errors.Wrap(err, "read manifest")
	}
	dt, err := content.ReadBlob(ctx, provider, mfst.Config)
	if err != nil {
		return ocispecs.Platform{}, errors.Wrap(err, "read config")
	}
	var img ocispecs.Image
	if err := json.Unmarshal(dt, &img); err != nil {
		return ocispecs.Platform{}, errors.Wrap(err, "unmarshal config")
	}
	return platforms.Normalize(img.Platform), nil
}
