package remotecache

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/labels"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/imageutil"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/worker"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// earthlyInlineCacheItem stores a relation between a simple solver cache key &
// a remote image descriptor. Used for inline caching.
type earthlyInlineCacheItem struct {
	Key        digest.Digest `json:"cacheKey"`
	Descriptor digest.Digest `json:"descriptor"`
}

// EarthlyInlineCacheRemotes produces a map of cache keys to remote sources by
// parsing inline-cache metadata from a remote image's config data.
func EarthlyInlineCacheRemotes(ctx context.Context, provider content.Provider, desc ocispecs.Descriptor, w worker.Worker) (map[digest.Digest]*solver.Remote, error) {
	dt, err := readBlob(ctx, provider, desc)
	if err != nil {
		return nil, err
	}

	manifestType, err := imageutil.DetectManifestBlobMediaType(dt)
	if err != nil {
		return nil, err
	}

	layerDone := progress.OneOff(ctx, fmt.Sprintf("inferred cache manifest type: %s", manifestType))
	layerDone(nil)

	configDesc, err := configDescriptor(dt, manifestType)
	if err != nil {
		return nil, err
	}

	if configDesc.Digest != "" {
		return nil, errors.New("expected empty digest value")
	}

	m := map[digest.Digest][]byte{}

	if err := allDistributionManifests(ctx, provider, dt, m); err != nil {
		return nil, err
	}

	remotes := map[digest.Digest]*solver.Remote{}

	for _, dt := range m {
		var m ocispecs.Manifest

		if err := json.Unmarshal(dt, &m); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal manifest")
		}

		if m.Config.Digest == "" || len(m.Layers) == 0 {
			continue
		}

		p, err := content.ReadBlob(ctx, provider, m.Config)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read blob")
		}

		var img image

		if err := json.Unmarshal(p, &img); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal image")
		}

		if len(img.Rootfs.DiffIDs) != len(m.Layers) {
			bklog.G(ctx).Warnf("invalid image with mismatching manifest and config")
			continue
		}

		if img.EarthlyInlineCache == nil {
			continue
		}

		cacheItems := []earthlyInlineCacheItem{}
		if err := json.Unmarshal(img.EarthlyInlineCache, &cacheItems); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal cache items")
		}

		layers, err := preprocessLayers(img, m)
		if err != nil {
			return nil, err
		}

		found := extractRemotes(provider, cacheItems, layers)
		for key, remote := range found {
			remotes[key] = remote
		}
	}

	return remotes, nil
}

// extractRemotes constructs a list of descriptors--which represent the layer
// chain for the given digest--for each of the items discovered in the inline
// metadata.
func extractRemotes(provider content.Provider, cacheItems []earthlyInlineCacheItem, layers []ocispecs.Descriptor) map[digest.Digest]*solver.Remote {

	remotes := map[digest.Digest]*solver.Remote{}

	for _, cacheItem := range cacheItems {
		descs := []ocispecs.Descriptor{}

		found := false
		for _, layer := range layers {
			descs = append(descs, layer)
			if layer.Digest == cacheItem.Descriptor {
				found = true
				break
			}
		}

		if found {
			remote := &solver.Remote{
				Descriptors: descs,
				Provider:    provider,
			}

			remotes[cacheItem.Key] = remote
		}
	}

	return remotes
}

// preprocessLayers adds custom annotations which are used later when
// reconstructing the ref.
func preprocessLayers(img image, m ocispecs.Manifest) ([]ocispecs.Descriptor, error) {
	createdDates, createdMsg, err := parseCreatedLayerInfo(img)
	if err != nil {
		return nil, err
	}

	ret := []ocispecs.Descriptor{}

	for i, layer := range m.Layers {
		if layer.Annotations == nil {
			layer.Annotations = map[string]string{}
		}

		if createdAt := createdDates[i]; createdAt != "" {
			layer.Annotations["buildkit/createdat"] = createdAt
		}

		if createdBy := createdMsg[i]; createdBy != "" {
			layer.Annotations["buildkit/description"] = createdBy
		}

		layer.Annotations[labels.LabelUncompressed] = img.Rootfs.DiffIDs[i].String()

		ret = append(ret, layer)
	}

	return ret, nil
}

func configDescriptor(dt []byte, manifestType string) (ocispecs.Descriptor, error) {
	var configDesc ocispecs.Descriptor

	switch manifestType {
	case images.MediaTypeDockerSchema2ManifestList, ocispecs.MediaTypeImageIndex:
		var mfst ocispecs.Index
		if err := json.Unmarshal(dt, &mfst); err != nil {
			return ocispecs.Descriptor{}, err
		}

		for _, m := range mfst.Manifests {
			if m.MediaType == v1.CacheConfigMediaTypeV0 {
				configDesc = m
				continue
			}
		}
	case images.MediaTypeDockerSchema2Manifest, ocispecs.MediaTypeImageManifest:
		var mfst ocispecs.Manifest
		if err := json.Unmarshal(dt, &mfst); err != nil {
			return ocispecs.Descriptor{}, err
		}

		if mfst.Config.MediaType == v1.CacheConfigMediaTypeV0 {
			configDesc = mfst.Config
		}
	default:
		return ocispecs.Descriptor{}, errors.Errorf("unsupported or uninferrable manifest type %s", manifestType)
	}

	return configDesc, nil
}

func allDistributionManifests(ctx context.Context, provider content.Provider, dt []byte, m map[digest.Digest][]byte) error {
	mt, err := imageutil.DetectManifestBlobMediaType(dt)
	if err != nil {
		return err
	}

	switch mt {
	case images.MediaTypeDockerSchema2Manifest, ocispecs.MediaTypeImageManifest:
		m[digest.FromBytes(dt)] = dt
	case images.MediaTypeDockerSchema2ManifestList, ocispecs.MediaTypeImageIndex:
		var index ocispecs.Index
		if err := json.Unmarshal(dt, &index); err != nil {
			return errors.WithStack(err)
		}

		for _, d := range index.Manifests {
			if _, ok := m[d.Digest]; ok {
				continue
			}
			p, err := content.ReadBlob(ctx, provider, d)
			if err != nil {
				return errors.WithStack(err)
			}
			if err := allDistributionManifests(ctx, provider, p, m); err != nil {
				return err
			}
		}
	}

	return nil
}
