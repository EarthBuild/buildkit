package registry

import (
	"context"
	"strconv"

	"github.com/containerd/containerd/remotes/docker"
	"github.com/moby/buildkit/cache/remotecache"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/contentutil"
	"github.com/moby/buildkit/util/resolver"
	"github.com/moby/buildkit/util/resolver/limited"
	"github.com/moby/buildkit/worker"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

// EarthlyInlineCacheRemotes fetches a group of remote sources based on values
// discovered in a remote image's inline-cache metadata field.
func EarthlyInlineCacheRemotes(ctx context.Context, sm *session.Manager, w worker.Worker, hosts docker.RegistryHosts, g session.Group, attrs map[string]string) (map[digest.Digest]*solver.Remote, error) {
	ref, err := canonicalizeRef(attrs[attrRef])
	if err != nil {
		return nil, err
	}

	refString := ref.String()

	insecure := false
	if v, ok := attrs[attrInsecure]; ok {
		val, err := strconv.ParseBool(v)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s", attrInsecure)
		}
		insecure = val
	}

	scope, hosts := registryConfig(hosts, ref, "pull", insecure)
	remote := resolver.DefaultPool.GetResolver(hosts, refString, scope, sm, g)

	xref, desc, err := remote.Resolve(ctx, refString)
	if err != nil {
		return nil, err
	}

	fetcher, err := remote.Fetcher(ctx, xref)
	if err != nil {
		return nil, err
	}

	src := &withDistributionSourceLabel{
		Provider: contentutil.FromFetcher(limited.Default.WrapFetcher(fetcher, refString)),
		ref:      refString,
		source:   w.ContentStore(),
	}

	return remotecache.EarthlyInlineCacheRemotes(ctx, src, desc, w)
}
