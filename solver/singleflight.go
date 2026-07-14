package solver

import (
	"context"
	"fmt"
	"sort"
	"strings"

	digest "github.com/opencontainers/go-digest"
)

// Cross-machine single-flight.
//
// A single buildkitd already single-flights internally: its scheduler
// edge-merges concurrent identical vertices, so two jobs needing the same thing
// attach to one execution. That is why one big daemon (an Earthly satellite)
// feels good. The moment you run N independent daemons — a CI matrix, a fleet of
// ephemeral runners — each has its own scheduler and the property is lost: two
// machines cheerfully compile the same crate at the same time.
//
// A Coordinator restores it. Before executing a vertex, a daemon claims its
// cache key with an external coordinator. The first claimant builds; the rest
// block, and are handed the leader's result as an OCI descriptor chain, which
// they materialize locally via worker.FromRemote — the same path every remote
// cache hit already takes.
//
// The key MUST be the content-addressed cache key (edge.commitOptions), not the
// vertex digest and not anything derived from solver.Result.ID():
//
//   - The vertex digest ignores content. `COPY src/ .` has the same op digest
//     whatever the sources say, so leasing on it would coalesce two builds that
//     must differ and hand back the wrong layer.
//   - solver.Result.ID() is "workerID::refID", and refID is a locally-assigned
//     UUID. Two machines give logically-identical snapshots different IDs, so a
//     key built from them would never match across machines.
//
// The cache key is only in scope in edge.execOp, so it is computed there and
// carried to the op through the context (see WithSingleFlightKey).
type Coordinator interface {
	// Claim the key. The leader gets (nil, false) and must build, then call
	// Publish. A follower blocks until the leader publishes, and gets
	// (remote, true) — the descriptors of what the leader produced.
	//
	// A coordinator that cannot answer (leader died, coordinator unreachable)
	// must return (nil, false) so the caller builds it themselves. Degrading to
	// duplicate work is always correct; blocking forever is not.
	Claim(ctx context.Context, key digest.Digest) (remote *Remote, follower bool, err error)

	// Publish the leader's result and wake every follower. Best-effort: a failed
	// publish costs the followers a rebuild, never correctness.
	Publish(ctx context.Context, key digest.Digest, remote *Remote) error

	// Abandon the claim without a result — we failed, or were cancelled. The
	// followers must be freed to re-elect rather than left waiting on a result
	// nobody is computing. A hang is a worse bug than the duplicate work this
	// whole mechanism exists to prevent.
	Abandon(ctx context.Context, key digest.Digest)
}

// LeaseKey derives a content-addressed, machine-stable identity for a cache key.
//
// BuildKit does not have one to hand, which is the trap in this whole feature:
//
//   - CacheKey.Digest() is the cacheMap digest — the op's own definition. It
//     ignores input content entirely.
//   - CacheKey.ID and cacheManager.getID() are LOCAL STORAGE ids. getIDFromDeps
//     (cachemanager.go:410) even falls back to identity.NewID() — a random value
//     — when it finds no local link. Two machines will never agree on one.
//   - solver.Result.ID() is "workerID::refID" with a locally-assigned UUID refID.
//
// Any of those would produce a lease that either never coalesces (harmless but
// pointless) or coalesces builds that must differ (silently wrong output).
//
// The one identity that IS content-addressed and machine-stable is the shape the
// exported cache manifest already uses: a record is rootKey(digest, output) and
// its dependencies are links (see exporter.ExportTo, exporter.go:127 and :234).
// So we hash exactly that chain. Content enters through the leaves — a local
// source's cacheMap digest is its content checksum — which is why `COPY src/`
// over different sources yields a different key here even though the op digest
// is identical.
//
// Dependencies within a slot are sorted, so two machines that discovered the
// same deps in a different order still agree. Memoized: a diamond-shaped DAG
// would otherwise be walked exponentially.
func LeaseKey(k *CacheKey) digest.Digest {
	memo := map[*CacheKey]string{}
	var walk func(k *CacheKey) string
	walk = func(k *CacheKey) string {
		if s, ok := memo[k]; ok {
			return s
		}
		// Placeholder before recursing: a malformed cyclic graph must not hang.
		memo[k] = ""

		var b strings.Builder
		fmt.Fprintf(&b, "k:%s@%d", k.Digest(), k.Output())
		for i, deps := range k.Deps() {
			parts := make([]string, 0, len(deps))
			for _, d := range deps {
				parts = append(parts, d.Selector.String()+"="+walk(d.CacheKey.CacheKey))
			}
			sort.Strings(parts)
			fmt.Fprintf(&b, "|d%d:%s", i, strings.Join(parts, ","))
		}
		s := b.String()
		memo[k] = s
		return s
	}
	return digest.FromString(walk(k))
}

type singleFlightKeyT struct{}

// WithSingleFlightKey carries the content-addressed cache key from edge.execOp
// (the only place it is in scope) down to the op that will execute.
func WithSingleFlightKey(ctx context.Context, key digest.Digest) context.Context {
	return context.WithValue(ctx, singleFlightKeyT{}, key)
}

// SingleFlightKey returns the key set by WithSingleFlightKey, or "" if this
// build is not coordinated.
func SingleFlightKey(ctx context.Context) digest.Digest {
	k, _ := ctx.Value(singleFlightKeyT{}).(digest.Digest)
	return k
}
