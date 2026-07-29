package solver

import (
	"context"
	"fmt"
	"sort"
	"strings"

	digest "github.com/opencontainers/go-digest"

	"github.com/moby/buildkit/version"
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
// So we hash exactly that chain.
//
// But NOT every key in that chain carries identity, and this is the trap that
// made the first version of this function silently useless. A local source's
// cache key is "session:<name>:<hash(SessionID,...)>"
// (source/local/source.go), and SourceOp.CacheMap then stamps the digest with a
// "random:" prefix (llbsolver/ops/source.go:97) — buildkit's marker for "never
// match this by identity". It is random PER RUN, so anything downstream of a
// COPY got a lease key no other machine could ever compute. Measured: two
// earthbuild instances building one target produced four keys and zero merges.
//
// BuildKit does not care, because it identifies local sources by CONTENT
// instead, via the slow cache: commitOptions (edge.go) puts that content key in
// the same dep slot, right beside the random one. So we drop the random keys and
// keep the content ones.
//
// If a dep has ONLY a random key, we have no cross-machine identity for it and
// return "" — no lease, build locally. That is the safe direction: a key we
// cannot compute identically elsewhere is at best useless, and a key that
// pretended otherwise would hand a follower the wrong layer.
//
// Dependencies within a slot are sorted, so two machines that discovered the
// same deps in a different order still agree. Memoized: a diamond-shaped DAG
// would otherwise be walked exponentially.
// leaseKeyCohort partitions the keyspace by BUILDER identity.
//
// Everything else in the key describes the build; nothing described the builder,
// so two daemons at different commits computed identical keys and would adopt
// each other's results. Silent, and unsound the moment LLB semantics differ
// between them: a layer produced under one daemon's rules handed to another that
// does not share them is exactly the "grid is multi-valued" failure principle 1
// exists to prevent.
//
// Partitioning is the conservative half of the fix — cohorts simply never merge
// across. It costs sharing during a rolling upgrade, which is the right trade:
// a slower build is recoverable, a wrong one is not. Letting compatible versions
// still share needs a compatibility policy for LLB semantics, and would be
// unsound without this underneath it.
//
// Overridable so tests can vary it; there is no reason to set it in production.
var leaseKeyCohort = defaultLeaseKeyCohort()

// Revision is the precise thing (a commit); Version is the fallback for builds
// whose ldflags were never set — which includes any plain `go build ./cmd/...`,
// so it is the common case in rigs rather than an edge case.
func defaultLeaseKeyCohort() string {
	if version.Revision != "" {
		return "buildkit:" + version.Revision
	}
	return "buildkit:" + version.Version
}

func LeaseKey(k *CacheKey) digest.Digest {
	s, ok := leaseKeyString(k)
	if !ok {
		return ""
	}
	return digest.FromString(s)
}

// leaseKeyString is LeaseKey's pre-hash input. Split out because when two
// machines disagree, the DIGEST tells you nothing — this string tells you which
// component diverged.
func leaseKeyString(k *CacheKey) (string, bool) {
	memo := map[*CacheKey]string{}
	noIdentity := false
	var walk func(k *CacheKey) string
	walk = func(k *CacheKey) string {
		if s, ok := memo[k]; ok {
			return s
		}
		// Placeholder before recursing: a malformed cyclic graph must not hang.
		memo[k] = ""

		if isRandomDigest(k.Digest()) {
			noIdentity = true
			return ""
		}

		var b strings.Builder
		fmt.Fprintf(&b, "k:%s@%d", k.Digest(), k.Output())
		for i, deps := range k.Deps() {
			// commitOptions puts up to two KINDS of key in one dep slot:
			//   fast — the dep's own cache keys, output >= 0. What buildkit
			//          matches on. Content-addressed for an image; "random:" for a
			//          local source, where it is per-run noise.
			//   slow — the contenthash of the dep's RESULT, output -1, no selector.
			//          Only present when ContentBasedHash is set.
			var fast, slow []CacheKeyWithSelector
			for _, d := range deps {
				switch {
				case d.CacheKey.CacheKey.Output() == slowCacheOutput:
					slow = append(slow, d)
				case isRandomDigest(d.CacheKey.CacheKey.Digest()):
					// per-run noise; the slow key is this dep's only identity
				default:
					fast = append(fast, d)
				}
			}
			// The slow key is a FALLBACK, not an extra ingredient. If the fast key
			// identifies the dep, buildkit caches on it ALONE — `RUN apt-get update`
			// on a fixed base is a cache hit on a second run even though apt fetched
			// different bytes, because the key is f(base, command) and never hashes
			// the output. Mixing the contenthash in as well would poison a key that
			// already agrees across machines, over bytes buildkit itself ignores.
			use := fast
			if len(use) == 0 {
				use = slow
			}
			if len(deps) > 0 && len(use) == 0 {
				noIdentity = true
				return ""
			}
			parts := make([]string, 0, len(use))
			for _, d := range use {
				parts = append(parts, d.Selector.String()+"="+walk(d.CacheKey.CacheKey))
			}
			sort.Strings(parts)
			fmt.Fprintf(&b, "|d%d:%s", i, strings.Join(parts, ","))
		}
		s := b.String()
		memo[k] = s
		return s
	}
	out := walk(k)
	if noIdentity {
		return "", false
	}
	// Prefix rather than hash separately: LeaseKeyDebugString is the only thing
	// that says WHICH component diverged, and "merged nothing" from a version
	// split is otherwise indistinguishable from an unreachable coordinator.
	return leaseKeyCohort + "|" + out, true
}

// LeaseKeyDebugString exposes the pre-hash input for diagnosing why two machines
// computed different keys for work that ought to be identical.
func LeaseKeyDebugString(k *CacheKey) string {
	s, _ := leaseKeyString(k)
	return s
}

// slowCacheOutput is the output index commitOptions gives the content-based
// (slow) cache key: `NewCacheKey(dgst, "", -1)` in edge.go. Real outputs are
// >= 0, so this identifies the slow key without guessing from the selector.
const slowCacheOutput = -1

// isRandomDigest reports buildkit's "never match this by identity" marker,
// stamped on any source whose cache key is session-scoped — i.e. every local
// source. See llbsolver/ops/source.go:97 and cachemanager.go:453.
func isRandomDigest(d digest.Digest) bool {
	return strings.HasPrefix(string(d), "random:")
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
