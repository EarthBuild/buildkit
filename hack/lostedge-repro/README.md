# lostedge-repro

Reproduces the solver failure behind `failed to get edge: inconsistent graph state`
([moby/buildkit#2303](https://github.com/moby/buildkit/issues/2303),
[EarthBuild/buildkit#22](https://github.com/EarthBuild/buildkit/pull/22)) against a real
buildkitd, with no cancellation, in about two minutes.

## Run it

Needs Docker and Go 1.21 or newer. `go.mod` pins the BuildKit client at v0.13.2, and `run.sh` writes `go.sum` with `go mod tidy`.

```sh
./run.sh earthbuild/buildkitd:v0.8.18-b0385986 -keep 0
```

`run.sh` starts the image with an empty cache, runs the driver, and removes the container. To
test your own build, pass any image whose entrypoint can run `buildkitd`. To point at a daemon
that is already running, build with `go mod tidy && go build` and run `./lostedge-repro -addr tcp://host:port`.

Exit status is 0 when no build lost an edge, 1 when one did, and 2 on a setup error.

## What it does

Each iteration uses fresh build-context content:

1. **Warm.** Build a chain X (copy the context, then 10 `RUN` steps) and a target PA that reads X
   through a read-only mount, so PA's snapshot does not depend on X's.
2. **Prune.** Delete the results of X's steps from `-keep` onwards. Their cache keys stay in
   the cache store, linked to PA's result.
3. **Race.** Session A builds PA again. That is a cache hit, so A's X edges only compute cache
   keys, and A ends. Session B starts 30ms later and builds a new target on the same X, so X has
   to run.

Each session gives its local context a different name, so A and B load X under different vertex
digests with equal cache keys, and B's X edges merge into A's. When A ends, its `Discard`
deletes the states those merged edges still use, and B fails looking one up:

```
failed to compute cache key: failed to get state for index 0 on sh -c sleep 0.3; echo s1 >> /src/h
```

This is the slow-cache path's lookup (`getState`). The production error comes from the
scheduler's lookup (`getEdge`) of the same deleted state.

A busy builder does steps 1 and 2 without help. GC prunes step results and keeps their keys,
and CI builds of unchanged code hit the cache while builds of changed code run the same shared
steps. On our builder, the session ends that preceded lost edges were ordinary successful ones.

`-noA` runs B without A, as a control.

## Results

`linux/arm64`, 8 iterations of 3 A/B pairs, so 24 B builds per run. The EKS rows ran on 32-vCPU
builders, and the others on Docker Desktop.

| buildkitd | Where | `-keep 3` | `-keep 0` |
|---|---|---|---|
| `51fe8fb9` (`earthbuild/buildkitd:v0.8.18-b0385986`) | local | 15 of 24 lost | 18 of 24 lost |
| `51fe8fb9`, with `-noA` | local | 0 of 24 | |
| `51fe8fb9` | EKS | 9 of 24 lost | 12 of 24 lost |
| `b1191ea90` (this backport: `main` plus moby/buildkit#4347 and #4887) | local | 0 of 24 | 0 of 24 |
| `2c7e20713` ([#22](https://github.com/EarthBuild/buildkit/pull/22) head: the upstream merge, which includes #4887) | local | 0 of 24 | 0 of 24 |
| `740e58a9e` ([#28](https://github.com/EarthBuild/buildkit/pull/28) head) | local | 0 of 24 | 0 of 24 |

[moby/buildkit#4887](https://github.com/moby/buildkit/pull/4887) fixes this failure. With it,
`setEdge` adds the merged-in state's jobs to the target state and its ancestors, so A's
`Discard` no longer deletes them.

This driver does not reach the narrower race that #4887 leaves open, where a `Discard` runs
between the index lookup and `setEdge`. Only the solver unit tests in #22 and #28 reproduce it.
It fails on stock moby/buildkit v0.33.0, and #28's guard fixes it.
