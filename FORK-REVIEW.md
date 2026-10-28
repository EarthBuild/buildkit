# EarthBuild buildkit fork review — `merge-in-buildkit-all-at-once` vs upstream `fa19659f`

Scope: all non-vendor fork changes (~125 files, ~5,800 insertions), cross-referenced against
usage in `../earthly` (pinned to `earthbuild/buildkit@51fe8fb`) and against upstream
`origin/master` for replaceability.

---

## 1. Changes that can be DROPPED or replaced with upstream

These reduce the fork delta with no feature loss (some fix active bugs by their removal):

| # | Change | Why droppable |
|---|--------|---------------|
| D1 | `solver/inconsistent_graph_state_error_tracker.go` + `dgstTracker` hooks in `solver/jobs.go` + `helpMe` string accumulation in `solver/scheduler.go` `dispatch()` | Debug instrumentation for scheduler edge-merge corruption that upstream has since fixed (`4e59c55b8` "always edge merge in one direction", `1b3daaa39` "prevent edge merge to inactive states", `ffdbd863b`, `100d3cb6b`) and superseded with its own diagnostics (`debugSchedulerInconsistentGraphState()`, `BUILDKIT_SCHEDULER_DEBUG`). The fork version also has **data races** (unlocked `dgstTrackerInst.String()` at scheduler.go:367; `%+v` reflection over live op state on every dispatch) and measurable hot-path allocation cost. |
| D2 | `session/configurabletimeout.go` + `ManagerHealthCfg` + `[health]` config + `util/appdefaults` health constants | The fork's original low-bandwidth healthcheck fix **was upstreamed** — upstream `monitorHealth` is now adaptive (`max(30s, 1.5×last)`), failure-tolerant, hang-safe, and supports per-session timeout override via header. The fork replacement is a tolerance *regression*: defaults (1s/10s/allowedFailures=1) kill a session on the **first** healthcheck >10s (off-by-one: "1 allowed failure" allows zero). If operator config is still wanted, map `[health]` onto upstream's `healthCheckConfig`. |
| D3 | `util/resolver/authorizer.go:337-343` — swallow `context.Canceled` in `fetchToken` | Obsolete and now **harmful**. Upstream no longer caches errored token fetches, and flightcontrol retries canceled calls. The patch converts a canceled fetch into a fake success with an empty token → concurrent pulls sharing the flightcontrol key send `Authorization: ""` → spurious 401s whenever any sharing build is canceled. |
| D4 | `session/auth/authprovider/podman.go` (`NewPodmanAuthProvider`) | Zero callers in fork or earthly. Earthly ships its own podman provider (`earthly/util/llbutil/authprovider/podman.go`). |
| D5 | `filesync.NewFSSyncTargetDir` | No callers anywhere. |
| D6 | `grpc_retry` dial options in `client/client.go:52-53` + `go-grpc-middleware` server chaining in `cmd/buildkitd/main.go` | The retry options are **dead code** (no retry interceptor is ever installed). Server chaining can use grpc-go native `grpc.ChainUnaryInterceptor`/`ChainStreamInterceptor` (what upstream uses). Together this drops the `go-grpc-middleware` dependency. |
| D7 | `util/entitlements/security/security_linux.go` device-cgroup change | Semantically identical to upstream (unset `Type` = wildcard "a" = upstream's c+b pair), only reachable under `security.insecure`. Revert to upstream text purely to shrink the diff. |
| D8 | `util/bklog/log.go` trace-ID gate (`EnableLogWithTraceID`) | Zero callers → traceID/spanID silently dropped from all daemon logs, losing upstream diagnostics. Either delete the gate (restore upstream behavior) or actually call it. Keep the version/revision log fields. |
| D9 | `os.Setenv("GRPC_ENFORCE_ALPN_ENABLED", "false")` in `cmd/buildkitd/main.go:124-127` | Ineffective: vendored grpc reads the env at package init, before `main()` runs. Set it in the container env/entrypoint instead (earthly's CLI already does this correctly via a dedicated init package). |
| D10 | Satellite-era, unused-in-OSS surface (needs a product decision): `Reserve` RPC + `client/reserve.go`, `WithDefaultGRPCDialer`, `WithAdditionalMetadataContext`, OCI hooks (`executor/oci/hooks.go` + `[[hook]]` config), `BUILDKIT_EXEC_TIMEOUT` | None referenced by OSS earthly. If EarthBuild no longer operates hosted satellites, all are droppable. `WithDefaultGRPCDialer` is also broken (see B14). |

### Upstream-PR candidates (shrink the delta over time)
- `util/sshutil/keyscan.go` — `[host]:port` known_hosts formatting fix (upstream still broken for non-22 ports).
- `exporter/containerimage/writer.go` — error instead of silent nil-ref fall-through (in-code TODO already says to upstream it).
- `util/urlutil/redact.go` `RedactAllCredentials` + gitutil stderr redaction — genuine credential-leak fix.

### Replace implementation, keep feature (longer-term)
- **Embedded registry**: the vendored `docker/distribution v2.8.3+incompatible` is archived/unmaintained and drags in `libtrust`, `redigo`, `gorilla/*`, `yaml.v2`. The fork only needs 4 read-only endpoints (GET/HEAD manifest, GET/HEAD blob); `eodriver` already fakes distribution's storage paths just to feed them back into distribution's handler stack. A ~200-line custom `net/http` handler over `MultiMultiProvider` would drop the entire vendored tree and its CVE surface.
- **Registry gRPC proxy** (`api/services/registry/server.go`): the 50 ms read-idle heuristic for end-of-response is unsound (see B2). Redesign with HTTP-framing-aware copying or one Proxy stream per request.

---

## 2. Changes that must stay (no upstream equivalent)

All verified as actively used by earthly (usage sites in parentheses):

- **LOCALLY execution** — `session/localhost/**`, magic-string interception in `solver/llbsolver/ops/exec.go` (`earthfile2llb/converter.go:638,1304,2815`). Core feature; upstream has no client-side exec.
- **earthlyoutputs exporter** — `exporter/earthlyoutputs/**` (`builder/solver.go:154`, `builder/image_solver.go:350`). Upstream now runs multiple exporters per solve, but nothing partitions one result into per-ref image/dir/push/local-registry outputs, and nothing has the pull-ping handshake.
- **pullping** — `session/pullping/**` (`builder/solver.go:189`).
- **Embedded registry + gRPC proxy service** — feature stays (see redesign note above) (`regproxy/`, `builder/builder.go:196-197`, gated by `CapEarthlyRegistryProxy` at `builder/builder.go:178`).
- **Gateway `Export` RPC** (mid-build export for WAIT/END) — `frontend/gateway/**`, `solver/llbsolver/bridge.go` (`earthfile2llb/wait_block.go:284,416`).
- **SOCKET + HOST_BIND mount types** — `solver/pb/ops.proto`, `client/llb/exec.go`, `solver/llbsolver/mounts/mount.go` (`converter.go:2721-2722`; `with_docker_run_*.go`, `runmount.go:157`). Upstream sshforward is client→daemon only; no host-bind equivalent.
- **socketforward** — `session/socketforward/**` (`cmd/earthly/subcmd/build_cmd.go:474+`).
- **Git attrs: LFSInclude / LogLevel / SSHCommand** — `solver/pb/attr.go`, `source/git/**`, `client/llb/source.go` (`buildcontext/git.go:278-292,458`). Checked `fa19659f..origin/master`: upstream only added `git.fetchbycommit`; still no LFS/ssh-command/log-level.
- **gitutil non-isolation (HOME passthrough) + `GIT_LFS_SKIP_SMUDGE` + base64 stderr smuggling** — earthly's git auth model depends on the mounted `/root/.gitconfig` (`util/errutil/earthly_git_stderr.go` parses the stderr blob).
- **Live runc stats streaming (progress stream 99)** — `executor/runcexecutor/monitor_stats.go`, `util/progress/logs`, `StatusRequest.statsStream` (`logbus/solvermon/vertexmon.go:221`, `logbus/formatter/formatter.go:313`). Upstream's `resources.Monitor` records into history, doesn't live-stream.
- **Idle shutdown / session history / Info fields** — `session/manager.go`, control RPCs (`debug_cmds.go:130-311`, `buildkitd/buildkitd.go:1188`, `entrypoint.sh:244-254`).
- **Parallelism status + GC analytics** — `util/semutil`, `cache/gcanalytics.go`, worker proto fields 101-104 (`buildkitd/buildkitd.go:1188-1215`, `debug_cmds.go:259-293`).
- **Session timeout interceptor** (`BUILDKIT_SESSION_TIMEOUT`) — `cmd/buildkitd/timeout_interceptor.go` (`entrypoint.sh:251-254`, default 24h).
- **gRPC 64MB message sizes + static window sizes** — deliberate satellite-link tuning. Note: static windows disable grpc BDP autotuning; keep only if benchmarked.
- **`RLIMIT_NOFILE` raise** — `cmd/buildkitd/main_unix.go` (Docker 29/containerd v2 lowered defaults).
- **archutil Rosetta proc-mount** — Apple Silicon emulation detection (but see B31 rootless regression).
- **dockerui `SetBuildContext`** — `frontend/dockerui/config.go` (`converter.go:528`, FROM DOCKERFILE).
- **verifier platforms warning downgrade** — without it every earthly multi-target build fails `CheckInvalidPlatforms`.
- **filesync multi-target + progress + `CopyToCallerWithMeta`** — backs the earthly exporter.
- **`ResolveImageConfig` bridge re-add + `ResolveImageConfigOpt` alias** — compat shims earthly still compiles against (`converter.go:3118-3120`).
- **eager cache-import wait in `bridge.go:143-161`** — needed because lazy resolution can outlive the session that holds registry credentials (but see B18: make it cancellable).

---

## 3. Earthly migration blockers (must fix before bumping the pin)

- **M1** — `ExportEntry.VerboseProgressCB` (fsutil callback) was replaced by `OnReceiveProgress func(bytes int, done bool)` wired only into the earthly multi-target path. `earthly/builder/solver.go:190` will not compile; per-file verbose progress on local/OCI export paths is silently lost. Coordinate an earthly-side change or restore the old field.
- **M2** — `NewDockerAuthProvider` signature changed upstream to take `DockerAuthProviderConfig`. Earthly calls the old 2-arg form (`build_cmd.go:441`, `util/llbutil/authprovider/podman.go`).
- **Rebase watch-item** — upstream `origin/master` (post merge-base) changed `ImageWriter.Commit` to `(…, compatibilityVersion int, exporterType string)` (commit `5fde446de`); `exporter/earthlyoutputs/export.go:427` will need updating on the next rebase.

---

## 4. Bug catalog

Severity: **[H]** data loss / crash / wedge, **[M]** correctness or user-visible failure under realistic conditions, **[L]** latent / leak / cosmetic. "(merge)" = introduced or exposed by this merge, i.e. incorrect conflict resolution against changed upstream semantics.

### Merge regressions

- **B1 [H] (merge)** `source/local/source.go:292-307` — umask-0 emulation for local sources is now nested under `if idmap != nil`, so it's **disabled on standard root buildkitd**. Original fork commit `280ec68aa` ran it unconditionally. Files COPY'd from a dev machine with umask 077 become root-only in images (`USER nonroot` steps fail), and content-hash cache keys vary by developer umask.
- **B2 [H] (merge)** `exporter/earthlyoutputs/export.go:64-67,285-289` — inline cache is silently broken: upstream replaced the `containerimage.inlinecache` metadata key with an `exptypes.InlineCache` callback keyed by *original* solve ref IDs (`image-N`), but `Commit` looks up by the synthetic source's keys (platform strings / nil Ref) → lookups always miss. Earthly's `saveInlineCache` (`builder/solver.go:142-144`) is a silent no-op.
- **B3 [M] (merge)** `session/auth/authprovider/authprovider.go:307` — `toTokenResponse(…, _ bool)` accepts but ignores the anonymous flag; `FetchTokenResponse.Anonymous` is never set. Earthly's rate-limit warning (`util/llbutil/authprovider/authprovider.go:139`) never fires. One-line fix (original: fork commit `594835b59`).
- **B4 [M] (merge)** `session/filesync/diffcopy.go:104-111` — merge dropped `MetadataOnly: metadataOnlyFilter` from `fsutil.ReceiveOpt`; metadata-only transfers (dockerfile lint/outline paths) silently transfer full content.
- **B5 [L] (merge)** `client/client.go:181-186` — `WithDefaultGRPCDialer` path: upstream now strips `tcp://` before this block; `split[1]` with guard `len(split) > 0` (always true) panics index-out-of-range for every intended use. Fix guard to `len(split) > 1` or delete (upstream does the stripping). Latent: unused in OSS earthly.
- **B6 [M] (merge)** healthcheck replacement — see D2; also missing upstream's post-error `ctx.Done()` re-check, so normal teardown logs "healthcheck failed too many times".

### Daemon crash / wedge

- **B7 [H]** `cache/manager.go:1080-1100, 1357` — **prune deadlock**: the GC-analytics forwarding goroutine exits on `ctx.Done()` without draining `interCh`, while `prune` does an unconditional buffered send **holding `cm.mu` and `cr.mu`**.