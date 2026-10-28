# EarthBuild buildkit fork review — `earthbuild/main` vs upstream, per-release merge plan

## Foreword

Purpose: examine the fork to cut maintenance burden by reducing the lines we add on top of upstream, leaning on
upstream features wherever they exist. Every line we carry in an upstream-owned file is a potential conflict on every
merge.

Strategy change: the original review (snapshot d8650558c) targeted a flag-day merge — `merge-in-buildkit-all-at-once`
against upstream `fa19659f`. That branch is abandoned. We now merge **one upstream minor release at a time**
(v0.14.x, then v0.15.x, …). Consequences for this doc:

- Every "use upstream feature X" recommendation carries a **min upstream** version (first `vX.Y.0` tag containing the
  commit).
- Every item carries an **act at**: `now` (on main, or on the in-flight #41 branch before it lands) or `v0.X merge`
  (do it in that merge step, not before).
- Several original findings were artefacts of the all-at-once merge and are not bugs on main; they are kept as
  watch-items for the release that introduces the upstream change.

## Baseline

| | |
|---|---|
| Date | 2026-10-28 |
| Fork `earthbuild/main` | 5fe7531de, based on upstream 585d8c719 (= v0.13.0-beta1-119). **No upstream release merged yet**, so `git diff 585d8c719 earthbuild/main` is pure fork delta (plus 5 upstream v0.13.0 cherry-picks, D17). |
| In flight | `chore-merge-v0.14.1` = PR #41, head 2fc50e525 (contains v0.13.0, v0.13.2, v0.14.0, v0.14.1, current main). Also carries 587328a25 (prune podman/dialers/exec-timeout; same as open PR #30). |
| Upstream | latest release v0.34.1; latest rc v0.34.0-rc2; `origin/master` c19b15f57. ~21 minors behind. |
| Consumer | `earthbuild` (`/Users/kieran/github/earthbuild` @139d384df) `go.mod:146` replaces buildkit with `EarthBuild/buildkit` @ **5fe7531de** (not `51fe8fb` as originally stated). CLI is `cmd/earth/...`. |
| Fork delta (main vs base) | 344 files, +56,342/−1,020 total = **vendor** 206 files +42,882 (docker/distribution tree + deps, go-grpc-middleware) · **generated** `*.pb.go` 9 files +7,052/−702 · **hand-written** 129 files +6,408/−318 (tests +663; CI/tooling +342/−119). |
| #41 vs v0.14.1 | 132 files +6,439/−341 hand-written; vendor +44,757. The first per-release merge has **not** shrunk the delta yet. |

Ref conventions: bare `path:line` = fork on `earthbuild/main` @5fe7531de. `[#41] path:line` = the v0.14.1 branch.
`earthbuild/<path>` = consumer repo. Commit → `vX.Y` = min upstream release containing it.

---

## 1. Changes that can be DROPPED or replaced with upstream

| # | Change | Status / why | Min upstream / act at |
|---|--------|--------------|-----------------------|
| D1 | `solver/inconsistent_graph_state_error_tracker.go` + `dgstTrackerInst.add` in `solver/jobs.go:349-570` + unconditional `helpMe` build in `solver/scheduler.go:112-216` | Debug instrumentation for edge-merge corruption upstream has fixed: 100d3cb6b, 4e59c55b8 (v0.13.0); 1b3daaa39, ffdbd863b (v0.14.0). Upstream's own gated diagnostic (`debugSchedulerInconsistentGraphState`, 2f18b835c) arrives v0.17.0 — no need to wait for it. **Data race**: unlocked `dgstTrackerInst.String()` at `scheduler.go:375` ([#41] :423); `%+v` over live edge state on every dispatch. Branch `fix/solver-debug-tracker-races` exists — prefer deletion. | fixes complete at v0.14.0 → **now / with #41** |
| D2 | `session/configurabletimeout.go` + `ManagerHealthCfg` (`session/manager.go:41-60`) + `[health]` (`cmd/buildkitd/config/config.go:37,56-60`, `main.go:509-518,766-768`) + `util/appdefaults/appdefaults_all.go:6-8` | Upstream adaptive `monitorHealth` (`max(30s,1.5×last)`, tolerates one failure; b63786184 v0.11.0) is **already in base**, commented out at `session/grpc.go:81-140`. Fork replacement kills on first slow probe (B6). Consumer sets no `[health]`. Hang-safety, `healthCheckConfig` and per-session timeout header come in 70b363e54 (v0.30.0); defaults retuned to 15s in 77f1a0329 (v0.30.0). Also removes the `NewManager` test fix-ups (U12). | **now** (re-enable upstream); map `[health]` → `healthCheckConfig` only if wanted at **v0.30 merge** |
| D3 | `util/resolver/authorizer.go:337-343` — swallow `context.Canceled` in `fetchToken` | Obsolete since 3a2c03052 (v0.9.0, in base: errors not cached; flightcontrol retries). Turns a cancel into a fake success with empty token → a waiter joining the in-flight fetch at cancel time sends no `Authorization`. Narrow window, no benefit. | in base → **now** |
| D4 | ~~`session/auth/authprovider/podman.go`~~ | Zero callers. **Removed on #41** (587328a25; PR #30). Consumer has its own (`earthbuild/util/llbutil/authprovider/podman.go`). | **done in #41** |
| D5 | ~~`filesync.NewFSSyncTargetDir`~~ | **Claim was wrong for main**: upstream code there, called by upstream `client/solve.go:227`. Upstream deleted it in 81b476229 (v0.13.0); #41 re-added it with no callers (`[#41] session/filesync/filesync.go:270-276`). | delete **in #41** |
| D6 | `grpc_retry` dial opts `client/client.go:51-52` (+ `go-grpc-middleware` server chaining) | Retry opts are dead (no retry interceptor installed; consumer doesn't use it). Server chaining on main is upstream's own code; upstream went native in c3105e49d (v0.14.0), so on #41 `client/client.go:51-52` is the **last** user of go-grpc-middleware. Delete → drops dep + 3 vendored pkgs. | v0.14.0 → **in #41** |
| D7 | `util/entitlements/security/security_linux.go:37-42` device-cgroup rule | Semantically identical to upstream (empty `Type` = `a` = c+b); only under `security.insecure`. Revert to upstream text to shrink diff. | **now** |
| D8 | ~~`util/bklog/log.go` trace-ID gate~~ | **Claim was wrong**: `EnableLogWithTraceID` is upstream code, called on main (`util/tracing/detect/detect.go:98`). Uncalled in upstream v0.14–v0.15 (228e250d7) — upstream behaviour, not ours. Gate deleted upstream in 366c355fd (v0.16.0). Only fork delta is `version`/`revision` fields (`log.go:8,54-59`) → keep (§2a). | take upstream at **v0.16 merge** |
| D9 | ~~`os.Setenv("GRPC_ENFORCE_ALPN_ENABLED","false")` in buildkitd main~~ | **Not on main or #41** — only on the abandoned all-at-once branch. ALPN enforcement arrives with grpc ≥1.67, first vendored upstream in v0.19.0 (grpc 1.68.1). If ALPN must be off then, set it in the container env/entrypoint or a blank-imported init pkg (as `earthbuild/cmd/earth/disable_alpn`), never in `main()` (grpc reads it at package init). | guidance for **v0.19 merge** |
| D10 | Satellite-era surface: `Reserve` RPC (`api/services/control/control.proto:28`, `control/control.go:594`, `session/manager.go:121`), ~~`client/reserve.go`~~, ~~`WithDefaultGRPCDialer`/`WithAdditionalMetadataContext`~~ (`client/client_earthly.go`), OCI hooks (`executor/oci/hooks.go`, `[[hook]]` `config.go:118`, `executor/runcexecutor/executor.go:56`), ~~`BUILDKIT_EXEC_TIMEOUT`~~ (`solver/llbsolver/ops/exec.go:65-72`) | None used by consumer. Struck items **removed on #41** (587328a25 / PR #30); that also moots B5. Remaining: **Reserve RPC (server + proto) and OCI hooks** — product decision; droppable if no hosted satellites. Hooks removal also undoes `NewWorkerOpt(..., hooks, …)` test fix-ups (U12). | **now** (needs satellite decision) |
| D11 | `cmd/buildkitd/main_unix.go:21-32` RLIMIT_NOFILE raise (c40c767e2, #10) | Redundant: consumer already sets `--ulimit nofile=1048576:1048576` (`earthbuild/buildkitd/buildkitd.go:736`) and `ulimit -n` in `earthbuild/buildkitd/entrypoint.sh:20`. Go ≥1.19 raises soft→hard anyway. ~14 lines. | **now** |
| D12 | `frontend/dockerui/config.go:283-289` `SetBuildContext` | Upstream `dockerui.initContext` already reads `client.Inputs(ctx)["context"]` under `CapFrontendInputs` (in base). Consumer wraps its `gwclient.Client` to override `Inputs()` (~20 lines, `earthbuild/earthfile2llb/converter.go:529`); fork −8. | in base → **now** (consumer first) |
| D13 | `session/socketforward/**` (+259) + SOCKET mount type (`solver/pb/ops.proto`, `client/llb/exec.go` `SocketTarget`, `solver/llbsolver/mounts/mount.go`, `frontend/gateway/container/container.go:168,243`, ~+140) | Clone of upstream `sshforward` (`Proxy` ≈ `ForwardAgent`, id in metadata). Consumer implements one multiplexing `sshforward.SSHServer`: ids `earthly_save_file`/`earthly_interactive` → existing callbacks, everything else → the `sshprovider` it already builds (`earthbuild/cmd/earth/subcmd/build_cmd.go:452,464`); `earthbuild/earthfile2llb/converter.go:2731-2732` → `llb.AddSSHSocket`. `CheckAgent` must report those ids. ~470 fork lines. HOST_BIND stays (§2a). | in base → **now** (consumer first) |
| D14 | `cmd/buildkitd/timeout_interceptor.go` (+119) `BUILDKIT_SESSION_TIMEOUT` | Consumer is the only OSS client (`earthbuild/buildkitd/entrypoint.sh:263-266`, 24h). A client-side deadline on `Solve`/`Build` ctx propagates over gRPC → same effect, zero fork code. If kept: replace hand-rolled goroutine with `context.WithTimeoutCause` (fork TODO at :36-38). | **now** (consumer first) |
| D15 | `cmd/buildkitd/main.go` `logrus.SetOutput(os.Stderr)` | No-op: logrus default output is stderr. | **now** |
| D16 | `.github/workflows/earthly-count.yml` (+185) | Rename-progress tracker; largest uncovered fork file. Drop or move to an org-level workflow. | **now** |
| D17 | Upstream cherry-picks on main: 6d042bf20 (=4e59c55b8), 422af4977 (=b7a0282ba, `solver/scheduler_test.go` +165), 54c74579a (=7aee2929e multiwriter panic), f7c497404 (=d3b1de8f9), 19c0eb77f (=074099460); 20d4e2ffa has no upstream match | All v0.13.0. Absent from #41's delta (confirmed). Check 20d4e2ffa resolves too. | v0.13.0 → **auto in #41** |
| D18 | CI / Dockerfile churn converging with upstream: artifact v4 + per-platform names; Dockerfile `RUNC_VERSION v1.1.12`/`ALPINE_VERSION 3.19`, integration-tests `+slirp4netns iproute2`; `lint.Dockerfile` `as`→`AS`; action SHA pinning (#25); Dockerfile minio→`pgsty/silo` aliased to `/usr/bin/minio` (#36) | artifact v4 3ea25b7ae, slirp4netns d844974ba, Dockerfile versions: v0.13.0 (gone on #41). SHA pinning 6ba462608 v0.29.0 (keep only `vars.DOCKERHUB_USERNAME`/registry-target lines). Silo 4509d3ac4 v0.34.0 — drop alias then. | **#41** / **v0.29** / **v0.34** merges |
| D19 | #41-only additions: regenerated `util/archutil/*_binary.go` (065c631e4), gocovmerge pin workaround (~12 lines) | Base64 blobs conflict on every merge — revert to upstream bytes unless validate CI forces it. gocovmerge: drop when upstream pins it. | **in #41** / when upstream fixes |
| D20 | Dockerfile `buildkit-export` `+iptables-legacy` | Only consumer needs it (`earthbuild/buildkitd/entrypoint.sh:86-94`, `dockerd-wrapper.sh:280+`). Move to `apk add` in `earthbuild/buildkitd/Earthfile:27-36`. | **now** (consumer first) |
| D21 | `hack/release`, `hack/util`, `hack/images`: SBOM/provenance attestations disabled; GHA cache commented out, `SETUP_BUILDX_VERSION` pinned v0.14.1 (TODO(jhorsts)) | Fork-only churn in shell/CI that conflicts on every merge. Restore upstream once the buildx/attest/gha-cache-v2 issue is fixed (likely when upstream CI uses current buildx). | when CI fixed; recheck each merge |

### Upstream-PR candidates (shrink the delta over time)
- ~~`util/sshutil/keyscan.go:15,30,34-44` `[host]:port` known_hosts fix~~ — **no PR needed**: upstream 6678be5be (v0.32.0) is equivalent; IPv6 follow-up 521a1bed3 (v0.34.0). Drop fork patch at **v0.32 merge** (or v0.34).
- `exporter/containerimage/writer.go:123` — error instead of silent nil-ref when `FindRef` fails. Still open on master (`writer.go:140-142`). Safe: nil `Refs` returns `(r.Ref,true)`. 2 lines.
- `util/urlutil/redact.go:36-50` `RedactAllCredentials` + apply to upstream's two `git stderr:` wraps (master `git_cli.go:295,315` still unredacted). Credential-leak fix. `EARTHLY_GIT_STDERR` envelope stays fork-side unless §2b gitutil lands.
- `util/archutil/check_unix.go` Rosetta `/proc` mount (+26) — generic; fix B9 first.
- `util/resolver/resolver.go` registry transport timeouts (U9, see B10) — as a config knob, not hard-coded.
- History knobs: provenance `mode: min` (`solver/llbsolver/solver.go`, 2b7fc7d64) + blob-writer lease 5m→20m (`solver/llbsolver/history.go`, 14ace8401). Upstream has `[history] maxAge/maxEntries` (v0.11.0) but no mode knob. One PR for both. File moves to `solver/llbsolver/history/buildhistory.go` upstream → conflict.
- `snapshot/diffapply_unix.go` `uint64(stat.Dev)` cast (mips64). File renamed upstream to `diffapply_linux.go` in c8c502475 (v0.16.0) → conflict at v0.16.
- Dockerfile `RELEASE_VERSION` + `version.Package=github.com/EarthBuild/buildkit` (#18): `ARG BUILDKIT_PKG`/`VERSION` override in upstream `buildkit-version` stage → zero delta. Consumer checks Package (`earthbuild/buildkitd/buildkitd.go:1311`) and passes `--RELEASE_VERSION` (`earthbuild/buildkitd/Earthfile:24-26`).

### Replace implementation, keep feature
- **Embedded registry** (`exporter/earthlyoutputs/registry/**`, `eodriver`): vendored `docker/distribution v2.8.2+incompatible` (not v2.8.3) is archived and **fork-only** (upstream never required it) — it plus `libtrust`, `redigo`, `gorilla/*`, `yaml.v2` is **~43k of the 56k delta lines** and all of its CVE surface. Fork needs 4 read-only endpoints (GET/HEAD manifest/blob); `eodriver` fakes distribution's storage paths to feed its handler stack. ~200-line `net/http` handler over `MultiMultiProvider` drops the tree. Version-independent → **any time**.
- ~~**Registry gRPC proxy** 50 ms read-idle heuristic~~ — **done in #24** (343d8bfee: half-close via `CloseWrite`, `api/services/registry/copy.go`, 444-line `proxy_test.go`). Consumer uses the same `registry.Copy` (`earthbuild/regproxy/proxy.go:81`). Follow-up cleanup (drop `StreamRW`, 80ed7fa6b) on branch `giles-registry-proxy-framing`. (Original said "see B2"; that was a wrong ref — B2 is inline cache.)

---

## 2. Changes that must stay

Verified against main and #41; consumer pins main. Sizes = hand-written lines (`git diff --numstat 585d8c719 earthbuild/main`, excl. `*.pb.go`).

### 2a. Must stay — no upstream equivalent up to `origin/master`

| Item | Fork files (~lines) | Consumer usage | Notes |
|------|---------------------|----------------|-------|
| LOCALLY execution | `session/localhost/**` (+748), magic strings in `solver/llbsolver/ops/exec.go` (shared +235/−10) | `earthbuild/earthfile2llb/converter.go:627,637,1293,2825`, `with_docker_run_local_tar.go:90` | Upstream has no client-side exec. Shrink conflicts: reduce `exec.go` edit to a single hook into fork-owned files. |
| earthlyoutputs exporter | `exporter/earthlyoutputs/export.go` (+808), `client/exporters.go`, `client/solve.go` | `earthbuild/builder/solver.go:154`, `builder/image_solver.go:354` | Upstream multi-exporter (7fb54984e) arrives **v0.13.0** — **not in our base**; even then each exporter gets the whole result, nothing partitions per-ref, no pull-ping. Could delegate push/local per ref to upstream exporter instances. See M3, B2. |
| pullping | `session/pullping/**` (+82) | `earthbuild/builder/solver.go:54,111`, `image_solver.go:360`, `util/gatewaycrafter/exportcoordinator.go:72` | — |
| Embedded registry + gRPC proxy | `exporter/earthlyoutputs/registry/**`, `api/services/registry/**`, `client/registry.go` (+1177 incl. 444 test) | `earthbuild/builder/builder.go:209` (`CapEarthlyRegistryProxy`), `:228` | Largest item. Rewrite per §1. |
| Gateway `Export` RPC (WAIT/END) | `frontend/gateway/**`, `solver/llbsolver/bridge.go` (+96/−8, +~50 bridge) | `earthbuild/earthfile2llb/wait_block.go:284,416`, `util/gwclientlogger/client.go:81` | `frontend/frontend.go` imports `cache` for it. |
| HOST_BIND mount type | `solver/pb/ops.proto` (=100), `client/llb/exec.go`, `mounts/mount.go` (~+70) | user-facing `RUN --mount type=bind-experimental` (`earthbuild/earthfile2llb/runmount.go:157`), `converter.go:2803`, `with_docker_run_{base,reg,tar}.go` | SOCKET half → D13. |
| Git attrs LFSInclude / SSHCommand | `solver/pb/attr.go:9-11`, `source/git/**`, `client/llb/source.go`, caps (~+60) | `earthbuild/buildcontext/git.go:288,292,458` | Upstream up to master: keepgitdir/auth/knownhosts/mountsshsock (v0.8.0), skipsubmodules (v0.24.0), mtime (v0.29.0), fetchbycommit (v0.30.0), advice (v0.33.0) — no LFS, no ssh command. LogLevel → §2b. |
| `GIT_LFS_SKIP_SMUDGE` | `util/gitutil/git_cli.go` (1 line) | implicit | — |
| Live runc stats stream (99) transport | `util/progress/logs`, `StatusRequest.statsStream`, `solver/progress.go` (+~120) | `earthbuild/logbus/solvermon/vertexmon.go:24,217`, `logbus/formatter/formatter.go:313-324` | Upstream `resources.Monitor` records to history only. Data source → §2b. |
| Idle shutdown / NumSessions / SecondsIdle | `session/manager.go`, `control/control.go`, `client/{info,shutdown}.go` (~+230) | `earthbuild/cmd/earth/subcmd/debug_cmds.go:76,83`, `earthbuild/buildkitd/buildkitd.go:1104,1365,1685` | Upstream `InfoResponse` still only `buildkitVersion`. |
| Parallelism + GC analytics | `cache/gcanalytics.go` (+114), `cache/manager.go` (+55), `worker.proto` 101-104, `client/workers.go` (+75) | `earthbuild/buildkitd/buildkitd.go:1364-1385`, `debug_cmds.go` | Upstream OTEL metrics (f3f5f546e, v0.31.0) cover no GC/parallelism. B7. semutil → §2b. |
| Eager cache-import wait | `solver/llbsolver/bridge.go:81-84,131-149` (~+20) | implicit (registry creds for cache import) | Master still lazy. B8. Re-test need after v0.26 (23a0dd7f7 session from `jobCtx`). |
| archutil Rosetta `/proc` mount | `util/archutil/check_unix.go` (+26) | Apple Silicon (`earthbuild/internal/engine/apple_container.go:319`) | B9. Upstream-PR candidate. |
| Verifier `CheckInvalidPlatforms` skip | `[#41] solver/llbsolver/solver.go:742-757` (~+15) | every multi-target build | Check arrives with acb1bed96 (**v0.14.0**). Not on main. Keep narrow `ExporterEarthly`-only skip; emitting `ExporterPlatformsKey` instead would warn on multi-target. |
| bklog `version`/`revision` fields | `util/bklog/log.go:8,54-59` | logs | Cheap; ex-D8. |
| Registry transport timeouts | `util/resolver/resolver.go` (2 lines) | daemon pulls (laptop sleep) | B10. Upstream-PR candidate. |
| History provenance `min` + 20m lease | `solver/llbsolver/{solver,history}.go` (2 lines) | perf | Upstream-PR candidate. |
| `Earthfile` `+build`, `AGENTS.md`, `.coderabbit.yaml` [#41], Dockerfile `RELEASE_VERSION`/Package | new files (+14, +7) / Dockerfile (~5) | `BUILDKIT_BASE_IMAGE=…:<sha>+build` | New files never conflict. Dockerfile bit → upstream-PR. |

### 2b. Can shrink / replace

| Item | Replacement | Min upstream / act at | ~Fork lines saved |
|------|-------------|-----------------------|-------------------|
| socketforward + SOCKET | upstream `sshforward` (D13) | in base / now | ~470 |
| Session timeout interceptor | client-side deadline (D14) | n/a / now | ~120 |
| `util/semutil/weighted.go` (+150, copy of x/sync/semaphore) | ~30-line wrapper over `golang.org/x/sync/semaphore` + atomic counters | n/a / now | ~120 |
| gitutil base64 stderr smuggling (`git_cli.go:230-281`) | upstream already wraps `git stderr:\n%s` (d34b2471c, in base; refined 035832e0c v0.14.0). Consumer parser `earthbuild/util/errutil/earth_git_stderr.go:10-12` switches to that suffix; keep only redaction (→ PR). | in base / now (consumer first) | ~40 |
| gitutil HOME passthrough (non-isolation) | `gitutil.WithHostGitConfig()` (5bc2459bf); `source/git` doesn't enable it → 1-line fork change remains. Longer term: consumer uses `AttrAuthTokenSecret`/`AuthHeaderSecret` (v0.8.0) instead of mounted `/root/.gitconfig`. | **v0.30 merge** | ~20 |
| Git `LogLevel` attr | `git.debugcommands` (c08835d1a) — partial: LogLevel also sets GIT_TRACE and `ssh -v` | **unreleased** (master only) | ~15 |
| SessionHistory + `BUILDKIT_SESSION_HISTORY_DURATION` | `ListenBuildHistory` (dbee61670, in base); debug-only use (`earthbuild/cmd/earth/subcmd/debug_cmds.go:117-154,289-298`, `earthbuild/buildkitd/entrypoint.sh:256-259`) | v0.11.0 (in base) / now (consumer first) | ~60 |
| Stats source `executor/runcexecutor/monitor_stats.go` (+78, polls `runc stats`) | feed stream 99 from upstream `executor/resources` sampler (6e87e4b45); consumer only shows totals. cgroup v2 only. | v0.12.0 (in base) / now | ~80 + `runc.Stats` dep |
| filesync multi-target + `CopyToCallerWithMeta` (`session/filesync/filesync.go` +57, `diffcopy.go` +14) | client half (`NewFSSyncMultiTarget`) → consumer-owned `FileSend` attachable on `fsutil.Receive`; server half → `exporter/earthlyoutputs`. Upstream per-id targets (v0.13.0) don't cover md-derived dirs. | n/a / after #41 (conflict-prone files) | ~70 moved out of upstream files |
| gRPC 64MB msg + static windows (`cmd/buildkitd/main.go:284-286`, `frontend/gateway/gateway.go:461-467`, `grpcclient/client.go:1168-1170`, `client/client.go:47-49`, `main_oci_worker.go:367-369`, `session/grpc.go:44`) | upstream consistent 16MB everywhere (29b4b1a53). Static windows disable BDP autotuning — drop unless benchmarked; if >16MB proven, one helper not six edits. | **v0.14.0 / in #41** | ~30 |
| RLIMIT_NOFILE | consumer `--ulimit` (D11) | now | 14 |
| dockerui `SetBuildContext` | `Inputs()` wrapper (D12) | in base / now | 8 |
| ~~`ResolveImageConfig` bridge re-add + `ResolveImageConfigOpt` alias~~ | Not a fork change on main; #41 does not add it → M4. | v0.13.0 | 0 (don't add) |

Total if all §2b done: ~900–1,000 hand-written lines (~15% of the 6.4k).

---

## 3. Consumer migration blockers (pin bumps)

- ~~**M1**~~ — **resolved by #49** (5fe7531de): `VerboseProgressCB` → `OnReceiveProgress` + `OnReceiveFile` (`client/solve.go:64-70,206`, `session/filesync/diffcopy.go:101-129`). Consumer migrated (`earthbuild/builder/solver.go:189-190`). Residual [L]: `NewFSSyncTarget`/`NewFSSyncTargetDir` (plain local/tar/OCI exporters) have no progress callback — unused by EarthBuild.
- **M2** — **act at v0.20 merge**: upstream 150e7985b changes `NewDockerAuthProvider` to take `DockerAuthProviderConfig`. Main/#41 still 2-arg (`session/auth/authprovider/authprovider.go:38`). Consumer breaks at `earthbuild/cmd/earth/subcmd/build_cmd.go:431` and 9 sites in `earthbuild/util/llbutil/authprovider/podman.go:73-124`. Land consumer change with the v0.20 pin bump. Same merge conflicts with fork `Anonymous` plumbing (B3).
- **M3** (was "rebase watch-item") — `ImageWriter.Commit` changes three times; update `exporter/earthlyoutputs/export.go` each step: +`inlineCache` param in 4c93f9d9b (**v0.13.0**, done on #41 at `export.go:477`; B2 trigger); +`compatibilityVersion int, exporterType string` in 5fde446de (**v0.30.0**); third return `*result.Result[*solver.Remote]` in 191ac2d19 (**v0.34.0**).
- **M4** (new) — `ResolveImageConfig` → `sourceresolver`: 30c069cb0 (**v0.13.0**, not in our base) replaces `llb.ResolveImageConfigOpt`/`ImageMetaResolver` with `sourceresolver.Opt`/`MetaResolver`. #41 does not re-add a shim (keep it that way). Consumer must migrate (~30 lines) before pinning #41: `earthbuild/earthfile2llb/converter.go:3128-3130`, `earthfile2llb/cachedmetaresolver.go:41-56`, `util/gwclientlogger/client.go:89-93` (type change only; `gwclient.Client.ResolveImageConfig(ctx, ref, sourceresolver.Opt)` still exists on master). **Act at #41.**
- **M5** (new) — consumer-first changes that unblock fork deletions: D12 (`Inputs()` wrapper), D13 (sshforward multiplexer), D14 (client deadline), D20 (iptables-legacy), §2b gitutil stderr parser, SessionHistory. Each: consumer change → pin → fork deletion.

---

## 4. Bug catalog

Severity: **[H]** data loss / crash / wedge, **[M]** correctness or user-visible failure under realistic conditions, **[L]** latent / leak / cosmetic. Origin: "(fork)" = our code; "(fork merge X)" = past conflict resolution; "(all-at-once)" = only on the abandoned branch → watch-item for the named merge.

### Merge-related

- **B1 [M/H] (fork merge 91fc05f2b #53, 2021)** `source/local/source.go:248-266` ([#41] :249-267) — umask-0 emulation for local sources only runs under `if idmap != nil` → **disabled on root buildkitd since 2021**; original 280ec68aa ran it unconditionally. Not an all-at-once regression. Files COPY'd from a umask-077 machine are root-only in images; content-hash keys vary by umask. Fixing changes modes and cache keys for every user → **product decision now**: restore, or delete the dead emulation.
- **B2 [H] (all-at-once; v0.13.0 trigger)** earthlyoutputs inline cache lookups miss after 4c93f9d9b moved inline cache to an `exptypes.InlineCache` callback keyed by original solve-ref ids. **Main unaffected** (metadata-key path, `exporter/earthlyoutputs/export.go:265-268`). **Fixed on #41** by 5c127fb46 (key remap, `[#41] export.go:255-477`). Needs a regression test: `SAVE IMAGE --push` with inline cache asserts cache config in pushed image (consumer `earthbuild/builder/solver.go:141-142,226-229`). **Act at #41.**
- **B3 [M] (all-at-once)** `toTokenResponse` ignored `anonymous`. **Correct on main** (`session/auth/authprovider/authprovider.go:280-287`, anonymous path :143) and #41 (:283-290). Consumer rate-limit warning depends on it (`earthbuild/util/llbutil/authprovider/authprovider.go:139`). Watch at **v0.17** (1a3fc0aa1 gogoproto removal regenerates `auth.pb.go` — `Anonymous` must survive), **v0.20** (150e7985b), **v0.27** (339740431), **v0.34** (8e20dccc7) — each restructures the file. Add a unit test: no-credentials `FetchToken` → `Anonymous=true`. **Test now.**
- **B4 [M] (all-at-once; v0.22.0 trigger)** `MetadataOnly: metadataOnlyFilter` dropped from `fsutil.ReceiveOpt` in `recvDiffCopy`. Param introduced by 7ae0d6a90 (v0.22.0); absent on main/#41. **Watch at v0.22 merge**; same merge bumps fsutil — re-check #49's `syncTargetDiffCopy` `onFile`/progress wiring survives.
- **B5 [L] (fork)** `WithDefaultGRPCDialer` `split[1]` with always-true `len(split) > 0` guard (`client/client.go:173-179`, `client/client_earthly.go:36-45`). Latent on main (panics only on scheme-less address). Upstream cda45ada8 (v0.14.0) strips `tcp://` first → would always panic after v0.14. **Removed on #41** (587328a25). Unused by consumer.
- **B6 [M] (fork)** healthcheck replacement (D2) — `session/configurabletimeout.go:39-48`, defaults `util/appdefaults/appdefaults_all.go:6-8` (1 failure / 1s / 10s): one probe >10s kills the session (off-by-one: `consecutiveFailures >= allowedFailures`); no post-error `ctx.Done()` recheck → teardown logs "healthcheck failed too many times". **Live in shipped earthbuild** (consumer sets no `[health]`). [#41] adds a discarded `WithTimeoutCause` cancel (timer leak). Fix **now** = D2.

### Daemon crash / wedge

- **B7 [H] (fork, d0949473c)** prune deadlock — `cache/manager.go:1065-1085` GC-analytics forwarder returns on `ctx.Done()` without draining the 1-buffered `interCh` (when `KeepBytes != 0`); `prune` sends unconditionally at `:1292-1294` holding `cm.mu` (:1241) and `cr.mu` (:1244) → daemon wedged. Trigger: `earth prune --size N` (`earthbuild/cmd/earth/subcmd/prune_cmds.go:115-121`) + Ctrl-C/disconnect. [#41] `:1046-1103`. No fix on any branch. Fix: drain `interCh` until close (select on ctx only for the outward send) or count inside `prune`. **Now.**
- **B8 [M] (fork)** (fills dangling "B18"; original text lost — reconstructed from code) eager cache-import wait not cancellable — `solver/llbsolver/bridge.go:84,131-149`: `eg.Go` blocks on `lazyCacheManager.wait()` (`<-waitCh`, :430) and resolution runs under `inBuilderContext(context.TODO(), …)` → a cancelled solve hangs until the registry fetch completes/times out; errors are discarded (`return nil`). Fix: select on `ctx.Done()` in the wait. **Now**; re-test need at v0.26.

### Platform / environment

- **B9 [M] (fork)** (fills dangling "B31"; original text lost — reconstructed from code) archutil Rosetta `/proc` mount is mandatory — `util/archutil/check_unix.go`: `mountProc` failure returns `"", err` from `check()` instead of falling through, so where mounting proc is not permitted (rootless / no CAP_SYS_ADMIN) every emulator check fails and cross-platform detection is lost. Repro on rootless unverified. Fix: best-effort mount (log and continue), then upstream. **Now.**
- **B10 [M] (fork, c09ea1a0d, 6c53e63d2)** registry transport timeouts — `util/resolver/resolver.go`: `ResponseHeaderTimeout 30s` can fail large blob-mount/HEAD on slow registries (upstream reverted the same idea, moby/buildkit#3995); `IdleConnTimeout 5s` defeats connection reuse under bursty parallel pulls. Added for laptop-sleep stale conns. Make configurable or loosen. **Now** (or upstream-PR).

---

## 5. Per-minor-release merge checklist

Versions with nothing from this doc are omitted. "Drop" = delete fork code; "adapt" = fork code must change; "watch" = verify conflict resolution / add test.

**v0.14 (PR #41; brings v0.13.0–v0.14.1)**
- Drop: D1 (fixes complete), D5 re-add, D6 `grpc_retry` → go-grpc-middleware gone, D17 cherry-picks (auto; check 20d4e2ffa), D18 artifact-v4/Dockerfile/lint items, D19 archutil blobs; gRPC 64MB/window overrides (29b4b1a53).
- Done on branch: D4, D10 partial, B5 (587328a25 / PR #30); B2 fix 5c127fb46.
- Adapt: M3 `Commit` +inlineCache; verifier `CheckInvalidPlatforms` skip (acb1bed96); filesync after 81b476229 multiplexing.
- Consumer: M4 sourceresolver migration before pinning.
- Watch: B2 regression test; B6 timer leak; trace IDs not logged v0.14–v0.15 (upstream 228e250d7, resolves at v0.16).

**v0.16** — D8: take upstream (366c355fd removes gate), keep version/revision fields. Watch `diffapply_unix.go`→`diffapply_linux.go` rename (c8c502475): carry or upstream the `stat.Dev` cast.

**v0.17** — Watch: gogoproto removal (1a3fc0aa1) regenerates every `*.pb.go` — fork protos (localhost, socketforward, pullping, registry, control 101+, worker 101-104, `ops.proto` HOST_BIND/SOCKET, auth `Anonymous`) need regen with the new toolchain (B3). Upstream scheduler diagnostic (2f18b835c) available if D1 not yet dropped.

**v0.19** — Watch: grpc ≥1.67 ALPN enforcement (D9). If needed, disable via env/init package, not `main()`.

**v0.20** — Adapt: M2 `NewDockerAuthProvider(DockerAuthProviderConfig)` + consumer change in lock-step. Watch B3.

**v0.22** — Watch: B4 keep `MetadataOnly: metadataOnlyFilter`; fsutil bump vs `syncTargetDiffCopy` `onFile`/progress wiring.

**v0.26** — Watch: B8 / eager cache-import wait — re-test whether still needed with session from `jobCtx` (23a0dd7f7).

**v0.27** — Watch: B3 (339740431 authprovider restructure).

**v0.29** — Drop: fork action SHA pinning (6ba462608 upstream-native); keep `vars.DOCKERHUB_USERNAME`/registry targets (D18).

**v0.30** — Adapt: M3 `Commit` +`compatibilityVersion, exporterType`. Optional: map `[health]` → `healthCheckConfig` (70b363e54, 77f1a0329) if D2 config still wanted. Shrink: gitutil HOME passthrough → `WithHostGitConfig()` (5bc2459bf).

**v0.32** — Drop: `util/sshutil/keyscan.go` patch (6678be5be).

**v0.34** — Adapt: M3 `Commit` third return (191ac2d19). Watch B3 (8e20dccc7). Drop: keyscan IPv6 follow-up taken (521a1bed3); Dockerfile silo→`minio` alias (4509d3ac4).

**Unreleased (master)** — `git.debugcommands` (c08835d1a) may partly replace the LogLevel git attr.

**Any time (version-independent)** — D2/B6, D3, D7, D10 rest, D11–D16, D20, B1 decision, B7–B10, embedded-registry rewrite, §2b semutil/stats/SessionHistory/gitutil-stderr/filesync, upstream PRs.

---

### Changelog
- d8650558c: original review against `merge-in-buildkit-all-at-once` / upstream `fa19659f`. Its bug catalog ended at **B7**; references to B14, B18, B31 were dangling (B14 → B5; B18 → new B8; B31 → new B9). Its "see B2" in the registry-proxy note was a wrong ref (B2 = inline cache).
- 2026-10-28 rewrite: rebased on `earthbuild/main` 5fe7531de and the per-release merge strategy; added min-upstream / act-at, D11–D21, M3–M5, B8–B10, §5 checklist. Corrected: D5, D8, D9 (not fork code / not on main), M1 (resolved), B1 (2021 fork merge, not all-at-once), B3/B4 (all-at-once only), multi-exporter and `sourceresolver` (v0.13.0, not in base), keyscan (fixed upstream v0.32.0), docker/distribution version (v2.8.2).
