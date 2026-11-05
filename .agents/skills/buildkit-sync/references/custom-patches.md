# EarthBuild Custom Patches (MADR-Lite)

Baseline: `moby/buildkit` **`v0.15.2`**

## Active Patches

| ID | Feature / Context | Primary Files | Retirement Criteria |
| :--- | :--- | :--- | :--- |
| **ADR-001** | Embedded registry proxy & `earthlyoutputs` exporter (PR #24 half-close fix) | `exporter/earthlyoutputs/export.go`, `exporter/earthlyoutputs/registry/eodriver/eodriver.go`, `api/services/registry/registry.proto`, `api/services/registry/server.go`, `api/services/registry/copy.go`, `client/registry.go` | Keep while `earthbuild` uses `earthlyoutputs` & `/earthly.registry.v1.Registry`. |
| **ADR-002** | Session providers (`localhost`, `pullping`, `socketforward`) & gRPC timeout interceptor | `session/localhost/localhost.go`, `session/pullping/pullping.go`, `session/socketforward/socket.go`, `session/configurabletimeout.go`, `session/grpc.go`, `session/manager.go`, `cmd/buildkitd/timeout_interceptor.go` | Keep while `earthbuild` uses `LOCALLY`, `pullping`, and `socketforward`. |
| **ADR-003** | Solver caps, exec timeouts/`SOCKET`/`HOST_BIND` mounts & graph error tracker (`helpMe`/`dgstTrackerInst`) | `solver/pb/ops.proto`, `solver/pb/caps.go`, `solver/llbsolver/ops/exec.go`, `solver/llbsolver/mounts/mount.go`, `solver/llbsolver/bridge.go`, `solver/inconsistent_graph_state_error_tracker.go`, `solver/scheduler.go`, `solver/progress.go` | Audit `solver/scheduler.go` (`helpMe`/`dgstTrackerInst`) & `solver/llbsolver/ops/exec.go` (`SOCKET`/`HOST_BIND`) on each bump; edge merge (`v0.14.1`) & `SECRET`/`SSH`/`TMPFS` (`v0.15.2`) upstreamed. |
| **ADR-004** | Runc container stats monitor & cache GC analytics | `executor/runcexecutor/monitor_stats.go`, `cache/gcanalytics.go`, `cache/manager.go`, `api/services/control/control.proto`, `api/types/worker.proto`, `control/control.go`, `client/workers.go`, `client/shutdown.go` | Keep while `earthbuild` queries worker stats & GC analytics. |
| **ADR-005** | Anonymous auth token detection, URL credential redaction, weighted sem & Git CLI | `session/auth/auth.proto`, `util/resolver/authorizer.go`, `util/urlutil/redact.go`, `util/semutil/weighted.go`, `worker/containerd/containerd.go` (`WorkerOptions.ParallelismSem`), `util/gitutil/git_cli.go`, `source/git/source.go` | Drop individual helpers when upstream adds native equivalents. |
| **ADR-006** | Client/frontend hooks used by `earthbuild`: preset dockerui build context (`SetBuildContext`) & nil-safe `FrontendAttrs` when gateway builds pass cache imports | `frontend/dockerui/config.go`, `client/solve.go` | `SetBuildContext`: keep while `earthbuild` `FROM DOCKERFILE` calls it. `FrontendAttrs` guard: drop when upstream `client/solve.go` handles nil `opt.FrontendAttrs`. |

## Retired Patches

- **RET-001 (`earthbuild/fsutil`)**: Dropped in `5fe7531de` (#49, `v0.14.1`) -> superseded by upstream `github.com/tonistiigi/fsutil`.
- **RET-002 (`getMountDeps` `SECRET`/`SSH`/`TMPFS` skip)**: Dropped in `v0.15.2` sync -> upstreamed by `d1d3ad8da`; fork keeps only `SOCKET`/`HOST_BIND` case (ADR-003).
