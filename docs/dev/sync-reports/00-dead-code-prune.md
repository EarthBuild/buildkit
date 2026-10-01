# Upstream Sync Report #00: Dead & Unused Code Pruning

- **Status**: Completed
- **Target Fork Baseline**: Merge Base `585d8c719ab77ed07380656dd95a986628bca78c` (Post-`v0.13.0-beta1`)
- **Downstream Consumer**: [`github.com/earthbuild/earthbuild`](https://github.com/earthbuild/earthbuild)
- **Date**: October 2026

---

## 1. Executive Summary

Prior to bumping minor or patch releases from upstream [`moby/buildkit`](https://github.com/moby/buildkit), we audited all non-vendor additions in `EarthBuild/buildkit` against the downstream consumer `earthbuild/earthbuild` (`../earthbuild`).

Four legacy components were identified as completely unused or redundant SaaS remnants. Pruning these components eliminates ~220 lines of fork diff and simplifies upstream merge reconciliation without any impact on `earth` CLI functionality or wire compatibility.

---

## 2. Changes Summary

| Action | File | Rationale |
| :--- | :--- | :--- |
| **Deleted** | `session/auth/authprovider/podman.go` | Unused in `buildkit`. Downstream `earthbuild` maintains its own standalone Podman provider in `util/llbutil/authprovider/podman.go`. |
| **Deleted** | `client/client_earthly.go` | Defined `WithDefaultGRPCDialer` and `WithAdditionalMetadataContext` for legacy Earthly satellite proxies. Not referenced anywhere in `earthbuild`. |
| **Deleted** | `client/reserve.go` | Client-side wrapper for multi-tenant SaaS worker reservation. Never called by `earthbuild`. |
| **Modified** | `client/client.go` | Surgically removed the deleted option hooks (`withAdditionalHeaders` and `withDefaultGRPCDialer`) from `New()`. |
| **Modified** | `solver/llbsolver/ops/exec.go` | Removed `BUILDKIT_EXEC_TIMEOUT` environment variable parsing and enforcement (a legacy SaaS timeout mechanism never configured by `earthbuild`). |

---

## 3. Verification & Test Results

### 3.1 Downstream Compatibility Check
- Grepped full [`earthbuild/earthbuild`](https://github.com/earthbuild/earthbuild) repository for:
  - `WithDefaultGRPCDialer`: **0 matches**
  - `WithAdditionalMetadataContext`: **0 matches**
  - `Reserve`: **0 matches**
  - `BUILDKIT_EXEC_TIMEOUT`: **0 matches**
  - `session/auth/authprovider/podman`: **0 matches**
- Downstream `buildkitd` package compilation verified (`go test -c ./buildkitd -o /dev/null` in `../earthbuild`).

### 3.2 Compilation Matrix
Cross-compilation checks under Linux constraints (`GOOS=linux`) succeeded across all critical packages:
- `exporter/earthlyoutputs`: **OK**
- `solver/llbsolver`: **OK**
- `solver/llbsolver/ops`: **OK**
- `client`: **OK**
- `cmd/buildkitd`: **OK**
- `cmd/buildctl`: **OK**

### 3.3 Unit Tests
Targeted unit tests in `solver` run and pass cleanly:
```bash
go test -run '^Test[^J]' ./solver
# ok  github.com/moby/buildkit/solver  6.535s
go test ./solver/bboltcachestorage ./solver/internal/pipe ./solver/testutil
# ok  github.com/moby/buildkit/solver/bboltcachestorage  0.572s
# ok  github.com/moby/buildkit/solver/internal/pipe        0.532s
# ok  github.com/moby/buildkit/solver/testutil             0.780s
```
*(Note: `TestJobsIntegration` requires an external `registry` binary in `PATH`, which is part of integration test harness rather than pure unit tests).*

---

## 4. Security Assessment

| Assessment Dimension | Finding |
| :--- | :--- |
| **Attack Surface** | **Reduced**: Removing custom gRPC dialer hooks (`WithDefaultGRPCDialer`) and custom metadata context injectors eliminates potential proxy-manipulation vectors in the client. |
| **Authentication** | **Hardened**: Removing the dead custom Podman auth provider eliminates an unmaintained credential parser in favor of upstream Moby standard auth resolvers. |
| **Denial of Service** | **Neutral**: Removing `BUILDKIT_EXEC_TIMEOUT` does not degrade security because local builds enforce execution control via context cancellation and Earthfile-level timeouts. |
| **Dependency Impact** | **Zero**: No external dependencies were added or altered. |
| **Wire Protocol** | **Preserved**: Zero impact on gRPC wire protocols (`moby.localhost.v1`, `session/pullping`, `exporter/earthlyoutputs`). |

---

## 5. Next Steps

With dead code pruned and verified, we are ready to proceed to **Step 1**:
- Evaluate upstream tag `v0.13.0` (or `v0.13.1` / `v0.13.2`).
- Check conflict diff against our preserved extensions (`session/localhost`, `exporter/earthlyoutputs`, `solver/scheduler.go`).
- Generate the Step 1 Sync & Friction Report.
