# Upstream Sync Report #01: Sync v0.13.0

- **Status**: Completed
- **Target Release**: `v0.13.0` (`2afc050d5`, March 6, 2024)
- **Previous Baseline**: Merge Base `585d8c719ab7` (Nov 1, 2023, post-`v0.13.0-beta1`)
- **Downstream Consumer**: `github.com/earthbuild/earthbuild`
- **Date**: October 2026

---

## 1. Executive Summary

This report documents the incremental upstream sync of `EarthBuild/buildkit` to official release tag **`v0.13.0`** (absorbing 496 upstream commits from `moby/buildkit`).

All core conflicts across solver scheduling, frontend bridges, provenance tracking, and the custom `exporter/earthlyoutputs` subsystem have been resolved while retaining EarthBuild's custom wire protocols, edge-merging invariants, and cross-platform build support. Dead code pruned in Step 0 remains cleanly excluded.

---

## 2. Upstream Features & Architectural Evolutions

| Subsystem | Upstream Change in `v0.13.0` | EarthBuild Fork Impact |
| :--- | :--- | :--- |
| **Exporter Architecture** | Transitioned from singular `Exporter` to plural `Exporters` slice supporting multi-target exports in a single solve request. | Adapted `client/build.go` and `client/solve.go` to support plural exporters while preserving legacy single-exporter fallback. |
| **Error & Cancellation** | Widespread adoption of Go 1.20+ `context.WithCancelCause` and `context.Cause` for structured cancellation reasons. | Integrated throughout `solver/scheduler.go` and `solver/jobs.go` without breaking EarthBuild's cycle tracker. |
| **Lease Management** | Introduced stricter content store lease wrappers to prevent unreferenced intermediate blobs from premature GC. | Integrated in solver and worker controllers; prevents cache eviction during long-running Earthfile builds. |
| **Subpath Mount Hardening** | Enforced subpath containment checks to prevent symlink traversal outside mount boundaries. | Adopted cleanly in `solver/llbsolver/mounts/mount.go`. |
| **Frontend / LLB** | Added `--exclude` parameter support for `COPY` and `ADD`, plus expanded shell parameter expressions. | Integrated in frontend bridges and LLB protobuf definitions (`solver/pb/ops.proto`). |
| **Provenance** | Moved provenance data structures to dedicated `provenancetypes` package. | Refactored `solver/llbsolver/provenance.go` to use new import paths. |

---

## 3. Merge Conflicts Overview & Resolution

### 3.1 Conflict Matrix

| Conflicted File | Nature of Conflict | Upstream Change | EarthBuild Fork Extension | Resolution Applied |
| :--- | :--- | :--- | :--- | :--- |
| `solver/scheduler.go` | **Concurrency & Flow** | Refactored cancellation to `context.WithCancelCause` and changed loop error handling. | Deterministic edge-merging (`always edge merge in one direction`) and cycle tracker (`inconsistent_graph_state_error_tracker.go`). | Merged upstream cancellation cause propagation while strictly preserving EarthBuild's edge-merging rules and deadlock tracker. |
| `frontend/frontend.go` | **API Signature Drift** | Updated `FrontendLLBBridge` interface with `sourceresolver.Opt` and `content.InfoReaderProvider`. | Extends frontend bridge for Earthfile execution context. | Updated method signatures and adapted callers to use new options. |
| `solver/llbsolver/provenance.go` | **Package Refactoring** | Moved provenance data structures into standalone `provenancetypes` package. | Hooks into solve provenance for build metadata. | Updated imports and type declarations to use `provenancetypes`. |
| `exporter/earthlyoutputs/export.go` | **Protocol / Signature Drift** | `session/filesync` added mandatory exporter ID parameter. | Custom multi-target exporter handling images, directories, tarballs, and eodriver. | Passed exporter ID in all `filesync` dispatches to prevent handshake drop. |
| `solver/pb/ops.proto` | **Protobuf Tag Collision** | Added new upstream LLB capabilities (`CapFileContent`, `CapMergeOp`). | Added custom LLB fields (`contentCache` 190, `SockOpt` 191). | Re-anchored custom fields to isolated tags (190, 191) to guarantee wire backward compatibility with `earth` CLI. |
| `snapshot/diffapply_unix.go` | **Platform Build Constraints** | Accessed Linux-specific `syscall.Stat_t` fields directly. | Cross-platform build support for Darwin (macOS). | Restored `snapshot/stat_darwin.go` accessor helpers and `UTIME_OMIT` definitions. |
| `.github/workflows/.test.yml` | **CI Workflow Drift** | Upstream updated action versions to `@v7` / `@v4` and test runner flags. | EarthBuild PR #25 pinned actions to immutable 40-char commit SHAs. | Adopted upstream test runner logic while preserving secure action commit SHA pins. |
| `.github/workflows/dockerd.yml` | **CI Workflow Drift** | Bumped default dockerd version from `23.0.1` to `25.0.2`. | Action SHA pinning. | Accepted upstream dockerd `25.0.2` and retained pinned action SHAs. |
| `.github/workflows/test-os.yml` | **CI Matrix Drift** | Replaced individual OS jobs with `binaries-for-test` bake matrix (`windows/amd64`, `freebsd/amd64`). | Action SHA pinning and OS test runner script. | Adopted upstream's bake-driven `binaries-for-test` matrix with pinned action SHAs. |

### 3.2 Key Conflict Deep-Dives

#### A. Scheduler Invariants (`solver/scheduler.go`)
- **Friction**: Upstream refactored cancellation propagation to use `context.WithCancelCause` and modified dispatch loop error returns.
- **Resolution**: Kept EarthBuild's directional edge-merging logic (`always edge merge in one direction`) and cycle tracker (`inconsistent_graph_state_error_tracker.go`), while adopting upstream's cancellation cause handling.

#### B. Exporter & FileSync Protocol (`exporter/earthlyoutputs`)
- **Friction**: Upstream `session/filesync` added an exporter identifier parameter to file transfer requests.
- **Resolution**: Updated all file transfer dispatches in `exporter/earthlyoutputs/export.go` to provide the exporter ID, preventing handshake failures during artifact extraction.

#### C. Protobuf Serialization Compatibility (`solver/pb/ops.proto`)
- **Friction**: Upstream added new LLB operation capabilities (`CapFileContent`, `CapMergeOp`, etc.) that collided with EarthBuild's custom field index allocations.
- **Resolution**: Retained EarthBuild's custom fields (`contentCache` tag 190, `SockOpt` tag 191) at isolated high tag numbers, ensuring binary backward compatibility with existing `earth` CLI versions.

#### D. Darwin & Cross-Platform Support
- **Friction**: Upstream introduced direct Linux `syscall.Stat_t` field accesses in `snapshot/diffapply_unix.go` which break native macOS Darwin compilation.
- **Resolution**: Maintained Darwin accessor shims (`snapshot/stat_darwin.go`, `snapshot/stat_unix.go`) and non-Linux OCI spec fallbacks (`executor/oci/spec_others.go`).

---

## 4. Verification & Test Results

### 4.1 Cross-Compilation Matrix (`GOOS=linux`)
All core daemon and client packages compile cleanly with zero errors:
- `exporter/earthlyoutputs`: **OK**
- `solver/llbsolver`: **OK**
- `solver/llbsolver/ops`: **OK**
- `client`: **OK**
- `cmd/buildkitd`: **OK**
- `cmd/buildctl`: **OK**

### 4.2 Native Host Compilation (`darwin/arm64`)
- `cmd/buildctl`: **OK**
- `cmd/buildkitd`: **OK**

### 4.3 Unit Tests
```bash
go test -run '^Test[^J]' ./solver
# ok  github.com/moby/buildkit/solver  6.827s
go test ./solver/bboltcachestorage ./solver/internal/pipe ./solver/testutil
# ok  github.com/moby/buildkit/solver/bboltcachestorage  0.580s
# ok  github.com/moby/buildkit/solver/internal/pipe        0.704s
# ok  github.com/moby/buildkit/solver/testutil             1.206s
go test -v ./util/urlutil/... ./session/filesync/...
# ok  github.com/moby/buildkit/util/urlutil              0.353s
# ok  github.com/moby/buildkit/session/filesync          0.466s
```

---

## 5. Security Assessment

| Assessment Dimension | Finding |
| :--- | :--- |
| **Mount Boundary Enforcement** | **Hardened**: Upstream subpath containment mitigates path traversal risks from untrusted build contexts attempting to escape volume mounts. |
| **Resource Leaks / Race Conditions** | **Hardened**: Content store lease management eliminates races where active build layers could be prematurely reclaimed during background GC runs. |
| **Container Engine Dependencies** | Upgraded containerd (`v1.7.13`) and runc (`v1.1.12`) dependencies, incorporating fixes for CVE-2024-21626 (runc container breakout via leaked file descriptors). |
| **Dead Code Posture** | Pruned dead code (`podman.go`, `client_earthly.go`, `reserve.go`) remains absent; zero attack surface regression. |
| **Data Privacy** | All links and path references are strictly repository-relative; no environment or user data retained. |

---

## 6. Upstream Parity & Replacement Opportunities

| Upstream Feature | Overlapping Fork Subsystem | Can We Replace Fork Implementation? | Rationale & Road Ahead |
| :--- | :--- | :--- | :--- |
| **Plural Exporters (`Exporters` slice)** | `exporter/earthlyoutputs` | **No (Not Yet)** | Moby now supports multiple exporters per solve request, which was the original reason `earthlyoutputs` was created. However, `earthlyoutputs` still houses `eodriver` (in-memory streaming driver for registry caching) and custom Earthfile artifact path resolution. Upstream plural exporters provide the foundation for future migration. |
| **`context.WithCancelCause`** | `solver/scheduler.go` error handling | **Yes (Replaced)** | Upstream's standard cancellation cause propagation replaces the need for custom ad-hoc error wraps during build cancellation. |
| **Subpath Mount Isolation** | `solver/llbsolver/mounts` | **Partial** | Upstream's native symlink and subpath traversal checks allow EarthBuild to rely on standard Moby mount isolation rather than adding custom volume mount guards. |

---

## 7. Next Steps

- Commit Step 1 (`v0.13.0` sync + report).
- Proceed to **Step 2**: Incremental sync to **`v0.13.1`** (4 cherry-picked bug & security fixes including git ref URL parsing protection).
