# Upstream Sync Report #03: Patch Sync v0.13.2

- **Status**: Completed
- **Target Release**: `v0.13.2` (`2e18d709f`, April 3, 2024) — Final Release of the `v0.13` Series
- **Previous Baseline**: `v0.13.1` (`c9c83424a`)
- **Downstream Consumer**: `github.com/earthbuild/earthbuild`
- **Date**: October 2026

---

## 1. Executive Summary

This report documents the incremental patch sync to upstream `moby/buildkit` tag **`v0.13.2`**, concluding the sync of the entire `v0.13` release cycle.

This patch brings essential bug fixes for `COPY --link`, garbage collection policy calculation, tar conversion diffID accuracy, and crucially incorporates the upstream dependency update resolving **CVE-2023-45288** (Go HTTP/2 CONTINUATION flood DoS).

---

## 2. Upstream Changes Absorbed

| Component | Upstream Commit | Description |
| :--- | :--- | :--- |
| **HTTP/2 Security Bump** | `dfe87d078` | **CVE-2023-45288**: Bumps `golang.org/x/net` from `0.18.0` to `0.23.0`, mitigating the HTTP/2 CONTINUATION frame denial-of-service vulnerability. |
| **`COPY --link` Mapping** | `6d689a39d` / `d647910aa` | Fixes missing source code location mapping for `COPY --link` instructions in Dockerfile frontend to LLB translation. |
| **Timestamp Rewrite** | `00935df14` | Resolves incompatibility between metadata timestamp rewriting and `COPY --link` layers in the container image exporter. |
| **Tar Converter** | `ecf1c4223` | Fixes `diffID` computation in `tarconverter`, ensuring accurate layer content digests during artifact conversion. |
| **GC Policy** | `38e6fcded` | Fixes percentage-based disk space GC policy calculation on Windows hosts. |
| **Windows Daemon** | `25108abda` | Allows `--group` flag on Windows daemon invocations. |
| **FSUtil Vendor** | `a0efecf1e` / `4d961cad1` | Adds hardlink reset options and cross-platform path normalization. |

---

## 3. Merge Conflicts Overview & Resolution

### 3.1 Conflict Matrix

| Conflicted File | Nature of Conflict | Upstream Change | EarthBuild Fork Extension | Resolution Applied |
| :--- | :--- | :--- | :--- | :--- |
| `vendor/github.com/tonistiigi/fsutil/receive.go` | **Source Code Drift** | Added cross-platform Windows path normalizations (`filepath.FromSlash`). | Custom verbose progress callback (`r.verboseProgressCb`). | Normalized paths first, then dispatched `verboseProgressCb` with normalized path. |
| `vendor/github.com/tonistiigi/fsutil/send.go` | **Source Code Drift** | Added `WithHardlinkReset(fs)` and `filepath.ToSlash` normalization. | Custom verbose progress callback (`verboseProgressCb`). | Combined `WithHardlinkReset` with `verboseProgressCb` in `Send()`, kept path normalizations in `walk()`, and added missing `"path/filepath"` import. |
| `go.sum` | **Checksum Manifest Drift** | Upstream updated `tonistiigi/fsutil` checksums to `91a3fc46842c`. | Fork maintainer replace directive. | Accepted upstream's updated module checksums. |
| `vendor/modules.txt` | **Vendor Manifest Drift** | Upstream declared `tonistiigi/fsutil v0.0.0-20240424095704-91a3fc46842c`. | Fork replace directive (`alexcb/fsutil`). | Preserved EarthBuild's replace mapping while updating version reference to `91a3fc46842c`. |

### 3.2 Key Conflict Deep-Dives

#### A. `vendor/github.com/tonistiigi/fsutil` (`send.go` & `receive.go`)
- **Friction**: Upstream introduced path normalizations (`filepath.FromSlash` in `receive.go` and `filepath.ToSlash` in `send.go`) and `WithHardlinkReset(fs)` in `send.go`. These collided with EarthBuild's custom `verboseProgressCb` progress hook.
- **Resolution**: Integrated upstream normalizations and hardlink reset while preserving EarthBuild's `verboseProgressCb` invocation. Added missing `"path/filepath"` import to `send.go`.

#### B. Dependency & Vendor Manifests (`go.sum` & `vendor/modules.txt`)
- **Friction**: Merge conflicts arose around the `fsutil` replace directive.
- **Resolution**: Kept EarthBuild's replace directive in `vendor/modules.txt` while accepting upstream's updated module checksums in `go.sum`.

---

## 4. Verification & Test Results

### 4.1 Cross-Compilation Matrix (`GOOS=linux`)
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
# ok  github.com/moby/buildkit/solver  6.725s
go test -v ./util/converter/tarconverter/...
# ok  github.com/moby/buildkit/util/converter/tarconverter  0.323s
```

---

## 5. Security Assessment

| Assessment Dimension | Finding |
| :--- | :--- |
| **CVE-2023-45288 Resolution** | **Critical Security Fix**: The Go standard HTTP/2 implementation prior to `x/net v0.23.0` was vulnerable to unbounded memory allocation and CPU starvation when receiving continuous streams of empty or small `CONTINUATION` frames. This update protects `buildkitd`'s gRPC and HTTP/2 endpoints from remote denial-of-service attacks. |
| **Digest Integrity** | **Correctness & Supply Chain**: The `diffID` computation fix in `tarconverter` prevents incorrect layer checksum calculation when converting tar artifacts. |
| **Data Privacy** | All links and path references are strictly repository-relative; no environment or user data retained. |

---

## 6. Upstream Parity & Replacement Opportunities

| Upstream Feature | Overlapping Fork Subsystem | Can We Replace Fork Implementation? | Rationale & Road Ahead |
| :--- | :--- | :--- | :--- |
| **`fsutil` Hardlinks & Normalization** | `replace github.com/tonistiigi/fsutil => alexcb/fsutil` | **Strong Opportunity** | EarthBuild currently uses a fork of `fsutil` solely for the `verboseProgressCb` progress callback. Upstream's inclusion of hardlink resets and path normalizations narrows the difference. If `verboseProgressCb` can be contributed upstream or adapted to BuildKit's standard session progress events, EarthBuild can drop the `fsutil` fork replace directive completely. |
| **`diffID` Computation in `tarconverter`** | `exporter/earthlyoutputs` | **Adopted Upstream** | Upstream's `diffID` computation fixes ensure that layer digests generated during image export align with standard container registry formats. |

---

## 7. Next Steps

- Commit Step 3 (`v0.13.2` patch sync + report).
- Concludes the **`v0.13` series**.
- Ready to begin **`v0.14` series**: starting with **`v0.14.0`** (or patch sequence `v0.14.0` -> `v0.14.1`).
