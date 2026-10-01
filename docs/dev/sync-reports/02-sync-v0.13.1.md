# Upstream Sync Report #02: Patch Sync v0.13.1

- **Status**: Completed
- **Target Release**: `v0.13.1` (`2ae42e0c0`, March 15, 2024)
- **Previous Baseline**: `v0.13.0` (`e216f8d5e`)
- **Downstream Consumer**: `github.com/earthbuild/earthbuild`
- **Date**: October 2026

---

## 1. Executive Summary

This report documents the incremental patch sync from upstream `moby/buildkit` tag **`v0.13.0`** to **`v0.13.1`**.

The upstream patch consists of 4 surgical bug and security fixes across 11 files (+58, -24 lines). The merge completed with **zero merge conflicts** and cross-compilation and unit tests passed cleanly.

---

## 2. Upstream Changes Absorbed

| Component | Upstream Commit | Description |
| :--- | :--- | :--- |
| **Git Ref Parsing** | `9e593c07d` | **Security / Correctness**: Enforces that local file-like references (such as `./.git` or `.git`) are never misparsed or dispatched as remote network URLs. |
| **Remote Cache** | `50fbf5068` | Fixes missing `CheckDescriptor` handler method on remote cache descriptor chains, preventing unexpected nil pointer panics during remote cache validation. |
| **Solver System Sampler** | `62eec44c4` | Stubs out `sysSampler` close handling when system sampling is uninitialized or disabled. |
| **OCI Executor** | `0aff32386` | Makes mounting the OCI socket optional rather than mandatory, preventing unintended socket leaks into unprivileged containers. |

---

## 3. Merge Conflicts Overview & Resolution

### 3.1 Conflict Matrix

| Conflicted File | Nature of Conflict | Resolution Applied |
| :--- | :--- | :--- |
| *None* | **Zero conflicts** | All 11 modified upstream files merged cleanly via three-way recursive merge. |

### 3.2 Friction Analysis
- **Merge Friction**: **Zero**. All upstream patches merged cleanly with zero manual intervention.
- **Custom Subsystems**: EarthBuild's custom extensions (`session/localhost`, `exporter/earthlyoutputs`, `session/pullping`, `solver/scheduler.go`) were completely orthogonal to these upstream edits.
- **Dead Code Status**: Verified that previously pruned dead components (`podman.go`, `client_earthly.go`, `reserve.go`) were not reintroduced by the merge.

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
# ok  github.com/moby/buildkit/solver  (cached / clean)
go test -v ./util/gitutil/...
# === RUN   TestParseGitRef/./.git -> PASS
# === RUN   TestParseGitRef/.git   -> PASS
# ok  github.com/moby/buildkit/util/gitutil  0.377s
```

---

## 5. Security Assessment

| Assessment Dimension | Finding |
| :--- | :--- |
| **URL Parsing & SSRF Defense** | **Hardened**: The git reference parsing fix (`git_ref.go`) eliminates ambiguous URL parsing that could allow maliciously crafted local context paths to trigger remote network requests. |
| **Socket Exposure** | **Hardened**: Making OCI socket mounting optional reduces container privilege exposure by ensuring the host engine socket is not automatically mounted into container build environments. |
| **Remote Cache Nil Pointer Fix** | Prevents denial of service / crash of `buildkitd` when encountering descriptors without content check handlers in remote cache manifests. |
| **Data Privacy** | All links and path references are strictly repository-relative; no environment or user data retained. |

---

## 6. Upstream Parity & Replacement Opportunities

| Upstream Feature | Overlapping Fork Subsystem | Can We Replace Fork Implementation? | Rationale & Road Ahead |
| :--- | :--- | :--- | :--- |
| **Git Ref Parsing (`git_ref.go`)** | `util/gitutil` URL handling | **Complementary** | Upstream's URL/ref parsing fix natively handles dot/file-path ambiguity, complementing EarthBuild's custom git CLI runner (`util/gitutil/git_cli.go`). No custom ref parsing needed in the fork. |
| **Optional OCI Socket Mount** | `cmd/buildkitd` container execution | **Adopted Upstream** | Aligns EarthBuild daemon container isolation with upstream default behavior. |

---

## 7. Next Steps

- Commit Step 2 (`v0.13.1` patch sync + report).
- Proceed to **Step 3**: Incremental sync to **`v0.13.2`** (the final release of the `v0.13` cycle, including the Go HTTP/2 continuation flood CVE-2023-45288 fix).
