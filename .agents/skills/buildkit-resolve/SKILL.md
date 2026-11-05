---
name: buildkit-resolve
description: Builds 2-layer `gh-stack` PRs for `moby/buildkit` tags, resolves merge conflicts with minimal diff, and retires patches.
argument-hint: "[moby-tag]"
context: fork
user-invocable: false
---

## ROE
- **Scope**: `WRITE_WORKSPACE`
- **Allowed**: `gh-stack` layer branches, conflict edits, retiring approved patches, `codebase-memory-mcp`, `gopls-mcp-server` (`go_diagnostics`).
- **Forbidden**: Unnecessary divergence from `moby/buildkit`; resolving conflicts in Layer 1.

## Protocol
1. **Layer 1 (`gh stack init chore-buildkit-merge-<target>-unresolved`)**: Run `tree=$(git merge-tree --write-tree --merge-base=<base> HEAD <target> || true); git read-tree -u --reset "${tree%%$'\n'*}"` using `<base>` from `custom-patches.md`; keep all `<<<<<<<`/`=======`/`>>>>>>>` conflict markers as a single-parent commit for squash-merge PR #1.
2. **Layer 2 (`gh stack add chore-buildkit-merge-<target>`)**: Branch atop Layer 1; follow `buildkit-audit` plan and `.agents/skills/buildkit-sync/references/custom-patches.md`.
3. Resolve conflict markers in Layer 2: preserve active ADRs (`ADR-001`..`ADR-005`), delete retired patches, keep `version.Package` (`github.com/EarthBuild/buildkit`), and run codegen/vendor updates so PR #2 isolates conflict resolutions.
4. Run `go_diagnostics` on modified `.go` files and return `BLUF-SBAR`.
