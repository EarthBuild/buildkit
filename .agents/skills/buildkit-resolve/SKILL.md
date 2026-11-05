---
name: buildkit-resolve
description: Builds 2-layer `gh-stack` PRs for `moby/buildkit` tags, resolves merge conflicts with minimal diff, and retires patches.
argument-hint: "[moby-tag]"
context: fork
user-invocable: false
---

## ROE
- **Scope**: `WRITE_WORKSPACE`
- **Allowed**: `gh-stack` layer branches, `git add`, `git commit`, conflict edits, retiring approved patches, `make generated-files`, `make vendor`, `codebase-memory-mcp`, `gopls-mcp-server` (`go_diagnostics`).
- **Forbidden**: Unnecessary divergence from `moby/buildkit`; resolving conflicts in Layer 1.

## Protocol
1. **Layer 1 (`gh stack init chore-buildkit-merge-<target>-unresolved --base "$fork"`)**: Require clean `git status --short` (else `STATUS: ESCALATE`) and record `fork=$(git branch --show-current)` before `gh stack init`. Read `<base>` from `custom-patches.md` (default `<target>`: next minor release with highest stable patch). Run `tree=$(git merge-tree --write-tree --merge-base=<base> "$fork" <target> || true); git read-tree -u --reset "${tree%%$'\n'*}"; git add -A && git commit -m "chore: merge <target> (unresolved)"` so PR #1 commits all `<<<<<<<`/`=======`/`>>>>>>>` conflict markers as a single-parent commit.
2. **Layer 2 (`gh stack add chore-buildkit-merge-<target>`)**: Branch atop Layer 1; follow `buildkit-audit` plan and `.agents/skills/buildkit-sync/references/custom-patches.md`.
3. Resolve conflict markers in Layer 2: preserve active ADRs (`ADR-001`..`ADR-006`), delete retired patches, keep `version.Package` (`github.com/EarthBuild/buildkit`), and run codegen/vendor updates so PR #2 isolates conflict resolutions.
4. Clean auto-merges can break fork code -> compile all packages + tests in container.
5. Run `go_diagnostics` on modified `.go` files and return `BLUF-SBAR`.
