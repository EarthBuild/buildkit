---
name: buildkit-sync
description: Syncs `moby/buildkit` releases via `buildkit-audit`, `buildkit-resolve`, `buildkit-verify`, and `buildkit-learn`.
argument-hint: "[moby-tag]"
user-invocable: true
---

## ROE
- **Scope**: Orchestrator (`READ_ONLY` in main context; delegates writes to subagents; `../earthbuild` is `READ_ONLY`).
- **Remotes**: `moby` (`moby/buildkit`), `origin` (`EarthBuild/buildkit`).
- **Default `<target>`**: Omitted -> next minor release after `<base>` with highest stable patch (`git tag -l 'v0.<next-minor>.*' --sort=v:refname | grep -E '^v0\.[0-9]+\.[0-9]+$' | tail -n1`).

## Workflow
1. **Audit (`buildkit-audit`)**: Diff `<base>..HEAD` vs `<base>..<target>` against `.agents/skills/buildkit-sync/references/custom-patches.md` and check `../earthbuild` imports.
2. **Resolve (`buildkit-resolve`)**: Build 2-layer `gh-stack`:
   - **Layer 1 (`gh stack init chore-buildkit-merge-<target>-unresolved --base "$fork"`)**: Require clean worktree, capture `fork=$(git branch --show-current)` before `gh stack init`, merge via `git merge-tree --write-tree --merge-base=<base> "$fork" <target>` + `git read-tree -u --reset <tree>`, and commit `<<<<<<<`/`=======`/`>>>>>>>` conflict markers as a single-parent commit for squash-merge PR #1.
   - **Layer 2 (`gh stack add chore-buildkit-merge-<target>`)**: Branch atop Layer 1; resolve conflict markers, preserve active ADRs, drop retired patches, keep `version.Package` (`github.com/EarthBuild/buildkit`), and update codegen/vendor so PR #2 isolates conflict resolutions.
3. **Verify (`buildkit-verify`)**: Run `all` (`lint`, `codegen`, `test`, and `client` against `../earthbuild`).
4. **Update Ledger (`buildkit-learn`)**: Update baseline tag and retired patches in `.agents/skills/buildkit-sync/references/custom-patches.md`.
5. Summarize each step using `BLUF-SBAR`.
