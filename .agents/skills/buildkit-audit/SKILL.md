---
name: buildkit-audit
description: Audits custom patches against `moby/buildkit` tags, checks `../earthbuild` impact, and maps conflict hotspots.
argument-hint: "[moby-tag]"
context: fork
user-invocable: true
---

## ROE
- **Scope**: `READ_ONLY` (`.` and `../earthbuild`).
- **Allowed**: Read-only `git` (`log`, `diff`, `show`, `merge-base`, `status`), `codebase-memory-mcp`, `gopls-mcp-server`, file reads.
- **Forbidden**: File writes or mutating `git` commands.

## Protocol
1. Read `<base>` from `.agents/skills/buildkit-sync/references/custom-patches.md`. Default `<target>` (if omitted): next minor release with highest stable patch (`git tag -l 'v0.<next-minor>.*' --sort=v:refname | grep -E '^v0\.[0-9]+\.[0-9]+$' | tail -n1`).
2. Intersect non-vendor diffs: `git diff --name-only <base>..HEAD -- ':!vendor'` vs `git diff --name-only <base>..<target> -- ':!vendor'`.
3. Check `github.com/moby/buildkit` imports in `../earthbuild` against upstream signature changes.
4. Per overlap/ADR:
   - Solved upstream -> flag for retirement.
   - Unused (no callers in `.` or `../earthbuild`) -> flag for deletion.
   - Superseded by upstream API -> trace `../earthbuild` call chain into fork code, find upstream equivalent (`git log -S<symbol>`, `git tag --contains <sha>`), and suggest retirement to user: upstream API, `../earthbuild` change needed, behaviour given up.
   - Still required -> rate conflict risk (`LOW` | `MEDIUM` | `HIGH`) and list symbols to preserve.
5. Return `BLUF-SBAR`.
