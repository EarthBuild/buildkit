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
1. Read `.agents/skills/buildkit-sync/references/custom-patches.md`.
2. Intersect non-vendor diffs: `git diff --name-only <base>..HEAD -- ':!vendor'` vs `git diff --name-only <base>..<target> -- ':!vendor'`.
3. Check `github.com/moby/buildkit` imports in `../earthbuild` against upstream signature changes.
4. Per overlap/ADR:
   - Solved upstream -> flag for retirement.
   - Still required -> rate conflict risk (`LOW` | `MEDIUM` | `HIGH`) and list symbols to preserve.
5. Return `BLUF-SBAR`.
