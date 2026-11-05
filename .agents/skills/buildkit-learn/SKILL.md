---
name: buildkit-learn
description: Creates, updates, and validates `.agents/skills/` and `custom-patches.md`. Use after upstream merges, patch retirements, or workflow improvements.
argument-hint: "[skill-name]"
context: fork
user-invocable: true
---

## ROE
- **Scope**: `WRITE_SCOPED(.agents/skills/, AGENTS.md)`
- **Allowed**: File read/write in scope, `codebase-memory-mcp`, read-only `git`, `earth +validate-skills`.
- **Forbidden**: Editing Go/build/vendor files; creating `CLAUDE.md`; breaking `.claude/skills -> ../.agents/skills` symlink.

## New Skill Gate
- **Create new skill** when a recurring workflow delivers measurable improvement: fewer tool calls/retries, lower main-context token use (`context: fork`), or elimination of a repeated failure mode.
- **Extend existing skill** if workflow overlaps an existing skill's scope; retire/merge skills that become redundant.
- **Justification**: State measured gain (baseline vs improved steps/tokens/errors) in `BLUF-SBAR` `Assessment`.

## Style & Schema Rules
1. **Telegraphic**: Imperative verbs, `Key: Value`, `<condition> -> <action>`. No filler, adjectives, or articles.
2. **Frontmatter Allowlist**: Only `name`, `description`, `argument-hint`, `compatibility`, `context`, `disable-model-invocation`, `license`, `metadata`, `user-invocable`.
3. **No Duplication / Links**: Never repeat `AGENTS.md` rules or use `[label](path)` links; use plain inline code (`buildkit-audit`, `solver/scheduler.go`).
4. **Budgets** (`scripts/validate_skills.py`): `AGENTS.md` `<= 30L/200W`; `description` `<= 20W/160C`; `SKILL.md` `<= 40L/220W`.

## Protocol
1. Create/edit `.agents/skills/<name>/SKILL.md` or `.agents/skills/buildkit-sync/references/custom-patches.md` within budgets.
2. Run `earth +validate-skills`.
3. Return `BLUF-SBAR`.
