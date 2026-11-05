# earthbuild/buildkit

Fork of `moby/buildkit` used by `earthbuild/earthbuild` (`../earthbuild`).

## Invariants

- **Naming**: New code/docs/commits -> `earth` (CLI) and `EarthBuild` (project), never `earthly`. Keep literal `earthly` identifiers (`+earthly-docker`, `earthlyoutputs`, image/repo names) until renamed.
- **Minimal Diff**: Minimize divergence from `moby/buildkit`. Fixed upstream -> suggest version bump, not local patch.
- **No Host Tooling**: Never run host build/test/lint tools (`go test`, `golangci-lint`). Run `Makefile` or `Earthfile` targets only.
- **Client Check**: Verify version bumps and public API edits against `../earthbuild` (`--BUILDKIT_PROJECT=../buildkit`) without mutating `../earthbuild`.
- **No Export Dedup**: Never deduplicate/suppress progress logs in `gatewayClient.Export`; fix redundant solves in `earthbuild` (`earthfile2llb/converter.go`).

## Multi-Agent Protocol

- **ROE**: Obey skill scope (`READ_ONLY`, `WRITE_SCOPED(<paths>)`, `WRITE_WORKSPACE`). Halt with `STATUS: ESCALATE` if upstream breaks `custom-patches.md` contracts or `../earthbuild`.
- **BLUF-SBAR** (start every subagent reply):
  ```markdown
  STATUS: APPROVED | CHANGES_REQUESTED | ESCALATE | AGREED | COUNTER_PROPOSAL
  BLUF: <1-sentence outcome>

  - Situation: <scope/commands>
  - Background: <moby vs EarthBuild context>
  - Assessment: <findings (file:line)>
  - Recommendation: <next steps>
  ```
- **Skills**: Track custom patches in `.agents/skills/buildkit-sync/references/custom-patches.md`. After resolving upstream conflicts, retiring patches, finding new invariants, or identifying repeatable workflows with measurable gains, create/update skills via `buildkit-learn`.
