---
name: buildkit-verify
description: Runs `Makefile` and `Earthfile` verification targets (including `../earthbuild` checks). Use after code edits or merges.
argument-hint: "[lint | codegen | test | client | all]"
context: fork
user-invocable: true
---

## ROE
- **Scope**: `WRITE_WORKSPACE` (only for `make vendor`, `make generated-files`, `make archutil`; `../earthbuild` is `READ_ONLY`).
- **Allowed**: `Makefile` targets, `Earthfile` targets, and `../earthbuild` `Earthfile` targets with `--BUILDKIT_PROJECT=../buildkit`.
- **Forbidden**: Host tools (`go`, `golangci-lint`, `protoc`); mutating `../earthbuild`; adding `//nolint` without approval.

## Protocol
1. Run requested scopes (default: infer from `git status --short`):
   - **`lint`**: `make lint`, `make validate-shfmt`
   - **`codegen`**: `.proto` edited -> `make generated-files` + `make validate-generated-files`; `go.mod` edited -> `make vendor` + `make validate-vendor`; `make validate-archutil`
   - **`test`**: `earth +build`, `make test` (or `TESTPKGS=./<pkg>/... SKIP_INTEGRATION_TESTS=1 make test`)
   - **`client` (`../earthbuild`)**: In `../earthbuild`, run `earth +unit-test --BUILDKIT_PROJECT=../buildkit`, `earth +lint --BUILDKIT_PROJECT=../buildkit`, `earth ./buildkitd+buildkitd --BUILDKIT_PROJECT=../buildkit`
2. Triage `../earthbuild` failures:
   - Broken EarthBuild patch -> `STATUS: CHANGES_REQUESTED`
   - Upstream `moby/buildkit` API change -> `STATUS: ESCALATE` with `../earthbuild` call sites
3. Confirm unstaged `git status --short` and return `BLUF-SBAR`.
