# Repository Information

This repository is a fork of [moby/buildkit](https://github.com/moby/buildkit). Its primary use is by [earthbuild/earthbuild](https://github.com/earthbuild/earthbuild).

## Naming

The project is being renamed from Earthly to EarthBuild. When writing new code,
comments, docs or commit messages:

- Use `earth` for the CLI / build tool (not `earthly`).
- Use `EarthBuild` for the project name (not `Earthly`).
- Leave existing `earthly` identifiers alone where they are literal: Earthfile
  target names (`+earthly-docker`) and published image/repo names. Those change
  only as part of the rename itself.

## Guidelines

- Any code changes must be kept to a minimum to reduce the potential for merge conflicts with upstream `moby/buildkit`.
- **Upstream Fixes & Version Bumping:** If upstream `moby/buildkit` has already resolved an issue in a newer version, suggest bumping BuildKit instead of implementing a temporary local fix.
- **Double Export & Output Duplication:** Do not patch BuildKit to deduplicate or suppress progress logs across concurrent solves or intermediate exports (`gatewayClient.Export`). BuildKit streams vertex output to all attached callers; preventing redundant solve requests belongs in EarthBuild (`earthfile2llb/converter.go`).
