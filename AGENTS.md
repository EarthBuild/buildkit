# Repository Information

This repository is a fork of [moby/buildkit](https://github.com/moby/buildkit). Its primary use is by [earthbuild/earthbuild](https://github.com/earthbuild/earthbuild).

## Guidelines

- Any code changes must be kept to a minimum to reduce the potential for merge conflicts with upstream `moby/buildkit`.
- **Upstream Fixes & Version Bumping:** If upstream `moby/buildkit` has already resolved an issue in a newer version, suggest bumping BuildKit instead of implementing a temporary local fix.
- **Double Export & Output Duplication:** Do not patch BuildKit to deduplicate or suppress progress logs across concurrent solves or intermediate exports (`gatewayClient.Export`). BuildKit streams vertex output to all attached callers; preventing redundant solve requests belongs in EarthBuild (`earthfile2llb/converter.go`).
