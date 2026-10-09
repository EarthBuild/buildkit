#!/usr/bin/env bash
# Usage: ./run.sh <buildkitd image> [lostedge-repro flags...]
# Starts the image as a privileged container with an empty cache on
# 127.0.0.1:${PORT:-12340}, runs lostedge-repro against it, and removes both.
# Exits with lostedge-repro's status: 0 none lost, 1 lost edges, 2 setup error.
set -euo pipefail
img=${1:?usage: ./run.sh <buildkitd image> [flags...]}
shift
port=${PORT:-12340}
name=lostedge-repro-bkd-$port
here=$(cd "$(dirname "$0")" && pwd)

cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker volume rm "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

(cd "$here" && go mod tidy && go build -o lostedge-repro .)
docker run -d --name "$name" --privileged -p "127.0.0.1:$port:1234" \
  -v "$name:/var/lib/buildkit" --entrypoint buildkitd "$img" \
  --addr tcp://0.0.0.0:1234 >/dev/null
for _ in $(seq 60); do
  docker logs "$name" 2>&1 | grep -q "running server" && break
  sleep 1
done
"$here/lostedge-repro" -addr "tcp://127.0.0.1:$port" "$@"
