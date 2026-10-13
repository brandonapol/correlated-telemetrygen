#!/usr/bin/env bash
# Runs the generator against Weaver live-check, which compares every span, log and resource with
# the registry in model/ and the policies in policies/, using the matchers in .weaver.toml.
# Exits non-zero if any finding is a violation. The JSON report is written to $REPORT_DIR.
set -euo pipefail

cd "$(dirname "$0")/.."
WEAVER_IMAGE=${WEAVER_IMAGE:-otel/weaver:v0.27.0@sha256:3049b4079049d4abb1b5632f511ada2c33505a1c60f3f8535e93f87f0696f056}
REPORT_DIR=${REPORT_DIR:-live-check-report}
DURATION=${DURATION:-10s}
mkdir -p "$REPORT_DIR"

# Build first: live-check stops after 10 seconds without telemetry.
bin=$(mktemp -d)/correlated-telemetrygen
go build -o "$bin" .

name=weaver-live-check-$$
docker run -d --name "$name" --network host --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD:/work" -w /work \
  "$WEAVER_IMAGE" registry live-check -r model --v2 --format json -o "/work/$REPORT_DIR" >/dev/null
trap 'docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$(dirname "$bin")"' EXIT

for _ in $(seq 60); do
  curl -sf http://127.0.0.1:4320/health >/dev/null && break
  sleep 1
done
curl -sf http://127.0.0.1:4320/health >/dev/null || { docker logs "$name"; exit 1; }

# Every database and payment failure mode is turned up so the check sees each code path.
generator=0
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 \
  "$bin" --duration "$DURATION" --rate 20 --seed 1 --error-ratio 0.25 --db-error-ratio 0.1 || generator=$?

status=$(docker wait "$name")
docker logs "$name" 2>&1 | grep -v -e 'is not yet stable' -e 'vdir_cache' -e '^$' || true
if [ -f "$REPORT_DIR/live_check.json" ]; then
  python3 scripts/summarize-live-check.py "$REPORT_DIR/live_check.json"
fi
if [ "$generator" -ne 0 ]; then
  echo "the generator failed to deliver its telemetry (exit $generator)" >&2
  exit "$generator"
fi
exit "$status"
