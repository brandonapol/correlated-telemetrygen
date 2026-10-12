#!/usr/bin/env bash
# Fails if a GitHub Action is referenced by tag or branch instead of a full commit SHA, or a
# container image by tag instead of a digest. Tags can be moved to point at other code; a SHA
# or digest can't. Dependabot keeps the pins (and the version comments next to them) current.
set -euo pipefail

cd "$(dirname "$0")/.."
status=0

# uses: owner/repo[/path]@<40-hex SHA>. Local actions (./path) are exempt.
while IFS= read -r line; do
  echo "action not pinned to a commit SHA: $line" >&2
  status=1
done < <(grep -HnE '^\s*-?\s*uses:\s*[^.[:space:]]' .github/workflows/*.yml | grep -vE '@[0-9a-f]{40}([[:space:]]|$)' || true)

# FROM image[:tag]@sha256:<digest>. Earlier build stages (FROM build) are exempt.
while IFS= read -r line; do
  echo "base image not pinned to a digest: $line" >&2
  status=1
done < <(grep -HniE '^\s*FROM\s' Dockerfile | grep -viE '@sha256:[0-9a-f]{64}' || true)

# Images in workflows and scripts, such as `docker run registry/name:tag`.
image='[a-z0-9][a-z0-9.-]*(/[a-z0-9._-]+)+:[A-Za-z0-9][A-Za-z0-9._-]*'
while IFS= read -r line; do
  echo "image not pinned to a digest: $line" >&2
  status=1
done < <(grep -HnoE "$image(@sha256:[0-9a-f]{64})?" --exclude=check-pins.sh .github/workflows/*.yml scripts/* | grep -vE "@sha256:" || true)

exit "$status"
