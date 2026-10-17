#!/usr/bin/env bash
set -euo pipefail

# Usage: version_test.sh <generated> <checked-in>
# Fails when the checked-in header no longer matches the version in //nuget:BUILD.

if ! diff -u "$2" "$1"; then
  echo "$2 is stale; run: bazel run //vstudio/wbtrv32:update_version" >&2
  exit 1
fi
