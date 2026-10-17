#!/usr/bin/env bash
set -euo pipefail

# Usage (via bazel run): update_version.sh <generated> <checked-in path>
# Copies the header generated from //nuget:BUILD over its checked-in copy, which
# the Visual Studio build reads since it can't consume Bazel outputs.

cp "$1" "$BUILD_WORKSPACE_DIRECTORY/$2"
chmod 644 "$BUILD_WORKSPACE_DIRECTORY/$2"
echo "Updated $2"
