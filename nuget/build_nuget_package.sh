#!/usr/bin/env bash
set -euo pipefail

# Builds the wbtrv32 shared library for every supported platform via bazel,
# stages the results into the NuGet runtimes/ layout, and packs
# nuget/wbtrv32.nuspec into a .nupkg.
#
# Usage: nuget/build_nuget_package.sh [output_dir] [version]
#   output_dir  Where the .nupkg is written. Relative paths are resolved
#               against the caller's working directory. Defaults to
#               nuget/dist.
#   version     Overrides the <version> in wbtrv32.nuspec.
#
# Requires bazel, and either a `nuget` CLI on PATH, mono + $NUGET_EXE
# pointing at nuget.exe, or mono + nuget.exe at ~/.local/bin/nuget.exe.
#
# This script also runs as the underlying action of the //nuget:package
# bazel rule (see nuget_package.bzl), which shells out to it directly.
# Because it recursively invokes `bazel build`, it must not run under the
# invoking bazel's own output_base/lock -- doing so would deadlock, since
# that server is already busy running this very action. So all nested
# bazel invocations below use a dedicated output_base and convenience
# symlink prefix, kept separate from the caller's own bazel state.

CALLER_PWD="$(pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"

OUTPUT_DIR="${1:-$SCRIPT_DIR/dist}"
case "$OUTPUT_DIR" in
  /*) ;;
  *) OUTPUT_DIR="$CALLER_PWD/$OUTPUT_DIR" ;;
esac
VERSION="${2:-}"

BAZEL_TARGET="//vstudio/wbtrv32:wbtrv32"

NESTED_OUTPUT_BASE="${NUGET_BAZEL_OUTPUT_BASE:-$HOME/.cache/bazel-wbtrv32-nuget}"
NESTED_SYMLINK_PREFIX="bazel-nuget-nested-"
NESTED_BIN_DIR="${NESTED_SYMLINK_PREFIX}bin"
# The zig-cc windows toolchain caches under $HOME/.cache/zig. Bazel's
# per-action sandbox only auto-grants write access to paths under /tmp (a
# private tmpfs per action) -- a real $HOME (even a brand new directory
# under it) is mounted read-only, so the zig compile actions fail with
# "ReadOnlyFileSystem"/"unable to open global cache directory" unless HOME
# itself resolves under /tmp for the nested bazel invocations.
NESTED_HOME="${NUGET_BAZEL_HOME:-${TMPDIR:-/tmp}/bazel-wbtrv32-nuget-home}"
mkdir -p "$NESTED_HOME"

# rid:bazel-config:lib-extension (empty config = native/default build)
PLATFORMS=(
  "linux-x64::so"
  "linux-arm64:linux_arm64:so"
  "linux-arm:linux_arm32:so"
  "win-x64:windows_amd64:dll"
  "win-x86:windows_x86:dll"
  "osx-x64:macos_amd64:dylib"
  "osx-arm64:macos_arm64:dylib"
)

if command -v nuget >/dev/null 2>&1; then
  NUGET_CMD=(nuget)
elif [[ -n "${NUGET_EXE:-}" ]]; then
  NUGET_CMD=(mono "$NUGET_EXE")
elif [[ -f "$HOME/.local/bin/nuget.exe" ]]; then
  NUGET_CMD=(mono "$HOME/.local/bin/nuget.exe")
else
  echo "error: no nuget CLI found. Install 'nuget' on PATH, or set NUGET_EXE to a nuget.exe path (run via mono)." >&2
  exit 1
fi

# Resolved explicitly (rather than relying on PATH) because when this
# script runs as a bazel action, PATH is bazel's restricted default, not
# the invoking shell's -- it wouldn't include e.g. ~/bin, a common
# bazelisk install location.
if command -v bazel >/dev/null 2>&1; then
  BAZEL_BIN="$(command -v bazel)"
elif [[ -n "${BAZEL_BIN:-}" ]]; then
  :
elif [[ -x "$HOME/bin/bazel" ]]; then
  BAZEL_BIN="$HOME/bin/bazel"
elif [[ -x "/usr/local/bin/bazel" ]]; then
  BAZEL_BIN="/usr/local/bin/bazel"
else
  echo "error: bazel not found. Add it to PATH, or set BAZEL_BIN to its path." >&2
  exit 1
fi

STAGING_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_DIR"' EXIT

cd "$REPO_ROOT"

for entry in "${PLATFORMS[@]}"; do
  IFS=':' read -r rid config ext <<< "$entry"
  echo "=== Building $BAZEL_TARGET for $rid ==="
  # --spawn_strategy=local: when this script itself runs as the
  # //nuget:package bazel action, letting this nested build sandbox its
  # own actions means sandboxing-inside-a-build-action, which breaks the
  # zig toolchain's cache-directory setup. Running unsandboxed avoids it.
  HOME="$NESTED_HOME" "$BAZEL_BIN" --output_base="$NESTED_OUTPUT_BASE" build \
    --symlink_prefix="$NESTED_SYMLINK_PREFIX" \
    --spawn_strategy=local \
    ${config:+--config="$config"} \
    "$BAZEL_TARGET"

  dest_dir="$STAGING_DIR/runtimes/$rid/native"
  mkdir -p "$dest_dir"
  cp "$NESTED_BIN_DIR/vstudio/wbtrv32/wbtrv32.$ext" "$dest_dir/wbtrv32.$ext"
done

mkdir -p "$OUTPUT_DIR"

PACK_ARGS=(pack "$SCRIPT_DIR/wbtrv32.nuspec" -OutputDirectory "$OUTPUT_DIR" -Properties "stagingDir=$STAGING_DIR" -NonInteractive)
if [[ -n "$VERSION" ]]; then
  PACK_ARGS+=(-Version "$VERSION")
fi

echo "=== Packing NuGet package ==="
"${NUGET_CMD[@]}" "${PACK_ARGS[@]}"

echo "Package written to $OUTPUT_DIR"
