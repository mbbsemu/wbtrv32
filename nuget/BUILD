load("//tools:version.bzl", "version_header")
load(":nuget_package.bzl", "nuget_package")

# The single version for both the NuGet package and WBTRV32.DLL (shown in
# Explorer > Properties > Details), as MAJOR.MINOR.POINT_RELEASE. Passed to
# `nuget pack -Version`, overriding whatever <version> is baked into
# wbtrv32.nuspec. After bumping it, run
#   bazel run //vstudio/wbtrv32:update_version
# to refresh the checked-in header the Visual Studio build reads; Bazel
# builds use this value directly, and //vstudio/wbtrv32:version_test fails
# if the checked-in copy is stale.
VERSION = "1.0.6"

version_header(
    name = "wbtrv32_version",
    out = "wbtrv32_version.h",
    version = VERSION,
    visibility = ["//vstudio/wbtrv32:__pkg__"],
)

# bazel build //nuget:package
# Output lands in bazel-bin/nuget/package/<id>.<version>.nupkg
# Tagged manual so wildcard builds/tests (e.g. `bazel test //...`) skip this
# slow nested cross-build; it only builds when named explicitly.
nuget_package(
    name = "package",
    version = VERSION,
    tags = ["manual"],
)

exports_files([
    "wbtrv32.nuspec",
    "build_nuget_package.sh",
])
