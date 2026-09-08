load(":nuget_package.bzl", "nuget_package")

# Bump this to publish a new NuGet package version. Passed to
# `nuget pack -Version`, overriding whatever <version> is baked into
# wbtrv32.nuspec.
NUGET_PACKAGE_VERSION = "1.0.3"

# bazel build //nuget:package
# Output lands in bazel-bin/nuget/package/<id>.<version>.nupkg
nuget_package(
    name = "package",
    version = NUGET_PACKAGE_VERSION,
)

exports_files([
    "wbtrv32.nuspec",
    "build_nuget_package.sh",
])
