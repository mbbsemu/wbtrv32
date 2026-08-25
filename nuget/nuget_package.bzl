"""Wraps build_nuget_package.sh so `bazel build //nuget:package` produces
the wbtrv32 NuGet package directly into bazel-bin.

The action recursively shells out to `bazel build` (once per target
platform, via a dedicated nested output_base -- see build_nuget_package.sh)
and to a system `nuget`/mono toolchain, so it can't be sandboxed, remotely
executed, or remotely cached. `dep` is never read by the script itself; it
exists purely so Bazel's ordinary dependency graph invalidates this action
whenever the underlying C++ sources change, since the declared `inputs`
otherwise wouldn't capture that.
"""

def _nuget_package_impl(ctx):
    output_dir = ctx.actions.declare_directory(ctx.label.name)

    ctx.actions.run(
        outputs = [output_dir],
        inputs = [ctx.file.script, ctx.file.nuspec] + ctx.files.dep,
        executable = ctx.file.script,
        arguments = [output_dir.path, ctx.attr.version],
        use_default_shell_env = True,
        execution_requirements = {
            "no-sandbox": "1",
            "no-cache": "1",
            "no-remote-cache": "1",
            "no-remote-exec": "1",
            "local": "1",
            "requires-network": "1",
        },
        mnemonic = "NugetPack",
        progress_message = "Cross-building wbtrv32 and packing NuGet package %{label}",
    )
    return [DefaultInfo(files = depset([output_dir]))]

nuget_package = rule(
    implementation = _nuget_package_impl,
    attrs = {
        "version": attr.string(
            mandatory = True,
            doc = "Overrides <version> from the nuspec via `nuget pack -Version`.",
        ),
        "script": attr.label(
            allow_single_file = True,
            default = "//nuget:build_nuget_package.sh",
        ),
        "nuspec": attr.label(
            allow_single_file = True,
            default = "//nuget:wbtrv32.nuspec",
        ),
        "dep": attr.label(
            default = "//vstudio/wbtrv32:wbtrv32",
            doc = "Not read directly; tracked only so source changes invalidate this action.",
        ),
    },
)
