"""Rules that turn the version in //nuget:BUILD into a header and a compiled
Windows resource, so every build stamps the same version into the DLL."""

def _version_header_impl(ctx):
    parts = ctx.attr.version.split(".")
    if len(parts) != 3 or not all([p.isdigit() and int(p) <= 65535 for p in parts]):
        fail("version %r must be MAJOR.MINOR.POINT_RELEASE" % ctx.attr.version)
    ctx.actions.write(
        output = ctx.outputs.out,
        content = "\n".join([
            "// Generated from the version in //nuget:BUILD. Do not edit; change",
            "// it there and run `bazel run //vstudio/wbtrv32:update_version`.",
            "#ifndef WBTRV32_VERSION_H",
            "#define WBTRV32_VERSION_H",
            "",
            "#define WBTRV32_VERSION_BIN %s,0" % ",".join(parts),
            "#define WBTRV32_VERSION_STR \"%s\"" % ctx.attr.version,
            "",
            "#endif  // WBTRV32_VERSION_H",
            "",
        ]),
    )

version_header = rule(
    implementation = _version_header_impl,
    attrs = {
        "out": attr.output(mandatory = True),
        "version": attr.string(
            mandatory = True,
            doc = "MAJOR.MINOR.POINT_RELEASE",
        ),
    },
)

def _windows_resource_impl(ctx):
    # Stage the .rc next to the generated headers so its #include "..."
    # finds them rather than any stale checked-in copies beside the source.
    staged = []
    for f in [ctx.file.src] + ctx.files.hdrs:
        out = ctx.actions.declare_file("%s/%s" % (ctx.label.name, f.basename))
        ctx.actions.symlink(output = out, target_file = f)
        staged.append(out)
    res = ctx.actions.declare_file(ctx.label.name + ".res")
    ctx.actions.run(
        outputs = [res],
        inputs = staged + ctx.files._zig_lib,
        executable = ctx.executable._zig,
        arguments = ["rc", "/nologo", staged[0].path, res.path],
        # zig rc compiles its resinator compiler into the zig cache on first
        # use, which takes about a minute. The sandbox gives every action a
        # fresh /tmp, so run unsandboxed to keep the compiled copy around.
        env = {
            "ZIG_GLOBAL_CACHE_DIR": "/tmp/zig-cache",
            "ZIG_LOCAL_CACHE_DIR": "/tmp/zig-cache",
        },
        execution_requirements = {"no-sandbox": "1"},
        mnemonic = "ZigRc",
        progress_message = "Compiling Windows resource %{label}",
    )
    return [DefaultInfo(files = depset([res]))]

windows_resource = rule(
    implementation = _windows_resource_impl,
    doc = "Compiles a .rc file into a .res that the zig-cc linker accepts.",
    attrs = {
        "src": attr.label(allow_single_file = [".rc"], mandatory = True),
        "hdrs": attr.label_list(allow_files = [".h"]),
        "_zig": attr.label(
            default = "@zig_sdk//:zig",
            allow_single_file = True,
            executable = True,
            cfg = "exec",
        ),
        "_zig_lib": attr.label(default = "@zig_config//:all", cfg = "exec"),
    },
)
