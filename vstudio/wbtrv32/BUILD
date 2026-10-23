load("@rules_cc//cc:defs.bzl", "cc_library", "cc_shared_library", "cc_test")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
load("//tools:version.bzl", "windows_resource")

TEST_SRCS = glob(["*_test.cc"])

cc_library(
  name = "libwbtrv32",
  srcs = ["bad_data.cc", "framework.cc", "logging.cc", "wbtrv32.cpp",
    "wbtrv32_version.h"] +
    select({
      # DllMain runs processAttach()/processDetach() on Windows; elsewhere a
      # constructor/destructor in wbtrv32.cpp does
      "@platforms//os:windows": ["dllmain.cpp"],
      "//conditions:default": [],
    }),
  hdrs = ["bad_data.h", "framework.h", "logging.h", "wbtrv32.h"],
  # On the library, so the DLL and everything else linking it get them.
  # CoCreateGuid/StringFromGUID2 (framework.cc) live in ole32.dll, and
  # MessageBoxW (DebugAttach) in user32.dll; MSVC pulls in both implicitly
  # via $(CoreLibraryDependencies), which Bazel has no equivalent of, so link
  # them explicitly. Abseil's stack trace symbolizer needs dbghelp.dll, which
  # its BUILD files only link for MSVC.
  linkopts = select({
    "@platforms//os:windows": ["-lole32", "-luser32", "-ldbghelp"],
    "//conditions:default": [],
  }),
  visibility = ["//test:__pkg__", "//util:__pkg__"],
  deps = [
    "//btrieve",
    "//btrieve:sqlite_database",
    "@abseil-cpp//absl/base:log_severity",
    "@abseil-cpp//absl/base:no_destructor",
    "@abseil-cpp//absl/log",
    "@abseil-cpp//absl/log:globals",
    "@abseil-cpp//absl/log:log_entry",
    "@abseil-cpp//absl/log:log_sink",
    "@abseil-cpp//absl/time",
  ],
)

cc_shared_library(
  name = "wbtrv32",
  # //nuget:package depends on this only to piggyback on bazel's dependency
  # graph for staleness tracking; see nuget/nuget_package.bzl.
  visibility = ["//nuget:__pkg__"],
  deps = [":libwbtrv32"],
  additional_linker_inputs = select({
    "@platforms//os:windows": [":wbtrv32_res"],
    "@platforms//os:macos": [],
    "//conditions:default": ["wbtrv32.lds"],
  }),
  shared_lib_name = select({
    "@platforms//os:windows": "wbtrv32.dll",
    "@platforms//os:macos": "wbtrv32.dylib",
    "//conditions:default": "wbtrv32.so",
  }),
  # win_def_file (and wbtrv32.def, still used by the real MSVC build) only
  # takes effect when the cc_toolchain enables Bazel's "targets_windows"
  # feature, which the windows-gnu zig-cc toolchain doesn't set (it's an
  # MSVC-toolchain-only feature) and has no other way to consume a .def
  # file. Exports are controlled at compile time instead; see
  # -fno-dll-export-fns in .bazelrc and WBTRV32_EXPORT in framework.h.
  # gcc_toolchain (used for arm32/arm64 cross builds) doesn't auto-link
  # libstdc++ for C++ sources; harmless no-op under the native toolchain.
  # Not needed (and not valid) for the zig-cc toolchains (windows-gnu and
  # macos-none), which link their C++ runtime in statically by default.
  user_link_flags = select({
    # zig's linker takes the compiled .res directly, like MSVC's link.exe.
    # ole32 and dbghelp come from libwbtrv32's linkopts.
    "@platforms//os:windows": ["$(location :wbtrv32_res)"],
    "@platforms//os:macos": [],
    # wbtrv32.lds does wbtrv32.def's job on Linux.
    "//conditions:default": [
      "-lstdc++",
      "-Wl,--version-script=$(location wbtrv32.lds)",
    ],
  }),
)

# The DLL's version resource, built from the version in //nuget:BUILD.
windows_resource(
  name = "wbtrv32_res",
  src = "wbtrv32.rc",
  hdrs = ["//nuget:wbtrv32_version"],
)

# The shell scripts below run on the build machine. Built for Windows, they'd
# need Bazel's native launcher, which only compiles with MSVC, so skip them
# there (e.g. bazel build --config=windows_amd64 //...).
NOT_WINDOWS = select({
  "@platforms//os:windows": ["@platforms//:incompatible"],
  "//conditions:default": [],
})

# The Visual Studio build can't read //nuget:BUILD, so it compiles wbtrv32.rc
# against a checked-in copy of the generated header. Refresh it with
#   bazel run //vstudio/wbtrv32:update_version
sh_binary(
  name = "update_version",
  target_compatible_with = NOT_WINDOWS,
  srcs = ["//tools:update_version.sh"],
  args = ["$(rootpath //nuget:wbtrv32_version)", "vstudio/wbtrv32/wbtrv32_version.h"],
  data = ["//nuget:wbtrv32_version"],
)

sh_test(
  name = "version_test",
  target_compatible_with = NOT_WINDOWS,
  srcs = ["//tools:version_test.sh"],
  args = ["$(rootpath //nuget:wbtrv32_version)", "$(rootpath wbtrv32_version.h)"],
  data = ["//nuget:wbtrv32_version", "wbtrv32_version.h"],
)

cc_test(
  name = "tests",
  srcs = TEST_SRCS + ["framework.h","wbtrv32.h"],
  data = ["//assets:test_dats", ":wbtrv32"],
  deps = [
    ":libwbtrv32",
    "//btrieve",
    "//btrieve:sqlite_database",
    "//btrieve:testbase",
    "@googletest//:gtest",
    "@googletest//:gtest_main",
 ],
)

