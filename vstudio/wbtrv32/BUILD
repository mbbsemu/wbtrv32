load("@rules_cc//cc:defs.bzl", "cc_library", "cc_shared_library", "cc_test")

TEST_SRCS = glob(["*_test.cc"])

cc_library(
  name = "libwbtrv32",
  srcs = ["bad_data.cc", "framework.cc", "wbtrv32.cpp"],
  hdrs = ["bad_data.h", "framework.h", "wbtrv32.h"],
  deps = [
    "//btrieve",
    "//btrieve:sqlite_database",
  ],
)

cc_shared_library(
  name = "wbtrv32",
  deps = [":libwbtrv32"],
  shared_lib_name = select({
    "@platforms//os:windows": "wbtrv32.dll",
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
  # Not needed (and not valid) for the windows-gnu zig-cc toolchain, which
  # links libstdc++ in statically by default.
  user_link_flags = select({
    # CoCreateGuid/StringFromGUID2 (framework.cc) live in ole32.dll; MSVC
    # pulls in ole32.lib implicitly via $(CoreLibraryDependencies), which
    # Bazel has no equivalent of, so link it explicitly.
    "@platforms//os:windows": ["-lole32"],
    "//conditions:default": ["-lstdc++"],
  }),
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

