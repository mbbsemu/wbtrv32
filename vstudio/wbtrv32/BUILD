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
  shared_lib_name = "wbtrv32.so",
  # gcc_toolchain (used for arm32/arm64 cross builds) doesn't auto-link
  # libstdc++ for C++ sources; harmless no-op under the native toolchain.
  user_link_flags = ["-lstdc++"],
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

