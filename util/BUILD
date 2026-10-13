load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")

cc_binary(
  name = "database_parser",
  srcs = ["DatabaseParser.cc"],
  deps = ["//btrieve"],
)

cc_binary(
  name = "database_converter",
  srcs = ["DatabaseConverter.cc"],
  deps = ["//btrieve", "//btrieve:sqlite_database",],
)

cc_library(
  name = "bcr_parser",
  srcs = ["BcrParser.cc"],
  hdrs = ["BcrParser.h"],
  deps = ["//btrieve", "//vstudio/wbtrv32:libwbtrv32"],
)

cc_binary(
  name = "create_database",
  srcs = ["CreateDatabase.cc"],
  deps = [
    ":bcr_parser",
    "//btrieve",
    "//vstudio/wbtrv32:libwbtrv32",
  ],
)

cc_test(
  name = "tests",
  srcs = glob(["*_test.cc"]),
  data = ["//assets:test_dats"],
  deps = [
    ":bcr_parser",
    "//btrieve",
    "//btrieve:sqlite_database",
    "//btrieve:testbase",
    "//vstudio/wbtrv32:libwbtrv32",
    "@googletest//:gtest",
    "@googletest//:gtest_main",
  ],
)
