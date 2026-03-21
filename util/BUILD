load("@rules_cc//cc:defs.bzl", "cc_binary")

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
