TEST_SRCS = glob(["*_test.cc"])

SQLITE_DATABASE_SRCS = ["SqliteDatabase.cc", "SqliteUtil.cc"]

SQLITE_DATABASE_HDRS = ["SqliteDatabase.h", "SqliteUtil.h", "SqlitePreparedStatement.h", "SqliteReader.h", "SqliteTransaction.h"]

cc_library(
  name = "btrieve",
  hdrs = glob(["*.h"], exclude = SQLITE_DATABASE_HDRS),
  visibility = ["//visibility:public"],
  srcs = glob(["*.cc"], exclude = TEST_SRCS + SQLITE_DATABASE_SRCS),
)

cc_library(
  name = "sqlite_database",
  srcs = SQLITE_DATABASE_SRCS,
  hdrs = SQLITE_DATABASE_HDRS,
  visibility = ["//visibility:public"],
  deps = [":btrieve", "//sqlite"],
)

cc_test(
  name = "tests",
  srcs = TEST_SRCS,
  data = ["//assets:test_dats"],
  deps = [
    ":btrieve",
    ":sqlite_database",
    "@gtest",
    "@gtest//:gtest_main",
 ],
)
