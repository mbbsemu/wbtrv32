TESTBASE_SRCS = ["TestBase.cc"]

TEST_SRCS = glob(["*_test.cc"])

SQLITE_DATABASE_SRCS = ["SqliteDatabase.cc", "SqliteUtil.cc"]

SQLITE_DATABASE_HDRS = [
  "SqliteDatabase.h",
  "SqlitePreparedStatement.h",
  "SqliteQuery.h",
  "SqliteReader.h",
  "SqliteTransaction.h",
  "SqliteUtil.h",
]

cc_library(
  name = "btrieve",
  hdrs = glob(["*.h"], exclude = SQLITE_DATABASE_HDRS + TESTBASE_SRCS),
  visibility = ["//visibility:public"],
  srcs = glob(["*.cc"], exclude = TEST_SRCS + TESTBASE_SRCS + SQLITE_DATABASE_SRCS),
)

cc_library(
  name = "sqlite_database",
  srcs = SQLITE_DATABASE_SRCS,
  hdrs = SQLITE_DATABASE_HDRS,
  visibility = ["//visibility:public"],
  deps = [":btrieve", "//sqlite"],
)

cc_library(
  name = "testbase",
  srcs = TESTBASE_SRCS,
  hdrs = ["TestBase.h"],
  visibility = ["//visibility:public"],
  testonly = True,
  deps = [
    ":btrieve",
    "@gtest",
    "@gtest//:gtest_main"
  ],
)

cc_test(
  name = "tests",
  srcs = TEST_SRCS,
  data = ["//assets:test_dats"],
  deps = [
    ":btrieve",
    ":testbase",
    ":sqlite_database",
    "@gtest",
    "@gtest//:gtest_main",
 ],
)
