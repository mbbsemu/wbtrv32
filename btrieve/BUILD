TEST_SRCS = glob(["*_test.cc"])

cc_library(
  name = "btrieve",
  hdrs = glob(["*.h"]),
  srcs = glob(["*.cc"], exclude = TEST_SRCS),
  deps = []
)

cc_test(
  name = "tests",
  srcs = TEST_SRCS,
  deps = [
    ":btrieve",
    "@gtest",
    "@gtest//:gtest_main",
 ],
)
