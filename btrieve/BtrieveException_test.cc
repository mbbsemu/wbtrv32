#include "BtrieveException.h"

#include "gtest/gtest.h"

using namespace btrieve;

TEST(BtrieveException, CopyKeepsTheError) {
  const BtrieveException original(BtrieveError::NotBtrieveFile, "bad page %d",
                                  7);
  const BtrieveException copy(original);

  EXPECT_EQ(copy.getError(), BtrieveError::NotBtrieveFile);
  EXPECT_EQ(copy.getErrorMessage(), "bad page 7");
}
