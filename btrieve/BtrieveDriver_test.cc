#include "BtrieveDriver.h"
#include "BtrieveException.h"
#include "SqliteDatabase.h"
#include "gtest/gtest.h"

using namespace btrieve;

TEST(BtrieveDriver, LoadsAndConverts) {
  BtrieveDriver driver(new SqliteDatabase());

  try {
    driver.open("assets/MBBSEMU.DAT");
  } catch (const BtrieveException &ex) {
    fprintf(stderr, "Failure: %s\n", ex.getErrorMessage().c_str());
    throw ex;
  }
}