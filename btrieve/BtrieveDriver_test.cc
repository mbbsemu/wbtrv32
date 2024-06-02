#include "BtrieveDriver.h"
#include "SqliteDatabase.h"
#include "gtest/gtest.h"

using namespace btrieve;

TEST(BtrieveDriver, LoadsAndConverts) {
  BtrieveDriver driver(new SqliteDatabase());

  driver.open("assets/MBBSEMU.DAT");
}