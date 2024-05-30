#include "BtrieveDatabase.h"
#include "gtest/gtest.h"

using namespace btrieve;

TEST(BtrieveDatabase, LoadsFile) {
  unsigned int recordCount = 0;
  BtrieveDatabase *database = nullptr;
  char blankACS[256];

  memset(blankACS, 0, sizeof(blankACS));

  ASSERT_TRUE(BtrieveDatabase::parseDatabase(
      "assets/MBBSEMU.DAT",
      [&database](const BtrieveDatabase &db) {
        database = new BtrieveDatabase(db);
        return true;
      },
      [&database, &recordCount](std::basic_string_view<uint8_t> record) {
        EXPECT_EQ(record.size(), database->getRecordLength());
        ++recordCount;
        return true;
      }));

  ASSERT_TRUE(database != nullptr);
  EXPECT_EQ(database->getKeys().size(), 4);
  EXPECT_EQ(database->getRecordLength(), 74);
  EXPECT_EQ(database->getRecordCount(), 4);
  EXPECT_EQ(database->getPhysicalRecordLength(), 90);
  EXPECT_EQ(database->getPageLength(), 512);
  EXPECT_EQ(database->getPageCount(), 5);
  EXPECT_FALSE(database->isLogKeyPresent());
  EXPECT_FALSE(database->isVariableLengthRecords());

  EXPECT_FALSE(database->getKeys()[0].isComposite());
  EXPECT_FALSE(database->getKeys()[1].isComposite());
  EXPECT_FALSE(database->getKeys()[2].isComposite());
  EXPECT_FALSE(database->getKeys()[3].isComposite());

  EXPECT_EQ(database->getKeys()[0].getPrimarySegment(),
            KeyDefinition(0, 32, 2, KeyDataType::Zstring,
                          Duplicates | UseExtendedDataType, false, 0, 0, 0,
                          blankACS));

  EXPECT_EQ(database->getKeys()[1].getPrimarySegment(),
            KeyDefinition(1, 4, 34, KeyDataType::Integer,
                          Modifiable | UseExtendedDataType, false, 0, 0, 0,
                          blankACS));

  EXPECT_EQ(database->getKeys()[2].getPrimarySegment(),
            KeyDefinition(2, 32, 38, KeyDataType::Zstring,
                          Duplicates | Modifiable | UseExtendedDataType, false,
                          0, 0, 0, blankACS));

  EXPECT_EQ(database->getKeys()[3].getPrimarySegment(),
            KeyDefinition(3, 4, 70, KeyDataType::AutoInc, UseExtendedDataType,
                          false, 0, 0, 0, blankACS));

  EXPECT_EQ(recordCount, database->getRecordCount());

  delete database;
}