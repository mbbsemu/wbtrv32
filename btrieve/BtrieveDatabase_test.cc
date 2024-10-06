#include "BtrieveDatabase.h"

#include "gtest/gtest.h"

using namespace btrieve;

TEST(BtrieveDatabase, LoadsMBBSEmuDat) {
  unsigned int recordCount = 0;
  BtrieveDatabase database;
  std::string acsName;
  std::vector<char> blankACS;

  database.parseDatabase(
      _TEXT("assets/MBBSEMU.DAT"), []() { return true; },
      [&database, &recordCount](std::basic_string_view<uint8_t> record) {
        EXPECT_EQ(record.size(), database.getRecordLength());
        ++recordCount;
        return true;
      });

  ASSERT_EQ(database.getKeys().size(), 4);
  EXPECT_EQ(database.getRecordLength(), 74);
  EXPECT_EQ(database.getRecordCount(), 4);
  EXPECT_EQ(database.getPhysicalRecordLength(), 90);
  EXPECT_EQ(database.getPageLength(), 512);
  EXPECT_EQ(database.getPageCount(), 6);
  EXPECT_FALSE(database.isVariableLengthRecords());

  EXPECT_FALSE(database.getKeys()[0].isComposite());
  EXPECT_FALSE(database.getKeys()[1].isComposite());
  EXPECT_FALSE(database.getKeys()[2].isComposite());
  EXPECT_FALSE(database.getKeys()[3].isComposite());

  EXPECT_EQ(database.getKeys()[0].getPrimarySegment(),
            KeyDefinition(0, 32, 2, KeyDataType::Zstring,
                          Duplicates | UseExtendedDataType, false, 0, 0, 0,
                          acsName, blankACS));

  EXPECT_EQ(database.getKeys()[1].getPrimarySegment(),
            KeyDefinition(1, 4, 34, KeyDataType::Integer,
                          Modifiable | UseExtendedDataType, false, 0, 0, 0,
                          acsName, blankACS));

  EXPECT_EQ(database.getKeys()[2].getPrimarySegment(),
            KeyDefinition(2, 32, 38, KeyDataType::Zstring,
                          Duplicates | Modifiable | UseExtendedDataType, false,
                          0, 0, 0, acsName, blankACS));

  EXPECT_EQ(database.getKeys()[3].getPrimarySegment(),
            KeyDefinition(3, 4, 70, KeyDataType::AutoInc, UseExtendedDataType,
                          false, 0, 0, 0, acsName, blankACS));

  EXPECT_EQ(recordCount, database.getRecordCount());
}

typedef struct _tagPOPULATE {
  uint32_t magic;
  uint16_t key1;
  uint16_t key2;
} POPULATE;

TEST(BtrieveDatabase, LoadsVariableDat) {
  unsigned int recordCount = 0;
  BtrieveDatabase database;
  std::string acsName;
  std::vector<char> blankACS;

  database.parseDatabase(
      _TEXT("assets/VARIABLE.DAT"), []() { return true; },
      [&database, &recordCount](std::basic_string_view<uint8_t> record) {
        EXPECT_GT(record.size(), database.getRecordLength() - 1);

        const POPULATE *data =
            reinterpret_cast<const POPULATE *>(record.data());
        EXPECT_EQ(data->magic, 0xDEADBEEF);
        EXPECT_EQ(data->key1, recordCount % 64);
        EXPECT_EQ(data->key2, recordCount);
        EXPECT_EQ(record.size(), sizeof(POPULATE) + recordCount);
        for (unsigned int i = 0; i < record.size() - sizeof(POPULATE); ++i) {
          EXPECT_EQ(record.data()[sizeof(POPULATE) + i],
                    static_cast<uint8_t>(i));
        }

        ++recordCount;
        return true;
      });

  ASSERT_EQ(database.getKeys().size(), 2);
  EXPECT_EQ(database.getRecordLength(), 8);
  EXPECT_EQ(database.getRecordCount(), 1024);
  EXPECT_EQ(database.getPhysicalRecordLength(), 20);
  EXPECT_EQ(database.getPageLength(), 512);
  EXPECT_EQ(database.getPageCount(), 1156);
  EXPECT_TRUE(database.isVariableLengthRecords());

  EXPECT_FALSE(database.getKeys()[0].isComposite());
  EXPECT_FALSE(database.getKeys()[1].isComposite());

  EXPECT_EQ(database.getKeys()[0].getPrimarySegment(),
            KeyDefinition(0, 2, 4, KeyDataType::Integer,
                          Duplicates | UseExtendedDataType, false, 0, 0, 0,
                          acsName, blankACS));

  EXPECT_EQ(database.getKeys()[1].getPrimarySegment(),
            KeyDefinition(1, 2, 6, KeyDataType::Integer, UseExtendedDataType,
                          false, 0, 0, 0, acsName, blankACS));

  EXPECT_EQ(recordCount, database.getRecordCount());
}

typedef struct _tagV6VARIABLEDATA {
  const char *key;
  uint32_t recordLength;
} VARIABLEDATA;

static const VARIABLEDATA variableData[] = {
    {"AACCOUNT", 448},   {"ADLTAGGED", 450},  {"ADOORS", 448},
    {"AEMAIL", 448},     {"AEXIT", 448},      {"AEXIT2", 450},
    {"AFINGER", 450},    {"AFORUMS", 448},    {"AFTP", 450},
    {"AFTPMENU", 828},   {"AGLOBALS", 448},   {"AHELP", 448},
    {"AHOST", 450},      {"AINFO", 828},      {"ALIBRARY", 448},
    {"ANETWORK", 1080},  {"APOLLS", 448},     {"AQWK", 448},
    {"AREGISTRY", 448},  {"AREMOTE", 448},    {"ARLOGIN", 450},
    {"ATELE", 450},      {"ATELNET", 450},    {"ATOP", 2214},
    {"AWORLNK", 450},    {"AYOURSYS", 448},   {"CACCOUNT", 450},
    {"CBROADCAST", 450}, {"CCSFTP", 450},     {"CDOORS", 450},
    {"CFINGER", 450},    {"CHOST", 450},      {"CLIBRARY", 450},
    {"CMESSAGE", 450},   {"CNETWORK", 1080},  {"CPOLLS", 450},
    {"CREGISTRY", 450},  {"CREMOTE", 450},    {"CRLOGIN", 450},
    {"CTELE", 448},      {"CTELNET", 450},    {"CTOP", 1962},
    {"CVIDSYSOP", 450},  {"CWORLDLINK", 702}, {"CWORLNK", 450},
    {"CWORMSG", 450},
};

TEST(BtrieveDatabase, LoadsVariableDatV6) {
  BtrieveDatabase database;

  std::unordered_map<std::string, uint32_t> expectedData;
  for (int i = 0; i < sizeof(variableData) / sizeof(variableData[0]); ++i) {
    expectedData[variableData[i].key] = variableData[i].recordLength;
  }

  database.parseDatabase(
      _TEXT("assets/WGSMENU2.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 448);

        return database.getRecordLength() == 448;
      },
      [&database, &expectedData](std::basic_string_view<uint8_t> record) {
        auto iter =
            expectedData.find(reinterpret_cast<const char *>(record.data()));

        EXPECT_TRUE(iter != expectedData.end());
        EXPECT_EQ(iter->second, record.size());

        expectedData.erase(iter);

        return true;
      });

  ASSERT_EQ(expectedData.size(), 0);

  ASSERT_EQ(database.getRecordCount(),
            sizeof(variableData) / sizeof(variableData[0]));

  ASSERT_EQ(database.getKeys().size(), 1);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getOffset(), 0);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getPosition(), 1);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getLength(), 17);
}

TEST(BtrieveDatabase, LoadsFixedDatV6) {
  unsigned int recordCount = 0;
  BtrieveDatabase database;

  database.parseDatabase(
      _TEXT("assets/GALTELA.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 950);

        return database.getRecordLength() == 950;
      },
      [&database, &recordCount](std::basic_string_view<uint8_t> record) {
        ++recordCount;

        EXPECT_EQ(record.size(), 950);

        return true;
      });

  ASSERT_EQ(recordCount, 73);
  ASSERT_EQ(database.getRecordCount(), 73);

  ASSERT_EQ(database.getKeys().size(), 3);

  ASSERT_EQ(database.getKeys()[0].getSegments().size(), 2);
  ASSERT_EQ(database.getKeys()[0].getLength(), 32);

  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getOffset(), 0);
  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getPosition(), 1);
  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getLength(), 16);

  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getOffset(), 16);
  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getPosition(), 17);
  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getLength(), 16);
}

TEST(BtrieveDatabase, LoadsMultiAcsDatV6) {
  BtrieveDatabase database;

  database.parseDatabase(
      _TEXT("assets/MULTIACS.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 128);

        return database.getRecordLength() == 128;
      },
      [](std::basic_string_view<uint8_t> record) { return true; });

  ASSERT_EQ(database.getRecordCount(), 0);

  ASSERT_EQ(database.getKeys().size(), 3);

  ASSERT_STREQ(database.getKeys().at(0).getACSName(), "ALLCAPS");
  ASSERT_STREQ(database.getKeys().at(2).getACSName(), "LOWER");
}
