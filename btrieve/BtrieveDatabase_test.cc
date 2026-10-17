#include "BtrieveDatabase.h"

#include <filesystem>
#include <fstream>

#include "Text.h"
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
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(database.getKeys().size(), 4u);
  EXPECT_EQ(database.getRecordLength(), 74u);
  EXPECT_EQ(database.getRecordCount(), 4u);
  EXPECT_EQ(database.getPhysicalRecordLength(), 90u);
  EXPECT_EQ(database.getPageLength(), 512u);
  EXPECT_EQ(database.getPageCount(), 6u);
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
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(database.getKeys().size(), 2u);
  EXPECT_EQ(database.getRecordLength(), 8u);
  EXPECT_EQ(database.getRecordCount(), 1024u);
  EXPECT_EQ(database.getPhysicalRecordLength(), 20u);
  EXPECT_EQ(database.getPageLength(), 512);
  EXPECT_EQ(database.getPageCount(), 1156u);
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
  for (size_t i = 0; i < sizeof(variableData) / sizeof(variableData[0]); ++i) {
    expectedData[variableData[i].key] = variableData[i].recordLength;
  }

  database.parseDatabase(
      _TEXT("assets/WGSMENU2.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 448u);

        return database.getRecordLength() == 448;
      },
      [&database, &expectedData](std::basic_string_view<uint8_t> record) {
        auto iter =
            expectedData.find(reinterpret_cast<const char *>(record.data()));

        EXPECT_TRUE(iter != expectedData.end());
        EXPECT_EQ(iter->second, record.size());

        expectedData.erase(iter);

        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(expectedData.size(), 0u);

  ASSERT_EQ(database.getRecordCount(),
            sizeof(variableData) / sizeof(variableData[0]));

  ASSERT_EQ(database.getKeys().size(), 1u);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getOffset(), 0);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getPosition(), 1);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getLength(), 17u);
}

TEST(BtrieveDatabase, LoadsFixedDatV6) {
  unsigned int recordCount = 0;
  BtrieveDatabase database;

  database.parseDatabase(
      _TEXT("assets/GALTELA.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 950u);

        return database.getRecordLength() == 950;
      },
      [&database, &recordCount](std::basic_string_view<uint8_t> record) {
        ++recordCount;

        EXPECT_EQ(record.size(), 950u);

        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(recordCount, 73u);
  ASSERT_EQ(database.getRecordCount(), 73u);

  ASSERT_EQ(database.getKeys().size(), 3u);

  ASSERT_EQ(database.getKeys()[0].getSegments().size(), 2u);
  ASSERT_EQ(database.getKeys()[0].getLength(), 32u);

  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getOffset(), 0);
  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getPosition(), 1);
  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getLength(), 16u);

  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getOffset(), 16);
  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getPosition(), 17);
  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getLength(), 16u);
}

TEST(BtrieveDatabase, LoadsMultiAcsDatV6) {
  BtrieveDatabase database;

  database.parseDatabase(
      _TEXT("assets/MULTIACS.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 128u);

        return database.getRecordLength() == 128;
      },
      [](std::basic_string_view<uint8_t> record) {
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(database.getRecordCount(), 0u);

  ASSERT_EQ(database.getKeys().size(), 3u);

  ASSERT_STREQ(database.getKeys().at(0).getACSName(), "ALLCAPS");
  ASSERT_STREQ(database.getKeys().at(2).getACSName(), "LOWER");
}

// NOCASE_KEY (0x400) shares a bit with NUMBERED_ACS (0x420), so a case
// insensitive key without an ACS must not be treated as having one.
TEST(BtrieveDatabase, LoadsCaseInsensitiveKeyWithoutACS) {
  std::filesystem::path dat(testing::TempDir());
  dat /= "NOCASE.DAT";
  std::filesystem::copy_file("assets/MBBSEMU.DAT", dat,
                             std::filesystem::copy_options::overwrite_existing);

  // key 0's attributes are at 0x118 in this v5 file, 0x0101 (duplicates and
  // extended type), so set the high byte to add NOCASE_KEY
  {
    std::fstream file(dat, std::ios::in | std::ios::out | std::ios::binary);
    file.seekg(0x118);
    char attributes[2];
    file.read(attributes, sizeof(attributes));
    ASSERT_EQ(attributes[0], 0x01);
    ASSERT_EQ(attributes[1], 0x01);

    file.seekp(0x119);
    file.put(0x05);
  }

  BtrieveDatabase database;
  database.parseDatabase(
      toWideString(dat).c_str(), []() { return true; },
      [](std::basic_string_view<uint8_t> record) {
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(database.getKeys().size(), 4u);
  const KeyDefinition &key = database.getKeys()[0].getPrimarySegment();
  EXPECT_EQ(key.getAttributes(), 0x0501);
  EXPECT_FALSE(key.requiresACS());
  EXPECT_EQ(key.getACS(), nullptr);
  EXPECT_EQ(database.getRecordCount(), 4u);
}

// A Btrieve v5 key table has one 30-byte entry per segment, not per key, so
// every key after a multi-segment key starts further along than its number
// suggests. Expected values are what Btrieve 6.15's Stat reports for this
// file (an empty data file from a Major BBS add-on).
TEST(BtrieveDatabase, LoadsV5KeysAfterSegmentedKeys) {
  BtrieveDatabase database;
  database.parseDatabase(
      _TEXT("assets/V5SEGKEY.DAT"), []() { return true; },
      [](std::basic_string_view<uint8_t> record) {
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  struct Segment {
    uint16_t offset;
    uint16_t length;
    KeyDataType type;
  };
  const std::vector<std::vector<Segment>> expected = {
      {{0, 4, KeyDataType::UnsignedBinary}},
      {{4, 30, KeyDataType::Zstring}},
      {{34, 30, KeyDataType::Zstring}},
      {{4, 30, KeyDataType::Zstring}, {0, 4, KeyDataType::UnsignedBinary}},
      {{34, 30, KeyDataType::Zstring}, {0, 4, KeyDataType::UnsignedBinary}},
      {{34, 30, KeyDataType::Zstring},
       {64, 1, KeyDataType::String},
       {0, 4, KeyDataType::UnsignedBinary}},
  };

  ASSERT_EQ(database.getKeys().size(), expected.size());
  EXPECT_EQ(database.getRecordLength(), 216u);
  for (size_t k = 0; k < expected.size(); ++k) {
    const auto &segments = database.getKeys()[k].getSegments();
    ASSERT_EQ(segments.size(), expected[k].size()) << "key " << k;
    for (size_t s = 0; s < segments.size(); ++s) {
      EXPECT_EQ(segments[s].getOffset(), expected[k][s].offset)
          << "key " << k << " segment " << s;
      EXPECT_EQ(segments[s].getLength(), expected[k][s].length)
          << "key " << k << " segment " << s;
      EXPECT_EQ(segments[s].getDataType(), expected[k][s].type)
          << "key " << k << " segment " << s;
    }
  }
}
