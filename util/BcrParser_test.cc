#include "util/BcrParser.h"

#include <cstring>
#include <filesystem>
#include <fstream>

#include "btrieve/AttributeMask.h"
#include "btrieve/BtrieveDatabase.h"
#include "btrieve/BtrieveDriver.h"
#include "btrieve/ErrorCode.h"
#include "btrieve/OperationCode.h"
#include "btrieve/SqliteDatabase.h"
#include "btrieve/TestBase.h"
#include "gtest/gtest.h"
#include "vstudio/wbtrv32/framework.h"
#include "vstudio/wbtrv32/wbtrv32.h"

extern "C" int __stdcall BTRCALL(WORD wOperation, LPVOID lpPositionBlock,
                                 LPVOID lpDataBuffer,
                                 LPDWORD lpdwDataBufferLength,
                                 LPVOID lpKeyBuffer, BYTE bKeyLength,
                                 CHAR sbKeyNumber);

using namespace btrieve;

namespace {

class BcrParserTest : public TestBase {
 protected:
  // Creates the database described by `description` at <temp>/<name>.dat
  // and opens the resulting <temp>/<name>.db.
  void createAndOpen(const bcr::Description &description, const char *name,
                     BtrieveDriver &driver) {
    std::filesystem::path path(tempPath->getTempPath());
    std::vector<uint8_t> buffer = bcr::toCreateBuffer(description, path);

    path /= std::string(name) + ".dat";
    std::string fileName = toStdString(path);
    DWORD dwDataBufferLength = static_cast<DWORD>(buffer.size());
    uint8_t posBlock[POSBLOCK_LENGTH] = {};
    ASSERT_EQ(BTRCALL(OperationCode::Create, posBlock, buffer.data(),
                      &dwDataBufferLength, fileName.data(), 0, -1),
              BtrieveError::Success);

    path.replace_extension("db");
    ASSERT_EQ(driver.open(toWideString(path).c_str()), BtrieveError::Success);
  }

  std::filesystem::path writeAcs(const char *fileName, const char *acsName,
                                 int (*transform)(int)) {
    wbtrv32::ACSCREATEDATA acs = {};
    acs.header = 0xAC;
    strncpy(acs.name, acsName, sizeof(acs.name));
    for (size_t i = 0; i < sizeof(acs.acs); ++i) {
      acs.acs[i] = static_cast<char>(transform(static_cast<int>(i)));
    }

    std::filesystem::path path(tempPath->getTempPath());
    path /= fileName;
    std::ofstream file(path, std::ios::binary);
    file.write(reinterpret_cast<const char *>(&acs), sizeof(acs));
    return path;
  }
};

void expectParseError(const char *text, const char *expectedMessage) {
  try {
    bcr::parse(text);
    FAIL() << "expected a parse error containing \"" << expectedMessage
           << "\"";
  } catch (const bcr::BcrParseException &ex) {
    EXPECT_NE(std::string(ex.what()).find(expectedMessage), std::string::npos)
        << ex.what();
  }
}

}  // namespace

TEST_F(BcrParserTest, CreatesDatabaseMatchingVariableDat) {
  bcr::Description description = bcr::parseFile("assets/VARIABLE.BCR");

  BtrieveDriver driver(new SqliteDatabase());
  createAndOpen(description, "variable", driver);

  BtrieveDatabase expected;
  expected.parseDatabase(
      _TEXT("assets/VARIABLE.DAT"), []() { return true; },
      [](std::basic_string_view<uint8_t>) {
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  EXPECT_EQ(driver.getRecordLength(), expected.getRecordLength());
  EXPECT_EQ(driver.isVariableLengthRecords(),
            expected.isVariableLengthRecords());
  EXPECT_EQ(driver.getRecordCount(), 0u);

  ASSERT_EQ(driver.getKeys().size(), expected.getKeys().size());
  for (size_t i = 0; i < expected.getKeys().size(); ++i) {
    EXPECT_EQ(driver.getKeys()[i].getSegments(),
              expected.getKeys()[i].getSegments())
        << "key " << i;
  }
}

TEST_F(BcrParserTest, ParsesAllFileElements) {
  bcr::Description description = bcr::parse(
      "/* every optional file element */\n"
      "RECORD=98 Variable=Y dupkey=2 truncate=y compress=n key=0 page=1024\n"
      "allocation=100 replace=n data=y fthreshold=20 vats=y balance=y\n"
      "usekeynum=n sysdataonrecord=n pagecompress=n\n");

  EXPECT_EQ(description.recordLength, 98);
  EXPECT_TRUE(description.variable);
  EXPECT_EQ(description.dupkey, 2);
  EXPECT_TRUE(description.truncate);
  EXPECT_FALSE(description.compress);
  EXPECT_TRUE(description.keys.empty());
  EXPECT_EQ(description.pageSize, 1024);
  EXPECT_EQ(description.allocation, 100);
  EXPECT_FALSE(description.keyOnly);
  EXPECT_EQ(description.freeSpaceThreshold, 20);
  EXPECT_TRUE(description.vats);
  EXPECT_TRUE(description.balance);

  std::vector<uint8_t> buffer = bcr::toCreateBuffer(description, ".");
  ASSERT_EQ(buffer.size(), sizeof(wbtrv32::FILESPEC));
  const auto *fileSpec =
      reinterpret_cast<const wbtrv32::FILESPEC *>(buffer.data());
  EXPECT_EQ(fileSpec->logicalFixedRecordLength, 98);
  EXPECT_EQ(fileSpec->pageSize, 1024);
  EXPECT_EQ(fileSpec->numberOfKeys, 0);
  EXPECT_EQ(fileSpec->numExtraPointers, 2);
  EXPECT_EQ(fileSpec->preallocatedPages, 100);
  EXPECT_EQ(fileSpec->fileFlags, 0x1 | 0x2 | 0x4 | 0x20 | 0x80 | 0x800);
}

TEST_F(BcrParserTest, ParsesKeyTypesAndSegments) {
  bcr::Description description = bcr::parse(
      "record=64 variable=n key=3 page=4096\n"
      "/* key 0: two segments */\n"
      "position=1 length=4 duplicates=n modifiable=n type=uns "
      "alternate=n segment=y\n"
      "position=5 length=10 duplicates=n modifiable=n type=zstring "
      "descending=y alternate=n segment=n\n"
      "/* key 1 */\n"
      "keynum=1 position=15 length=4 duplicates=y modifiable=y "
      "type=unsigned binary alternate=n repeatdup=y nullkey=allsegs value=ff "
      "segment=n\n"
      "/* key 2, with the old style null element */\n"
      "position=19 length=8 duplicates=n modifiable=y type=FLO alternate=n "
      "null=anyseg value=0 segment=n\n");

  ASSERT_EQ(description.keys.size(), 3u);
  ASSERT_EQ(description.keys[0].segments.size(), 2u);
  EXPECT_EQ(description.keys[0].segments[0].type, KeyDataType::UnsignedBinary);
  EXPECT_TRUE(description.keys[0].segments[0].segment);
  EXPECT_EQ(description.keys[0].segments[1].type, KeyDataType::Zstring);
  EXPECT_TRUE(description.keys[0].segments[1].descending);

  ASSERT_EQ(description.keys[1].segments.size(), 1u);
  const bcr::KeySegment &key1 = description.keys[1].segments[0];
  EXPECT_EQ(key1.type, KeyDataType::UnsignedBinary);
  EXPECT_TRUE(key1.repeatingDuplicates);
  EXPECT_EQ(key1.nullKey, bcr::NullKey::AllSegments);
  EXPECT_EQ(key1.nullValue, 0xFF);

  const bcr::KeySegment &key2 = description.keys[2].segments[0];
  EXPECT_EQ(key2.type, KeyDataType::Float);
  EXPECT_EQ(key2.nullKey, bcr::NullKey::AnySegment);

  BtrieveDriver driver(new SqliteDatabase());
  createAndOpen(description, "segments", driver);

  ASSERT_EQ(driver.getKeys().size(), 3u);
  ASSERT_EQ(driver.getKeys()[0].getSegments().size(), 2u);
  EXPECT_EQ(driver.getKeys()[0].getSegments()[0],
            KeyDefinition(0, 4, 0, KeyDataType::UnsignedBinary,
                          UseExtendedDataType | SegmentedKey, true, 0, 0, 0,
                          "", std::vector<char>()));
  EXPECT_TRUE(driver.getKeys()[0].getSegments()[0].isSegment());
  EXPECT_EQ(driver.getKeys()[0].getSegments()[1],
            KeyDefinition(0, 10, 4, KeyDataType::Zstring,
                          UseExtendedDataType | DescendingKeySegment, false, 0,
                          1, 0, "", std::vector<char>()));
  EXPECT_FALSE(driver.getKeys()[0].getSegments()[1].isSegment());
  EXPECT_EQ(driver.getKeys()[1].getPrimarySegment(),
            KeyDefinition(1, 4, 14, KeyDataType::UnsignedBinary,
                          UseExtendedDataType | Duplicates | Modifiable |
                              RepeatingDuplicatesKey | NullAllSegments,
                          false, 0, 0, 0xFF, "", std::vector<char>()));
  EXPECT_EQ(driver.getKeys()[2].getPrimarySegment(),
            KeyDefinition(2, 8, 18, KeyDataType::Float,
                          UseExtendedDataType | Modifiable | NullAnySegment,
                          false, 0, 0, 0, "", std::vector<char>()));
}

TEST_F(BcrParserTest, SharedAcsAtEndOfFile) {
  writeAcs("UPPER.ALT", "UPPER", toupper);

  bcr::Description description = bcr::parse(
      "record=32 variable=n key=2 page=512\n"
      "position=1 length=16 duplicates=n modifiable=n type=string "
      "alternate=y segment=n\n"
      "position=17 length=16 duplicates=y modifiable=n type=string "
      "alternate=y segment=n\n"
      "name=UPPER.ALT\n");
  EXPECT_EQ(description.acsFileName, "UPPER.ALT");

  BtrieveDriver driver(new SqliteDatabase());
  createAndOpen(description, "sharedacs", driver);

  ASSERT_EQ(driver.getKeys().size(), 2u);
  for (const Key &key : driver.getKeys()) {
    ASSERT_TRUE(key.requiresACS());
    EXPECT_STREQ(key.getACSName(), "UPPER");
    EXPECT_EQ(key.getACS()['a'], 'A');
  }
}

TEST_F(BcrParserTest, PerKeyAcs) {
  writeAcs("LOWER.ALT", "LOWER", tolower);
  writeAcs("UPPER.ALT", "UPPER", toupper);

  bcr::Description description = bcr::parse(
      "record=32 variable=n key=3 page=512\n"
      "position=1 length=8 duplicates=n modifiable=n type=string "
      "alternate=y segment=n name=LOWER.ALT\n"
      "position=9 length=8 duplicates=n modifiable=n type=integer "
      "alternate=n segment=n\n"
      "position=17 length=16 duplicates=n modifiable=n type=string "
      "alternate=y segment=n\n"
      "name=UPPER.ALT\n");

  std::vector<uint8_t> buffer =
      bcr::toCreateBuffer(description, tempPath->getTempPath());
  const auto *keySpecs = reinterpret_cast<const wbtrv32::KEYSPEC *>(
      buffer.data() + sizeof(wbtrv32::FILESPEC));
  EXPECT_EQ(keySpecs[0].attributes & MultipleACS, MultipleACS);
  EXPECT_EQ(keySpecs[0].acsNumber, 0);
  EXPECT_EQ(keySpecs[1].attributes & NumberedACS, 0);
  EXPECT_EQ(keySpecs[2].attributes & MultipleACS, MultipleACS);
  EXPECT_EQ(keySpecs[2].acsNumber, 1);

  BtrieveDriver driver(new SqliteDatabase());
  createAndOpen(description, "perkeyacs", driver);

  ASSERT_EQ(driver.getKeys().size(), 3u);
  EXPECT_STREQ(driver.getKeys()[0].getACSName(), "LOWER");
  EXPECT_EQ(driver.getKeys()[0].getACS()['A'], 'a');
  EXPECT_FALSE(driver.getKeys()[1].requiresACS());
  EXPECT_STREQ(driver.getKeys()[2].getACSName(), "UPPER");
  EXPECT_EQ(driver.getKeys()[2].getACS()['a'], 'A');
}

TEST_F(BcrParserTest, MissingAcsFile) {
  bcr::Description description = bcr::parse(
      "record=16 variable=n key=1 page=512\n"
      "position=1 length=16 duplicates=n modifiable=n type=string "
      "alternate=y segment=n\n");

  EXPECT_THROW(bcr::toCreateBuffer(description, tempPath->getTempPath()),
               bcr::BcrParseException);

  description.acsFileName = "DOESNOTEXIST.ALT";
  EXPECT_THROW(bcr::toCreateBuffer(description, tempPath->getTempPath()),
               bcr::BcrParseException);
}

TEST_F(BcrParserTest, CreateFailsIfFileExists) {
  bcr::Description description =
      bcr::parse("record=16 variable=n key=0 page=512");

  BtrieveDriver driver(new SqliteDatabase());
  createAndOpen(description, "exists", driver);

  std::vector<uint8_t> buffer = bcr::toCreateBuffer(description, ".");
  std::filesystem::path path(tempPath->getTempPath());
  path /= "exists.dat";
  std::string fileName = toStdString(path);
  DWORD dwDataBufferLength = static_cast<DWORD>(buffer.size());
  EXPECT_EQ(BTRCALL(OperationCode::Create, nullptr, buffer.data(),
                    &dwDataBufferLength, fileName.data(), 0, -1),
            BtrieveError::FileAlreadyExists);
}

TEST(BcrParser, Errors) {
  expectParseError("", "empty");
  expectParseError("record=8 key=0 page=512", "expected 'variable='");
  expectParseError("record=8 variable=n page=512 key=0",
                   "expected 'key=' but found 'page='");
  expectParseError("record=8 variable=x key=0 page=512", "must be y or n");
  expectParseError("record=abc variable=n key=0 page=512", "must be a number");
  expectParseError("record=8 variable=n key=0 page=600", "invalid page size");
  expectParseError("record=8 variable=n truncate=y key=0 page=512",
                   "truncate=y requires variable=y");
  expectParseError("record=8 variable=n key=0 page=512 /* oops",
                   "unterminated comment");
  expectParseError("record=8 variable=n key=1 page=512",
                   "expected 'position=' but reached end of file");
  expectParseError(
      "record=8 variable=n key=1 page=512\n"
      "position=7 length=4 duplicates=n modifiable=n type=int alternate=n "
      "segment=n",
      "extends past the record length");
  expectParseError(
      "record=8 variable=n key=1 page=512\n"
      "position=1 length=4 duplicates=n modifiable=n type=wstring "
      "alternate=n segment=n",
      "unsupported key type 'wstring'");
  expectParseError(
      "record=8 variable=n key=1 page=512\n"
      "position=1 length=4 duplicates=n modifiable=n type=int alternate=y "
      "segment=n",
      "alternate=y only applies to string");
  expectParseError(
      "record=8 variable=n key=1 page=512\n"
      "position=1 length=2 duplicates=n modifiable=n type=float alternate=n "
      "segment=n",
      "float keys must be 4 or 8 bytes");
  expectParseError(
      "record=8 variable=n key=1 page=512\n"
      "position=1 length=4 duplicates=n modifiable=n type=int alternate=n "
      "nullkey=allsegs segment=n",
      "requires 'value='");
  expectParseError(
      "record=8 variable=n key=1 page=512\n"
      "position=1 length=4 duplicates=n modifiable=n type=int alternate=n "
      "repeatdup=y segment=n",
      "repeatdup=y requires duplicates=y");
  expectParseError(
      "record=8 variable=n key=1 page=512\n"
      "position=1 length=4 duplicates=n modifiable=n type=int alternate=n "
      "segment=n\n"
      "position=5 length=4 duplicates=n modifiable=n type=int alternate=n "
      "segment=n",
      "line 3: unexpected 'position='");
  expectParseError(
      "record=8 variable=n key=1 page=512\n"
      "keynum=3 position=1 length=4 duplicates=n modifiable=n type=int "
      "alternate=n segment=n",
      "non-sequential key numbers are not supported");
}
