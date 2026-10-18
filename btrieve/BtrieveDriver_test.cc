#include "BtrieveDriver.h"

#include <stdlib.h>

#include <cstdio>

#include "BtrieveException.h"
#include "SqliteDatabase.h"
#include "gtest/gtest.h"
#ifndef WIN32
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#else
#define _WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif
#include <sys/types.h>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "TestBase.h"

/* Data layout as follows:

sqlite> select * from data_t;
    id          data        key_0       key_1       key_2       key_3
    ----------  ----------  ----------  ----------  ----------  ----------
    1                       Sysop       3444        3444        1
    2                       Sysop       7776        7776        2
    3                       Sysop       1052234073  StringValu  3
    4                       Sysop       -615634567  stringValu  4
*/

using namespace btrieve;

static int sqlite_exec(sqlite3 *db, const char *sql,
                       std::function<void(int, char **, char **)> onData) {
  return sqlite3_exec(
      db, sql,
      [](void *param, int numResults, char **data, char **columns) {
        std::function<void(int, char **, char **)> *onData =
            reinterpret_cast<std::function<void(int, char **, char **)> *>(
                param);
        (*onData)(numResults, data, columns);
        return 0;
      },
      &onData, nullptr);
}

static std::string fromPath(const std::filesystem::path &path) {
#ifdef WIN32
  char ret[MAX_PATH];
  size_t unused;
  wcstombs_s(&unused, ret, path.c_str(), _TRUNCATE);
  return std::string(ret);
#else
  return path.c_str();
#endif
}

class BtrieveDriverTest : public TestBase {};

TEST_F(BtrieveDriverTest, MultithreadMutexingEnabled) {
  ASSERT_NE(sqlite3_threadsafe(), 0);
}

TEST_F(BtrieveDriverTest, LoadsAndConverts) {
  std::string convertedDbPath;

  {
    std::string acsName;
    std::vector<char> blankACS;
    BtrieveDriver driver(new SqliteDatabase());

    auto mbbsEmuDat = tempPath->copyToTempPath("assets/MBBSEMU.DAT");
    driver.open(mbbsEmuDat.c_str());

    std::filesystem::path dbPath(mbbsEmuDat);
    dbPath.remove_filename();
    dbPath /= "MBBSEMU.db";
    convertedDbPath = fromPath(dbPath);

#ifdef WIN32
    ASSERT_NE(GetFileAttributesA(convertedDbPath.c_str()),
              INVALID_FILE_ATTRIBUTES);
#else
    struct stat statbuf;
    ASSERT_EQ(stat(dbPath.c_str(), &statbuf), 0);
#endif

    EXPECT_EQ(driver.getRecordLength(), 74u);
    EXPECT_FALSE(driver.isVariableLengthRecords());
    EXPECT_EQ(driver.getKeys().size(), 4u);

    EXPECT_FALSE(driver.getKeys()[0].isComposite());
    EXPECT_FALSE(driver.getKeys()[1].isComposite());
    EXPECT_FALSE(driver.getKeys()[2].isComposite());
    EXPECT_FALSE(driver.getKeys()[3].isComposite());

    EXPECT_EQ(driver.getKeys()[0].getPrimarySegment(),
              KeyDefinition(0, 32, 2, KeyDataType::Zstring,
                            Duplicates | UseExtendedDataType, false, 0, 0, 0,
                            acsName, blankACS));

    EXPECT_EQ(driver.getKeys()[1].getPrimarySegment(),
              KeyDefinition(1, 4, 34, KeyDataType::Integer,
                            Modifiable | UseExtendedDataType, false, 0, 0, 0,
                            acsName, blankACS));

    EXPECT_EQ(driver.getKeys()[2].getPrimarySegment(),
              KeyDefinition(2, 32, 38, KeyDataType::Zstring,
                            Duplicates | Modifiable | UseExtendedDataType,
                            false, 0, 0, 0, acsName, blankACS));

    EXPECT_EQ(driver.getKeys()[3].getPrimarySegment(),
              KeyDefinition(3, 4, 70, KeyDataType::AutoInc, UseExtendedDataType,
                            false, 0, 0, 0, acsName, blankACS));
  }

  // database should be closed now, open manually via sqlite methods to query
  // for correctness
  sqlite3 *db;
  ASSERT_EQ(sqlite3_open_v2(convertedDbPath.c_str(), &db, SQLITE_OPEN_READONLY,
                            nullptr),
            SQLITE_OK);

  ASSERT_EQ(sqlite_exec(db, "SELECT version FROM metadata_t",
                        [](int numResults, char **data, char **columns) {
                          ASSERT_EQ(numResults, 1);
                          ASSERT_STREQ(data[0], "4");
                        }),
            SQLITE_OK);

  static const char *EXPECTED_METADATA_T_SQL =
      "CREATE TABLE "
      "metadata_t(record_length INTEGER NOT NULL, physical_record_length "
      "INTEGER "
      "NOT NULL, page_length INTEGER NOT NULL, variable_length_records INTEGER "
      "NOT "
      "NULL, version INTEGER NOT NULL)";

  ASSERT_EQ(
      sqlite_exec(db, "SELECT sql FROM sqlite_master WHERE name = 'metadata_t'",
                  [](int numResults, char **data, char **columns) {
                    ASSERT_EQ(numResults, 1);
                    ASSERT_STREQ(data[0], EXPECTED_METADATA_T_SQL);
                  }),
      SQLITE_OK);

  static const char *EXPECTED_KEYS_T_SQL =
      "CREATE TABLE keys_t(id "
      "INTEGER PRIMARY KEY, number INTEGER NOT NULL, segment INTEGER NOT "
      "NULL, attributes INTEGER NOT NULL, data_type INTEGER NOT NULL, offset "
      "INTEGER NOT NULL, length INTEGER NOT NULL, null_value INTEGER NOT "
      "NULL, acs_name STRING, acs BLOB, UNIQUE(number, segment))";

  ASSERT_EQ(
      sqlite_exec(db, "SELECT sql FROM sqlite_master WHERE name = 'keys_t'",
                  [](int numResults, char **data, char **columns) {
                    ASSERT_EQ(numResults, 1);
                    ASSERT_STREQ(data[0], EXPECTED_KEYS_T_SQL);
                  }),
      SQLITE_OK);

  static const char *EXPECTED_DATA_T_SQL =
      "CREATE TABLE data_t(id INTEGER PRIMARY KEY, "
      "data BLOB NOT NULL, key_0 TEXT, key_1 INTEGER NOT NULL UNIQUE, key_2 "
      "TEXT, key_3 INTEGER NOT NULL UNIQUE)";

  ASSERT_EQ(
      sqlite_exec(db, "SELECT sql FROM sqlite_master WHERE name = 'data_t'",
                  [](int numResults, char **data, char **columns) {
                    ASSERT_EQ(numResults, 1);
                    ASSERT_STREQ(data[0], EXPECTED_DATA_T_SQL);
                  }),
      SQLITE_OK);

  static const char *EXPECTED_TRIGGERS_SQL =
      "CREATE TRIGGER non_modifiable "
      "BEFORE UPDATE ON data_t BEGIN SELECT CASE WHEN NEW.key_0 IS NOT "
      "OLD.key_0 THEN RAISE (ABORT,'You modified a non-modifiable key_0!') "
      "WHEN NEW.key_3 IS NOT OLD.key_3 THEN RAISE (ABORT,'You modified a "
      "non-modifiable key_3!') END; END";

  bool foundTrigger = false;
  ASSERT_EQ(
      sqlite_exec(db,
                  "SELECT name, tbl_name, sql FROM sqlite_master WHERE "
                  "type = 'trigger'",
                  [&foundTrigger](int numResults, char **data, char **columns) {
                    foundTrigger = true;
                    ASSERT_EQ(numResults, 3);
                    ASSERT_STREQ(data[0], "non_modifiable");
                    ASSERT_STREQ(data[1], "data_t");
                    ASSERT_STREQ(data[2], EXPECTED_TRIGGERS_SQL);
                  }),
      SQLITE_OK);
  ASSERT_TRUE(foundTrigger);

  int recordCount = 0;
  ASSERT_EQ(
      sqlite_exec(db, "SELECT * FROM data_t",
                  [&recordCount](int numResults, char **data, char **columns) {
                    char index[32];
                    snprintf(index, sizeof(index), "%d", ++recordCount);

                    ASSERT_STREQ(data[0], index);
                  }),
      SQLITE_OK);

  ASSERT_EQ(recordCount, 4);

  ASSERT_EQ(sqlite3_close(db), SQLITE_OK);
}

TEST_F(BtrieveDriverTest, LoadsPreexistingSqliteDatabase) {
  std::string acsName;
  std::vector<char> blankACS;
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  EXPECT_EQ(driver.getRecordLength(), 74u);
  EXPECT_FALSE(driver.isVariableLengthRecords());
  EXPECT_EQ(driver.getKeys().size(), 4u);

  EXPECT_FALSE(driver.getKeys()[0].isComposite());
  EXPECT_FALSE(driver.getKeys()[1].isComposite());
  EXPECT_FALSE(driver.getKeys()[2].isComposite());
  EXPECT_FALSE(driver.getKeys()[3].isComposite());

  EXPECT_EQ(driver.getKeys()[0].getPrimarySegment(),
            KeyDefinition(0, 32, 2, KeyDataType::Zstring,
                          Duplicates | UseExtendedDataType, false, 0, 0, 0,
                          acsName, blankACS));

  EXPECT_EQ(driver.getKeys()[1].getPrimarySegment(),
            KeyDefinition(1, 4, 34, KeyDataType::Integer,
                          Modifiable | UseExtendedDataType, false, 0, 0, 0,
                          acsName, blankACS));

  EXPECT_EQ(driver.getKeys()[2].getPrimarySegment(),
            KeyDefinition(2, 32, 38, KeyDataType::Zstring,
                          Duplicates | Modifiable | UseExtendedDataType, false,
                          0, 0, 0, acsName, blankACS));

  EXPECT_EQ(driver.getKeys()[3].getPrimarySegment(),
            KeyDefinition(3, 4, 70, KeyDataType::AutoInc, UseExtendedDataType,
                          false, 0, 0, 0, acsName, blankACS));
}

#pragma pack(push, 1)
typedef struct _tagMBBSEmuRecordStruct {
  uint16_t header;
  char key0[32];
  int32_t key1;
  char key2[32];
  uint32_t key3;
} MBBSEmuRecordStruct;
#pragma pack(pop)

static_assert(sizeof(MBBSEmuRecordStruct) == 74);

TEST_F(BtrieveDriverTest, StepNext) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepFirst),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 1u);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            3444);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 2u);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            7776);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 3u);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            1052234073);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 4u);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            -615634567);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::EndOfFile);

  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, StepPrevious) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepLast),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 4u);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            -615634567);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 3u);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            1052234073);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 2u);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            7776);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 1u);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            3444);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::EndOfFile);

  ASSERT_EQ(driver.getPosition(), 1u);
}

TEST_F(BtrieveDriverTest, RandomAccess) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  std::pair<bool, Record> data(driver.getRecord(4));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  auto dbValue = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  EXPECT_STREQ(dbValue->key0, "Sysop");
  EXPECT_EQ(dbValue->key1, -615634567);
  EXPECT_STREQ(dbValue->key2, "stringValue");
  EXPECT_EQ(dbValue->key3, 4u);

  data = driver.getRecord(3);
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            1052234073);

  data = driver.getRecord(2);
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            7776);

  data = driver.getRecord(1);
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            3444);
}

TEST_F(BtrieveDriverTest, RandomInvalidAccess) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  std::pair<bool, Record> data(driver.getRecord(5));
  ASSERT_FALSE(data.first);
  ASSERT_EQ(data.second.getData().size(), 0u);

  data = driver.getRecord(0);
  ASSERT_FALSE(data.first);
  ASSERT_EQ(data.second.getData().size(), 0u);

  driver.setPosition(5);
  data = driver.getRecord();
  ASSERT_FALSE(data.first);
  ASSERT_EQ(data.second.getData().size(), 0u);

  driver.setPosition(0);
  data = driver.getRecord();
  ASSERT_FALSE(data.first);
  ASSERT_EQ(data.second.getData().size(), 0u);
}

TEST_F(BtrieveDriverTest, GetRecordCount) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.getRecordCount(), 4u);
}

// Once a .DAT has been converted, the .DB is the live copy of the data. A
// .DAT that is later touched, restored from backup, or re-shipped by a module
// installer must never cause the .DB (and everything written to it) to be
// thrown away.
TEST_F(BtrieveDriverTest, NewerDatDoesNotReplaceExistingDb) {
  auto mbbsEmuDat = tempPath->copyToTempPath("assets/MBBSEMU.DAT");

  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
    ASSERT_EQ(driver.getRecordCount(), 4u);

    // Live data now differs from the .DAT.
    ASSERT_EQ(driver.deleteAll(), BtrieveError::Success);
    ASSERT_EQ(driver.getRecordCount(), 0u);
  }

  std::filesystem::last_write_time(
      mbbsEmuDat, std::filesystem::file_time_type::clock::now() +
                      std::chrono::hours(1));

  BtrieveDriver driver(new SqliteDatabase());
  ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
  ASSERT_EQ(driver.getRecordCount(), 0u);
}

// Puts a different Btrieve file (MBMGEPLT.DAT, 22 records) where dat is, with
// a newer timestamp, as a module upgrade would.
static void replaceWithDifferentDat(TempPath *tempPath,
                                    const std::filesystem::path &dat) {
  auto other = tempPath->copyToTempPath("assets/MBMGEPLT.DAT");
  std::filesystem::copy_file(other, dat,
                             std::filesystem::copy_options::overwrite_existing);
  std::filesystem::remove(other);
  std::filesystem::last_write_time(
      dat, std::filesystem::file_time_type::clock::now() +
               std::chrono::hours(1));
}

// Every database moved aside into the archive folder beside dat.
static std::vector<std::filesystem::path> retiredDatabases(
    const std::filesystem::path &dat) {
  std::vector<std::filesystem::path> ret;
  const auto folder = archiveFolder(dat);
  if (!std::filesystem::exists(folder)) {
    return ret;
  }
  for (const auto &entry :
       std::filesystem::recursive_directory_iterator(folder)) {
    const auto extension = entry.path().extension().string();
    if (extension == ".DB" || extension == ".db") {
      ret.push_back(entry.path());
    }
  }
  return ret;
}

static std::string readConversionLog(const std::filesystem::path &dat) {
  std::ifstream log(archiveFolder(dat) / "CONVERSIONS.LOG");
  return std::string(std::istreambuf_iterator<char>(log),
                     std::istreambuf_iterator<char>());
}

// A .DAT with different contents from the one the .DB was converted from (a
// module upgrade, a restore) replaces the data, as it would under stock
// Btrieve, but the old .DB is kept.
TEST_F(BtrieveDriverTest, ChangedDatIsConvertedAndOldDbKept) {
  auto mbbsEmuDat = tempPath->copyToTempPath("assets/MBBSEMU.DAT");

  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
    ASSERT_EQ(driver.deleteAll(), BtrieveError::Success);
  }

  replaceWithDifferentDat(tempPath, mbbsEmuDat);

  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
    EXPECT_EQ(driver.getRecordCount(), 22u);
  }

  auto retired = retiredDatabases(mbbsEmuDat);
  ASSERT_EQ(retired.size(), 1u);
  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(toWideString(retired[0]).c_str()), BtrieveError::Success);
    EXPECT_EQ(driver.getRecordCount(), 0u);
  }
  EXPECT_NE(readConversionLog(mbbsEmuDat).find(
                "MBBSEMU.DAT has changed and was converted (22 records)"),
            std::string::npos);

  // converted once: opening again keeps the live data
  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
    ASSERT_EQ(driver.deleteAll(), BtrieveError::Success);
  }
  BtrieveDriver driver(new SqliteDatabase());
  ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
  EXPECT_EQ(driver.getRecordCount(), 0u);
  EXPECT_EQ(retiredDatabases(mbbsEmuDat).size(), 1u);
}

// A .DB from before fingerprints were kept can't say which .DAT it came from,
// so the .DAT beside it is taken as its source rather than reconverted.
TEST_F(BtrieveDriverTest, DbWithoutFingerprintAdoptsDat) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  auto mbbsEmuDat = tempPath->copyToTempPath("assets/MBBSEMU.DAT");
  replaceWithDifferentDat(tempPath, mbbsEmuDat);

  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
    EXPECT_EQ(driver.getRecordCount(), 4u);
  }
  EXPECT_TRUE(retiredDatabases(mbbsEmuDat).empty());

  // adopted, so a later change is recognized
  // copies over mbbsEmuDat
  auto original = tempPath->copyToTempPath("assets/MBBSEMU.DAT");
  std::filesystem::last_write_time(
      original, std::filesystem::file_time_type::clock::now() +
                    std::chrono::hours(2));
  BtrieveDriver driver(new SqliteDatabase());
  ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
  EXPECT_EQ(driver.getRecordCount(), 4u);
  EXPECT_EQ(retiredDatabases(mbbsEmuDat).size(), 1u);
}

// A replacement .DAT that can't be converted leaves the old .DB in use.
TEST_F(BtrieveDriverTest, UnconvertibleDatKeepsExistingDb) {
  auto mbbsEmuDat = tempPath->copyToTempPath("assets/MBBSEMU.DAT");

  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
    ASSERT_EQ(driver.deleteAll(), BtrieveError::Success);
  }

  {
    std::ofstream junk(std::filesystem::path(mbbsEmuDat),
                       std::ios::binary | std::ios::trunc);
    junk << std::string(4096, 'x');
  }

  BtrieveDriver driver(new SqliteDatabase());
  ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
  EXPECT_EQ(driver.getRecordCount(), 0u);
  EXPECT_TRUE(retiredDatabases(mbbsEmuDat).empty());
  EXPECT_NE(readConversionLog(mbbsEmuDat).find("kept the previous MBBSEMU."),
            std::string::npos);
}

// With tracking off (the parity shim, where stock Btrieve writes the .DATs),
// the .DB always wins.
TEST_F(BtrieveDriverTest, ChangedDatIgnoredWhenTrackingOff) {
  auto mbbsEmuDat = tempPath->copyToTempPath("assets/MBBSEMU.DAT");

  {
    BtrieveDriver driver(new SqliteDatabase(), false);
    ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
    ASSERT_EQ(driver.deleteAll(), BtrieveError::Success);
  }

  replaceWithDifferentDat(tempPath, mbbsEmuDat);

  BtrieveDriver driver(new SqliteDatabase(), false);
  ASSERT_EQ(driver.open(mbbsEmuDat.c_str()), BtrieveError::Success);
  EXPECT_EQ(driver.getRecordCount(), 0u);
  EXPECT_TRUE(retiredDatabases(mbbsEmuDat).empty());
}

TEST_F(BtrieveDriverTest, DeleteAll) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.getRecordCount(), 4u);

  ASSERT_EQ(driver.deleteAll(), BtrieveError::Success);

  ASSERT_EQ(driver.getRecordCount(), 0u);
  ASSERT_EQ(driver.getPosition(), 0u);
}

#ifdef WIN32
#define PATH_SEPARATOR "\\"
#else
#define PATH_SEPARATOR "/"
#endif

TEST_F(BtrieveDriverTest, Delete) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb =
      tempPath->copyToTempPath("assets" PATH_SEPARATOR "MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  driver.setPosition(2);

  ASSERT_EQ(driver.getRecordCount(), 4u);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::Delete),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(driver.getRecordCount(), 3u);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::Delete),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(driver.getRecordCount(), 3u);

  ASSERT_FALSE(driver.getRecord().first);
}

TEST_F(BtrieveDriverTest, RecordDeleteOneIteration) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  driver.setPosition(2);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::Delete),
            BtrieveError::Success);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepFirst),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 1u);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            3444);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 3u);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            1052234073);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 4u);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            -615634567);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::EndOfFile);

  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, InsertionTest) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0, "Paladine");
  record.key1 = 31337;
  strcpy(record.key2, "In orbe terrarum, optimus sum");

  ASSERT_EQ(driver.insertRecord(std::basic_string_view<uint8_t>(
                reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            std::make_pair(BtrieveError::Success, 5u));

  std::pair<bool, Record> data(driver.getRecord(5));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Paladine");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, optimus sum");
  ASSERT_EQ(dbRecord->key3, 5u);

  ASSERT_EQ(driver.getRecordCount(), 5u);
}

TEST_F(BtrieveDriverTest, InsertionTestManualAutoincrementedValue) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0, "Paladine");
  record.key1 = 31337;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 4444;  // set a manual value for the autoincrement value

  ASSERT_EQ(driver.insertRecord(std::basic_string_view<uint8_t>(
                reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            std::make_pair(BtrieveError::Success, 5u));

  std::pair<bool, Record> data(driver.getRecord(5));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Paladine");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, optimus sum");
  ASSERT_EQ(dbRecord->key3, 4444u);

  ASSERT_EQ(driver.getRecordCount(), 5u);
}

TEST_F(BtrieveDriverTest, InsertionTestSubSize) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0, "Paladine");
  record.key1 = 31337;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 4444;  // set a manual value for the autoincrement value

  // chop off the last 14 bytes rather than the full 74
  ASSERT_EQ(driver.insertRecord(std::basic_string_view<uint8_t>(
                reinterpret_cast<uint8_t *>(&record), sizeof(record) - 14)),
            std::make_pair(BtrieveError::Success, 5u));
  std::pair<bool, Record> data(driver.getRecord(5));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Paladine");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, opti");  // cut off
  ASSERT_EQ(dbRecord->key3, 5u);

  ASSERT_EQ(driver.getRecordCount(), 5u);
}

TEST_F(BtrieveDriverTest, InsertionConstraintFailure) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0, "Paladine");
  record.key1 = 3444;  // key constraint right here
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 4444;  // set a manual value for the autoincrement value

  ASSERT_EQ(driver.insertRecord(std::basic_string_view<uint8_t>(
                reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            std::make_pair(BtrieveError::DuplicateKeyValue, 0u));

  ASSERT_EQ(driver.getRecordCount(), 4u);
}

TEST_F(BtrieveDriverTest, UpdateTest) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0, "Sysop");
  record.key1 = 31337;  // key constraint right here
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 1;

  ASSERT_EQ(driver.updateRecord(
                1, std::basic_string_view<uint8_t>(
                       reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            BtrieveError::Success);

  std::pair<bool, Record> data(driver.getRecord(1));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Sysop");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, optimus sum");
  ASSERT_EQ(dbRecord->key3, 1u);

  ASSERT_EQ(driver.getRecordCount(), 4u);
}

TEST_F(BtrieveDriverTest, UpdateTestSubSize) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0, "Sysop");
  record.key1 = 31337;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 0x02020202;

  // we shorten the data by 3 bytes, meaning Key3 data is still valid, and is a
  // single byte of 2. The code will upsize to the full 74 bytes, filling in 0
  // for the rest of Key3 data, so Key3 starts as 0x02 but grows to 0x02000000
  // (little endian == 2) We have to keep Key3 as 2 since this key is marked
  // non-modifiable
  ASSERT_EQ(driver.updateRecord(2, std::basic_string_view<uint8_t>(
                                       reinterpret_cast<uint8_t *>(&record),
                                       sizeof(record) - 3)),
            BtrieveError::Success);

  std::pair<bool, Record> data(driver.getRecord(2));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Sysop");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, optimus sum");
  ASSERT_EQ(dbRecord->key3, 2u);  // truncated down to 2

  ASSERT_EQ(driver.getRecordCount(), 4u);
}

TEST_F(BtrieveDriverTest, UpdateConstraintFailedTest) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  // key 0 is not modifiable, so keep it: changing it as well makes Btrieve
  // (and this driver) report NonModifiableKeyValue instead
  strcpy(record.key0, "Sysop");
  record.key1 = 7776;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 1;

  ASSERT_EQ(driver.updateRecord(
                1, std::basic_string_view<uint8_t>(
                       reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            BtrieveError::DuplicateKeyValue);

  std::pair<bool, Record> data(driver.getRecord(1));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Sysop");
  ASSERT_EQ(dbRecord->key1, 3444);
  ASSERT_STREQ(dbRecord->key2, "3444");
  ASSERT_EQ(dbRecord->key3, 1u);

  ASSERT_EQ(driver.getRecordCount(), 4u);
}

TEST_F(BtrieveDriverTest, UpdateTestNonModifiableKeyModifiedFailed) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0,
         "Changed");  // this key isn't modifiable, but we try anyways
  record.key1 = 3444;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 333333;  // this key isn't modifiable, but we try anyways

  EXPECT_EQ(driver.updateRecord(
                1, std::basic_string_view<uint8_t>(
                       reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            BtrieveError::NonModifiableKeyValue);

  std::pair<bool, Record> data(driver.getRecord(1));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  EXPECT_STREQ(dbRecord->key0, "Sysop");
  EXPECT_EQ(dbRecord->key1, 3444);
  EXPECT_STREQ(dbRecord->key2, "3444");
  EXPECT_EQ(dbRecord->key3, 1u);

  ASSERT_EQ(driver.getRecordCount(), 4u);
}

TEST_F(BtrieveDriverTest, UpdateInvalidKeyNumber) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0,
         "Changed");  // this key isn't modifiable, but we try anyways
  record.key1 = 3444;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 333333;  // this key isn't modifiable, but we try anyways

  EXPECT_EQ(driver.updateRecord(
                5, std::basic_string_view<uint8_t>(
                       reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.getRecordCount(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyStringDuplicates) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("Sysop"), 255);  // tests key truncation

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyStringDuplicatesUpAndDown) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("Sysop"), 5);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4u);

  // let's go backwards now
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1u);

  // forward for one last test
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2u);
  // back one last time to test in-middle previous
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1u);
}

TEST_F(BtrieveDriverTest, SeekByKeyString) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("StringValue"), 11);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3u);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4u);

  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4u);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3u);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1u);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1u);
}

TEST_F(BtrieveDriverTest, SeekByKeyInteger) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 1052234073;

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3u);

  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1u);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4u);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("Sysop2"), 6);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::KeyValueNotFound);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
}

TEST_F(BtrieveDriverTest, SeekByKeyFirstString) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryFirst),
            BtrieveError::Success);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "3444");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "7776");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 3u);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "StringValue");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 4u);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyFirstInteger) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryFirst),
            BtrieveError::Success);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, -615634567);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 3u);
}

TEST_F(BtrieveDriverTest, SeekByKeyFirstNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  driver.deleteAll();

  ASSERT_EQ(driver.performOperation(0, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryFirst),
            BtrieveError::EndOfFile);

  ASSERT_EQ(driver.performOperation(0, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.performOperation(0, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
}

TEST_F(BtrieveDriverTest, SeekByKeyLastString) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryLast),
            BtrieveError::Success);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::EndOfFile);

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "StringValue");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "7776");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "3444");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 1u);
}

TEST_F(BtrieveDriverTest, SeekByKeyLastInteger) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryLast),
            BtrieveError::Success);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, -615634567);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyLastNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  driver.deleteAll();

  ASSERT_EQ(driver.performOperation(0, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryLast),
            BtrieveError::EndOfFile);

  ASSERT_EQ(driver.performOperation(0, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.performOperation(0, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
}

TEST_F(BtrieveDriverTest, SeekByKeyGreaterString) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("7776"), 4);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryGreater),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "StringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyGreaterInteger) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 3444;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), 4);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryGreater),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 3u);
}

TEST_F(BtrieveDriverTest, SeekByKeyGreaterNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 2000000000;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryGreater),
            BtrieveError::EndOfFile);
}

TEST_F(BtrieveDriverTest, SeekByKeyGreaterOrEqualString) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("7776"), 4);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryGreaterOrEqual),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "7776");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "StringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyGreaterOrEqualInteger) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 3444;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), 4);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryGreaterOrEqual),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 3u);
}

TEST_F(BtrieveDriverTest, SeekByKeyGreaterOrEqualNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 2000000000;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryGreaterOrEqual),
            BtrieveError::EndOfFile);
}

TEST_F(BtrieveDriverTest, SeekByKeyLessString) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("7776"), 4);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryLess),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "3444");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 1u);
}

TEST_F(BtrieveDriverTest, SeekByKeyLessInteger) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 7776;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), 4);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryLess),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, -615634567);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyLessNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = -2000000000;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryLess),
            BtrieveError::EndOfFile);
}

TEST_F(BtrieveDriverTest, SeekByKeyLessOrEqualString) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("7776"), 4);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryLessOrEqual),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "7776");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "3444");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 1u);
}

TEST_F(BtrieveDriverTest, SeekByKeyLessOrEqualInteger) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 7776;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), 4);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryLessOrEqual),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, -615634567);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyLessOrEqualNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = -2000000000;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryLessOrEqual),
            BtrieveError::EndOfFile);
}

TEST_F(BtrieveDriverTest, SeekByKeyEqualWithDuplicates) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  const char *sysopKey = "Sysop";
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(sysopKey), strlen(sysopKey));

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 1u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 3u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 4u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 4u);
}

TEST_F(BtrieveDriverTest, SeekByKeyEqualWithDuplicatesUpAndDown) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  const char *sysopKey = "Sysop";
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(sysopKey), strlen(sysopKey));

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 1u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 3u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 4u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::EndOfFile);
  ASSERT_EQ(driver.getPosition(), 4u);

  // now go backwards
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 3u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 1u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);

  // and up one and back down again for good measure
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 1u);
}

// MBMGEPLT.DAT has 22 records whose key 2 (2-byte unsigned binary, duplicates
// allowed) is 1, 2 or 3. Btrieve returns duplicates in insertion (position)
// order when moving forward and the reverse when moving backward.
static const std::vector<unsigned int> MBMGEPLT_KEY2_ORDER = {
    7, 8, 10, 16, 18, 19, 20, 21,  // key 2 == 1
    1, 2, 3,  11, 12, 13, 14, 22,  // key 2 == 2
    4, 5, 6,  9,  15, 17,          // key 2 == 3
};

static std::vector<unsigned int> collectPositions(BtrieveDriver &driver,
                                                  OperationCode continuation) {
  std::vector<unsigned int> positions;
  do {
    positions.push_back(driver.getPosition());
  } while (driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                   continuation) == BtrieveError::Success);
  return positions;
}

static void verifyMbmgepltKey2Ordering(BtrieveDriver &driver) {
  const uint16_t two = 2;
  const auto keyTwo = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&two), sizeof(two));
  const std::vector<unsigned int> &forward = MBMGEPLT_KEY2_ORDER;
  const std::vector<unsigned int> reverse(forward.rbegin(), forward.rend());

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryFirst),
            BtrieveError::Success);
  EXPECT_EQ(collectPositions(driver, OperationCode::QueryNext), forward);

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryLast),
            BtrieveError::Success);
  EXPECT_EQ(collectPositions(driver, OperationCode::QueryPrevious), reverse);

  // MBBSEmu issue #641: agebtv(key 2 == 2) followed by qnxbtv
  ASSERT_EQ(driver.performOperation(2, keyTwo, OperationCode::QueryEqual),
            BtrieveError::Success);
  EXPECT_EQ(collectPositions(driver, OperationCode::QueryNext),
            std::vector<unsigned int>(forward.begin() + 8, forward.end()));

  ASSERT_EQ(
      driver.performOperation(2, keyTwo, OperationCode::QueryGreaterOrEqual),
      BtrieveError::Success);
  EXPECT_EQ(collectPositions(driver, OperationCode::QueryNext),
            std::vector<unsigned int>(forward.begin() + 8, forward.end()));

  ASSERT_EQ(driver.performOperation(2, keyTwo, OperationCode::QueryGreater),
            BtrieveError::Success);
  EXPECT_EQ(collectPositions(driver, OperationCode::QueryNext),
            std::vector<unsigned int>(forward.begin() + 16, forward.end()));

  ASSERT_EQ(driver.performOperation(2, keyTwo, OperationCode::QueryLessOrEqual),
            BtrieveError::Success);
  EXPECT_EQ(collectPositions(driver, OperationCode::QueryPrevious),
            std::vector<unsigned int>(reverse.begin() + 6, reverse.end()));

  ASSERT_EQ(driver.performOperation(2, keyTwo, OperationCode::QueryLess),
            BtrieveError::Success);
  EXPECT_EQ(collectPositions(driver, OperationCode::QueryPrevious),
            std::vector<unsigned int>(reverse.begin() + 14, reverse.end()));

  // changing direction in the middle of a run of duplicates
  ASSERT_EQ(driver.performOperation(2, keyTwo, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 1u);
  for (auto [operation, expectedPosition] :
       std::vector<std::pair<OperationCode, unsigned int>>{
           {OperationCode::QueryNext, 2},
           {OperationCode::QueryNext, 3},
           {OperationCode::QueryPrevious, 2},
           {OperationCode::QueryPrevious, 1},
           {OperationCode::QueryPrevious, 21},
           {OperationCode::QueryNext, 1},
       }) {
    ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                      operation),
              BtrieveError::Success);
    EXPECT_EQ(driver.getPosition(), expectedPosition);
  }

  // gabbtv(position 12, key 2) followed by qnxbtv / qprbtv
  ASSERT_EQ(driver.logicalCurrencySeek(2, 12), BtrieveError::Success);
  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  EXPECT_EQ(driver.getPosition(), 13u);

  ASSERT_EQ(driver.logicalCurrencySeek(2, 12), BtrieveError::Success);
  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);
  EXPECT_EQ(driver.getPosition(), 11u);
}

TEST_F(BtrieveDriverTest, SeekByKeyDuplicatesOrderedByPosition) {
  BtrieveDriver driver(new SqliteDatabase());

  auto dat = tempPath->copyToTempPath("assets/MBMGEPLT.DAT");
  ASSERT_EQ(driver.open(dat.c_str()), BtrieveError::Success);

  verifyMbmgepltKey2Ordering(driver);
}

// Duplicate ordering must not depend on SQLite happening to walk the key's
// index. Without the index, SQLite sorts and keeps duplicates in table (id)
// order even for DESC, so backwards traversal breaks unless id is an explicit
// tiebreaker.
TEST_F(BtrieveDriverTest, SeekByKeyDuplicatesOrderedByPositionWithoutIndex) {
  auto dat = tempPath->copyToTempPath("assets/MBMGEPLT.DAT");

  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(dat.c_str()), BtrieveError::Success);
  }

  std::filesystem::path dbPath(dat);
  dbPath.replace_extension("db");

  sqlite3 *db;
  ASSERT_EQ(sqlite3_open_v2(fromPath(dbPath).c_str(), &db,
                            SQLITE_OPEN_READWRITE, nullptr),
            SQLITE_OK);
  ASSERT_EQ(sqlite3_exec(db, "DROP INDEX key_2_index", nullptr, nullptr,
                         nullptr),
            SQLITE_OK);
  sqlite3_close(db);

  BtrieveDriver driver(new SqliteDatabase());
  ASSERT_EQ(driver.open(dat.c_str()), BtrieveError::Success);

  verifyMbmgepltKey2Ordering(driver);
}

const unsigned int ACS_RECORD_LENGTH = 128;

static std::vector<char> upperACS() {
  std::vector<char> ret(ACS_LENGTH);
  char *acs = ret.data();

  for (int i = 0; i < ACS_LENGTH; ++i) {
    acs[i] = (char)i;
  }
  // make uppercase
  for (int i = 'a'; i <= 'z'; ++i) {
    acs[i] = toupper(acs[i]);
  }

  return ret;
}

static std::vector<uint8_t> createRecord(const char *username, float value1 = 0,
                                         double value2 = 0) {
  std::vector<uint8_t> record(ACS_RECORD_LENGTH);

  memset(record.data(), 0xFF, record.size());
  strcpy(reinterpret_cast<char *>(record.data() + 2), username);

  *reinterpret_cast<float *>(record.data() + 64) = value1;
  *reinterpret_cast<double *>(record.data() + 68) = value2;

  return record;
}

static BtrieveDatabase createACSBtrieveDatabase(KeyDataType stringKeyDataType) {
  std::vector<Key> keys;
  uint16_t pageLength = 512;
  unsigned int pageCount = 1;
  unsigned int recordLength = ACS_RECORD_LENGTH;
  unsigned int physicalRecordLength = ACS_RECORD_LENGTH;
  unsigned int recordCount = 0;
  unsigned int fileLength = pageCount * pageLength;

  std::vector<char> acs = upperACS();

  KeyDefinition keyDefinition(0, 30, 2, stringKeyDataType,
                              UseExtendedDataType | NumberedACS, false, 0, 0, 0,
                              "acsName", acs);

  keys.push_back(Key(&keyDefinition, 1));

  keyDefinition = KeyDefinition(1, 4, 64, KeyDataType::Float,
                                UseExtendedDataType | Duplicates, false, 0, 0,
                                0, "", std::vector<char>());

  keys.push_back(Key(&keyDefinition, 1));

  keyDefinition = KeyDefinition(2, 8, 68, KeyDataType::Float,
                                UseExtendedDataType | Duplicates, false, 0, 0,
                                0, "", std::vector<char>());

  keys.push_back(Key(&keyDefinition, 1));

  return BtrieveDatabase(keys, pageLength, pageCount, recordLength,
                         physicalRecordLength, recordCount, fileLength,
                         RecordType::Fixed, false, 0);
}

TEST_F(BtrieveDriverTest, ACSSeekByKey_Zstring) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader = database->create(
      _TEXT("unused.db"), createACSBtrieveDatabase(KeyDataType::Zstring));
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Sysop").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 1u));

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Paladine").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 2u));

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Testing").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 3u));

  std::basic_string_view<uint8_t> key(
      reinterpret_cast<const uint8_t *>("paladine"), 8);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Paladine", 8), 0);
}

TEST_F(BtrieveDriverTest, ACSSeekByKey_String) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader = database->create(
      _TEXT("unused.db"), createACSBtrieveDatabase(KeyDataType::String));
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Sysop").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 1u));

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Paladine").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 2u));

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Testing").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 3u));

  char searchKey[30];
  memset(searchKey, 0xFF, sizeof(searchKey));
  strcpy(searchKey, "paladine");

  std::basic_string_view<uint8_t> key(
      reinterpret_cast<const uint8_t *>(searchKey), 30);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Paladine", 8), 0);
  ASSERT_EQ(data.second.getData().data()[10], 0);
  ASSERT_EQ(memcmp(data.second.getData().data() + 11, searchKey + 9, 21), 0);
}

TEST_F(BtrieveDriverTest, ACSSeekByKey_Lstring) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader = database->create(
      _TEXT("unused.db"), createACSBtrieveDatabase(KeyDataType::Lstring));
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Sysop").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 1u));

  auto record = createRecord("Paladine");
  // make this an Lstring
  record.data()[2] = 8;
  memcpy(record.data() + 3, "Paladine", 8);

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                record.data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 2u));

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Testing").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 3u));

  char searchKey[30];
  memset(searchKey, 0xFF, sizeof(searchKey));
  searchKey[0] = 8;
  memcpy(searchKey + 1, "paladine", 8);

  std::basic_string_view<uint8_t> key(
      reinterpret_cast<const uint8_t *>(searchKey), 30);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(data.second.getData()[2], 8);  // Lstring length
  ASSERT_EQ(memcmp(data.second.getData().data() + 3, "Paladine", 8), 0);
  ASSERT_EQ(data.second.getData().data()[11], 0xFF);  // no null terminator
}

TEST_F(BtrieveDriverTest, ACSInsertDuplicateFails) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader = database->create(
      _TEXT("unused.db"), createACSBtrieveDatabase(KeyDataType::Zstring));
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Sysop").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 1u));

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("sysop").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::DuplicateKeyValue, 0u));

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("SysOp").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::DuplicateKeyValue, 0u));

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("SysoP").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::DuplicateKeyValue, 0u));
}

TEST_F(BtrieveDriverTest, FloatSeekByKey) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader = database->create(
      _TEXT("unused.db"), createACSBtrieveDatabase(KeyDataType::Zstring));
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Sysop", 1.0f, 2.0).data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 1u));

  ASSERT_EQ(
      database->insertRecord(std::basic_string_view<uint8_t>(
          createRecord("Paladine", -1.0f, -2.0).data(), ACS_RECORD_LENGTH)),
      std::make_pair(BtrieveError::Success, 2u));

  float fkey = -1.0f;
  std::basic_string_view<uint8_t> key(reinterpret_cast<const uint8_t *>(&fkey),
                                      sizeof(fkey));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Paladine", 8), 0);
  ASSERT_EQ(*reinterpret_cast<const float *>(data.second.getData().data() + 64),
            -1.0f);
  ASSERT_EQ(
      *reinterpret_cast<const double *>(data.second.getData().data() + 68),
      -2.0);

  double dkey = 2.0f;
  key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&dkey), sizeof(dkey));

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Sysop", 6), 0);
  ASSERT_EQ(*reinterpret_cast<const float *>(data.second.getData().data() + 64),
            1.0f);
  ASSERT_EQ(
      *reinterpret_cast<const double *>(data.second.getData().data() + 68),
      2.0);
}

static BtrieveDatabase createKeylessBtrieveDatabase() {
  std::vector<Key> keys;
  uint16_t pageLength = 512;
  unsigned int pageCount = 1;
  unsigned int recordLength = ACS_RECORD_LENGTH;
  unsigned int physicalRecordLength = ACS_RECORD_LENGTH;
  unsigned int recordCount = 0;
  unsigned int fileLength = pageCount * pageLength;

  return BtrieveDatabase(keys, pageLength, pageCount, recordLength,
                         physicalRecordLength, recordCount, fileLength,
                         RecordType::Fixed, false, 0);
}

TEST_F(BtrieveDriverTest, KeylessDatabaseEnumeration) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader =
      database->create(_TEXT("unused.db"), createKeylessBtrieveDatabase());
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Sysop").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Paladine").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Testing").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("paladine").data(), ACS_RECORD_LENGTH)),
            std::make_pair(BtrieveError::Success, 4u));

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepFirst),
            BtrieveError::Success);
  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Sysop", 5), 0);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::EndOfFile);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Paladine", 8), 0);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Testing", 7), 0);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4u);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "paladine", 5), 0);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::EndOfFile);
}

TEST_F(BtrieveDriverTest, KeylessDataQueryFails) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader =
      database->create(_TEXT("unused.db"), createKeylessBtrieveDatabase());
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Sysop").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Paladine").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Testing").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordsComplete();

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryEqual),
            BtrieveError::InvalidKeyNumber);
}

TEST_F(BtrieveDriverTest, SeekByKeyChangeKeyNumbers) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 7776;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), 4);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryLessOrEqual),
            BtrieveError::Success);

  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2u);
  ASSERT_EQ(data.second.getData().size(), 74u);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::DifferentKeyNumber);
  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::DifferentKeyNumber);
  ASSERT_EQ(driver.performOperation(3, key, OperationCode::QueryNext),
            BtrieveError::DifferentKeyNumber);
  ASSERT_EQ(driver.performOperation(4, key, OperationCode::QueryNext),
            BtrieveError::DifferentKeyNumber);
}

TEST_F(BtrieveDriverTest, SeekByKeyInvalidKey) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 7776;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), 4);

  ASSERT_EQ(driver.performOperation(-1, key, OperationCode::QueryLessOrEqual),
            BtrieveError::InvalidKeyNumber);

  ASSERT_EQ(driver.performOperation(4, key, OperationCode::QueryLessOrEqual),
            BtrieveError::InvalidKeyNumber);
}

#pragma pack(push, 1)
typedef struct _tagCompositeRecordStruct {
  int16_t a;
  char name[8];
  uint32_t b;
  uint16_t padding;
} CompositeRecordStruct;
#pragma pack(pop)

static_assert(sizeof(CompositeRecordStruct) == 16);

static std::vector<uint8_t> createCompositeRecord(int16_t a, const char *name,
                                                  uint32_t b) {
  CompositeRecordStruct record;
  memset(&record, 0, sizeof(record));
  record.a = a;
  strncpy(record.name, name, sizeof(record.name));
  record.b = b;

  auto data = reinterpret_cast<const uint8_t *>(&record);
  return std::vector<uint8_t>(data, data + sizeof(record));
}

// key 0 is (a, name), key 1 is (b, a)
static std::vector<uint8_t> createCompositeKey0(int16_t a, const char *name) {
  auto record = createCompositeRecord(a, name, 0);
  record.resize(offsetof(CompositeRecordStruct, b));
  return record;
}

static std::vector<uint8_t> createCompositeKey1(uint32_t b, int16_t a) {
  std::vector<uint8_t> key(sizeof(b) + sizeof(a));
  memcpy(key.data(), &b, sizeof(b));
  memcpy(key.data() + sizeof(b), &a, sizeof(a));
  return key;
}

static std::vector<Key> createCompositeKeys() {
  std::vector<Key> keys;

  KeyDefinition key0[2] = {
      KeyDefinition(0, 2, 0, KeyDataType::Integer,
                    UseExtendedDataType | SegmentedKey, true, 0, 0, 0, "",
                    std::vector<char>()),
      KeyDefinition(0, 8, 2, KeyDataType::Zstring, UseExtendedDataType, false,
                    0, 1, 0, "", std::vector<char>())};
  keys.push_back(Key(key0, 2));

  KeyDefinition key1[2] = {
      KeyDefinition(
          1, 4, 10, KeyDataType::Unsigned,
          UseExtendedDataType | Duplicates | Modifiable | SegmentedKey, true, 1,
          0, 0, "", std::vector<char>()),
      KeyDefinition(1, 2, 0, KeyDataType::Integer,
                    UseExtendedDataType | Duplicates | Modifiable, false, 1, 1,
                    0, "", std::vector<char>())};
  keys.push_back(Key(key1, 2));

  return keys;
}

static BtrieveDatabase createCompositeBtrieveDatabase() {
  return BtrieveDatabase(
      createCompositeKeys(), 512, 1, sizeof(CompositeRecordStruct),
      sizeof(CompositeRecordStruct), 0, 512, RecordType::Fixed, false, 0);
}

// Values chosen so that comparing the segments' raw little endian bytes, as a
// single blob would, orders them incorrectly.
static const struct {
  int16_t a;
  const char *name;
  uint32_t b;
} COMPOSITE_RECORDS[] = {
    {256, "x", 0x100},     // position 1
    {1, "x", 1},           // position 2
    {-1, "x", 1},          // position 3
    {2, "x", 0x10000},     // position 4
    {1, "b", 0xFFFFFFFF},  // position 5
    {1, "a", 1},           // position 6
};

// positions sorted by (a, name)
static const unsigned int COMPOSITE_KEY0_ORDER[] = {3, 6, 5, 2, 4, 1};
// positions sorted by (b, a), duplicates by position
static const unsigned int COMPOSITE_KEY1_ORDER[] = {3, 2, 6, 1, 4, 5};

static void insertCompositeRecords(BtrieveDriver &driver) {
  unsigned int position = 1;
  for (auto &record : COMPOSITE_RECORDS) {
    auto data = createCompositeRecord(record.a, record.name, record.b);
    ASSERT_EQ(driver.insertRecord(
                  std::basic_string_view<uint8_t>(data.data(), data.size())),
              std::make_pair(BtrieveError::Success, position++));
  }
}

static void verifyCompositeOrdering(BtrieveDriver &driver, int keyNumber,
                                    const unsigned int *expected,
                                    size_t numExpected) {
  std::basic_string_view<uint8_t> unused;

  ASSERT_EQ(
      driver.performOperation(keyNumber, unused, OperationCode::QueryFirst),
      BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), expected[0]);
  for (size_t i = 1; i < numExpected; ++i) {
    ASSERT_EQ(
        driver.performOperation(keyNumber, unused, OperationCode::QueryNext),
        BtrieveError::Success);
    ASSERT_EQ(driver.getPosition(), expected[i]);
  }
  ASSERT_EQ(
      driver.performOperation(keyNumber, unused, OperationCode::QueryNext),
      BtrieveError::EndOfFile);

  ASSERT_EQ(
      driver.performOperation(keyNumber, unused, OperationCode::QueryLast),
      BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), expected[numExpected - 1]);
  for (size_t i = numExpected - 1; i > 0; --i) {
    ASSERT_EQ(driver.performOperation(keyNumber, unused,
                                      OperationCode::QueryPrevious),
              BtrieveError::Success);
    ASSERT_EQ(driver.getPosition(), expected[i - 1]);
  }
  ASSERT_EQ(
      driver.performOperation(keyNumber, unused, OperationCode::QueryPrevious),
      BtrieveError::EndOfFile);
}

static std::basic_string_view<uint8_t> view(const std::vector<uint8_t> &data) {
  return std::basic_string_view<uint8_t>(data.data(), data.size());
}

class CompositeKeyTest : public BtrieveDriverTest {
 protected:
  virtual void SetUp() override {
    BtrieveDriverTest::SetUp();

    database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
    driver.reset(new BtrieveDriver(database));

    auto recordLoader =
        database->create(_TEXT("unused.db"), createCompositeBtrieveDatabase());
    recordLoader->onRecordsComplete();

    insertCompositeRecords(*driver);
  }

  SqliteDatabase *database;
  std::unique_ptr<BtrieveDriver> driver;
};

TEST_F(CompositeKeyTest, OrdersSegmentsByValue) {
  verifyCompositeOrdering(*driver, 0, COMPOSITE_KEY0_ORDER,
                          std::size(COMPOSITE_KEY0_ORDER));
  verifyCompositeOrdering(*driver, 1, COMPOSITE_KEY1_ORDER,
                          std::size(COMPOSITE_KEY1_ORDER));
}

TEST_F(CompositeKeyTest, QueryEqual) {
  ASSERT_EQ(driver->performOperation(0, view(createCompositeKey0(1, "b")),
                                     OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 5u);

  ASSERT_EQ(driver->performOperation(0, view(createCompositeKey0(256, "x")),
                                     OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 1u);

  ASSERT_EQ(driver->performOperation(0, view(createCompositeKey0(1, "c")),
                                     OperationCode::QueryEqual),
            BtrieveError::KeyValueNotFound);

  ASSERT_EQ(driver->performOperation(1, view(createCompositeKey1(1, 1)),
                                     OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);
}

TEST_F(CompositeKeyTest, QueryRanges) {
  auto key = createCompositeKey0(1, "a");
  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryGreater),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 5u);

  ASSERT_EQ(driver->performOperation(0, view(key),
                                     OperationCode::QueryGreaterOrEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 6u);

  key = createCompositeKey0(2, "x");
  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryLess),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);

  ASSERT_EQ(
      driver->performOperation(0, view(key), OperationCode::QueryLessOrEqual),
      BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 4u);

  key = createCompositeKey0(256, "x");
  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryGreater),
            BtrieveError::EndOfFile);

  key = createCompositeKey0(-1, "x");
  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryLess),
            BtrieveError::EndOfFile);
}

TEST_F(CompositeKeyTest, ChangeDirectionThroughDuplicates) {
  std::basic_string_view<uint8_t> unused;

  ASSERT_EQ(driver->performOperation(1, view(createCompositeKey1(1, 1)),
                                     OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);

  ASSERT_EQ(driver->performOperation(1, unused, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 6u);

  ASSERT_EQ(driver->performOperation(1, unused, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 1u);

  ASSERT_EQ(driver->performOperation(1, unused, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 6u);

  ASSERT_EQ(driver->performOperation(1, unused, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);

  ASSERT_EQ(driver->performOperation(1, unused, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 3u);

  ASSERT_EQ(driver->performOperation(1, unused, OperationCode::QueryPrevious),
            BtrieveError::EndOfFile);
}

TEST_F(CompositeKeyTest, UniqueAcrossAllSegments) {
  auto record = createCompositeRecord(1, "a", 1234);
  ASSERT_EQ(driver->insertRecord(view(record)),
            std::make_pair(BtrieveError::DuplicateKeyValue, 0u));

  // same first segment, different second segment is fine
  record = createCompositeRecord(1, "c", 1234);
  ASSERT_EQ(driver->insertRecord(view(record)),
            std::make_pair(BtrieveError::Success, 7u));
}

TEST_F(CompositeKeyTest, UpdateNonModifiableSegmentFails) {
  ASSERT_EQ(driver->performOperation(0, view(createCompositeKey0(1, "a")),
                                     OperationCode::QueryEqual),
            BtrieveError::Success);

  // key 1 is modifiable
  auto record = createCompositeRecord(1, "a", 1234);
  ASSERT_EQ(database->updateRecord(6, view(record)), BtrieveError::Success);

  // key 0's second segment isn't
  record = createCompositeRecord(1, "z", 1234);
  ASSERT_EQ(database->updateRecord(6, view(record)),
            BtrieveError::NonModifiableKeyValue);
}

// Version 3 stored composite keys as a blob of the concatenated segment
// bytes, opening one should rebuild the per-segment key columns.
TEST_F(BtrieveDriverTest, UpgradesCompositeKeysFromVersion3) {
  std::string dbPath = tempPath->getTempPath() + "/COMPOSITE.DB";

  sqlite3 *db;
  ASSERT_EQ(
      sqlite3_open_v2(dbPath.c_str(), &db,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr),
      SQLITE_OK);

  std::stringstream sql;
  sql << "CREATE TABLE metadata_t(record_length INTEGER NOT NULL, "
         "physical_record_length INTEGER NOT NULL, page_length INTEGER NOT "
         "NULL, variable_length_records INTEGER NOT NULL, version INTEGER NOT "
         "NULL);"
      << "INSERT INTO metadata_t VALUES(16, 16, 512, 0, 3);"
      << "CREATE TABLE keys_t(id INTEGER PRIMARY KEY, number INTEGER NOT "
         "NULL, segment INTEGER NOT NULL, attributes INTEGER NOT NULL, "
         "data_type INTEGER NOT NULL, offset INTEGER NOT NULL, length INTEGER "
         "NOT NULL, null_value INTEGER NOT NULL, acs_name STRING, acs BLOB, "
         "UNIQUE(number, segment));";
  for (auto &key : createCompositeKeys()) {
    for (auto &segment : key.getSegments()) {
      sql << "INSERT INTO keys_t(number, segment, attributes, data_type, "
             "offset, length, null_value) VALUES("
          << segment.getNumber() << ", " << segment.getSegmentIndex() << ", "
          << segment.getAttributes() << ", "
          << static_cast<int>(segment.getDataType()) << ", "
          << segment.getOffset() << ", " << segment.getLength() << ", 0);";
    }
  }
  sql << "CREATE TABLE data_t(id INTEGER PRIMARY KEY, data BLOB NOT NULL, "
         "key_0 BLOB NOT NULL UNIQUE, key_1 BLOB NOT NULL);"
      << "CREATE UNIQUE INDEX key_0_index on data_t(key_0);"
      << "CREATE INDEX key_1_index on data_t(key_1);"
      << "CREATE TRIGGER non_modifiable BEFORE UPDATE ON data_t BEGIN SELECT "
         "CASE WHEN NEW.key_0 != OLD.key_0 THEN RAISE (ABORT,'You modified a "
         "non-modifiable key_0!') END; END;";
  ASSERT_EQ(sqlite3_exec(db, sql.str().c_str(), nullptr, nullptr, nullptr),
            SQLITE_OK);

  // insert with the concatenated segment bytes version 3 used as keys
  sqlite3_stmt *insert;
  ASSERT_EQ(sqlite3_prepare_v2(db,
                               "INSERT INTO data_t(id, data, key_0, key_1) "
                               "VALUES(?, ?, ?, ?)",
                               -1, &insert, nullptr),
            SQLITE_OK);
  sqlite3_int64 id = 1;
  for (auto &record : COMPOSITE_RECORDS) {
    auto data = createCompositeRecord(record.a, record.name, record.b);
    auto key0 = createCompositeKey0(record.a, record.name);
    auto key1 = createCompositeKey1(record.b, record.a);

    sqlite3_reset(insert);
    sqlite3_bind_int64(insert, 1, id++);
    sqlite3_bind_blob(insert, 2, data.data(), data.size(), SQLITE_TRANSIENT);
    sqlite3_bind_blob(insert, 3, key0.data(), key0.size(), SQLITE_TRANSIENT);
    sqlite3_bind_blob(insert, 4, key1.data(), key1.size(), SQLITE_TRANSIENT);
    ASSERT_EQ(sqlite3_step(insert), SQLITE_DONE);
  }
  sqlite3_finalize(insert);
  sqlite3_close(db);

  {
    BtrieveDriver driver(new SqliteDatabase());
    ASSERT_EQ(driver.open(toWideString(dbPath.c_str()).c_str()),
              BtrieveError::Success);

    verifyCompositeOrdering(driver, 0, COMPOSITE_KEY0_ORDER,
                            std::size(COMPOSITE_KEY0_ORDER));
    verifyCompositeOrdering(driver, 1, COMPOSITE_KEY1_ORDER,
                            std::size(COMPOSITE_KEY1_ORDER));

    // uniqueness and the non-modifiable trigger survive the rebuild
    auto record = createCompositeRecord(1, "a", 1234);
    ASSERT_EQ(driver.insertRecord(view(record)),
              std::make_pair(BtrieveError::DuplicateKeyValue, 0u));
  }

  ASSERT_EQ(sqlite3_open_v2(dbPath.c_str(), &db, SQLITE_OPEN_READONLY, nullptr),
            SQLITE_OK);
  std::vector<std::string> schema;
  ASSERT_EQ(sqlite_exec(db,
                        "SELECT sql FROM sqlite_master WHERE tbl_name = "
                        "'data_t' AND sql IS NOT NULL ORDER BY name",
                        [&](int numResults, char **data, char **columns) {
                          schema.push_back(data[0]);
                        }),
            SQLITE_OK);
  ASSERT_EQ(sqlite_exec(db, "SELECT version FROM metadata_t",
                        [](int numResults, char **data, char **columns) {
                          ASSERT_STREQ(data[0], "4");
                        }),
            SQLITE_OK);
  sqlite3_close(db);

  EXPECT_EQ(schema,
            std::vector<std::string>(
                {"CREATE TABLE \"data_t\"(id INTEGER PRIMARY KEY, data BLOB "
                 "NOT NULL, key_0_0 INTEGER NOT NULL, key_0_1 TEXT NOT NULL, "
                 "key_1_0 INTEGER NOT NULL, key_1_1 INTEGER NOT NULL)",
                 "CREATE UNIQUE INDEX key_0_index on data_t(key_0_0, key_0_1)",
                 "CREATE INDEX key_1_index on data_t(key_1_0, key_1_1)",
                 "CREATE TRIGGER non_modifiable BEFORE UPDATE ON data_t BEGIN "
                 "SELECT CASE WHEN NEW.key_0_0 IS NOT OLD.key_0_0 THEN RAISE "
                 "(ABORT,'You modified a non-modifiable key_0!') WHEN "
                 "NEW.key_0_1 IS NOT OLD.key_0_1 THEN RAISE (ABORT,'You "
                 "modified a non-modifiable key_0!') END; END"}));
}

#pragma pack(push, 1)
typedef struct _tagIntStringRecordStruct {
  int16_t a;
  char name[8];
} IntStringRecordStruct;
#pragma pack(pop)

static_assert(sizeof(IntStringRecordStruct) == 10);

// name is copied with nameLength bytes, or up to its terminator when 0, so
// callers can put bytes after the terminator
static std::vector<uint8_t> createIntStringRecord(int16_t a, const char *name,
                                                  size_t nameLength = 0) {
  IntStringRecordStruct record;
  memset(&record, 0, sizeof(record));
  record.a = a;
  memcpy(record.name, name, nameLength ? nameLength : strlen(name));

  auto data = reinterpret_cast<const uint8_t *>(&record);
  return std::vector<uint8_t>(data, data + sizeof(record));
}

// Creates an in memory database with one composite key over the record's
// (a INTEGER, name ZSTRING), or (name, a) when stringFirst. The null value is
// 0, so a segment is null when all zero: a == 0, or name is empty.
static std::unique_ptr<BtrieveDriver> createIntStringDriver(
    uint16_t attributes, bool stringFirst = false) {
  KeyDefinition intSegment(0, 2, 0, KeyDataType::Integer,
                           UseExtendedDataType | attributes, true, 0, 0, 0, "",
                           std::vector<char>());
  KeyDefinition stringSegment(0, 8, 2, KeyDataType::Zstring,
                              UseExtendedDataType | attributes, true, 0, 0, 0,
                              "", std::vector<char>());
  KeyDefinition segments[2] = {stringFirst ? stringSegment : intSegment,
                               stringFirst ? intSegment : stringSegment};

  auto *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  std::unique_ptr<BtrieveDriver> driver(new BtrieveDriver(database));
  database
      ->create(_TEXT("unused.db"),
               BtrieveDatabase({Key(segments, 2)}, 512, 1,
                               sizeof(IntStringRecordStruct),
                               sizeof(IntStringRecordStruct), 0, 512,
                               RecordType::Fixed, false, 0))
      ->onRecordsComplete();
  return driver;
}

static const struct {
  int16_t a;
  const char *name;
} INT_STRING_RECORDS[] = {
    {1, ""},       // position 1
    {-1, "a"},     // position 2
    {0, ""},       // position 3
    {-32768, "z"}, // position 4
    {32767, ""},   // position 5
    {-256, ""},    // position 6
    {-1, ""},      // position 7
    {0, "a"},      // position 8
    {255, "a"},    // position 9
    {-32768, ""},  // position 10
};

static void insertIntStringRecords(BtrieveDriver &driver) {
  unsigned int position = 1;
  for (auto &record : INT_STRING_RECORDS) {
    ASSERT_EQ(driver.insertRecord(
                  view(createIntStringRecord(record.a, record.name))),
              std::make_pair(BtrieveError::Success, position++));
  }
}

// Negative and positive integers sort by value, -256 (raw bytes 00 FF) before
// -1 (FF FF), and an empty string sorts first within the same integer.
TEST_F(BtrieveDriverTest, CompositeIntStringOrdersSignsAndEmptyStrings) {
  auto driver = createIntStringDriver(Duplicates);
  insertIntStringRecords(*driver);

  static const unsigned int EXPECTED[] = {10, 4, 6, 7, 2, 3, 8, 1, 9, 5};
  verifyCompositeOrdering(*driver, 0, EXPECTED, std::size(EXPECTED));
}

TEST_F(BtrieveDriverTest, CompositeStringIntOrdersEmptyStringsAndSigns) {
  auto driver = createIntStringDriver(Duplicates, /* stringFirst= */ true);
  insertIntStringRecords(*driver);

  // ("", ints by value), then ("a", ints by value), then ("z", -32768)
  static const unsigned int EXPECTED[] = {10, 6, 7, 3, 1, 5, 2, 8, 9, 4};
  verifyCompositeOrdering(*driver, 0, EXPECTED, std::size(EXPECTED));
}

TEST_F(BtrieveDriverTest, CompositeIntStringPartialKeySearch) {
  auto driver = createIntStringDriver(Duplicates);
  insertIntStringRecords(*driver);

  auto key = createIntStringRecord(-1, "");
  ASSERT_EQ(driver->performOperation(0, view(key),
                                     OperationCode::QueryGreaterOrEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 7u);

  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 7u);

  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryLess),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 6u);

  key = createIntStringRecord(0, "");
  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryLess),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);
}

// Applications often pass search buffers with leftover bytes after a
// ZSTRING's terminator, which aren't part of the key.
TEST_F(BtrieveDriverTest, CompositeSearchIgnoresBytesAfterZstringTerminator) {
  auto driver = createIntStringDriver(Duplicates);
  insertIntStringRecords(*driver);

  ASSERT_EQ(
      driver->performOperation(0, view(createIntStringRecord(-1, "a\0ZZZZ", 7)),
                               OperationCode::QueryEqual),
      BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);
}

// A partial key search leaves trailing segments low. Under NullAnySegment a
// zeroed segment matches the null value, but the search key must still
// compare as a value rather than NULL, which matches nothing.
TEST_F(BtrieveDriverTest, NullAnySegmentPartialKeySearch) {
  auto driver = createIntStringDriver(Duplicates | NullAnySegment);
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(-5, "b"))),
            std::make_pair(BtrieveError::Success, 1u));
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(5, "a"))),
            std::make_pair(BtrieveError::Success, 2u));
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(7, "b"))),
            std::make_pair(BtrieveError::Success, 3u));

  auto key = createIntStringRecord(5, "");
  ASSERT_EQ(driver->performOperation(0, view(key),
                                     OperationCode::QueryGreaterOrEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);

  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryGreater),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);

  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryLess),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 1u);

  ASSERT_EQ(
      driver->performOperation(0, view(key), OperationCode::QueryLessOrEqual),
      BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 1u);
}

TEST_F(BtrieveDriverTest, NullAllSegmentsAllZeroKeySearch) {
  auto driver = createIntStringDriver(Duplicates | NullAllSegments);
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(-5, "a"))),
            std::make_pair(BtrieveError::Success, 1u));
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(0, "a"))),
            std::make_pair(BtrieveError::Success, 2u));
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(7, "b"))),
            std::make_pair(BtrieveError::Success, 3u));

  auto key = createIntStringRecord(0, "");
  ASSERT_EQ(driver->performOperation(0, view(key),
                                     OperationCode::QueryGreaterOrEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 2u);

  ASSERT_EQ(driver->performOperation(0, view(key), OperationCode::QueryLess),
            BtrieveError::Success);
  ASSERT_EQ(driver->getPosition(), 1u);
}

// The same applies to a single segment nullable key that isn't a string.
TEST_F(BtrieveDriverTest, NullableIntegerKeySearchWithNullValue) {
  KeyDefinition keyDefinition(0, 2, 0, KeyDataType::Integer,
                              UseExtendedDataType | Duplicates |
                                  NullAllSegments,
                              false, 0, 0, 0, "", std::vector<char>());

  auto *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);
  database
      ->create(_TEXT("unused.db"),
               BtrieveDatabase({Key(&keyDefinition, 1)}, 512, 1,
                               sizeof(IntStringRecordStruct),
                               sizeof(IntStringRecordStruct), 0, 512,
                               RecordType::Fixed, false, 0))
      ->onRecordsComplete();

  ASSERT_EQ(driver.insertRecord(view(createIntStringRecord(-3, "a"))),
            std::make_pair(BtrieveError::Success, 1u));
  ASSERT_EQ(driver.insertRecord(view(createIntStringRecord(4, "b"))),
            std::make_pair(BtrieveError::Success, 2u));

  auto key = createIntStringRecord(0, "");
  key.resize(sizeof(int16_t));
  ASSERT_EQ(driver.performOperation(0, view(key),
                                    OperationCode::QueryGreaterOrEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 2u);

  ASSERT_EQ(driver.performOperation(0, view(key), OperationCode::QueryLess),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 1u);
}

// A unique nullable key can hold any number of null keys, since Btrieve
// doesn't index them.
TEST_F(BtrieveDriverTest, UniqueNullableCompositeKeyAllowsMultipleNulls) {
  auto driver = createIntStringDriver(NullAnySegment);
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(1, ""))),
            std::make_pair(BtrieveError::Success, 1u));
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(2, ""))),
            std::make_pair(BtrieveError::Success, 2u));
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(3, "a"))),
            std::make_pair(BtrieveError::Success, 3u));
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(3, "a"))),
            std::make_pair(BtrieveError::DuplicateKeyValue, 0u));
}

// A null key is stored as NULL columns, which != never compares unequal to.
TEST_F(BtrieveDriverTest, NonModifiableNullableKeyCantChangeToOrFromNull) {
  auto driver = createIntStringDriver(Duplicates | NullAllSegments);
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(0, ""))),
            std::make_pair(BtrieveError::Success, 1u));
  ASSERT_EQ(driver->insertRecord(view(createIntStringRecord(2, "x"))),
            std::make_pair(BtrieveError::Success, 2u));

  ASSERT_EQ(driver->updateRecord(1, view(createIntStringRecord(9, "q"))),
            BtrieveError::NonModifiableKeyValue);
  ASSERT_EQ(driver->updateRecord(2, view(createIntStringRecord(0, ""))),
            BtrieveError::NonModifiableKeyValue);

  // updating a null key to null is no change
  ASSERT_EQ(driver->updateRecord(1, view(createIntStringRecord(0, ""))),
            BtrieveError::Success);
}
