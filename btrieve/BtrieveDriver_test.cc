#include "BtrieveDriver.h"
#include "BtrieveException.h"
#include "SqliteDatabase.h"
#include "gtest/gtest.h"
#include <cstdio>
#include <dirent.h>
#include <filesystem>
#include <sys/types.h>

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

#pragma pack(push, 1)
typedef struct _taguuid {
  uint32_t a;
  uint16_t b;
  uint16_t c;
  uint16_t d;
  uint16_t e;
  uint32_t f;
} uuid;
#pragma pack(pop)

static_assert(sizeof(uuid) == 16);

class TempPath {
public:
  TempPath() = default;

  bool create() {
    char buf[64];
    uuid id;
    std::unique_ptr<FILE, decltype(&fclose)> f(fopen("/dev/random", "r"),
                                               &fclose);
    if (!f) {
      return false;
    }

    if (fread(&id, 1, sizeof(id), f.get()) != sizeof(id)) {
      return false;
    }

    snprintf(buf, sizeof(buf), "%X-%X-%X-%X-%X%X", id.a, id.b, id.c, id.d, id.e,
             id.f);
    buf[sizeof(buf) - 1] = 0;

    tempFolder = buf;

    std::string tempPath = getTempPath();
    int result = mkdir(tempPath.c_str(), 0700);

    if (result == 0) {
      return true;
    } else if (result == EEXIST) {
      deleteAllFiles(tempPath.c_str());
      return true;
    }

    return false;
  }

  ~TempPath() {
    auto tempPath = getTempPath();

    deleteAllFiles(tempPath.c_str());

    rmdir(tempPath.c_str());
  }

  std::string getTempPath() {
    std::filesystem::path tempPath(testing::TempDir());
    tempPath /= std::filesystem::path(tempFolder.c_str());

    return tempPath.c_str();
  }

  std::string copyToTempPath(const char *filePath) {
    const size_t bufferSize = 32 * 1024;

    std::filesystem::path destPath(getTempPath());
    destPath /= std::filesystem::path(filePath).filename();

    {
      std::unique_ptr<FILE, decltype(&fclose)> sourceFile(fopen(filePath, "r"),
                                                          &fclose);
      if (!sourceFile) {
        throw BtrieveException("Can't open %s\n", filePath);
      }

      std::unique_ptr<FILE, decltype(&fclose)> destFile(
          fopen(destPath.c_str(), "w"), &fclose);
      if (!destFile) {
        throw BtrieveException("Can't open %s\n", destPath.c_str());
      }

      size_t numRead;
      size_t numWritten;
      std::unique_ptr<uint8_t> buffer(new uint8_t[bufferSize]);
      while ((numRead = fread(reinterpret_cast<void *>(buffer.get()), 1,
                              bufferSize, sourceFile.get())) > 0) {
        numWritten = fwrite(reinterpret_cast<void *>(buffer.get()), 1, numRead,
                            destFile.get());
        if (numWritten != numRead) {
          throw BtrieveException("Can't write data\n");
        }
      }
    }

    return destPath;
  }

private:
  void deleteAllFiles(const char *filePath) {
    std::list<std::string> filesToUnlink;
    {
      struct closedir_deleter {
        void operator()(DIR *d) const { closedir(d); }
      };

      std::unique_ptr<DIR, closedir_deleter> dir(opendir(filePath));
      if (dir) {
        struct dirent *d;

        while ((d = readdir(dir.get()))) {
          if (strcmp(d->d_name, ".") && strcmp(d->d_name, "..")) {
            filesToUnlink.push_back(d->d_name);
          }
        }
      }
    }

    for (auto &fileName : filesToUnlink) {
      std::filesystem::path path(filePath);
      path /= fileName;

      EXPECT_EQ(unlink(path.c_str()), 0);
    }
  }

  std::string tempFolder;
};

class BtrieveDriverTest : public ::testing::Test {
protected:
  TempPath *tempPath;

public:
  BtrieveDriverTest() = default;

  virtual ~BtrieveDriverTest() = default;

  virtual void SetUp() {
    tempPath = new TempPath();
    ASSERT_TRUE(tempPath->create());
  }

  virtual void TearDown() { delete tempPath; }
};

TEST_F(BtrieveDriverTest, LoadsAndConverts) {
  std::string convertedDbPath;

  {
    std::string acsName;
    std::vector<char> blankACS;
    struct stat statbuf;
    BtrieveDriver driver(new SqliteDatabase());

    auto mbbsEmuDat = tempPath->copyToTempPath("assets/MBBSEMU.DAT");
    driver.open(mbbsEmuDat.c_str());

    std::filesystem::path dbPath(mbbsEmuDat);
    dbPath.remove_filename();
    dbPath /= "MBBSEMU.db";
    convertedDbPath = dbPath.c_str();

    ASSERT_EQ(stat(dbPath.c_str(), &statbuf), 0);

    EXPECT_EQ(driver.getRecordLength(), 74);
    EXPECT_FALSE(driver.isVariableLengthRecords());
    EXPECT_EQ(driver.getKeys().size(), 4);

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
                          ASSERT_STREQ(data[0], "2");
                        }),
            SQLITE_OK);

  static const char *EXPECTED_METADATA_T_SQL =
      "CREATE TABLE "
      "metadata_t(record_length INTEGER NOT NULL, physical_record_length "
      "INTEGER "
      "NOT NULL, page_length INTEGER NOT NULL, variable_length_records INTEGER "
      "NOT "
      "NULL, version INTEGER NOT NULL, acs_name STRING, acs BLOB)";

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
      "NULL, UNIQUE(number, segment))";

  ASSERT_EQ(sqlite_exec(db,
                        "SELECT sql FROM sqlite_master WHERE name = 'keys_t'",
                        [](int numResults, char **data, char **columns) {
                          ASSERT_EQ(numResults, 1);
                          ASSERT_STREQ(data[0], EXPECTED_KEYS_T_SQL);
                        }),
            SQLITE_OK);

  static const char *EXPECTED_DATA_T_SQL =
      "CREATE TABLE data_t(id INTEGER PRIMARY KEY, "
      "data BLOB NOT NULL, key_0 TEXT, key_1 INTEGER NOT NULL UNIQUE, key_2 "
      "TEXT, key_3 INTEGER NOT NULL UNIQUE)";

  ASSERT_EQ(sqlite_exec(db,
                        "SELECT sql FROM sqlite_master WHERE name = 'data_t'",
                        [](int numResults, char **data, char **columns) {
                          ASSERT_EQ(numResults, 1);
                          ASSERT_STREQ(data[0], EXPECTED_DATA_T_SQL);
                        }),
            SQLITE_OK);

  static const char *EXPECTED_TRIGGERS_SQL =
      "CREATE TRIGGER non_modifiable "
      "BEFORE UPDATE ON data_t BEGIN SELECT CASE WHEN NEW.key_0 != "
      "OLD.key_0 THEN RAISE (ABORT,'You modified a non-modifiable key_0!') "
      "WHEN NEW.key_3 != OLD.key_3 THEN RAISE (ABORT,'You modified a "
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

  EXPECT_EQ(driver.getRecordLength(), 74);
  EXPECT_FALSE(driver.isVariableLengthRecords());
  EXPECT_EQ(driver.getKeys().size(), 4);

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

  ASSERT_EQ(driver.getPosition(), 1);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            3444);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 2);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            7776);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 3);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            1052234073);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 4);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            -615634567);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.getPosition(), 4);
}

TEST_F(BtrieveDriverTest, StepPrevious) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepLast),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 4);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            -615634567);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 3);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            1052234073);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 2);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            7776);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 1);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            3444);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.getPosition(), 1);
}

TEST_F(BtrieveDriverTest, RandomAccess) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  std::pair<bool, Record> data(driver.getRecord(4));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  auto dbValue = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  EXPECT_STREQ(dbValue->key0, "Sysop");
  EXPECT_EQ(dbValue->key1, -615634567);
  EXPECT_STREQ(dbValue->key2, "stringValue");
  EXPECT_EQ(dbValue->key3, 4);

  data = driver.getRecord(3);
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            1052234073);

  data = driver.getRecord(2);
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            7776);

  data = driver.getRecord(1);
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
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
  ASSERT_EQ(data.second.getData().size(), 0);

  data = driver.getRecord(0);
  ASSERT_FALSE(data.first);
  ASSERT_EQ(data.second.getData().size(), 0);

  driver.setPosition(5);
  data = driver.getRecord();
  ASSERT_FALSE(data.first);
  ASSERT_EQ(data.second.getData().size(), 0);

  driver.setPosition(0);
  data = driver.getRecord();
  ASSERT_FALSE(data.first);
  ASSERT_EQ(data.second.getData().size(), 0);
}

TEST_F(BtrieveDriverTest, GetRecordCount) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.getRecordCount(), 4);
}

TEST_F(BtrieveDriverTest, DeleteAll) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  ASSERT_EQ(driver.getRecordCount(), 4);

  ASSERT_EQ(driver.deleteAll(), BtrieveError::Success);

  ASSERT_EQ(driver.getRecordCount(), 0);
  ASSERT_EQ(driver.getPosition(), 0);
}

TEST_F(BtrieveDriverTest, Delete) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  driver.setPosition(2);

  ASSERT_EQ(driver.getRecordCount(), 4);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::Delete),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(driver.getRecordCount(), 3);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::Delete),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(driver.getRecordCount(), 3);

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

  ASSERT_EQ(driver.getPosition(), 1);
  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            3444);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 3);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            1052234073);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);

  ASSERT_EQ(driver.getPosition(), 4);
  data = std::move(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  EXPECT_EQ(reinterpret_cast<const MBBSEmuRecordStruct *>(
                data.second.getData().data())
                ->key1,
            -615634567);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.getPosition(), 4);
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
            5);
  std::pair<bool, Record> data(driver.getRecord(5));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Paladine");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, optimus sum");
  ASSERT_EQ(dbRecord->key3, 5);

  ASSERT_EQ(driver.getRecordCount(), 5);
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
  record.key3 = 4444; // set a manual value for the autoincrement value

  ASSERT_EQ(driver.insertRecord(std::basic_string_view<uint8_t>(
                reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            5);
  std::pair<bool, Record> data(driver.getRecord(5));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Paladine");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, optimus sum");
  ASSERT_EQ(dbRecord->key3, 4444);

  ASSERT_EQ(driver.getRecordCount(), 5);
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
  record.key3 = 4444; // set a manual value for the autoincrement value

  // chop off the last 14 bytes rather than the full 74
  ASSERT_EQ(driver.insertRecord(std::basic_string_view<uint8_t>(
                reinterpret_cast<uint8_t *>(&record), sizeof(record) - 14)),
            5);
  std::pair<bool, Record> data(driver.getRecord(5));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Paladine");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, opti"); // cut off
  ASSERT_EQ(dbRecord->key3, 5);

  ASSERT_EQ(driver.getRecordCount(), 5);
}

TEST_F(BtrieveDriverTest, InsertionConstraintFailure) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0, "Paladine");
  record.key1 = 3444; // key constraint right here
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 4444; // set a manual value for the autoincrement value

  ASSERT_EQ(driver.insertRecord(std::basic_string_view<uint8_t>(
                reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            0);

  ASSERT_EQ(driver.getRecordCount(), 4);
}

TEST_F(BtrieveDriverTest, UpdateTest) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0, "Sysop");
  record.key1 = 31337; // key constraint right here
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 1;

  ASSERT_EQ(driver.updateRecord(
                1, std::basic_string_view<uint8_t>(
                       reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            BtrieveError::Success);

  std::pair<bool, Record> data(driver.getRecord(1));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Sysop");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, optimus sum");
  ASSERT_EQ(dbRecord->key3, 1);

  ASSERT_EQ(driver.getRecordCount(), 4);
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
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Sysop");
  ASSERT_EQ(dbRecord->key1, 31337);
  ASSERT_STREQ(dbRecord->key2, "In orbe terrarum, optimus sum");
  ASSERT_EQ(dbRecord->key3, 2); // truncated down to 2

  ASSERT_EQ(driver.getRecordCount(), 4);
}

TEST_F(BtrieveDriverTest, UpdateConstraintFailedTest) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  record.key1 = 7776;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 1;

  ASSERT_EQ(driver.updateRecord(
                1, std::basic_string_view<uint8_t>(
                       reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            BtrieveError::DuplicateKeyValue);

  std::pair<bool, Record> data(driver.getRecord(1));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key0, "Sysop");
  ASSERT_EQ(dbRecord->key1, 3444);
  ASSERT_STREQ(dbRecord->key2, "3444");
  ASSERT_EQ(dbRecord->key3, 1);

  ASSERT_EQ(driver.getRecordCount(), 4);
}

TEST_F(BtrieveDriverTest, UpdateTestNonModifiableKeyModifiedFailed) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0,
         "Changed"); // this key isn't modifiable, but we try anyways
  record.key1 = 3444;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 333333; // this key isn't modifiable, but we try anyways

  EXPECT_EQ(driver.updateRecord(
                1, std::basic_string_view<uint8_t>(
                       reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            BtrieveError::NonModifiableKeyValue);

  std::pair<bool, Record> data(driver.getRecord(1));
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  EXPECT_STREQ(dbRecord->key0, "Sysop");
  EXPECT_EQ(dbRecord->key1, 3444);
  EXPECT_STREQ(dbRecord->key2, "3444");
  EXPECT_EQ(dbRecord->key3, 1);

  ASSERT_EQ(driver.getRecordCount(), 4);
}

TEST_F(BtrieveDriverTest, UpdateInvalidKeyNumber) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  MBBSEmuRecordStruct record;
  memset(&record, 0, sizeof(record));
  strcpy(record.key0,
         "Changed"); // this key isn't modifiable, but we try anyways
  record.key1 = 3444;
  strcpy(record.key2, "In orbe terrarum, optimus sum");
  record.key3 = 333333; // this key isn't modifiable, but we try anyways

  EXPECT_EQ(driver.updateRecord(
                5, std::basic_string_view<uint8_t>(
                       reinterpret_cast<uint8_t *>(&record), sizeof(record))),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.getRecordCount(), 4);
}

TEST_F(BtrieveDriverTest, SeekByKeyStringDuplicates) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("Sysop"), 5);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4);
}

TEST_F(BtrieveDriverTest, SeekByKeyStringDuplicatesUpAndDown) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("Sysop"), 5);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4);

  // let's go backwards now
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1);

  // forward for one last test
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2);
  // back one last time to test in-middle previous
  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1);
}

TEST_F(BtrieveDriverTest, SeekByKeyString) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("StringValue"), 11);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4);

  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1);

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1);
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
  ASSERT_EQ(driver.getRecord().second.getPosition(), 3);

  std::pair<bool, Record> data(driver.getRecord());
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 2);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 1);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getRecord().second.getPosition(), 4);
}

TEST_F(BtrieveDriverTest, SeekByKeyNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>("Sysop2"), 6);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::InvalidPositioning);

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
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "3444");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "7776");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 3);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "StringValue");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  ASSERT_EQ(driver.getPosition(), 4);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 4);
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
  ASSERT_EQ(driver.getPosition(), 4);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, -615634567);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 3);
}

TEST_F(BtrieveDriverTest, SeekByKeyFirstNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  driver.deleteAll();

  ASSERT_EQ(driver.performOperation(0, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryFirst),
            BtrieveError::InvalidPositioning);

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
  ASSERT_EQ(driver.getPosition(), 4);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "StringValue");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "7776");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "3444");

  ASSERT_EQ(driver.performOperation(2, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 1);
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
  ASSERT_EQ(driver.getPosition(), 3);
  ASSERT_EQ(data.second.getData().size(), 74);
  const MBBSEmuRecordStruct *dbRecord =
      reinterpret_cast<const MBBSEmuRecordStruct *>(
          data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, -615634567);

  ASSERT_EQ(driver.performOperation(1, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 4);
}

TEST_F(BtrieveDriverTest, SeekByKeyLastNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  driver.deleteAll();

  ASSERT_EQ(driver.performOperation(0, std::basic_string_view<uint8_t>(),
                                    OperationCode::QueryLast),
            BtrieveError::InvalidPositioning);

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
  ASSERT_EQ(driver.getPosition(), 3);
  ASSERT_EQ(data.second.getData().size(), 74);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "StringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 4);
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
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 3);
}

TEST_F(BtrieveDriverTest, SeekByKeyGreaterNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 2000000000;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryGreater),
            BtrieveError::InvalidPositioning);
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
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "7776");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "StringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "stringValue");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 4);
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
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), 74);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 1052234073);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryNext),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 3);
}

TEST_F(BtrieveDriverTest, SeekByKeyGreaterOrEqualNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = 2000000000;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryGreaterOrEqual),
            BtrieveError::InvalidPositioning);
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
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), 74);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "3444");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 1);
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
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), 74);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, -615634567);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 4);
}

TEST_F(BtrieveDriverTest, SeekByKeyLessNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = -2000000000;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryLess),
            BtrieveError::InvalidPositioning);
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
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "7776");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_STREQ(dbRecord->key2, "3444");

  ASSERT_EQ(driver.performOperation(2, key, OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 1);
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
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
  const auto *dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 7776);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);

  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, 3444);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4);
  ASSERT_EQ(data.second.getData().size(), 74);
  dbRecord = reinterpret_cast<const MBBSEmuRecordStruct *>(
      data.second.getData().data());

  ASSERT_EQ(dbRecord->key1, -615634567);

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryPrevious),
            BtrieveError::InvalidPositioning);
  ASSERT_EQ(driver.getPosition(), 4);
}

TEST_F(BtrieveDriverTest, SeekByKeyLessOrEqualNotFound) {
  BtrieveDriver driver(new SqliteDatabase());

  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  driver.open(mbbsEmuDb.c_str());

  uint32_t value = -2000000000;
  auto key = std::basic_string_view<uint8_t>(
      reinterpret_cast<const uint8_t *>(&value), sizeof(value));

  ASSERT_EQ(driver.performOperation(1, key, OperationCode::QueryLessOrEqual),
            BtrieveError::InvalidPositioning);
}

// TODO add tests for queryEqual/Less/Greater when there are duplicates

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

static std::vector<uint8_t> createRecord(const char *username) {
  std::vector<uint8_t> record(ACS_RECORD_LENGTH);

  memset(record.data(), 0xFF, record.size());
  strcpy(reinterpret_cast<char *>(record.data() + 2), username);

  return record;
}

static BtrieveDatabase createACSBtrieveDatabase() {
  std::vector<Key> keys;
  uint16_t pageLength = 512;
  unsigned int pageCount = 1;
  unsigned int recordLength = ACS_RECORD_LENGTH;
  unsigned int physicalRecordLength = ACS_RECORD_LENGTH;
  unsigned int recordCount = 0;
  unsigned int fileLength = pageCount * pageLength;

  std::vector<char> acs = upperACS();

  KeyDefinition keyDefinition(0, 30, 2, KeyDataType::String,
                              UseExtendedDataType | NumberedACS, false, 0, 0, 0,
                              "acsName", acs);

  keys.push_back(Key(&keyDefinition, 1));

  return BtrieveDatabase(keys, pageLength, pageCount, recordLength,
                         physicalRecordLength, recordCount, fileLength,
                         /* variableLengthRecords= */ false);
}

TEST_F(BtrieveDriverTest, ACSSeekByKey) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader = database->create("unused.db", createACSBtrieveDatabase());
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Sysop").data(), ACS_RECORD_LENGTH)),
            1);

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Paladine").data(), ACS_RECORD_LENGTH)),
            2);

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Testing").data(), ACS_RECORD_LENGTH)),
            3);

  std::basic_string_view<uint8_t> key(
      reinterpret_cast<const uint8_t *>("paladine"), 8);

  ASSERT_EQ(driver.performOperation(0, key, OperationCode::QueryEqual),
            BtrieveError::Success);
  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Paladine", 8), 0);
}

TEST_F(BtrieveDriverTest, ACSInsertDuplicateFails) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader = database->create("unused.db", createACSBtrieveDatabase());
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("Sysop").data(), ACS_RECORD_LENGTH)),
            1);

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("sysop").data(), ACS_RECORD_LENGTH)),
            0);

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("SysOp").data(), ACS_RECORD_LENGTH)),
            0);

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("SysoP").data(), ACS_RECORD_LENGTH)),
            0);
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
                         /* variableLengthRecords= */ false);
}

TEST_F(BtrieveDriverTest, KeylessDatabaseEnumeration) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader =
      database->create("unused.db", createKeylessBtrieveDatabase());
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Sysop").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Paladine").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordLoaded(std::basic_string_view<uint8_t>(
      createRecord("Testing").data(), ACS_RECORD_LENGTH));
  recordLoader->onRecordsComplete();

  ASSERT_EQ(database->insertRecord(std::basic_string_view<uint8_t>(
                createRecord("paladine").data(), ACS_RECORD_LENGTH)),
            4);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepFirst),
            BtrieveError::Success);
  auto data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 1);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Sysop", 5), 0);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepPrevious),
            BtrieveError::InvalidPositioning);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Paladine", 8), 0);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 3);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "Testing", 7), 0);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::Success);
  data = driver.getRecord();
  ASSERT_TRUE(data.first);
  ASSERT_EQ(driver.getPosition(), 4);
  ASSERT_EQ(data.second.getData().size(), ACS_RECORD_LENGTH);
  ASSERT_EQ(memcmp(data.second.getData().data() + 2, "paladine", 5), 0);

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepNext),
            BtrieveError::InvalidPositioning);
}

TEST_F(BtrieveDriverTest, KeylessDataQueryFails) {
  SqliteDatabase *database = new SqliteDatabase(SQLITE_OPEN_MEMORY);
  BtrieveDriver driver(database);

  auto recordLoader =
      database->create("unused.db", createKeylessBtrieveDatabase());
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
  ASSERT_EQ(driver.getPosition(), 2);
  ASSERT_EQ(data.second.getData().size(), 74);
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