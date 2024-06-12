#include "BtrieveDriver.h"
#include "BtrieveException.h"
#include "SqliteDatabase.h"
#include "gtest/gtest.h"

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

TEST(BtrieveDriver, LoadsAndConverts) {
  {
    std::string acsName;
    std::vector<char> blankACS;
    struct stat statbuf;

    BtrieveDriver driver(new SqliteDatabase());

    driver.open("assets/MBBSEMU.DAT");

    ASSERT_EQ(stat("assets/MBBSEMU.db", &statbuf), 0);

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
  ASSERT_EQ(
      sqlite3_open_v2("assets/MBBSEMU.db", &db, SQLITE_OPEN_READONLY, nullptr),
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

  ASSERT_EQ(sqlite_exec(db,
                        "SELECT name, tbl_name, sql FROM sqlite_master WHERE "
                        "type = 'trigger'",
                        [](int numResults, char **data, char **columns) {
                          ASSERT_EQ(numResults, 3);
                          ASSERT_STREQ(data[0], "non_modifiable");
                          ASSERT_STREQ(data[1], "data_t");
                          ASSERT_STREQ(data[2], EXPECTED_TRIGGERS_SQL);
                        }),
            SQLITE_OK);

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

TEST(BtrieveDriver, LoadsPreexistingSqliteDatabase) {
  std::string acsName;
  std::vector<char> blankACS;

  BtrieveDriver driver(new SqliteDatabase());

  driver.open("assets/MBBSEMU.DB");

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
  uint32_t key1;
  char key2[32];
  uint32_t key3;
} MBBSEmuRecordStruct;
#pragma pack(pop)

static_assert(sizeof(MBBSEmuRecordStruct) == 74);

TEST(BtrieveDriver, StepNext) {
  BtrieveDriver driver(new SqliteDatabase());

  driver.open("assets/MBBSEMU.DB");

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepFirst),
            true);

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
            true);

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
            true);

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
            true);

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
            false);

  ASSERT_EQ(driver.getPosition(), 4);
}

TEST(BtrieveDriver, StepPrevious) {
  BtrieveDriver driver(new SqliteDatabase());

  driver.open("assets/MBBSEMU.DB");

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepLast),
            true);

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
            true);

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
            true);

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
            true);

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
            false);

  ASSERT_EQ(driver.getPosition(), 1);
}

TEST(BtrieveDriver, RandomAccess) {
  BtrieveDriver driver(new SqliteDatabase());

  driver.open("assets/MBBSEMU.DB");

  ASSERT_EQ(driver.performOperation(-1, std::basic_string_view<uint8_t>(),
                                    OperationCode::StepLast),
            true);

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