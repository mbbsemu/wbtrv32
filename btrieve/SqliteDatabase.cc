#include "SqliteDatabase.h"
#include "BindableValue.h"
#include "BtrieveException.h"
#include "SqlitePreparedStatement.h"
#include "SqliteTransaction.h"
#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"
#include <atomic>
#include <sstream>

namespace btrieve {

static const unsigned int CURRENT_VERSION = 2;

static std::string
commaDelimitedKey(const std::vector<Key> &keys,
                  std::function<std::string(const Key &key)> func) {
  std::stringstream sb;
  bool first = true;
  for (auto &key : keys) {
    if (!first) {
      sb << ", ";
    }
    first = false;

    sb << func(key);
  }
  return sb.str();
}

class SqliteCreationRecordLoader : public RecordLoader {
public:
  SqliteCreationRecordLoader(std::shared_ptr<sqlite3> database_,
                             const BtrieveDatabase &database)
      : database(database_), keys(database.getKeys()) {}
  virtual ~SqliteCreationRecordLoader() {}

  void createSqliteInsertionCommand() {
    std::string insertSql;
    if (!keys.empty()) {
      std::stringstream sb;
      sb << "INSERT INTO data_t(data, ";
      sb << commaDelimitedKey(
          keys, [](const Key &key) { return key.getSqliteKeyName(); });
      sb << ") VALUES(@data, ";
      sb << commaDelimitedKey(
          keys, [](const Key &key) { return "@" + key.getSqliteKeyName(); });
      sb << ");";
      insertSql = sb.str();
    } else {
      insertSql = "INSERT INTO data_t(data) VALUES (@data)";
    }

    transaction.reset(new SqliteTransaction(this->database));
    insertionCommand.reset(
        new SqlitePreparedStatement(this->database, insertSql.c_str()));
  }

private:
  virtual bool onRecordLoaded(std::basic_string_view<uint8_t> record) {
    insertionCommand->reset();
    insertionCommand->bindParameter(1, record);

    unsigned int parameterNumber = 2;
    for (auto &key : keys) {
      insertionCommand->bindParameter(
          parameterNumber++, key.extractKeyInRecordToSqliteObject(record));
    }

    insertionCommand->execute();

    return true;
  };

  virtual void onRecordsComplete() {
    try {
      transaction->commit();
    } catch (const BtrieveException &ex) {
      transaction->rollback();
      throw ex;
    }
  }

  std::shared_ptr<sqlite3> database;
  std::unique_ptr<SqliteTransaction> transaction;
  std::unique_ptr<SqlitePreparedStatement> insertionCommand;
  std::vector<Key> keys;
};

// Opens a Btrieve database as a sql backed file. Will convert a legacy file
// in place if required. Throws a BtrieveException if something fails.
void SqliteDatabase::open(const char *fileName) {
  sqlite3 *db;
  int errorCode = sqlite3_open_v2(fileName, &db,
                                  SQLITE_OPEN_READWRITE | openFlags, nullptr);
  if (errorCode != SQLITE_OK) {
    throwException(errorCode);
  }

  this->database = std::shared_ptr<sqlite3>(db, &sqlite3_close);

  std::string acsName;
  std::vector<char> acs;

  loadSqliteMetadata(acsName, acs);
  loadSqliteKeys(acsName, acs);
}

void SqliteDatabase::loadSqliteMetadata(std::string &acsName,
                                        std::vector<char> &acs) {
  SqlitePreparedStatement command(
      database,
      "SELECT record_length, variable_length_records, version, acs_name, acs "
      "FROM metadata_t");
  std::unique_ptr<SqliteReader> reader = command.executeReader();
  if (!reader->read()) {
    throw BtrieveException("Can't read metadata_t");
  }

  recordLength = reader->getInt32(0);
  variableLengthRecords = reader->getBoolean(1);
  acsName = reader->getString(3);

  if (!reader->isDBNull(4)) {
    std::vector<uint8_t> readAcs = reader->getBlob(4);
    if (readAcs.size() != ACS_LENGTH) {
      throw BtrieveException(
          "The ACS length is not 256 in the database. This is corrupt.");
    }

    acs.reserve(ACS_LENGTH);
    std::copy(readAcs.begin(), readAcs.end(), std::back_inserter(acs));
  }

  // reserved for future upgrade paths
  /*
  var version = reader.GetInt32(2);
  if (version != CURRENT_VERSION){
    UpgradeDatabaseFromVersion(version);
  }
  */
}

void SqliteDatabase::loadSqliteKeys(const std::string &acsName,
                                    const std::vector<char> &acs) {
  unsigned int numKeys = 0;
  {
    SqlitePreparedStatement keyCountCommand(database,
                                            "SELECT MAX(number) FROM keys_t");
    std::unique_ptr<SqliteReader> reader = keyCountCommand.executeReader();
    if (!reader->read()) {
      // we have no keys, strange, but valid
      return;
    }

    numKeys = reader->getInt32(0) + 1;
  }

  keys.resize(numKeys);

  SqlitePreparedStatement command(
      database, "SELECT number, segment, attributes, data_type, offset, length "
                "FROM keys_t ORDER BY number, segment");
  std::unique_ptr<SqliteReader> reader = command.executeReader();

  unsigned int segmentIndex = 0;
  while (reader->read()) {
    unsigned int number = reader->getInt32(0);

    uint8_t nullValue = 0; // TODO why isn't this in database?

    KeyDefinition keyDefinition(
        number, reader->getInt32(5), reader->getInt32(4),
        (KeyDataType)reader->getInt32(3), reader->getInt32(2),
        reader->getBoolean(1), reader->getBoolean(1) ? number : 0, segmentIndex,
        nullValue, acsName, acs);

    keys[number].addSegment(keyDefinition);
  }

  for (auto &key : keys) {
    key.updateSegmentIndices();
  }
}

std::unique_ptr<RecordLoader>
SqliteDatabase::create(const char *fileName, const BtrieveDatabase &database) {
  sqlite3 *db;
  int errorCode = sqlite3_open_v2(
      fileName, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | openFlags,
      nullptr);
  if (errorCode != SQLITE_OK) {
    throwException(errorCode);
  }

  this->database = std::shared_ptr<sqlite3>(db, &sqlite3_close);

  recordLength = database.getRecordLength();
  variableLengthRecords = database.isVariableLengthRecords();
  keys = database.getKeys();

  createSqliteMetadataTable(database);
  createSqliteKeysTable(database);
  createSqliteDataTable(database);
  createSqliteDataIndices(database);
  createSqliteTriggers(database);

  auto recordLoader = std::unique_ptr<SqliteCreationRecordLoader>(
      new SqliteCreationRecordLoader(this->database, database));
  recordLoader->createSqliteInsertionCommand();
  return recordLoader;
}

void SqliteDatabase::createSqliteMetadataTable(
    const BtrieveDatabase &database) {
  const char *const createTableStatement =
      "CREATE TABLE metadata_t(record_length INTEGER NOT NULL, "
      "physical_record_length INTEGER NOT NULL, page_length INTEGER NOT NULL, "
      "variable_length_records INTEGER NOT NULL, version INTEGER NOT NULL, "
      "acs_name STRING, acs BLOB)";

  SqlitePreparedStatement createTableCommand(this->database,
                                             createTableStatement);
  createTableCommand.execute();

  const char *const insertIntoTableStatement =
      "INSERT INTO metadata_t(record_length, physical_record_length, "
      "page_length, variable_length_records, version, acs_name, acs) "
      "VALUES(@record_length, @physical_record_length, @page_length, "
      "@variable_length_records, @version, @acs_name, @acs)";

  SqlitePreparedStatement command(this->database, insertIntoTableStatement);
  command.bindParameter(1, BindableValue(database.getRecordLength()));
  command.bindParameter(2, BindableValue(database.getPhysicalRecordLength()));
  command.bindParameter(3, BindableValue(database.getPageLength()));
  command.bindParameter(4, BindableValue(database.isVariableLengthRecords()));
  command.bindParameter(5, BindableValue(CURRENT_VERSION));
  command.bindParameter(6, BindableValue(database.getKeys()[0].getACSName()));
  command.bindParameter(7, BindableValue(database.getKeys()[0].getACS()));

  command.execute();
}

void SqliteDatabase::createSqliteKeysTable(const BtrieveDatabase &database) {
  const char *const createTableStatement =
      "CREATE TABLE keys_t(id INTEGER PRIMARY KEY, number INTEGER NOT NULL, "
      "segment INTEGER NOT NULL, attributes INTEGER NOT NULL, data_type "
      "INTEGER NOT NULL, offset INTEGER NOT NULL, length INTEGER NOT NULL, "
      "null_value INTEGER NOT NULL, UNIQUE(number, segment))";

  SqlitePreparedStatement createTableCommand(this->database,
                                             createTableStatement);
  createTableCommand.execute();

  const char *const insertIntoTableStatement =
      "INSERT INTO keys_t(number, segment, attributes, data_type, offset, "
      "length, null_value) VALUES(@number, @segment, @attributes, @data_type, "
      "@offset, @length, @null_value)";

  SqlitePreparedStatement insertIntoTableCommand(this->database,
                                                 insertIntoTableStatement);

  for (auto &key : database.getKeys()) {
    for (auto &keyDefinition : key.getSegments()) {
      insertIntoTableCommand.reset();

      insertIntoTableCommand.bindParameter(
          1, BindableValue(keyDefinition.getNumber()));
      insertIntoTableCommand.bindParameter(
          2, BindableValue(keyDefinition.getSegmentIndex()));
      insertIntoTableCommand.bindParameter(
          3, BindableValue(keyDefinition.getAttributes()));
      insertIntoTableCommand.bindParameter(
          4, BindableValue(keyDefinition.getDataType()));
      insertIntoTableCommand.bindParameter(
          5, BindableValue(keyDefinition.getOffset()));
      insertIntoTableCommand.bindParameter(
          6, BindableValue(keyDefinition.getLength()));
      insertIntoTableCommand.bindParameter(
          7, BindableValue(keyDefinition.getNullValue()));

      insertIntoTableCommand.execute();
    }
  }
}

void SqliteDatabase::createSqliteDataTable(const BtrieveDatabase &database) {
  std::stringstream sb;
  sb << "CREATE TABLE data_t(id INTEGER PRIMARY KEY, data BLOB NOT NULL";
  for (auto &key : database.getKeys()) {
    sb << ", " << key.getSqliteKeyName() << " " << key.getSqliteColumnSql();
  }

  sb << ");";

  SqlitePreparedStatement createTableStatement(this->database,
                                               sb.str().c_str());
  createTableStatement.execute();
}

void SqliteDatabase::createSqliteDataIndices(const BtrieveDatabase &database) {
  for (auto &key : database.getKeys()) {
    const char *possiblyUnique = key.isUnique() ? "UNIQUE" : "";
    auto sqliteKeyName = key.getSqliteKeyName();
    SqlitePreparedStatement command(
        this->database, "CREATE %s INDEX %s_index on data_t(%s)",
        possiblyUnique, sqliteKeyName.c_str(), sqliteKeyName.c_str());
    command.execute();
  }
}

void SqliteDatabase::createSqliteTriggers(const BtrieveDatabase &database) {
  std::vector<Key> nonModifiableKeys;
  std::copy_if(database.getKeys().begin(), database.getKeys().end(),
               std::back_inserter(nonModifiableKeys),
               [](const Key &key) { return !key.isModifiable(); });

  if (nonModifiableKeys.empty()) {
    return;
  }

  std::stringstream builder;
  builder << "CREATE TRIGGER non_modifiable BEFORE UPDATE ON data_t BEGIN "
             "SELECT CASE ";

  for (auto &key : nonModifiableKeys) {
    builder << "WHEN NEW." << key.getSqliteKeyName() << " != OLD."
            << key.getSqliteKeyName()
            << " THEN "
               "RAISE (ABORT,'You modified a non-modifiable "
            << key.getSqliteKeyName() << "!') ";
  }

  builder << "END; END;";

  SqlitePreparedStatement cmd(this->database, builder.str().c_str());
  cmd.execute();
}

// Closes an opened database.
void SqliteDatabase::close() {
  preparedStatements.clear();
  database.reset();
}

SqlitePreparedStatement &
SqliteDatabase::getPreparedStatement(const char *sql) const {
  auto iter = preparedStatements.find(sql);
  if (iter == preparedStatements.end()) {
    iter =
        preparedStatements.emplace(sql, SqlitePreparedStatement(database, sql))
            .first;
  }

  iter->second.reset(); // reset it before we return it
  return iter->second;
}

bool SqliteDatabase::stepFirst() {
  SqlitePreparedStatement &command =
      getPreparedStatement("SELECT id, data FROM data_t ORDER BY id LIMIT 1");
  auto reader = command.executeReader();
  if (!reader->read()) {
    return false;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return true;
}

bool SqliteDatabase::stepLast() {
  SqlitePreparedStatement &command = getPreparedStatement(
      "SELECT id, data FROM data_t ORDER BY id DESC LIMIT 1");
  auto reader = command.executeReader();
  if (!reader->read()) {
    return false;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return true;
}

bool SqliteDatabase::stepNext() {
  SqlitePreparedStatement &command = getPreparedStatement(
      "SELECT id, data FROM data_t WHERE id > @position ORDER BY id LIMIT 1");

  command.bindParameter(1, position);

  auto reader = command.executeReader();
  if (!reader->read()) {
    return false;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return true;
}

bool SqliteDatabase::stepPrevious() {
  SqlitePreparedStatement &command =
      getPreparedStatement("SELECT id, data FROM data_t WHERE id < @position "
                           "ORDER BY id DESC LIMIT 1");

  command.bindParameter(1, position);

  auto reader = command.executeReader();
  if (!reader->read()) {
    return false;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return true;
}

const Record &SqliteDatabase::cacheBtrieveRecord(unsigned int position,
                                                 const SqliteReader &reader,
                                                 unsigned int columnOrdinal) {
  const Record &record = readRecord(position, reader, columnOrdinal);
  cache.cache(position, record);
  return record;
}

std::pair<bool, Record> SqliteDatabase::selectRecord(unsigned int position) {
  this->position = position;

  SqlitePreparedStatement &command =
      getPreparedStatement("SELECT data FROM data_t WHERE id = @offset");
  command.bindParameter(1, position);

  auto reader = command.executeReader();
  if (!reader->read()) {
    return std::pair<bool, Record>(false, Record());
  }

  return std::pair<bool, Record>(true, readRecord(position, *reader, 0));
}

unsigned int SqliteDatabase::getRecordCount() const {
  const SqlitePreparedStatement &command =
      getPreparedStatement("SELECT COUNT(*) FROM data_t");
  auto reader = command.executeReader();
  if (!reader->read()) {
    return -1;
  }

  return reader->getInt32(0);
}

void SqliteDatabase::deleteAll() {
  getPreparedStatement("DELETE FROM data_t").execute();

  cache.clear();
  setPosition(0);
}

} // namespace btrieve