#include "SqliteDatabase.h"
#include "BindableValue.h"
#include "BtrieveException.h"
#include "SqlitePreparedStatement.h"
#include "SqliteQuery.h"
#include "SqliteTransaction.h"
#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"
#include <atomic>
#include <sstream>

namespace btrieve {

static const unsigned int CURRENT_VERSION = 2;

template <class InputIt, class UnaryPred>
static std::string commaDelimited(InputIt first, InputIt last, UnaryPred pred) {
  std::stringstream sb;
  bool firstRun = true;
  for (; first != last; ++first) {
    if (!firstRun) {
      sb << ", ";
    }
    firstRun = false;

    sb << pred(*first);
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
      sb << commaDelimited(keys.begin(), keys.end(), [](const Key &key) {
        return key.getSqliteKeyName();
      });
      sb << ") VALUES(@data, ";
      sb << commaDelimited(keys.begin(), keys.end(), [](const Key &key) {
        return "@" + key.getSqliteKeyName();
      });
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

BtrieveError SqliteDatabase::stepFirst() {
  SqlitePreparedStatement &command =
      getPreparedStatement("SELECT id, data FROM data_t ORDER BY id LIMIT 1");
  auto reader = command.executeReader();
  if (!reader->read()) {
    return BtrieveError::InvalidPositioning;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return BtrieveError::Success;
}

BtrieveError SqliteDatabase::stepLast() {
  SqlitePreparedStatement &command = getPreparedStatement(
      "SELECT id, data FROM data_t ORDER BY id DESC LIMIT 1");
  auto reader = command.executeReader();
  if (!reader->read()) {
    return BtrieveError::InvalidPositioning;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return BtrieveError::Success;
}

BtrieveError SqliteDatabase::stepNext() {
  SqlitePreparedStatement &command = getPreparedStatement(
      "SELECT id, data FROM data_t WHERE id > @position ORDER BY id LIMIT 1");

  command.bindParameter(1, position);

  auto reader = command.executeReader();
  if (!reader->read()) {
    return BtrieveError::InvalidPositioning;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return BtrieveError::Success;
}

BtrieveError SqliteDatabase::stepPrevious() {
  SqlitePreparedStatement &command =
      getPreparedStatement("SELECT id, data FROM data_t WHERE id < @position "
                           "ORDER BY id DESC LIMIT 1");

  command.bindParameter(1, position);

  auto reader = command.executeReader();
  if (!reader->read()) {
    return BtrieveError::InvalidPositioning;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return BtrieveError::Success;
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

BtrieveError SqliteDatabase::deleteAll() {
  bool ret = getPreparedStatement("DELETE FROM data_t").executeNoThrow();

  if (ret) {
    cache.clear();
    setPosition(0);
  }

  return ret ? BtrieveError::Success : BtrieveError::IOError;
}

BtrieveError SqliteDatabase::deleteRecord() {
  cache.remove(position);

  SqlitePreparedStatement &command =
      getPreparedStatement("DELETE FROM data_t WHERE id=@position");
  command.bindParameter(1, BindableValue(position));
  bool ret = command.executeNoThrow();
  if (!ret) {
    return BtrieveError::IOError;
  }

  return sqlite3_changes(database.get()) == 1
             ? BtrieveError::Success
             : BtrieveError::InvalidPositioning;
}

unsigned int
SqliteDatabase::insertRecord(std::basic_string_view<uint8_t> record) {
  BtrieveError error;
  std::vector<uint8_t> data(record.size());
  memcpy(data.data(), record.data(), record.size());

  if (!variableLengthRecords && record.size() != recordLength) {
    //_logger.Warn(
    //    $"Btrieve Record Size Mismatch TRUNCATING. Expected Length
    //    {RecordLength}, Actual Length {record.Length}");
    data.resize(recordLength, 0);
    record = std::basic_string_view<uint8_t>(data.data(), recordLength);
  }

  SqliteTransaction transaction(database);

  error = insertAutoincrementValues(data);
  if (error != BtrieveError::Success) {
    transaction.rollback();
    return error;
  }

  std::string insertSql;
  if (!keys.empty()) {
    std::stringstream sb;
    sb << "INSERT INTO data_t(data, ";
    sb << commaDelimited(keys.begin(), keys.end(),
                         [](const Key &key) { return key.getSqliteKeyName(); });
    sb << ") VALUES(@data, ";
    sb << commaDelimited(keys.begin(), keys.end(), [](const Key &key) {
      return "@" + key.getSqliteKeyName();
    });
    sb << ");";
    insertSql = sb.str();
  } else {
    insertSql = "INSERT INTO data_t(data) VALUES (@data)";
  }

  SqlitePreparedStatement &insertCmd = getPreparedStatement(insertSql.c_str());
  insertCmd.bindParameter(1, BindableValue(record));

  unsigned int parameterNumber = 2;
  for (auto &key : keys) {
    insertCmd.bindParameter(parameterNumber++,
                            key.extractKeyInRecordToSqliteObject(record));
  }

  if (!insertCmd.executeNoThrow()) {
    transaction.rollback();
    return 0;
  }

  int numRowsAffected = sqlite3_changes(database.get());
  unsigned int lastInsertRowId = sqlite3_last_insert_rowid(database.get());

  try {
    transaction.commit();
  } catch (const BtrieveException &ex) {
    //_logger.Log(logLevel, $"Failed to commit during insert: {ex.Message}");
    transaction.rollback();
    numRowsAffected = 0;
  }

  if (numRowsAffected == 0) {
    return 0;
  }

  cache.cache(lastInsertRowId, Record(lastInsertRowId, data));
  return lastInsertRowId;
}

BtrieveError
SqliteDatabase::insertAutoincrementValues(std::vector<uint8_t> &record) {
  // first we need to fetch the next autoincrement values
  std::list<std::string> zeroedKeyAutoincrementedClauses;
  std::list<const Key *> autoincrementedKeys;
  for (const Key &key : keys) {
    if (key.getPrimarySegment().getDataType() == KeyDataType::AutoInc &&
        key.isNullKeyInRecord(
            std::basic_string_view<uint8_t>(record.data(), record.size()))) {
      std::stringstream maxWhere;
      maxWhere << "(MAX(" << key.getSqliteKeyName() << ") + 1)";

      zeroedKeyAutoincrementedClauses.push_back(maxWhere.str());
      autoincrementedKeys.push_back(&key);
    }
  }

  if (zeroedKeyAutoincrementedClauses.size() == 0) {
    return BtrieveError::Success;
  }

  std::stringstream sb;
  sb << "SELECT "
     << commaDelimited(
            zeroedKeyAutoincrementedClauses.begin(),
            zeroedKeyAutoincrementedClauses.end(),
            [](const std::string &whereClause) { return whereClause; });
  sb << " FROM data_t;";

  SqlitePreparedStatement &cmd = getPreparedStatement(sb.str().c_str());
  auto reader = cmd.executeReader();
  if (!reader->read()) {
    //_logger.Error("Unable to query for MAX autoincremented values, unable to
    // update");
    return BtrieveError::IOError;
  }

  // and once we have the values, insert them into the record so it ends up
  // in the database record.
  unsigned int i = 0;
  for (const Key *key : autoincrementedKeys) {
    for (const KeyDefinition &keyDefinition : key->getSegments()) {
      uint64_t value = reader->getInt64(i++);
      switch (keyDefinition.getLength()) {
      case 8:
        record.data()[keyDefinition.getOffset() + 7] = (value >> 56) & 0xFF;
        record.data()[keyDefinition.getOffset() + 6] = (value >> 48) & 0xFF;
        record.data()[keyDefinition.getOffset() + 5] = (value >> 40) & 0xFF;
        record.data()[keyDefinition.getOffset() + 4] = (value >> 32) & 0xFF;
        // fall through
      case 4:
        record.data()[keyDefinition.getOffset() + 3] = (value >> 24) & 0xFF;
        record.data()[keyDefinition.getOffset() + 2] = (value >> 16) & 0xFF;
        // fall through
      case 2:
        record.data()[keyDefinition.getOffset() + 1] = (value >> 8) & 0xFF;
        record.data()[keyDefinition.getOffset()] = value & 0xFF;
        break;
      default:
        return BtrieveError::BadKeyLength;
      }
    }
  }
  return BtrieveError::Success;
}

BtrieveError
SqliteDatabase::updateRecord(unsigned int id,
                             std::basic_string_view<uint8_t> record) {
  std::vector<uint8_t> data(record.size());
  memcpy(data.data(), record.data(), record.size());
  BtrieveError error;

  if (!variableLengthRecords && record.size() != recordLength) {
    //_logger.Warn(
    //    $"Btrieve Record Size Mismatch TRUNCATING. Expected Length
    //    {RecordLength}, Actual Length {record.Length}");
    data.resize(recordLength, 0);
    record = std::basic_string_view<uint8_t>(data.data(), recordLength);
  }

  SqliteTransaction transaction(database);

  error = insertAutoincrementValues(data);
  if (error != BtrieveError::Success) {
    transaction.rollback();
    return error;
  }

  std::string updateSql;
  if (!keys.empty()) {
    std::stringstream sb;
    sb << "UPDATE data_t SET data=@data, ";
    sb << commaDelimited(keys.begin(), keys.end(), [](const Key &key) {
      char buf[128];
      snprintf(buf, sizeof(buf), "%s=@%s", key.getSqliteKeyName().c_str(),
               key.getSqliteKeyName().c_str());
      buf[sizeof(buf) - 1] = 0;

      return std::string(buf);
    });
    sb << " WHERE id=@id;";
    updateSql = sb.str();
  } else {
    updateSql = "UPDATE data_t SET data=@data WHERE id=@id";
  }

  SqlitePreparedStatement &updateCmd = getPreparedStatement(updateSql.c_str());
  updateCmd.bindParameter(1, BindableValue(record));

  unsigned int parameterNumber = 2;
  for (auto &key : keys) {
    updateCmd.bindParameter(parameterNumber++,
                            key.extractKeyInRecordToSqliteObject(record));
  }
  updateCmd.bindParameter(parameterNumber, id);

  if (!updateCmd.executeNoThrow()) {
    int errorCode = sqlite3_errcode(database.get());
    int extendedErrorCode = sqlite3_extended_errcode(database.get());

    transaction.rollback();

    if (errorCode == SQLITE_CONSTRAINT) {
      if (extendedErrorCode == SQLITE_CONSTRAINT_UNIQUE) {
        return BtrieveError::DuplicateKeyValue;
      } else if (extendedErrorCode == SQLITE_CONSTRAINT_TRIGGER) {
        return BtrieveError::NonModifiableKeyValue;
      }
    }

    return BtrieveError::IOError;
  }

  int numRowsAffected = sqlite3_changes(database.get());

  try {
    transaction.commit();
  } catch (const BtrieveException &ex) {
    //_logger.Log(logLevel, $"Failed to commit during insert: {ex.Message}");
    transaction.rollback();
    numRowsAffected = 0;
  }

  if (numRowsAffected == 0) {
    return BtrieveError::InvalidPositioning;
  }

  cache.cache(id, Record(id, data));
  return BtrieveError::Success;
}

std::unique_ptr<Query>
SqliteDatabase::newQuery(unsigned int position, const Key *key,
                         std::basic_string_view<uint8_t> keyData) {
  return std::unique_ptr<Query>(new SqliteQuery(this, position, key, keyData));
}

BtrieveError SqliteDatabase::nextReader(Query *query,
                                        CursorDirection cursorDirection) {
  auto record = query->next(cursorDirection);

  if (!record.first) {
    return BtrieveError::InvalidPositioning;
  }

  position = query->getPosition();
  cache.cache(position, record.second);
  return BtrieveError::Success;
}

BtrieveError SqliteDatabase::getByKeyEqual(Query *query) {
  std::stringstream sql;

  auto sqliteObject =
      query->getKey()->keyDataToSqliteObject(query->getKeyData());

  sql << "SELECT id, " << query->getKey()->getSqliteKeyName()
      << ", data FROM data_t WHERE " << query->getKey()->getSqliteKeyName();
  if (sqliteObject.isNull()) {
    sql << " IS NULL";
  } else {
    sql << " = @value ORDER BY " << query->getKey()->getSqliteKeyName()
        << " ASC";
  }

  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());
  if (!sqliteObject.isNull()) {
    command.bindParameter(1, sqliteObject);
  }

  static_cast<SqliteQuery *>(query)->setReader(command.executeReader());
  query->setCursorDirection(CursorDirection::Seek);
  return nextReader(query, CursorDirection::Seek);
}

BtrieveError SqliteDatabase::getByKeyNext(Query *query) {
  return nextReader(query, CursorDirection::Forward);
}

BtrieveError SqliteDatabase::getByKeyPrevious(Query *query) {
  return nextReader(query, CursorDirection::Reverse);
}

BtrieveError SqliteDatabase::getByKeyFirst(Query *query) {
  std::stringstream sql;

  sql << "SELECT id, " << query->getKey()->getSqliteKeyName()
      << ", data FROM data_t ORDER BY " << query->getKey()->getSqliteKeyName()
      << " ASC";
  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());

  static_cast<SqliteQuery *>(query)->setReader(command.executeReader());
  query->setCursorDirection(CursorDirection::Forward);
  return nextReader(query, CursorDirection::Forward);
}

BtrieveError SqliteDatabase::getByKeyLast(Query *query) {
  std::stringstream sql;

  sql << "SELECT id, " << query->getKey()->getSqliteKeyName()
      << ", data FROM data_t ORDER BY " << query->getKey()->getSqliteKeyName()
      << " DESC";
  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());

  static_cast<SqliteQuery *>(query)->setReader(command.executeReader());
  query->setCursorDirection(CursorDirection::Reverse);
  return nextReader(query, CursorDirection::Reverse);
}

BtrieveError SqliteDatabase::getByKeyGreater(Query *query,
                                             const char *opurator) {
  std::stringstream sql;

  sql << "SELECT id, " << query->getKey()->getSqliteKeyName()
      << ", data FROM data_t WHERE " << query->getKey()->getSqliteKeyName()
      << " " << opurator << " @value ORDER BY "
      << query->getKey()->getSqliteKeyName() << " ASC";

  auto sqliteObject =
      query->getKey()->keyDataToSqliteObject(query->getKeyData());

  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());
  command.bindParameter(1, sqliteObject);

  static_cast<SqliteQuery *>(query)->setReader(command.executeReader());
  query->setCursorDirection(CursorDirection::Forward);
  return nextReader(query, CursorDirection::Forward);
}

BtrieveError SqliteDatabase::getByKeyLess(Query *query, const char *opurator) {
  std::stringstream sql;

  sql << "SELECT id, " << query->getKey()->getSqliteKeyName()
      << ", data FROM data_t WHERE " << query->getKey()->getSqliteKeyName()
      << " " << opurator << " @value ORDER BY "
      << query->getKey()->getSqliteKeyName() << " DESC";

  auto sqliteObject =
      query->getKey()->keyDataToSqliteObject(query->getKeyData());

  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());
  command.bindParameter(1, sqliteObject);

  static_cast<SqliteQuery *>(query)->setReader(command.executeReader());
  query->setCursorDirection(CursorDirection::Reverse);
  return nextReader(query, CursorDirection::Reverse);
}

} // namespace btrieve