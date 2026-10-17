#include "SqliteDatabase.h"

#include <atomic>
#include <sstream>

#include "BindableValue.h"
#include "BtrieveException.h"
#include "SqlitePreparedStatement.h"
#include "SqliteQuery.h"
#include "SqliteTransaction.h"
#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"

#ifndef WIN32
#include <unistd.h>
#endif

namespace btrieve {

static const unsigned int CURRENT_VERSION = 4;

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

// Builds the INSERT for a row's data and every key column, optionally with an
// explicit id.
static std::string buildInsertSql(const std::vector<Key> &keys,
                                  const char *table, bool includeId) {
  std::vector<std::string> columns;
  if (includeId) {
    columns.push_back("id");
  }
  columns.push_back("data");
  for (auto &key : keys) {
    for (auto &column : key.getSqliteColumnNames()) {
      columns.push_back(column);
    }
  }

  std::stringstream sb;
  sb << "INSERT INTO " << table << "(";
  sb << commaDelimited(columns.begin(), columns.end(),
                       [](const std::string &column) { return column; });
  sb << ") VALUES(";
  sb << commaDelimited(columns.begin(), columns.end(),
                       [](const std::string &column) { return "@" + column; });
  sb << ")";
  return sb.str();
}

// Binds every key column of record starting at parameterNumber, in the same
// order as buildInsertSql, and returns the next unused parameter number.
static unsigned int bindRecordKeys(SqlitePreparedStatement &command,
                                   unsigned int parameterNumber,
                                   const std::vector<Key> &keys,
                                   std::basic_string_view<uint8_t> record) {
  for (auto &key : keys) {
    for (auto &value : key.extractKeyInRecordToSqliteObjects(record)) {
      command.bindParameter(parameterNumber++, value);
    }
  }
  return parameterNumber;
}

class SqliteCreationRecordLoader : public RecordLoader {
 public:
  SqliteCreationRecordLoader(std::shared_ptr<sqlite3> database_,
                             const BtrieveDatabase &database)
      : database(database_), keys(database.getKeys()) {}
  virtual ~SqliteCreationRecordLoader() {}

  void createSqliteInsertionCommand() {
    std::string insertSql = buildInsertSql(keys, "data_t", false);

    transaction.reset(new SqliteTransaction(this->database));
    insertionCommand.reset(
        new SqlitePreparedStatement(this->database, insertSql.c_str()));
  }

 private:
  virtual BtrieveDatabase::LoadRecordResult onRecordLoaded(
      std::basic_string_view<uint8_t> record) {
    insertionCommand->reset();
    insertionCommand->bindParameter(1, record);
    bindRecordKeys(*insertionCommand, 2, keys, record);

    try {
      insertionCommand->execute();
    } catch (BtrieveException &ex) {
      // silently ignore, some databases have bad data
      return BtrieveDatabase::LoadRecordResult::SKIP_COUNT;
    }
    return BtrieveDatabase::LoadRecordResult::COUNT;
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
BtrieveError SqliteDatabase::open(const wchar_t *filename, OpenMode openMode) {
  sqlite3 *db;
  unsigned int openFlags =
      SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_READWRITE | this->openFlags;

  int errorCode =
      sqlite3_open_v2(toStdString(filename).c_str(), &db, openFlags, nullptr);
  if (errorCode != SQLITE_OK) {
    throwException(errorCode);
    return BtrieveError::IOError;
  }

  this->database = std::shared_ptr<sqlite3>(db, &sqlite3_close);

  initializeConnection();
  uint32_t version = loadSqliteMetadata();
  if (version < 3) {
    upgradeDatabaseFrom2To3();
  }
  // the 3 to 4 upgrade needs the keys to rebuild the key columns
  loadSqliteKeys();
  if (version < 4) {
    upgradeDatabaseFrom3To4();
  }

  if (openMode == OpenMode::ReadOnly) {
    errorCode = sqlite3_exec(database.get(), "PRAGMA query_only = 1;", nullptr,
                             nullptr, nullptr);
    if (errorCode != SQLITE_OK) {
      throwException(errorCode);
      return BtrieveError::IOError;
    }
  }

  return BtrieveError::Success;
}

// Prepares a freshly opened connection: drops per-connection caches left from
// any previous connection, then applies the performance pragmas.
//
// Every Btrieve Insert/Update/Delete is its own transaction, so the default
// rollback journal with synchronous=FULL costs an fsync (~10ms) per operation.
// WAL with synchronous=NORMAL remains durable across application crashes and
// only risks the most recent commits on OS crash/power loss. The larger page
// cache (32 MiB, the default is 2 MiB) keeps random key lookups out of the OS.
// Checkpoints are the only fsyncs left under synchronous=NORMAL, so let the
// WAL grow to ~40 MiB (10000 pages, the default is 1000) between them.
void SqliteDatabase::initializeConnection() {
  transactionStatements.clear();
  insertSql.clear();
  updateSql.clear();

  const char *const pragmas =
      "PRAGMA journal_mode = WAL;"
      "PRAGMA synchronous = NORMAL;"
      "PRAGMA cache_size = -32768;"
      "PRAGMA wal_autocheckpoint = 10000;";

  int errorCode =
      sqlite3_exec(database.get(), pragmas, nullptr, nullptr, nullptr);
  if (errorCode != SQLITE_OK) {
    throwException(errorCode);
  }
}

// Loads the metadata and returns the database's version.
uint32_t SqliteDatabase::loadSqliteMetadata() {
  SqlitePreparedStatement command(
      database,
      "SELECT record_length, variable_length_records, version "
      "FROM metadata_t");
  std::unique_ptr<SqliteReader> reader = command.executeReader();
  if (!reader->read()) {
    throw BtrieveException(BtrieveError::IOError, "Can't read metadata_t");
  }

  recordLength = reader->getInt32(0);
  variableLengthRecords = reader->getBoolean(1);

  return reader->getInt32(2);
}

void SqliteDatabase::upgradeDatabaseFrom2To3() {
  SqliteTransaction transaction(database);
  try {
    // update the ACS tables
    {
      SqlitePreparedStatement statement(
          database, "ALTER TABLE keys_t ADD COLUMN acs_name STRING");
      statement.execute();
    }
    {
      SqlitePreparedStatement statement(
          database, "ALTER TABLE keys_t ADD COLUMN acs BLOB");
      statement.execute();
    }
    // now copy the acs values
    {
      SqlitePreparedStatement statement(database,
                                        "SELECT acs_name, acs FROM metadata_t");

      auto reader = statement.executeReader();
      if (reader->read()) {
        const BindableValue &acsName = reader->getBindableValue(0);
        const BindableValue &acs = reader->getBindableValue(1);
        if (!acsName.isNull() && !acs.isNull()) {
          SqlitePreparedStatement insert(
              database,
              "UPDATE keys_t SET acs_name = @acs_name, acs = @acs WHERE "
              "attributes & %d",
              NumberedACS);

          insert.bindParameter(1, acsName);
          insert.bindParameter(2, acs);
          insert.execute();
        }
      }
    }
    // remove old acs tables
    {
      SqlitePreparedStatement statement(
          database, "ALTER TABLE metadata_t DROP COLUMN acs");
      statement.execute();
    }
    {
      SqlitePreparedStatement statement(
          database, "ALTER TABLE metadata_t DROP COLUMN acs_name");
      statement.execute();
    }
    // bump version
    {
      SqlitePreparedStatement statement(database,
                                        "UPDATE metadata_t SET version = 3");
      statement.execute();
    }

    transaction.commit();
  } catch (const BtrieveException &ex) {
    transaction.rollback();
    throw ex;
  }
}

// Version 3 stored a composite key as one BLOB of its concatenated segment
// bytes, which memcmp misorders for little endian and signed integers, floats
// and strings. Version 4 stores each segment in its own column, so data_t is
// rebuilt with the new columns and every row's keys are recomputed from its
// data. Row ids are kept since they're the records' positions.
void SqliteDatabase::upgradeDatabaseFrom3To4() {
  SqliteTransaction transaction(database);
  try {
    if (std::any_of(keys.begin(), keys.end(),
                    [](const Key &key) { return key.isComposite(); })) {
      createSqliteDataTable("data_t_v4");

      {
        SqlitePreparedStatement select(database, "SELECT id, data FROM data_t");
        SqlitePreparedStatement insert(
            database, buildInsertSql(keys, "data_t_v4", true).c_str());

        auto reader = select.executeReader();
        while (reader->read()) {
          std::vector<uint8_t> data = reader->getBlob(1);
          std::basic_string_view<uint8_t> record(data.data(), data.size());

          insert.reset();
          insert.bindParameter(1, BindableValue(reader->getInt64(0)));
          insert.bindParameter(2, BindableValue(record));
          bindRecordKeys(insert, 3, keys, record);
          insert.execute();
        }
      }

      // dropping data_t drops its indices and trigger too
      {
        SqlitePreparedStatement statement(database, "DROP TABLE data_t");
        statement.execute();
      }
      {
        SqlitePreparedStatement statement(
            database, "ALTER TABLE data_t_v4 RENAME TO data_t");
        statement.execute();
      }

      createSqliteDataIndices();
      createSqliteTriggers();
    }

    {
      SqlitePreparedStatement statement(database,
                                        "UPDATE metadata_t SET version = 4");
      statement.execute();
    }

    transaction.commit();
  } catch (const BtrieveException &ex) {
    transaction.rollback();
    throw ex;
  }
}

void SqliteDatabase::loadSqliteKeys() {
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
      database,
      "SELECT number, segment, attributes, "
      "data_type, offset, length, null_value, acs_name, acs "
      "FROM keys_t ORDER BY number, segment");
  std::unique_ptr<SqliteReader> reader = command.executeReader();

  unsigned int segmentIndex = 0;
  while (reader->read()) {
    unsigned int number = reader->getInt32(0);
    std::string acsName;
    std::vector<char> acs;

    acsName = reader->getString(7);
    if (!reader->isDBNull(8)) {
      std::vector<uint8_t> acsBlob = reader->getBlob(8);
      if (acsBlob.size() != ACS_LENGTH) {
        throw BtrieveException(
            BtrieveError::InvalidACS,
            "The ACS length is not 256 bytes, this is a corrupt database.");
      }

      acs.reserve(ACS_LENGTH);
      std::copy(acsBlob.begin(), acsBlob.end(), std::back_inserter(acs));
    }

    // the segment column holds the segment's index within its key, whereas
    // KeyDefinition's segment flag means "another segment follows", which
    // is what the SegmentedKey attribute records
    uint16_t attributes = reader->getInt32(2);
    bool segment = attributes & SegmentedKey;
    KeyDefinition keyDefinition(
        number, reader->getInt32(5), reader->getInt32(4),
        (KeyDataType)reader->getInt32(3), attributes, segment,
        segment ? number : 0, segmentIndex, reader->getInt32(6), acsName, acs);

    keys[number].addSegment(keyDefinition);
  }

  for (auto &key : keys) {
    key.updateSegmentIndices();
  }
}

#ifdef WIN32
#define unlink _unlink
#endif

std::unique_ptr<RecordLoader> SqliteDatabase::create(
    const wchar_t *fileName, const BtrieveDatabase &database) {
  sqlite3 *db;
  int flags = SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_READWRITE | openFlags;

  // remove the file if is exists since we're creating it anew
  if (fileName != nullptr) {
    unlink(toStdString(fileName).c_str());
    flags |= SQLITE_OPEN_CREATE;
  } else {
    flags |= SQLITE_OPEN_MEMORY;
  }

  int errorCode = sqlite3_open_v2(
      fileName != nullptr ? toStdString(fileName).c_str() : nullptr, &db, flags,
      nullptr);
  if (errorCode != SQLITE_OK) {
    throwException(errorCode);
  }

  this->database = std::shared_ptr<sqlite3>(db, &sqlite3_close);

  initializeConnection();

  recordLength = database.getRecordLength();
  variableLengthRecords = database.isVariableLengthRecords();
  keys = database.getKeys();

  createSqliteMetadataTable(database);
  createSqliteKeysTable(database);
  createSqliteDataTable("data_t");
  createSqliteDataIndices();
  createSqliteTriggers();

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
      "variable_length_records INTEGER NOT NULL, version INTEGER NOT NULL)";

  SqlitePreparedStatement createTableCommand(this->database,
                                             createTableStatement);
  createTableCommand.execute();

  const char *const insertIntoTableStatement =
      "INSERT INTO metadata_t(record_length, physical_record_length, "
      "page_length, variable_length_records, version) "
      "VALUES(@record_length, @physical_record_length, @page_length, "
      "@variable_length_records, @version)";

  SqlitePreparedStatement command(this->database, insertIntoTableStatement);
  command.bindParameter(1, BindableValue(database.getRecordLength()));
  command.bindParameter(2, BindableValue(database.getPhysicalRecordLength()));
  command.bindParameter(3, BindableValue(database.getPageLength()));
  command.bindParameter(4, BindableValue(database.isVariableLengthRecords()));
  command.bindParameter(5, BindableValue(CURRENT_VERSION));

  command.execute();
}

void SqliteDatabase::createSqliteKeysTable(const BtrieveDatabase &database) {
  const char *const createTableStatement =
      "CREATE TABLE keys_t(id INTEGER PRIMARY KEY, number INTEGER NOT NULL, "
      "segment INTEGER NOT NULL, attributes INTEGER NOT NULL, data_type "
      "INTEGER NOT NULL, offset INTEGER NOT NULL, length INTEGER NOT NULL, "
      "null_value INTEGER NOT NULL, acs_name STRING, acs BLOB, UNIQUE(number, "
      "segment))";

  SqlitePreparedStatement createTableCommand(this->database,
                                             createTableStatement);
  createTableCommand.execute();

  const char *const insertIntoTableStatement =
      "INSERT INTO keys_t(number, segment, attributes, data_type, offset, "
      "length, null_value, acs_name, acs) VALUES(@number, @segment, "
      "@attributes, @data_type, "
      "@offset, @length, @null_value, @acs_name, @acs)";

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
      insertIntoTableCommand.bindParameter(8, BindableValue(key.getACSName()));
      if (key.getACS() == nullptr) {
        insertIntoTableCommand.bindParameter(9, BindableValue());
      } else {
        insertIntoTableCommand.bindParameter(
            9,
            BindableValue(std::basic_string_view<uint8_t>(
                reinterpret_cast<const uint8_t *>(key.getACS()), ACS_LENGTH)));
      }

      insertIntoTableCommand.execute();
    }
  }
}

void SqliteDatabase::createSqliteDataTable(const char *table) {
  std::stringstream sb;
  sb << "CREATE TABLE " << table
     << "(id INTEGER PRIMARY KEY, data BLOB NOT NULL";
  for (auto &key : keys) {
    for (auto &definition : key.getSqliteColumnDefinitions()) {
      sb << ", " << definition;
    }
  }

  sb << ");";

  SqlitePreparedStatement createTableStatement(this->database,
                                               sb.str().c_str());
  createTableStatement.execute();
}

void SqliteDatabase::createSqliteDataIndices() {
  for (auto &key : keys) {
    const char *possiblyUnique = key.isUnique() ? "UNIQUE" : "";
    SqlitePreparedStatement command(
        this->database, "CREATE %s INDEX %s_index on data_t(%s)",
        possiblyUnique, key.getSqliteKeyName().c_str(),
        sqliteKeyColumns(key).c_str());
    command.execute();
  }
}

void SqliteDatabase::createSqliteTriggers() {
  std::vector<Key> nonModifiableKeys;
  std::copy_if(keys.begin(), keys.end(), std::back_inserter(nonModifiableKeys),
               [](const Key &key) { return !key.isModifiable(); });

  if (nonModifiableKeys.empty()) {
    return;
  }

  std::stringstream builder;
  builder << "CREATE TRIGGER non_modifiable BEFORE UPDATE ON data_t BEGIN "
             "SELECT CASE ";

  for (auto &key : nonModifiableKeys) {
    for (auto &column : key.getSqliteColumnNames()) {
      builder << "WHEN NEW." << column << " != OLD." << column
              << " THEN "
                 "RAISE (ABORT,'You modified a non-modifiable "
              << key.getSqliteKeyName() << "!') ";
    }
  }

  builder << "END; END;";

  SqlitePreparedStatement cmd(this->database, builder.str().c_str());
  cmd.execute();
}

// Closes an opened database.
void SqliteDatabase::close() {
  preparedStatements.clear();
  transactionStatements.clear();
  database.reset();

  keys.clear();
  insertSql.clear();
  updateSql.clear();
  cache.clear();
}

SqlitePreparedStatement &SqliteDatabase::getPreparedStatement(
    const char *sql) const {
  auto iter = preparedStatements.find(sql);
  if (iter == preparedStatements.end()) {
    iter =
        preparedStatements.emplace(sql, SqlitePreparedStatement(database, sql))
            .first;
  }

  iter->second.reset();  // reset it before we return it
  return iter->second;
}

BtrieveError SqliteDatabase::stepFirst() {
  SqlitePreparedStatement &command =
      getPreparedStatement("SELECT id, data FROM data_t ORDER BY id LIMIT 1");
  auto reader = command.executeReader();
  if (!reader->read()) {
    return BtrieveError::EndOfFile;
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
    return BtrieveError::EndOfFile;
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
    return BtrieveError::EndOfFile;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return BtrieveError::Success;
}

BtrieveError SqliteDatabase::stepPrevious() {
  SqlitePreparedStatement &command = getPreparedStatement(
      "SELECT id, data FROM data_t WHERE id < @position "
      "ORDER BY id DESC LIMIT 1");

  command.bindParameter(1, position);

  auto reader = command.executeReader();
  if (!reader->read()) {
    return BtrieveError::EndOfFile;
  }

  position = reader->getInt32(0);
  cacheBtrieveRecord(position, *reader, 1);
  return BtrieveError::Success;
}

const Record &SqliteDatabase::cacheBtrieveRecord(unsigned int position,
                                                 const SqliteReader &reader,
                                                 unsigned int columnOrdinal) {
  const Record &record = readRecord(position, reader, columnOrdinal);
  return cache.cache(position, record);
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
  try {
    command.execute();
  } catch (const BtrieveException &ex) {
    return ex.getError();
  }

  return sqlite3_changes(database.get()) == 1
             ? BtrieveError::Success
             : BtrieveError::InvalidPositioning;
}

class SqliteErrorConverter {
 public:
  SqliteErrorConverter(sqlite3 *database) : error(BtrieveError::IOError) {
    int errorCode = sqlite3_errcode(database);
    int extendedErrorCode = sqlite3_extended_errcode(database);

    if (errorCode == SQLITE_CONSTRAINT) {
      if (extendedErrorCode == SQLITE_CONSTRAINT_UNIQUE) {
        error = BtrieveError::DuplicateKeyValue;
      } else if (extendedErrorCode == SQLITE_CONSTRAINT_TRIGGER) {
        error = BtrieveError::NonModifiableKeyValue;
      }
    } else if (errorCode == SQLITE_READONLY) {
      error = BtrieveError::AccessDenied;
    }
  }

  BtrieveError getError() { return error; }

 private:
  BtrieveError error;
};

std::pair<BtrieveError, unsigned int> SqliteDatabase::insertRecord(
    std::basic_string_view<uint8_t> record) {
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

  SqliteTransaction transaction(database, &transactionStatements);

  error = insertAutoincrementValues(data);
  if (error != BtrieveError::Success) {
    transaction.rollback();
    return std::make_pair(error, 0);
  }

  // insertAutoincrementValues only mutates data, not record, so repoint record
  // at it -- otherwise the row we insert below still has the caller's original
  // (unincremented) placeholder value rather than the one we just computed.
  record = std::basic_string_view<uint8_t>(data.data(), data.size());

  SqlitePreparedStatement &insertCmd =
      getPreparedStatement(getInsertSql().c_str());
  insertCmd.bindParameter(1, BindableValue(record));
  bindRecordKeys(insertCmd, 2, keys, record);

  if (!insertCmd.executeNoThrow()) {
    SqliteErrorConverter errorConverter(database.get());

    transaction.rollback();

    return std::make_pair(errorConverter.getError(), 0);
  }

  int numRowsAffected = sqlite3_changes(database.get());
  error = BtrieveError::Success;
  unsigned int lastInsertRowId =
      static_cast<unsigned int>(sqlite3_last_insert_rowid(database.get()));

  try {
    transaction.commit();
  } catch (const BtrieveException &) {
    SqliteErrorConverter errorConverter(database.get());

    transaction.rollback();

    numRowsAffected = 0;
    error = errorConverter.getError();
  }

  if (numRowsAffected == 0) {
    return std::make_pair(error, 0);
  }

  cache.cache(lastInsertRowId, Record(lastInsertRowId, data));
  return std::make_pair(error, lastInsertRowId);
}

BtrieveError SqliteDatabase::insertAutoincrementValues(
    std::vector<uint8_t> &record) {
  // first we need to fetch the next autoincrement values
  std::list<std::string> zeroedKeyAutoincrementedClauses;
  std::list<const Key *> autoincrementedKeys;
  for (const Key &key : keys) {
    // autoincrement keys can't be segmented
    if (!key.isComposite() &&
        key.getPrimarySegment().getDataType() == KeyDataType::AutoInc &&
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
      // guard against writing past the end of a record that's shorter than
      // this key's position + length (e.g. a malformed/short variable-length
      // record) -- without this check the writes below are an out-of-bounds
      // heap write, not just a thrown exception.
      if (static_cast<size_t>(keyDefinition.getOffset()) +
              keyDefinition.getLength() >
          record.size()) {
        return BtrieveError::BadRecordLength;
      }

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

const std::string &SqliteDatabase::getInsertSql() {
  if (!insertSql.empty()) {
    return insertSql;
  }

  insertSql = buildInsertSql(keys, "data_t", false);
  return insertSql;
}

const std::string &SqliteDatabase::getUpdateSql() {
  if (!updateSql.empty()) {
    return updateSql;
  }

  std::stringstream sb;
  sb << "UPDATE data_t SET data=@data";
  for (auto &key : keys) {
    for (auto &column : key.getSqliteColumnNames()) {
      sb << ", " << column << "=@" << column;
    }
  }
  sb << " WHERE id=@id";
  updateSql = sb.str();
  return updateSql;
}

BtrieveError SqliteDatabase::updateRecord(
    unsigned int id, std::basic_string_view<uint8_t> record) {
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

  SqliteTransaction transaction(database, &transactionStatements);

  error = insertAutoincrementValues(data);
  if (error != BtrieveError::Success) {
    transaction.rollback();
    return error;
  }

  SqlitePreparedStatement &updateCmd =
      getPreparedStatement(getUpdateSql().c_str());
  updateCmd.bindParameter(1, BindableValue(record));
  unsigned int parameterNumber = bindRecordKeys(updateCmd, 2, keys, record);
  updateCmd.bindParameter(parameterNumber, id);

  if (!updateCmd.executeNoThrow()) {
    SqliteErrorConverter errorConverter(database.get());

    transaction.rollback();

    return errorConverter.getError();
  }

  int numRowsAffected = sqlite3_changes(database.get());

  try {
    transaction.commit();
  } catch (const BtrieveException &) {
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

std::unique_ptr<Query> SqliteDatabase::newQuery(
    unsigned int position, const Key *key,
    std::basic_string_view<uint8_t> keyData) {
  return std::unique_ptr<Query>(new SqliteQuery(this, position, key, keyData));
}

BtrieveError SqliteDatabase::nextReader(Query *query,
                                        CursorDirection cursorDirection) {
  auto record = query->next(cursorDirection);

  if (!record.first) {
    return cursorDirection == CursorDirection::Seek
               ? BtrieveError::KeyValueNotFound
               : BtrieveError::EndOfFile;
  }

  position = query->getPosition();
  cache.cache(position, record.second);
  return BtrieveError::Success;
}

BtrieveError SqliteDatabase::getByKeyEqual(Query *query) {
  const Key &key = *query->getKey();
  auto values = key.keyDataToSqliteObjects(query->getKeyData());

  std::stringstream sql;
  sql << "SELECT id, " << sqliteKeyColumns(key) << ", data FROM data_t WHERE "
      << sqliteKeyEquals(key, values) << " ORDER BY "
      << sqliteKeyColumns(key, " ASC") << ", id ASC";

  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());
  unsigned int parameterNumber = 1;
  for (auto &value : values) {
    if (!value.isNull()) {
      command.bindParameter(parameterNumber++, value);
    }
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

  sql << "SELECT id, " << sqliteKeyColumns(*query->getKey())
      << ", data FROM data_t ORDER BY "
      << sqliteKeyColumns(*query->getKey(), " ASC") << ", id ASC";
  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());

  static_cast<SqliteQuery *>(query)->setReader(command.executeReader());
  query->setCursorDirection(CursorDirection::Forward);
  return nextReader(query, CursorDirection::Forward);
}

BtrieveError SqliteDatabase::getByKeyLast(Query *query) {
  std::stringstream sql;

  sql << "SELECT id, " << sqliteKeyColumns(*query->getKey())
      << ", data FROM data_t ORDER BY "
      << sqliteKeyColumns(*query->getKey(), " DESC") << ", id DESC";
  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());

  static_cast<SqliteQuery *>(query)->setReader(command.executeReader());
  query->setCursorDirection(CursorDirection::Reverse);
  return nextReader(query, CursorDirection::Reverse);
}

static bool shouldConvertSqliteObjectToEmptyStringForComparison(
    const BindableValue &value, const btrieve::Key *key) {
  return value.isNull() && !key->isComposite() &&
         key->getPrimarySegment().isString();
}

// Runs a range query comparing the key against the query's key data with
// opurator, ordered by direction (" ASC" or " DESC").
BtrieveError SqliteDatabase::getByKeyRange(Query *query, const char *opurator,
                                           const char *direction,
                                           CursorDirection cursorDirection) {
  const Key &key = *query->getKey();
  std::stringstream sql;

  sql << "SELECT id, " << sqliteKeyColumns(key) << ", data FROM data_t WHERE "
      << sqliteKeyComparison(key, opurator) << " ORDER BY "
      << sqliteKeyColumns(key, direction) << ", id" << direction;

  auto values = key.keyDataToSqliteObjects(query->getKeyData());

  if (shouldConvertSqliteObjectToEmptyStringForComparison(values[0], &key)) {
    values[0] = BindableValue("");
  }

  SqlitePreparedStatement &command = getPreparedStatement(sql.str().c_str());
  for (unsigned int i = 0; i < values.size(); ++i) {
    command.bindParameter(i + 1, values[i]);
  }

  static_cast<SqliteQuery *>(query)->setReader(command.executeReader());
  query->setCursorDirection(cursorDirection);
  return nextReader(query, cursorDirection);
}

BtrieveError SqliteDatabase::getByKeyGreater(Query *query,
                                             const char *opurator) {
  return getByKeyRange(query, opurator, " ASC", CursorDirection::Forward);
}

BtrieveError SqliteDatabase::getByKeyLess(Query *query, const char *opurator) {
  return getByKeyRange(query, opurator, " DESC", CursorDirection::Reverse);
}

std::unique_ptr<Query> SqliteDatabase::logicalCurrencySeek(
    int keyNumber, unsigned int position, BtrieveError &error) {
  if (static_cast<unsigned int>(keyNumber) >= keys.size()) {
    error = BtrieveError::InvalidKeyNumber;
    return nullptr;
  }

  auto record = getRecord(position);
  if (!record.first) {
    error = BtrieveError::InvalidPositioning;
    return nullptr;
  }

  auto keyBytes = keys.at(keyNumber).extractKeyDataFromRecord(
      std::basic_string_view<uint8_t>(record.second.getData().data(),
                                      record.second.getData().size()));
  auto keyView =
      std::basic_string_view<uint8_t>(keyBytes.data(), keyBytes.size());

  auto ret = newQuery(position, &keys.at(keyNumber), keyView);

  static_cast<SqliteQuery *>(ret.get())->setLastKey(
      keys.at(keyNumber).keyDataToSqliteObjects(keyView));

  error = BtrieveError::Success;
  return ret;
}
}  // namespace btrieve
