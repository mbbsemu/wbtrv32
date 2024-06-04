#include "SqliteDatabase.h"
#include "BindableValue.h"
#include "BtrieveException.h"
#include "sqlite/sqlite3.h"
#include <sstream>

namespace btrieve {

static const unsigned int CURRENT_VERSION = 2;

static void throwException(int errorCode) {
  const char *sqlite3ErrMsg = sqlite3_errstr(errorCode);

  throw BtrieveException(sqlite3ErrMsg == nullptr ? "Sqlite error: [%d]"
                                                  : "Sqlite error: [%d] - [%s]",
                         errorCode, sqlite3ErrMsg);
}

// Opens a Btrieve database as a sql backed file. Will convert a legacy file
// in place if required. Throws a BtrieveException if something fails.
void SqliteDatabase::open(const char *fileName) {}

void SqliteDatabase::create(const char *fileName,
                            const BtrieveDatabase &database) {
  sqlite3 *db;
  int errorCode = sqlite3_open_v2(
      fileName, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_MEMORY, nullptr);
  if (errorCode != SQLITE_OK) {
    throwException(errorCode);
  }

  this->database = std::shared_ptr<sqlite3>(db, &sqlite3_close);

  recordLength = database.getRecordLength();
  pageLength = database.getPageLength();
  variableLengthRecords = database.isVariableLengthRecords();
  keys = database.getKeys();

  createSqliteMetadataTable(database);
  createSqliteKeysTable(database);
  createSqliteDataTable(database);
  createSqliteDataIndices(database);
  createSqliteTriggers();
  populateSqliteDataTable(database);
}

class SqlitePreparedStatement {
public:
  SqlitePreparedStatement(std::shared_ptr<sqlite3> database_,
                          const char *sqlFormat, ...)
      : database(database_), statement(nullptr, &sqlite3_finalize) {
    char sql[512];
    int len;
    int errorCode;
    va_list args;
    sqlite3_stmt *statement;

    va_start(args, sqlFormat);
    len = vsnprintf(sql, sizeof(sql), sqlFormat, args);
    va_end(args);

    // just in case
    sql[sizeof(sql) - 1] = 0;

    errorCode =
        sqlite3_prepare_v2(database.get(), sqlFormat, len, &statement, nullptr);
    if (errorCode != SQLITE_OK) {
      throwException(errorCode);
    }

    this->statement.reset(statement);
  }

  void reset() { sqlite3_reset(statement.get()); }

  void bindParameter(unsigned int parameter, const BindableValue &value) {
    int errorCode;
    switch (value.getType()) {
    case BindableValue::Type::Null:
      errorCode = sqlite3_bind_null(statement.get(), parameter);
      if (errorCode != SQLITE_OK) {
        throwException(errorCode);
      }
      break;
    case BindableValue::Type::Integer:
      errorCode = sqlite3_bind_int64(statement.get(), parameter,
                                     value.getIntegerValue());
      if (errorCode != SQLITE_OK) {
        throwException(errorCode);
      }
      break;
    case BindableValue::Type::Double:
      errorCode = sqlite3_bind_double(statement.get(), parameter,
                                      value.getDoubleValue());
      if (errorCode != SQLITE_OK) {
        throwException(errorCode);
      }
      break;
    case BindableValue::Type::Text: {
      const std::string &text = value.getStringValue();
      char *copy = strdup(text.c_str());
      errorCode = sqlite3_bind_text(statement.get(), parameter, copy,
                                    text.length(), ::free);
      if (errorCode != SQLITE_OK) {
        throwException(errorCode);
      }
    } break;
    case BindableValue::Type::Blob:
      // TODO
      break;
    }
  }

  void execute() {
    int errorCode = sqlite3_step(statement.get());
    // SQLITE_DONE is expected, meaning the statement has finished
    if (errorCode == SQLITE_DONE) {
      return;
    }
    // OK indicates more things may happen, but still a valid response. If not
    // OK, we goofed
    if (errorCode != SQLITE_OK) {
      throwException(errorCode);
    }
  }

private:
  std::shared_ptr<sqlite3> database;
  std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement;
};

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

void SqliteDatabase::createSqliteDataIndices(const BtrieveDatabase &database) {}
void SqliteDatabase::createSqliteTriggers() {}
void SqliteDatabase::populateSqliteDataTable(const BtrieveDatabase &database) {}

// Closes an opened database.
void SqliteDatabase::close() { database.reset(); }
} // namespace btrieve