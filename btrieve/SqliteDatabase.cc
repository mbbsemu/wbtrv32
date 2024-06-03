#include "SqliteDatabase.h"
#include "BindableValue.h"
#include "BtrieveException.h"
#include "sqlite/sqlite3.h"

namespace btrieve {

static const unsigned int CURRENT_VERSION = 2;

// Opens a Btrieve database as a sql backed file. Will convert a legacy file
// in place if required. Throws a BtrieveException if something fails.
void SqliteDatabase::open(const char *fileName) {}

void SqliteDatabase::create(const char *fileName,
                            const BtrieveDatabase &database) {
  sqlite3 *db;
  sqlite3_open_v2(fileName, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_MEMORY,
                  nullptr);

  // assign it
  this->database.reset(db);

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

static void throwException(int errorCode, const char *sqlite3ErrMsg) {
  BtrieveException exception(sqlite3ErrMsg == nullptr
                                 ? "Sqlite error: [%d]"
                                 : "Sqlite error: [%d] - [%s]",
                             errorCode, sqlite3ErrMsg);

  if (sqlite3ErrMsg != nullptr) {
    sqlite3_free(const_cast<char *>(sqlite3ErrMsg));
  }

  throw exception;
}

class SqlitePreparedStatement {
public:
  SqlitePreparedStatement(sqlite3 *database_, const char *sqlFormat, ...)
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
        sqlite3_prepare_v2(database_, sqlFormat, len, &statement, nullptr);
    if (errorCode != SQLITE_OK) {
      throwException(errorCode, nullptr);
    }

    this->statement.reset(statement);
  }

  void bindParameter(unsigned int parameter, const BindableValue &value) {
    int errorCode;
    switch (value.getType()) {
    case BindableValue::Type::Null:
      errorCode = sqlite3_bind_null(statement.get(), parameter);
      if (errorCode != SQLITE_OK) {
        throwException(errorCode, nullptr);
      }
      break;
    case BindableValue::Type::Integer:
      errorCode = sqlite3_bind_int64(statement.get(), parameter,
                                     value.getIntegerValue());
      if (errorCode != SQLITE_OK) {
        throwException(errorCode, nullptr);
      }
      break;
    case BindableValue::Type::Double:
      errorCode = sqlite3_bind_double(statement.get(), parameter,
                                      value.getDoubleValue());
      if (errorCode != SQLITE_OK) {
        throwException(errorCode, nullptr);
      }
      break;
    case BindableValue::Type::Text: {
      const std::string &text = value.getStringValue();
      char *copy = strdup(text.c_str());
      errorCode = sqlite3_bind_text(statement.get(), parameter, copy,
                                    text.length(), ::free);
      if (errorCode != SQLITE_OK) {
        throwException(errorCode, nullptr);
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
      throwException(errorCode, nullptr);
    }
  }

private:
  sqlite3 *database;
  std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement;
};

void SqliteDatabase::createSqliteMetadataTable(
    const BtrieveDatabase &database) {
  char *errorMsg;
  int errorCode;
  const char *const createTableStatement =
      "CREATE TABLE metadata_t(record_length INTEGER NOT NULL, "
      "physical_record_length INTEGER NOT NULL, page_length INTEGER NOT NULL, "
      "variable_length_records INTEGER NOT NULL, version INTEGER NOT NULL, "
      "acs_name STRING, acs BLOB)";

  errorCode = sqlite3_exec(this->database.get(), createTableStatement, nullptr,
                           nullptr, &errorMsg);
  if (errorCode != SQLITE_OK) {
    throwException(errorCode, errorMsg);
  }

  const char *const insertIntoTableStatement =
      "INSERT INTO metadata_t(record_length, physical_record_length, "
      "page_length, variable_length_records, version, acs_name, acs) "
      "VALUES(@record_length, @physical_record_length, @page_length, "
      "@variable_length_records, @version, @acs_name, @acs)";
  // not using GetSqlitePreparedStatement since this is used once and caching it
  // provides no benefit
  SqlitePreparedStatement command(this->database.get(),
                                  insertIntoTableStatement);
  command.bindParameter(1, BindableValue(database.getRecordLength()));
  command.bindParameter(2, BindableValue(database.getPhysicalRecordLength()));
  command.bindParameter(3, BindableValue(database.getPageLength()));
  command.bindParameter(4, BindableValue(database.isVariableLengthRecords()));
  command.bindParameter(5, BindableValue(CURRENT_VERSION));
  // command.bindParameter("@acs_name", SqliteNullable(btrieveFile.ACSName));
  // command.bindParameter("@acs", SqliteNullable(btrieveFile.ACS));
  command.bindParameter(6, BindableValue());
  command.bindParameter(7, BindableValue());

  command.execute();
}
void SqliteDatabase::createSqliteKeysTable(const BtrieveDatabase &database) {}
void SqliteDatabase::createSqliteDataTable(const BtrieveDatabase &database) {}
void SqliteDatabase::createSqliteDataIndices(const BtrieveDatabase &database) {}
void SqliteDatabase::createSqliteTriggers() {}
void SqliteDatabase::populateSqliteDataTable(const BtrieveDatabase &database) {}

// Closes an opened database.
void SqliteDatabase::close() {
  auto deleter = database.get_deleter();
  auto sqlite3db = database.release();
  if (sqlite3db != nullptr) {
    deleter(sqlite3db);
  }
}
} // namespace btrieve