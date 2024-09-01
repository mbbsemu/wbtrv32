#ifndef __SQLITE_PREPARED_STATEMENT_H_
#define __SQLITE_PREPARED_STATEMENT_H_

#include "SqliteReader.h"
#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"
#include <memory>

#ifndef WIN32
#define _strdup strdup
#endif

namespace btrieve {
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
        sqlite3_prepare_v2(database.get(), sql, len, &statement, nullptr);
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
      char *copy = _strdup(text.c_str());
      errorCode = sqlite3_bind_text(statement.get(), parameter, copy,
                                    static_cast<int>(text.length()), ::free);
      if (errorCode != SQLITE_OK) {
        throwException(errorCode);
      }
    } break;
    case BindableValue::Type::Blob:
      const std::vector<uint8_t> &blob = value.getBlobValue();
      uint8_t *copy = reinterpret_cast<uint8_t *>(malloc(blob.size()));
      memcpy(copy, blob.data(), blob.size());

      errorCode = sqlite3_bind_blob(statement.get(), parameter, copy,
                                    static_cast<int>(blob.size()), ::free);
      if (errorCode != SQLITE_OK) {
        throwException(errorCode);
      }
      break;
    }
  }

  bool executeNoThrow() {
    int errorCode = sqlite3_step(statement.get());
    // SQLITE_DONE is expected, meaning the statement has finished
    if (errorCode == SQLITE_DONE) {
      return true;
    }
    // OK indicates more things may happen, but still a valid response. If not
    // OK, we goofed
    return errorCode == SQLITE_OK;
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

  std::unique_ptr<SqliteReader> executeReader() const {
    return std::unique_ptr<SqliteReader>(new SqliteReader(statement.get()));
  }

private:
  std::shared_ptr<sqlite3> database;
  std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement;
};

} // namespace btrieve
#endif
