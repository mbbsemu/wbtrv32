#ifndef __SQLITE_PREPARED_STATEMENT_H_
#define __SQLITE_PREPARED_STATEMENT_H_

#include <cstdarg>
#include <memory>
#include <vector>

#include "SqliteReader.h"
#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"

namespace btrieve {
class SqlitePreparedStatement {
 public:
  SqlitePreparedStatement(std::shared_ptr<sqlite3> database_,
                          const char *sqlFormat, ...)
      : database(database_), statement(nullptr, &sqlite3_finalize) {
    int errorCode;
    va_list args;
    va_list argsCopy;
    sqlite3_stmt *statement;

    // size the buffer first, since statements built from every key column
    // can be arbitrarily long
    va_start(args, sqlFormat);
    va_copy(argsCopy, args);
    int len = vsnprintf(nullptr, 0, sqlFormat, argsCopy);
    va_end(argsCopy);

    std::vector<char> sql(len + 1);
    vsnprintf(sql.data(), sql.size(), sqlFormat, args);
    va_end(args);

    errorCode = sqlite3_prepare_v2(database.get(), sql.data(), len, &statement,
                                   nullptr);
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
        errorCode = sqlite3_bind_text(statement.get(), parameter, text.data(),
                                      static_cast<int>(text.length()),
                                      SQLITE_TRANSIENT);
        if (errorCode != SQLITE_OK) {
          throwException(errorCode);
        }
      } break;
      case BindableValue::Type::Blob:
        const std::vector<uint8_t> &blob = value.getBlobValue();
        // sqlite binds a null pointer as NULL, not as an empty blob
        static const uint8_t empty = 0;
        errorCode = sqlite3_bind_blob(
            statement.get(), parameter, blob.empty() ? &empty : blob.data(),
            static_cast<int>(blob.size()), SQLITE_TRANSIENT);
        if (errorCode != SQLITE_OK) {
          throwException(errorCode);
        }
        break;
    }
  }

  // Steps the statement once and returns sqlite's raw result code.
  int step() { return sqlite3_step(statement.get()); }

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

}  // namespace btrieve
#endif
