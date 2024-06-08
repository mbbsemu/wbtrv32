#ifndef __SQLITE_READER_H_
#define __SQLITE_READER_H_

#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"

namespace btrieve {
class SqliteReader {
public:
  bool read() {
    int errorCode = sqlite3_step(statement);
    if (errorCode == SQLITE_ROW) {
      return true;
    } else if (errorCode != SQLITE_DONE) {
      throwException(errorCode);
    }

    return false;
  }

  int getInt32(unsigned int columnOrdinal) const {
    return sqlite3_column_int(statement, columnOrdinal);
  }

  int64_t getInt64(unsigned int columnOrdinal) const {
    return sqlite3_column_int64(statement, columnOrdinal);
  }

  bool getBoolean(unsigned int columnOrdinal) const {
    return getInt32(columnOrdinal) != 0;
  }

  bool isDBNull(unsigned int columnOrdinal) const {
    return sqlite3_column_type(statement, columnOrdinal) == SQLITE_NULL;
  }

  std::string getString(unsigned int columnOrdinal) const {
    const char *str = reinterpret_cast<const char *>(
        sqlite3_column_text(statement, columnOrdinal));

    if (str != nullptr) {
      return std::string(str);
    } else {
      return std::string();
    }
  }

  std::vector<uint8_t> getBlob(unsigned int columnOrdinal) const {
    int bytes = sqlite3_column_bytes(statement, columnOrdinal);
    std::vector<uint8_t> ret(bytes);
    if (bytes > 0) {
      const void *data = sqlite3_column_blob(statement, columnOrdinal);
      memcpy(ret.data(), data, bytes);
    }
    return ret;
  }

private:
  friend class SqlitePreparedStatement;

  SqliteReader(sqlite3_stmt *statement_) : statement(statement_){};

  // can't use a shared_ptr here since sqlite3_stmt is an incomplete type
  sqlite3_stmt *statement;
};
} // namespace btrieve
#endif
