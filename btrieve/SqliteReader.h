#ifndef __SQLITE_READER_H_
#define __SQLITE_READER_H_

#include "Reader.h"
#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"

namespace btrieve {
class SqliteReader : public Reader {
 public:
  virtual ~SqliteReader() {}

  virtual bool read() override {
    int errorCode = sqlite3_step(statement);
    if (errorCode == SQLITE_ROW) {
      return true;
    } else if (errorCode != SQLITE_DONE) {
      throwException(errorCode);
    }

    return false;
  }

  virtual int getInt32(unsigned int columnOrdinal) const override {
    return sqlite3_column_int(statement, columnOrdinal);
  }

  virtual int64_t getInt64(unsigned int columnOrdinal) const override {
    return sqlite3_column_int64(statement, columnOrdinal);
  }

  virtual double getDouble(unsigned int columnOrdinal) const override {
    return sqlite3_column_double(statement, columnOrdinal);
  }

  virtual bool getBoolean(unsigned int columnOrdinal) const override {
    return getInt32(columnOrdinal) != 0;
  }

  virtual bool isDBNull(unsigned int columnOrdinal) const override {
    return sqlite3_column_type(statement, columnOrdinal) == SQLITE_NULL;
  }

  virtual std::string getString(unsigned int columnOrdinal) const override {
    const char *str = reinterpret_cast<const char *>(
        sqlite3_column_text(statement, columnOrdinal));

    if (str != nullptr) {
      return std::string(str);
    } else {
      return std::string();
    }
  }

  virtual std::vector<uint8_t> getBlob(
      unsigned int columnOrdinal) const override {
    int bytes = sqlite3_column_bytes(statement, columnOrdinal);
    std::vector<uint8_t> ret(bytes);
    if (bytes > 0) {
      const void *data = sqlite3_column_blob(statement, columnOrdinal);
      memcpy(ret.data(), data, bytes);
    }
    return ret;
  }

  virtual BindableValue getBindableValue(
      unsigned int columnOrdinal) const override {
    int bytes;
    const uint8_t *data;
    int columnType = sqlite3_column_type(statement, columnOrdinal);

    switch (columnType) {
      case SQLITE_INTEGER:
        return BindableValue(static_cast<int64_t>(
            sqlite3_column_int64(statement, columnOrdinal)));
      case SQLITE_FLOAT:
        return BindableValue(sqlite3_column_double(statement, columnOrdinal));
      case SQLITE_TEXT:
        return BindableValue(reinterpret_cast<const char *>(
            sqlite3_column_text(statement, columnOrdinal)));
      case SQLITE_BLOB:
        bytes = sqlite3_column_bytes(statement, columnOrdinal);
        data = reinterpret_cast<const uint8_t *>(
            sqlite3_column_blob(statement, columnOrdinal));
        return BindableValue(std::basic_string_view<uint8_t>(data, bytes));
      case SQLITE_NULL:
      default:
        return BindableValue();
    }
  }

 private:
  friend class SqlitePreparedStatement;

  SqliteReader(sqlite3_stmt *statement_) : statement(statement_) {};

  // can't use a shared_ptr here since sqlite3_stmt is an incomplete type
  sqlite3_stmt *statement;
};
}  // namespace btrieve
#endif
