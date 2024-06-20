#ifndef __SQLITE_QUERY_H_
#define __SQLITE_QUERY_H_

#include "Key.h"
#include "Query.h"
#include "SqlitePreparedStatement.h"
#include <memory>
#include <sstream>

namespace btrieve {

class SqliteQuery : public Query {
public:
  SqliteQuery(SqliteDatabase *database_, unsigned int position_,
              const Key *key_, std::basic_string_view<uint8_t> keyData_)
      : Query(position_, key_, keyData_), database(database_) {}

  void setReader(std::unique_ptr<Reader> &&reader) {
    this->reader = std::move(reader);
  }

  virtual std::pair<bool, Record>
  next(CursorDirection cursorDirection) override {
    if (this->cursorDirection != cursorDirection) {
      reader.reset(nullptr);
      changeDirection(cursorDirection);
    }

    // out of records?
    if (!reader || !reader->read()) {
      reader.reset(nullptr);
      return std::pair<bool, Record>(false, Record());
    }

    position = reader->getInt32(0);
    lastKey.reset(new BindableValue(std::move(reader->getBindableValue(1))));

    return std::pair<bool, Record>(true, Record(position, reader->getBlob(2)));
  }

private:
  void seekTo(unsigned int position) {
    while (reader->read()) {
      unsigned int cursorPosition = reader->getInt32(0);
      if (cursorPosition == position) {
        return;
      }
    }

    // at end, nothing left
    reader.reset(nullptr);
  }

  void changeDirection(CursorDirection newDirection) {
    if (!lastKey) {
      return;
    }

    std::stringstream sql;
    sql << "SELECT id, " << key->getSqliteKeyName()
        << ", data FROM data_t WHERE " << key->getSqliteKeyName() << " ";
    switch (newDirection) {
    case CursorDirection::Forward:
      sql << ">= @value ORDER BY " << key->getSqliteKeyName() << " ASC";
      break;
    case CursorDirection::Reverse:
      sql << "<= @value ORDER BY " << key->getSqliteKeyName() << " DESC";
      break;
    default:
      // could log an error here
      return;
    }

    SqlitePreparedStatement &command =
        database->getPreparedStatement(sql.str().c_str());
    BindableValue *value = lastKey.get();
    command.bindParameter(1, *value);
    setReader(command.executeReader());

    this->cursorDirection = newDirection;

    // due to duplicate keys, we need to seek past the current position since we
    // might serve data already served.
    //
    // For example, if you have 4 identical keys with id 1,2,3,4 and are
    // currently at id 2 and seek previous expecting id 1, sqlite might return a
    // cursor counting from 4,3,2,1 and the cursor would point to 4, returning
    // the wrong result. This next call skips 4,3,2 until the cursor is at the
    // proper point.
    seekTo(position);
  }

  std::unique_ptr<BindableValue> lastKey;
  std::unique_ptr<Reader> reader;
  SqliteDatabase *database;
};
} // namespace btrieve
#endif