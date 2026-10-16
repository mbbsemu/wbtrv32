// 26.10.15.5B 11:20PM
#ifndef __SQLITE_QUERY_H_
#define __SQLITE_QUERY_H_

#include <memory>
#include <sstream>

#include "Key.h"
#include "Query.h"
#include "SqlitePreparedStatement.h"

namespace btrieve {

class SqliteQuery : public Query {
 public:
  SqliteQuery(SqliteDatabase *database_, unsigned int position_,
              const Key *key_, std::basic_string_view<uint8_t> keyData_)
      : Query(position_, key_, keyData_), database(database_) {}

  void setReader(std::unique_ptr<Reader> &&reader) {
    this->reader = std::move(reader);
  }

  // Lets go of the cursor between Btrieve calls. SqliteDatabase then resets
  // the statement, so the connection holds no read snapshot while idle; the
  // next call resumes just after the current record (see resume()).
  virtual void suspend() override {
    if (reader) {
      reader.reset(nullptr);
      suspended = true;
    }
  }

  virtual std::pair<bool, Record> next(
      CursorDirection cursorDirection) override {
    if (suspended) {
      suspended = false;
      resume(cursorDirection);
    } else if (this->cursorDirection != cursorDirection) {
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

  void setLastKey(const BindableValue &value) {
    lastKey.reset(new BindableValue(value));
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
    // Duplicate keys are ordered by id (insertion order) so the ordering is
    // stable across queries and independent of SQLite's query plan, which
    // seekTo() below relies on. SqliteDatabase's key queries do the same.
    switch (newDirection) {
      case CursorDirection::Forward:
        sql << ">= @value ORDER BY " << key->getSqliteKeyName()
            << " ASC, id ASC";
        break;
      case CursorDirection::Reverse:
        sql << "<= @value ORDER BY " << key->getSqliteKeyName()
            << " DESC, id DESC";
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

  // Reopens a suspended cursor at the record after (key, id) =
  // (lastKey, position) in newDirection, the same order every key query
  // uses. Unlike changeDirection() it needs no seek, so it is a single index
  // lookup, and it still works when the current record has been deleted in
  // the meantime (Btrieve then continues with its neighbour).
  void resume(CursorDirection newDirection) {
    this->cursorDirection = newDirection;
    if (!lastKey ||
        (newDirection != CursorDirection::Forward &&
         newDirection != CursorDirection::Reverse)) {
      return;
    }

    const std::string name = key->getSqliteKeyName();
    const bool forward = newDirection == CursorDirection::Forward;
    std::stringstream sql;
    sql << "SELECT id, " << name << ", data FROM data_t WHERE ";
    if (lastKey->isNull()) {
      // NULL sorts first: forward runs through the rest of the NULLs and then
      // every other key, reverse only through the earlier NULLs
      if (forward) {
        sql << "(" << name << " IS NULL AND id > @position) OR " << name
            << " IS NOT NULL";
      } else {
        sql << name << " IS NULL AND id < @position";
      }
    } else {
      // A row-value comparison is a single index range. Reverse leaves out
      // NULL keys, as changeDirection() always has; no converted Major BBS
      // file has any.
      sql << "(" << name << ", id) " << (forward ? ">" : "<")
          << " (@value, @position)";
    }
    sql << " ORDER BY " << name << (forward ? " ASC, id ASC" : " DESC, id DESC");

    SqlitePreparedStatement &command =
        database->getPreparedStatement(sql.str().c_str());
    unsigned int parameter = 1;
    if (!lastKey->isNull()) {
      command.bindParameter(parameter++, *lastKey);
    }
    command.bindParameter(parameter, BindableValue(position));
    setReader(command.executeReader());
  }

  std::unique_ptr<BindableValue> lastKey;
  std::unique_ptr<Reader> reader;
  SqliteDatabase *database;
  bool suspended = false;
};
}  // namespace btrieve
#endif
