#ifndef __SQLITE_TRANSACTION_H_
#define __SQLITE_TRANSACTION_H_

#include <atomic>
#include <memory>

#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"

namespace btrieve {

class SqliteTransaction {
 public:
  SqliteTransaction(std::shared_ptr<sqlite3> database_) : database(database_) {
    beginTransaction();
  }

  // If neither commit() nor rollback() ran -- e.g. an exception other than
  // BtrieveException unwound through a caller that only catches that type --
  // don't leave the transaction open on the connection indefinitely.
  ~SqliteTransaction() {
    if (!finished) {
      sqlite3_exec(database.get(), "ROLLBACK", nullptr, nullptr, nullptr);
    }
  }

  void commit() {
    execute("COMMIT");
    finished = true;
  }

  void rollback() {
    execute("ROLLBACK");
    finished = true;
  }

 private:
  void beginTransaction() { execute("BEGIN"); }

  void execute(const char *sql) {
    int errorCode =
        sqlite3_exec(database.get(), sql, nullptr, nullptr, nullptr);
    if (errorCode != SQLITE_OK) {
      throwException(errorCode);
    }
  }

  std::shared_ptr<sqlite3> database;
  bool finished = false;
};
}  // namespace btrieve

#endif
