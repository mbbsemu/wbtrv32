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

  void commit() { execute("COMMIT"); }

  void rollback() { execute("ROLLBACK"); }

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
};
}  // namespace btrieve

#endif
