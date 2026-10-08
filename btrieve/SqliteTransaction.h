#ifndef __SQLITE_TRANSACTION_H_
#define __SQLITE_TRANSACTION_H_

#include <atomic>
#include <memory>

#include "SqlitePreparedStatement.h"
#include "SqliteUtil.h"
#include "sqlite/sqlite3.h"

namespace btrieve {

// Prepared BEGIN/COMMIT/ROLLBACK statements, owned by a connection that runs
// many short transactions so each one doesn't re-parse its control SQL.
// Statements are prepared lazily on first use.
struct SqliteTransactionStatements {
  std::unique_ptr<SqlitePreparedStatement> beginImmediate;
  std::unique_ptr<SqlitePreparedStatement> beginDeferred;
  std::unique_ptr<SqlitePreparedStatement> commit;
  std::unique_ptr<SqlitePreparedStatement> rollback;

  void clear() {
    beginImmediate.reset();
    beginDeferred.reset();
    commit.reset();
    rollback.reset();
  }
};

class SqliteTransaction {
 public:
  SqliteTransaction(std::shared_ptr<sqlite3> database_,
                    SqliteTransactionStatements *statements_ = nullptr)
      : database(database_), statements(statements_) {
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
    execute(statements ? &statements->commit : nullptr, "COMMIT");
    finished = true;
  }

  void rollback() {
    execute(statements ? &statements->rollback : nullptr, "ROLLBACK");
    finished = true;
  }

 private:
  // IMMEDIATE takes the write lock up front: every transaction here writes,
  // and a deferred BEGIN can fail mid-transaction with SQLITE_BUSY when it
  // tries to upgrade its read lock while another connection is writing.
  //
  // A read-only connection (query_only) refuses IMMEDIATE outright, so fall
  // back to a deferred BEGIN there and let the write statement itself fail,
  // which callers already map to the proper Btrieve error (AccessDenied).
  void beginTransaction() {
    int errorCode =
        run(statements ? &statements->beginImmediate : nullptr,
            "BEGIN IMMEDIATE");
    if (errorCode == SQLITE_READONLY) {
      execute(statements ? &statements->beginDeferred : nullptr, "BEGIN");
    } else if (errorCode != SQLITE_OK) {
      throwException(errorCode);
    }
  }

  void execute(std::unique_ptr<SqlitePreparedStatement> *cached,
               const char *sql) {
    int errorCode = run(cached, sql);
    if (errorCode != SQLITE_OK) {
      throwException(errorCode);
    }
  }

  // Runs sql through the cached prepared statement when one is available,
  // otherwise through sqlite3_exec. Returns SQLITE_OK on success.
  int run(std::unique_ptr<SqlitePreparedStatement> *cached, const char *sql) {
    if (cached == nullptr) {
      return sqlite3_exec(database.get(), sql, nullptr, nullptr, nullptr);
    }

    if (!*cached) {
      cached->reset(new SqlitePreparedStatement(database, sql));
    }

    int errorCode = (*cached)->step();
    (*cached)->reset();
    return errorCode == SQLITE_DONE ? SQLITE_OK : errorCode;
  }

  std::shared_ptr<sqlite3> database;
  SqliteTransactionStatements *statements;
  bool finished = false;
};
}  // namespace btrieve

#endif
