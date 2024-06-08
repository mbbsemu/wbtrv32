#ifndef __SQLITE_DATABASE_H_
#define __SQLITE_DATABASE_H_

#include "OperationCode.h"
#include "SqlDatabase.h"
#include "SqlitePreparedStatement.h"
#include "SqliteTransaction.h"
#include "sqlite/sqlite3.h"
#include <memory>

namespace btrieve {
class SqliteDatabase : public SqlDatabase {
public:
  SqliteDatabase(unsigned int openFlags_ = 0)
      : openFlags(openFlags_), database(nullptr, &sqlite3_close) {}

  virtual ~SqliteDatabase() { close(); }

  virtual const char *getFileExtension() override { return "db"; };

  virtual void open(const char *fileName);

  virtual std::unique_ptr<RecordLoader> create(const char *fileName,
                                               const BtrieveDatabase &database);

  virtual void close();

  virtual bool performOperation(unsigned int keyNumber,
                                std::basic_string_view<uint8_t> key,
                                OperationCode btrieveOperationCode);

private:
  SqlitePreparedStatement &getPreparedStatement(const char *sql);
  void getAndCacheBtrieveRecord(uint id, const SqliteReader &reader,
                                unsigned int columnOrdinal);

  void createSqliteMetadataTable(const BtrieveDatabase &database);
  void createSqliteKeysTable(const BtrieveDatabase &database);
  void createSqliteDataTable(const BtrieveDatabase &database);
  void createSqliteDataIndices(const BtrieveDatabase &database);
  void createSqliteTriggers(const BtrieveDatabase &database);

  void loadSqliteMetadata(std::string &acsName, std::vector<char> &acs);
  void loadSqliteKeys(const std::string &acsName, const std::vector<char> &acs);

  bool stepFirst();
  bool stepLast();
  bool stepNext();
  bool stepPrevious();

  unsigned int openFlags;
  std::unordered_map<std::string, SqlitePreparedStatement> preparedStatements;
  std::shared_ptr<sqlite3> database;
};

} // namespace btrieve
#endif