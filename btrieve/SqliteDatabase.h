#ifndef __SQLITE_DATABASE_H_
#define __SQLITE_DATABASE_H_

#include "SqlDatabase.h"
#include "sqlite/sqlite3.h"
#include <memory>

namespace btrieve {
class SqliteDatabase : public SqlDatabase {
public:
  SqliteDatabase() : database(nullptr, &sqlite3_close) {}

  virtual ~SqliteDatabase() { close(); }

  virtual const char *getFileExtension() override { return "db"; };

  virtual void open(const char *fileName);

  virtual void create(const char *fileName, const BtrieveDatabase &database);

  virtual void close();

private:
  void createSqliteMetadataTable(const BtrieveDatabase &database);
  void createSqliteKeysTable(const BtrieveDatabase &database);
  void createSqliteDataTable(const BtrieveDatabase &database);
  void createSqliteDataIndices(const BtrieveDatabase &database);
  void createSqliteTriggers(const BtrieveDatabase &database);
  void populateSqliteDataTable(const BtrieveDatabase &database);

  std::shared_ptr<sqlite3> database;

  unsigned int recordLength;
  unsigned int pageLength;
  bool variableLengthRecords;
  std::vector<Key> keys;
};

} // namespace btrieve
#endif