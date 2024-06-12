#ifndef __SQLITE_DATABASE_H_
#define __SQLITE_DATABASE_H_

#include "OperationCode.h"
#include "Record.h"
#include "SqlDatabase.h"
#include "SqlitePreparedStatement.h"
#include "SqliteTransaction.h"
#include "sqlite/sqlite3.h"
#include <memory>

namespace btrieve {
class SqliteDatabase : public SqlDatabase {
public:
  SqliteDatabase(unsigned int openFlags_ = 0)
      : SqlDatabase(/* maxCacheSize= */ 64), openFlags(openFlags_),
        database(nullptr, &sqlite3_close) {}

  virtual ~SqliteDatabase() { close(); }

  virtual const char *getFileExtension() override { return "db"; };

  virtual void open(const char *fileName) override;

  virtual std::unique_ptr<RecordLoader>
  create(const char *fileName, const BtrieveDatabase &database) override;

  virtual void close() override;

protected:
  virtual std::pair<bool, Record> selectRecord(unsigned int position) override;

private:
  SqlitePreparedStatement &getPreparedStatement(const char *sql) const;

  Record readRecord(unsigned int position, const SqliteReader &reader,
                    unsigned int columnOrdinal) {
    return Record(position, reader.getBlob(columnOrdinal));
  }

  const Record &cacheBtrieveRecord(unsigned int position,
                                   const SqliteReader &reader,
                                   unsigned int columnOrdinal);

  void createSqliteMetadataTable(const BtrieveDatabase &database);
  void createSqliteKeysTable(const BtrieveDatabase &database);
  void createSqliteDataTable(const BtrieveDatabase &database);
  void createSqliteDataIndices(const BtrieveDatabase &database);
  void createSqliteTriggers(const BtrieveDatabase &database);

  void loadSqliteMetadata(std::string &acsName, std::vector<char> &acs);
  void loadSqliteKeys(const std::string &acsName, const std::vector<char> &acs);

  virtual bool stepFirst() override;
  virtual bool stepLast() override;
  virtual bool stepNext() override;
  virtual bool stepPrevious() override;

  virtual unsigned int getRecordCount() const override;

  virtual void deleteAll() override;

  unsigned int openFlags;
  mutable std::unordered_map<std::string, SqlitePreparedStatement>
      preparedStatements;
  std::shared_ptr<sqlite3> database;
};

} // namespace btrieve
#endif