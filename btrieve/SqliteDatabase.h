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

  virtual BtrieveError stepFirst() override;
  virtual BtrieveError stepLast() override;
  virtual BtrieveError stepNext() override;
  virtual BtrieveError stepPrevious() override;
  virtual BtrieveError deleteRecord() override;

  virtual unsigned int getRecordCount() const override;

  virtual BtrieveError deleteAll() override;

  virtual unsigned int
  insertRecord(std::basic_string_view<uint8_t> record) override;

  virtual BtrieveError
  updateRecord(unsigned int offset,
               std::basic_string_view<uint8_t> record) override;

  virtual BtrieveError getByKeyFirst(Query *query) override;
  virtual BtrieveError getByKeyLast(Query *query) override;
  virtual BtrieveError getByKeyEqual(Query *query) override;
  virtual BtrieveError getByKeyNext(Query *query) override;
  virtual BtrieveError getByKeyPrevious(Query *query) override;

  virtual std::unique_ptr<Query>
  newQuery(unsigned int position, const Key *key,
           std::basic_string_view<uint8_t> keyData) override;

  BtrieveError insertAutoincrementValues(std::vector<uint8_t> &record);

  BtrieveError nextReader(Query *query, CursorDirection cursorDirection);

  unsigned int openFlags;
  mutable std::unordered_map<std::string, SqlitePreparedStatement>
      preparedStatements;
  std::shared_ptr<sqlite3> database;

  friend class SqliteQuery;
};

} // namespace btrieve
#endif