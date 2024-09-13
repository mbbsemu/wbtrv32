#ifndef __SQLITE_DATABASE_H_
#define __SQLITE_DATABASE_H_

#include <memory>

#include "OperationCode.h"
#include "Record.h"
#include "SqlDatabase.h"
#include "SqlitePreparedStatement.h"
#include "SqliteTransaction.h"
#include "Text.h"
#include "sqlite/sqlite3.h"

namespace btrieve {
class SqliteDatabase : public SqlDatabase {
 public:
  SqliteDatabase(unsigned int openFlags_ = 0)
      : SqlDatabase(/* maxCacheSize= */ 64),
        openFlags(openFlags_),
        database(nullptr, &sqlite3_close) {}

  virtual ~SqliteDatabase() { close(); }

  virtual const tchar *getFileExtension() override { return TEXT("db"); };

  virtual void open(const tchar *fileName) override;

  virtual std::unique_ptr<RecordLoader> create(
      const tchar *fileName, const BtrieveDatabase &database) override;

  virtual void close() override;

  virtual BtrieveError stepFirst() override;
  virtual BtrieveError stepLast() override;
  virtual BtrieveError stepNext() override;
  virtual BtrieveError stepPrevious() override;
  virtual BtrieveError deleteRecord() override;

  virtual unsigned int getRecordCount() const override;

  virtual BtrieveError deleteAll() override;

  virtual std::pair<BtrieveError, unsigned int> insertRecord(
      std::basic_string_view<uint8_t> record) override;

  virtual BtrieveError updateRecord(
      unsigned int offset, std::basic_string_view<uint8_t> record) override;

  virtual BtrieveError getByKeyFirst(Query *query) override;
  virtual BtrieveError getByKeyLast(Query *query) override;
  virtual BtrieveError getByKeyEqual(Query *query) override;

  virtual std::unique_ptr<Query> logicalCurrencySeek(
      int keyNumber, unsigned int position, BtrieveError &error) override;

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

  BtrieveError getByKeyGreater(Query *query, const char *opurator);
  virtual BtrieveError getByKeyGreater(Query *query) override {
    return getByKeyGreater(query, ">");
  };
  virtual BtrieveError getByKeyGreaterOrEqual(Query *query) override {
    return getByKeyGreater(query, ">=");
  }
  BtrieveError getByKeyLess(Query *query, const char *opurator);
  virtual BtrieveError getByKeyLess(Query *query) override {
    return getByKeyLess(query, "<");
  };
  virtual BtrieveError getByKeyLessOrEqual(Query *query) override {
    return getByKeyLess(query, "<=");
  }
  virtual BtrieveError getByKeyNext(Query *query) override;
  virtual BtrieveError getByKeyPrevious(Query *query) override;

  virtual std::unique_ptr<Query> newQuery(
      unsigned int position, const Key *key,
      std::basic_string_view<uint8_t> keyData) override;

  BtrieveError insertAutoincrementValues(std::vector<uint8_t> &record);

  BtrieveError nextReader(Query *query, CursorDirection cursorDirection);

  unsigned int openFlags;
  mutable std::unordered_map<std::string, SqlitePreparedStatement>
      preparedStatements;
  std::shared_ptr<sqlite3> database;

  friend class SqliteQuery;
};

}  // namespace btrieve
#endif
