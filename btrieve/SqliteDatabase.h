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

  virtual const wchar_t *getFileExtension() override { return _TEXT("db"); };

  virtual BtrieveError open(const wchar_t *fileName,
                            OpenMode openMode = OpenMode::Normal) override;

  virtual std::unique_ptr<RecordLoader> create(
      const wchar_t *fileName, const BtrieveDatabase &database) override;

  virtual void close() override;

  virtual std::optional<SourceFingerprint> getSourceFingerprint() override;

  virtual void setSourceFingerprint(
      const SourceFingerprint &fingerprint) override;

  virtual void beginTransaction() override;
  virtual void commitTransaction() override;
  virtual void rollbackTransaction() override;

  virtual BtrieveError stepFirst() override;
  virtual BtrieveError stepLast() override;
  virtual BtrieveError stepNext() override;
  virtual BtrieveError stepPrevious() override;
  virtual BtrieveError deleteRecord() override;

  virtual unsigned int getRecordCount() const override;
  virtual unsigned int getUniqueKeyCount(const Key &key) const override;

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
  // the data, index and trigger creation use keys, so set it first
  void createSqliteDataTable(const char *table);
  void createSqliteDataIndices();
  void createSqliteTriggers();

  void initializeConnection();
  uint32_t loadSqliteMetadata();
  void loadSqliteKeys();

  BtrieveError getByKeyRange(Query *query, const char *opurator, bool reverse,
                             CursorDirection cursorDirection);
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

  const std::string &getInsertSql();
  const std::string &getUpdateSql();

  BtrieveError nextReader(Query *query, CursorDirection cursorDirection);

  void upgradeDatabaseFrom2To3();
  void upgradeDatabaseFrom3To4();

  unsigned int openFlags;
  mutable std::unordered_map<std::string, SqlitePreparedStatement>
      preparedStatements;
  SqliteTransactionStatements transactionStatements;
  // built on first use from keys, which don't change while the file is open
  std::string insertSql;
  std::string updateSql;
  std::shared_ptr<sqlite3> database;
  // the client transaction, if one is open
  std::unique_ptr<SqliteTransaction> clientTransaction;

  friend class SqliteQuery;
};

}  // namespace btrieve
#endif
