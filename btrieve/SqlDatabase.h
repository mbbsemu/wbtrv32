#ifndef __SQL_DATABASE_H_
#define __SQL_DATABASE_H_

#include <memory>

#include "BtrieveDatabase.h"
#include "ErrorCode.h"
#include "LRUCache.h"
#include "OperationCode.h"
#include "Query.h"
#include "Text.h"

namespace btrieve {

class RecordLoader {
 public:
  virtual ~RecordLoader() = default;

  virtual bool onRecordLoaded(std::basic_string_view<uint8_t> record) = 0;

  virtual void onRecordsComplete() = 0;
};

// An interface that abstracts a SQL-compatible implementation of
// BtrieveDatabase.
class SqlDatabase {
 public:
  SqlDatabase(unsigned int maxCacheSize) : cache(maxCacheSize) {}

  virtual ~SqlDatabase() = default;

  virtual const tchar *getFileExtension() = 0;

  // Opens a Btrieve database as a sql backed file.
  virtual void open(const tchar *fileName) = 0;

  // Creates a new sql backed file using database as the source of records
  virtual std::unique_ptr<RecordLoader> create(
      const tchar *fileName, const BtrieveDatabase &database) = 0;

  // Closes an opened database.
  virtual void close() = 0;

  virtual unsigned int getRecordLength() const { return recordLength; }

  virtual bool isVariableLengthRecords() const { return variableLengthRecords; }

  virtual const std::vector<Key> &getKeys() const { return keys; }

  unsigned int getPosition() const { return position; }

  void setPosition(unsigned int position_) { position = position_; }

  std::pair<bool, Record> getRecord(unsigned int position) {
    std::shared_ptr<Record> data = cache.get(position);
    if (data) {
      return std::pair<bool, Record>(true, *data);
    }

    std::pair<bool, Record> ret = selectRecord(position);
    if (ret.first) {
      cache.cache(position, ret.second);
    }
    return ret;
  }

  virtual BtrieveError stepFirst() = 0;
  virtual BtrieveError stepLast() = 0;
  virtual BtrieveError stepPrevious() = 0;
  virtual BtrieveError stepNext() = 0;
  virtual unsigned int getRecordCount() const = 0;
  virtual BtrieveError deleteAll() = 0;
  virtual BtrieveError deleteRecord() = 0;
  virtual std::pair<BtrieveError, unsigned int> insertRecord(
      std::basic_string_view<uint8_t> record) = 0;
  virtual BtrieveError updateRecord(unsigned int offset,
                                    std::basic_string_view<uint8_t> record) = 0;

  virtual BtrieveError getByKeyFirst(Query *query) = 0;
  virtual BtrieveError getByKeyLast(Query *query) = 0;
  virtual BtrieveError getByKeyEqual(Query *query) = 0;
  virtual BtrieveError getByKeyGreater(Query *query) = 0;
  virtual BtrieveError getByKeyGreaterOrEqual(Query *query) = 0;
  virtual BtrieveError getByKeyLess(Query *query) = 0;
  virtual BtrieveError getByKeyLessOrEqual(Query *query) = 0;
  virtual BtrieveError getByKeyNext(Query *query) = 0;
  virtual BtrieveError getByKeyPrevious(Query *query) = 0;

  virtual std::unique_ptr<Query> newQuery(
      unsigned int position, const Key *key,
      std::basic_string_view<uint8_t> keyData) = 0;

  virtual std::unique_ptr<Query> logicalCurrencySeek(int keyNumber,
                                                     unsigned int position,
                                                     BtrieveError &error) = 0;

 protected:
  virtual std::pair<bool, Record> selectRecord(unsigned int position) = 0;

  unsigned int recordLength;
  unsigned int position;
  bool variableLengthRecords;
  std::vector<Key> keys;
  LRUCache<unsigned int, Record> cache;
};
}  // namespace btrieve

#endif
