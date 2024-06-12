#ifndef __SQL_DATABASE_H_
#define __SQL_DATABASE_H_

#include "BtrieveDatabase.h"
#include "LRUCache.h"
#include "OperationCode.h"
#include <memory>

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

  virtual const char *getFileExtension() = 0;

  // Opens a Btrieve database as a sql backed file.
  virtual void open(const char *fileName) = 0;

  // Creates a new sql backed file using database as the source of records
  virtual std::unique_ptr<RecordLoader>
  create(const char *fileName, const BtrieveDatabase &database) = 0;

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

  virtual bool stepFirst() = 0;
  virtual bool stepLast() = 0;
  virtual bool stepPrevious() = 0;
  virtual bool stepNext() = 0;

protected:
  virtual std::pair<bool, Record> selectRecord(unsigned int position) = 0;

  unsigned int recordLength;
  unsigned int position;
  bool variableLengthRecords;
  std::vector<Key> keys;
  LRUCache<unsigned int, Record> cache;
};
} // namespace btrieve

#endif