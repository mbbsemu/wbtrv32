#ifndef __SQL_DATABASE_H_
#define __SQL_DATABASE_H_

#include "BtrieveDatabase.h"
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
  virtual ~SqlDatabase() = default;

  virtual const char *getFileExtension() = 0;

  // Opens a Btrieve database as a sql backed file.
  virtual void open(const char *fileName) = 0;

  // Creates a new sql backed file using database as the source of records
  virtual std::unique_ptr<RecordLoader>
  create(const char *fileName, const BtrieveDatabase &database) = 0;

  // Closes an opened database.
  virtual void close() = 0;
};
} // namespace btrieve

#endif