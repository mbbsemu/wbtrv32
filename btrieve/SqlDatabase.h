#ifndef __SQL_DATABASE_H_
#define __SQL_DATABASE_H_

#include "BtrieveDatabase.h"

namespace btrieve {

// An interface that abstracts a SQL-compatible implementation of BtrieveDatabase.
class SqlDatabase {
public:
  virtual ~SqlDatabase() = default;

  virtual const char *getFileExtension() = 0;

  // Opens a Btrieve database as a sql backed file.
  virtual void open(const char *fileName) = 0;

  // Creates a new sql backed file using database as the source of records
  virtual void create(const char *fileName,
                      const BtrieveDatabase &database) = 0;

  // Closes an opened database.
  virtual void close() = 0;
};
} // namespace btrieve

#endif