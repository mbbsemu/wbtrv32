#ifndef __SQLITE_DATABASE_H_
#define __SQLITE_DATABASE_H_

#include "SqlDatabase.h"

namespace btrieve {
class SqliteDatabase : public SqlDatabase {
public:
  SqliteDatabase() {}

  virtual ~SqliteDatabase() { close(); }

  virtual const char *getFileExtension() override { return "db"; };

  virtual void open(const char *fileName);

  virtual void create(const char *fileName, const BtrieveDatabase &database);

  virtual void close();
};
} // namespace btrieve
#endif