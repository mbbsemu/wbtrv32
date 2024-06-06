#ifndef __BTRIEVE_DRIVER_H_
#define __BTRIEVE_DRIVER_H_

#include "SqlDatabase.h"
#include <memory>

namespace btrieve {
// Implements the Btrieve Driver functionality, e.g. querying and iterating
// through records.
class BtrieveDriver {
public:
  BtrieveDriver(SqlDatabase *sqlDatabase_) : sqlDatabase(sqlDatabase_) {}

  ~BtrieveDriver();

  void open(const char *fileName);

  // Closes an opened database.
  void close();

  unsigned int getRecordLength() const {
    return sqlDatabase->getRecordLength();
  }

  bool isVariableLengthRecords() const {
    return sqlDatabase->isVariableLengthRecords();
  }

  const std::vector<Key> &getKeys() const { return sqlDatabase->getKeys(); }

private:
  std::unique_ptr<SqlDatabase> sqlDatabase;
};
} // namespace btrieve
#endif