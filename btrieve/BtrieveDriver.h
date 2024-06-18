#ifndef __BTRIEVE_DRIVER_H_
#define __BTRIEVE_DRIVER_H_

#include "OperationCode.h"
#include "Record.h"
#include "SqlDatabase.h"
#include <memory>

namespace btrieve {
// Implements the Btrieve Driver functionality, e.g. querying and iterating
// through records.
class BtrieveDriver {
public:
  BtrieveDriver(SqlDatabase *sqlDatabase_) : sqlDatabase(sqlDatabase_) {}

  BtrieveDriver(BtrieveDriver &&driver)
      : sqlDatabase(std::move(driver.sqlDatabase)) {}

  ~BtrieveDriver();

  void open(const char *fileName);

  // Closes an opened database.
  void close();

  unsigned int getRecordLength() const {
    return sqlDatabase->getRecordLength();
  }

  unsigned int getRecordCount() const { return sqlDatabase->getRecordCount(); }

  bool isVariableLengthRecords() const {
    return sqlDatabase->isVariableLengthRecords();
  }

  const std::vector<Key> &getKeys() const { return sqlDatabase->getKeys(); }

  unsigned int getPosition() const { return sqlDatabase->getPosition(); }

  void setPosition(unsigned int position) {
    return sqlDatabase->setPosition(position);
  }

  std::pair<bool, Record> getRecord() { return getRecord(getPosition()); }

  std::pair<bool, Record> getRecord(unsigned int position) {
    return sqlDatabase->getRecord(position);
  }

  bool deleteAll() { return sqlDatabase->deleteAll(); }

  unsigned int insertRecord(std::basic_string_view<uint8_t> record) {
    return sqlDatabase->insertRecord(record);
  }

  BtrieveError updateRecord(unsigned int id,
                            std::basic_string_view<uint8_t> record) {
    return sqlDatabase->updateRecord(id, record);
  }

  bool performOperation(unsigned int keyNumber,
                        std::basic_string_view<uint8_t> key,
                        OperationCode operationCode);

private:
  std::unique_ptr<SqlDatabase> sqlDatabase;
};
} // namespace btrieve
#endif