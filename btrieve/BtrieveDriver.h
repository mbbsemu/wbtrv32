#ifndef __BTRIEVE_DRIVER_H_
#define __BTRIEVE_DRIVER_H_

#include <memory>

#include "ErrorCode.h"
#include "OperationCode.h"
#include "Record.h"
#include "SqlDatabase.h"
#include "Text.h"

namespace btrieve {
// Implements the Btrieve Driver functionality, e.g. querying and iterating
// through records.
class BtrieveDriver {
 public:
  BtrieveDriver(SqlDatabase *sqlDatabase_) : sqlDatabase(sqlDatabase_) {}

  BtrieveDriver(BtrieveDriver &&driver)
      : sqlDatabase(std::move(driver.sqlDatabase)) {}

  ~BtrieveDriver();

  BtrieveError open(const tchar *fileName);

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

  std::pair<BtrieveError, unsigned int> insertRecord(
      std::basic_string_view<uint8_t> record) {
    return sqlDatabase->insertRecord(record);
  }

  BtrieveError updateRecord(unsigned int id,
                            std::basic_string_view<uint8_t> record) {
    return sqlDatabase->updateRecord(id, record);
  }

  BtrieveError performOperation(int keyNumber,
                                std::basic_string_view<uint8_t> key,
                                OperationCode operationCode);

  BtrieveError logicalCurrencySeek(int keyNumber, unsigned int position) {
    BtrieveError ret;

    previousQuery = sqlDatabase->logicalCurrencySeek(keyNumber, position, ret);

    return ret;
  }

  std::basic_string<tchar> getOpenedFilename() { return openedFilename; }

 private:
  std::unique_ptr<SqlDatabase> sqlDatabase;
  std::unique_ptr<Query> previousQuery;
  std::basic_string<tchar> openedFilename;
};
}  // namespace btrieve
#endif
