// 26.10.15.3B 10:20PM
#ifndef __BTRIEVE_DRIVER_H_
#define __BTRIEVE_DRIVER_H_

#include <filesystem>
#include <memory>

#include "ErrorCode.h"
#include "OpenMode.h"
#include "OperationCode.h"
#include "Record.h"
#include "SqlDatabase.h"
#include "Text.h"

namespace btrieve {
// Implements the Btrieve Driver functionality, e.g. querying and iterating
// through records.
class BtrieveDriver {
 public:
  // With trackSourceFile_, a .DB remembers the .DAT it was converted from, and
  // a different .DAT put in its place later (a module upgrade, a restore) is
  // converted afresh after the old .DB is moved aside into
  // "CONVERTED DATABASES". Turn it off when something else keeps writing the
  // .DAT, as stock Btrieve does under the parity shim.
  BtrieveDriver(SqlDatabase *sqlDatabase_, bool trackSourceFile_ = true)
      : sqlDatabase(sqlDatabase_), trackSourceFile(trackSourceFile_) {}

  BtrieveDriver(BtrieveDriver &&driver)
      : sqlDatabase(std::move(driver.sqlDatabase)),
        trackSourceFile(driver.trackSourceFile) {}

  ~BtrieveDriver();

  BtrieveError open(const wchar_t *fileName,
                    OpenMode openMode = OpenMode::Normal);

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

  std::basic_string<wchar_t> getOpenedFilename() { return openedFilename; }

  void beginTransaction() { sqlDatabase->beginTransaction(); }

  void commitTransaction() { sqlDatabase->commitTransaction(); }

  void rollbackTransaction() {
    previousQuery.reset();
    sqlDatabase->rollbackTransaction();
  }

 private:
  BtrieveError refreshFromChangedSource(const wchar_t *fileName,
                                        const std::filesystem::path &dbPath,
                                        OpenMode openMode);

  BtrieveError convert(const wchar_t *fileName,
                       const std::filesystem::path &dbPath, OpenMode openMode);

  std::unique_ptr<SqlDatabase> sqlDatabase;
  bool trackSourceFile;
  std::unique_ptr<Query> previousQuery;
  std::basic_string<wchar_t> openedFilename;
};
}  // namespace btrieve
#endif
