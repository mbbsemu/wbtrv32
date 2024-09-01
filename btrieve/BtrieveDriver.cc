#include "BtrieveDriver.h"
#include "BtrieveDatabase.h"
#include <filesystem>
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef _WIN32
#include <io.h>
#include <stdio.h>
#define WIN32_LEAN_AND_MEAN // Exclude rarely-used stuff from Windows headers
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace btrieve {
BtrieveDriver::~BtrieveDriver() { close(); }

static inline bool fileExists(const tchar *filename,
                              int64_t &fileModificationNanos) {
#ifdef WIN32
  WIN32_FILE_ATTRIBUTE_DATA fileAttributeData;

  if (GetFileAttributesEx(filename, GetFileExInfoStandard,
                          &fileAttributeData) == 0) {
    return false;
  }

  // TODO check this
  fileModificationNanos =
      *reinterpret_cast<int64_t *>(&fileAttributeData.ftLastWriteTime);
  return true;
#else
  struct stat stbuf;
  if (stat(filename, &stbuf)) {
    return false;
  }

  fileModificationNanos =
      (stbuf.st_mtim.tv_sec * 1000000000l) + stbuf.st_mtim.tv_nsec;
  return true;
#endif
}

static std::basic_string<tchar> toUpper(const std::basic_string<tchar> &value) {
  std::basic_string<tchar> ret(value);
  for (size_t i = 0; i < ret.size(); ++i) {
    ret[i] = toupper(ret[i]);
  }
  return ret;
}

#ifdef _WIN32
#define unlink _wunlink
#endif

void BtrieveDriver::open(const tchar *fileName) {
  int64_t fileModificationTimeDat;
  int64_t fileModificationTimeDb;

  std::filesystem::path dbPath =
      std::filesystem::path(fileName).replace_extension(
          sqlDatabase->getFileExtension());

  bool datExists = fileExists(fileName, fileModificationTimeDat);
  bool dbExists = fileExists(dbPath.c_str(), fileModificationTimeDb);
  if (!dbExists) {
    // failed to find db, let's uppercase and try again
    std::filesystem::path dbPathUpper = dbPath;
    dbPathUpper.replace_extension(toUpper(sqlDatabase->getFileExtension()));
    dbExists = fileExists(dbPathUpper.c_str(), fileModificationTimeDb);
    if (dbExists) {
      dbPath = dbPathUpper;
    }
  }

  // if both DAT/DB exist, check if the DAT has an a newer time, if so
  // we want to reconvert it by deleting the DB
  if (datExists && dbExists &&
      fileModificationTimeDat > fileModificationTimeDb) {
    //_logger.Warn($"{fullPathDAT} is newer than {fullPathDB}, reconverting the
    // DAT -> DB");
    unlink(dbPath.c_str());
    dbExists = false;
  }

  if (dbExists) {
    sqlDatabase->open(dbPath.c_str());
  } else {
    BtrieveDatabase btrieveDatabase;
    std::unique_ptr<RecordLoader> recordLoader;
    btrieveDatabase.parseDatabase(
        fileName,
        [this, &dbPath, &btrieveDatabase, &recordLoader]() {
          recordLoader = sqlDatabase->create(dbPath.c_str(), btrieveDatabase);
          return true;
        },
        [&recordLoader](const std::basic_string_view<uint8_t> record) {
          return recordLoader->onRecordLoaded(record);
        },
        [&recordLoader]() { recordLoader->onRecordsComplete(); });
  }

  // Set Position to First Record
  performOperation(0, std::basic_string_view<uint8_t>(),
                   OperationCode::StepFirst);
}

void BtrieveDriver::close() {
  sqlDatabase->close();
  // release ownership and delete
  sqlDatabase.reset(nullptr);
}

BtrieveError
BtrieveDriver::performOperation(int keyNumber,
                                std::basic_string_view<uint8_t> keyData,
                                OperationCode operationCode) {
  switch (operationCode) {
  case OperationCode::Delete:
    return sqlDatabase->deleteRecord();
  case OperationCode::StepFirst:
    return sqlDatabase->stepFirst();
  case OperationCode::StepLast:
    return sqlDatabase->stepLast();
  case OperationCode::StepNext:
  case OperationCode::StepNextExtended:
    return sqlDatabase->stepNext();
  case OperationCode::StepPrevious:
  case OperationCode::StepPreviousExtended:
    return sqlDatabase->stepPrevious();
  default:
      // fall through
      ;
  }

  bool newQuery = !usesPreviousQuery(operationCode);
  if (newQuery || !previousQuery) {
    if (keyNumber < 0 ||
        static_cast<unsigned int>(keyNumber) >= sqlDatabase->getKeys().size()) {
      return BtrieveError::InvalidKeyNumber;
    }

    const Key *key = &(sqlDatabase->getKeys()[keyNumber]);

    previousQuery = std::move(
        sqlDatabase->newQuery(sqlDatabase->getPosition(), key, keyData));
  } else if (!previousQuery) {
    return BtrieveError::InvalidKeyPosition;
  } else if (previousQuery->getKey()->getNumber() != keyNumber) {
    return BtrieveError::DifferentKeyNumber;
  }

  // always using previousQuery from this point onward
  switch (operationCode) {
  case OperationCode::AcquireFirst:
  case OperationCode::QueryFirst:
    return sqlDatabase->getByKeyFirst(previousQuery.get());
  case OperationCode::AcquireLast:
  case OperationCode::QueryLast:
    return sqlDatabase->getByKeyLast(previousQuery.get());
  case OperationCode::AcquireEqual:
  case OperationCode::QueryEqual:
    return sqlDatabase->getByKeyEqual(previousQuery.get());
  case OperationCode::AcquireGreater:
  case OperationCode::QueryGreater:
    return sqlDatabase->getByKeyGreater(previousQuery.get());
  case OperationCode::AcquireGreaterOrEqual:
  case OperationCode::QueryGreaterOrEqual:
    return sqlDatabase->getByKeyGreaterOrEqual(previousQuery.get());
  case OperationCode::AcquireLess:
  case OperationCode::QueryLess:
    return sqlDatabase->getByKeyLess(previousQuery.get());
  case OperationCode::AcquireLessOrEqual:
  case OperationCode::QueryLessOrEqual:
    return sqlDatabase->getByKeyLessOrEqual(previousQuery.get());
  case OperationCode::AcquireNext:
  case OperationCode::QueryNext:
    return sqlDatabase->getByKeyNext(previousQuery.get());
  case OperationCode::AcquirePrevious:
  case OperationCode::QueryPrevious:
    return sqlDatabase->getByKeyPrevious(previousQuery.get());
  default:
    return BtrieveError::OperationNotAllowed;
  }
}

} // namespace btrieve
