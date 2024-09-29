#include "BtrieveDriver.h"

#include <sys/stat.h>
#include <sys/types.h>

#include <filesystem>
#include <memory>

#include "BtrieveDatabase.h"
#ifdef _WIN32
#include <io.h>
#include <stdio.h>
#define WIN32_LEAN_AND_MEAN  // Exclude rarely-used stuff from Windows headers
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

BtrieveError BtrieveDriver::open(const tchar *fileName) {
  int64_t fileModificationTimeDat;
  int64_t fileModificationTimeDb;

  std::filesystem::path dbPath =
      std::filesystem::path(fileName).replace_extension(
          sqlDatabase->getFileExtension());

  openedFilename = fileName;

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

  BtrieveError error;

  if (dbExists) {
    error = sqlDatabase->open(dbPath.c_str());
  } else {
    BtrieveDatabase btrieveDatabase;
    std::unique_ptr<RecordLoader> recordLoader;
    error = btrieveDatabase.parseDatabase(
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

  if (error == BtrieveError::Success) {
    // Set Position to First Record
    performOperation(0, std::basic_string_view<uint8_t>(),
                     OperationCode::StepFirst);
  }

  return error;
}

void BtrieveDriver::close() {
  sqlDatabase->close();
  // release ownership and delete
  sqlDatabase.reset(nullptr);
}

BtrieveError BtrieveDriver::performOperation(
    int keyNumber, std::basic_string_view<uint8_t> keyData,
    OperationCode operationCode) {
  switch (operationCode) {
    case OperationCode::Delete:
      return sqlDatabase->deleteRecord();
      /* lock biases, which we don't support / care about
      +100 Single wait record lock.
      +200 Single no-wait record lock.
      +300 Multiple wait record lock.
      +400 Multiple no-wait record lock.
      */
    case OperationCode::StepFirst:
    case OperationCode::StepFirst + 100:
    case OperationCode::StepFirst + 200:
    case OperationCode::StepFirst + 300:
    case OperationCode::StepFirst + 400:
      return sqlDatabase->stepFirst();
    case OperationCode::StepLast:
    case OperationCode::StepLast + 100:
    case OperationCode::StepLast + 200:
    case OperationCode::StepLast + 300:
    case OperationCode::StepLast + 400:
      return sqlDatabase->stepLast();
    case OperationCode::StepNext:
    case OperationCode::StepNext + 100:
    case OperationCode::StepNext + 200:
    case OperationCode::StepNext + 300:
    case OperationCode::StepNext + 400:
    case OperationCode::StepNextExtended:
    case OperationCode::StepNextExtended + 100:
    case OperationCode::StepNextExtended + 200:
    case OperationCode::StepNextExtended + 300:
    case OperationCode::StepNextExtended + 400:
      return sqlDatabase->stepNext();
    case OperationCode::StepPrevious:
    case OperationCode::StepPrevious + 100:
    case OperationCode::StepPrevious + 200:
    case OperationCode::StepPrevious + 300:
    case OperationCode::StepPrevious + 400:
    case OperationCode::StepPreviousExtended:
    case OperationCode::StepPreviousExtended + 100:
    case OperationCode::StepPreviousExtended + 200:
    case OperationCode::StepPreviousExtended + 300:
    case OperationCode::StepPreviousExtended + 400:
      return sqlDatabase->stepPrevious();
    default:
        // fall through
        ;
  }

  if (usesPreviousQuery(operationCode)) {
    if (!previousQuery) {
      return BtrieveError::InvalidPositioning;
    } else if (previousQuery->getKey()->getNumber() != keyNumber) {
      return BtrieveError::DifferentKeyNumber;
    }
  } else {
    // this is a new query
    if (keyNumber < 0 ||
        static_cast<unsigned int>(keyNumber) >= sqlDatabase->getKeys().size()) {
      return BtrieveError::InvalidKeyNumber;
    }

    const Key *key = &(sqlDatabase->getKeys()[keyNumber]);

    previousQuery = std::move(
        sqlDatabase->newQuery(sqlDatabase->getPosition(), key, keyData));
  }

  // always using previousQuery from this point onward
  BtrieveError error;
  switch (operationCode) {
    // these operations continue from a set logical position
    case OperationCode::AcquireNext:
    case OperationCode::QueryNext:
      return sqlDatabase->getByKeyNext(previousQuery.get());
    case OperationCode::AcquirePrevious:
    case OperationCode::QueryPrevious:
      return sqlDatabase->getByKeyPrevious(previousQuery.get());
    // the following operations set logical position
    case OperationCode::AcquireFirst:
    case OperationCode::QueryFirst:
      error = sqlDatabase->getByKeyFirst(previousQuery.get());
      break;
    case OperationCode::AcquireLast:
    case OperationCode::QueryLast:
      error = sqlDatabase->getByKeyLast(previousQuery.get());
      break;
    case OperationCode::AcquireEqual:
    case OperationCode::QueryEqual:
      error = sqlDatabase->getByKeyEqual(previousQuery.get());
      break;
    case OperationCode::AcquireGreater:
    case OperationCode::QueryGreater:
      error = sqlDatabase->getByKeyGreater(previousQuery.get());
      break;
    case OperationCode::AcquireGreaterOrEqual:
    case OperationCode::QueryGreaterOrEqual:
      error = sqlDatabase->getByKeyGreaterOrEqual(previousQuery.get());
      break;
    case OperationCode::AcquireLess:
    case OperationCode::QueryLess:
      error = sqlDatabase->getByKeyLess(previousQuery.get());
      break;
    case OperationCode::AcquireLessOrEqual:
    case OperationCode::QueryLessOrEqual:
      error = sqlDatabase->getByKeyLessOrEqual(previousQuery.get());
      break;
    default:
      return BtrieveError::OperationNotAllowed;
  }

  // if we had an error, clear out previousQuery as if it were never set above
  if (error != BtrieveError::Success) {
    previousQuery.reset(nullptr);
  }

  return error;
}

}  // namespace btrieve
