#include "BtrieveDriver.h"
#include "BtrieveDatabase.h"
#include <filesystem>
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace btrieve {
BtrieveDriver::~BtrieveDriver() { close(); }

static inline uint64_t toNanos(struct timespec *ts) {
  return static_cast<uint64_t>(ts->tv_sec) * 1000000 + ts->tv_nsec;
}

static std::string toUpper(const std::string &value) {
  std::string ret(value);
  for (size_t i = 0; i < ret.size(); ++i) {
    ret[i] = toupper(ret[i]);
  }
  return ret;
}

void BtrieveDriver::open(const char *fileName) {
  struct stat statbufdat;
  struct stat statbufdb;

  memset(&statbufdat, 0, sizeof(statbufdat));
  memset(&statbufdb, 0, sizeof(statbufdb));

  std::filesystem::path dbPath =
      std::filesystem::path(fileName).replace_extension(
          sqlDatabase->getFileExtension());

  bool datExists = stat(fileName, &statbufdat) == 0;
  bool dbExists = stat(dbPath.c_str(), &statbufdb) == 0;
  if (!dbExists) {
    // failed to find db, let's uppercase and try again
    std::filesystem::path dbPathUpper = dbPath;
    dbPathUpper.replace_extension(toUpper(sqlDatabase->getFileExtension()));
    dbExists = stat(dbPathUpper.c_str(), &statbufdb) == 0;
    if (dbExists) {
      dbPath = dbPathUpper;
    }
  }

  // if both DAT/DB exist, check if the DAT has an a newer time, if so
  // we want to reconvert it by deleting the DB
  if (datExists && dbExists &&
      toNanos(&statbufdat.st_mtim) > toNanos(&statbufdb.st_mtim)) {
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
    const Key *key = nullptr;
    if (keyNumber >= 0 &&
        static_cast<size_t>(keyNumber) < sqlDatabase->getKeys().size()) {
      key = &(sqlDatabase->getKeys()[keyNumber]);
    }

    previousQuery = std::move(
        sqlDatabase->newQuery(sqlDatabase->getPosition(), key, keyData));
  } else if (!previousQuery) {
    return BtrieveError::InvalidKeyPosition;
  }

  // always using previousQuery from this point onward
  switch (operationCode) {
  case OperationCode::AcquireFirst:
  case OperationCode::QueryFirst:
    return sqlDatabase->getByKeyFirst(previousQuery.get());
  case OperationCode::AcquireEqual:
  case OperationCode::QueryEqual:
    return sqlDatabase->getByKeyEqual(previousQuery.get());
  case OperationCode::AcquireNext:
  case OperationCode::QueryNext:
    return sqlDatabase->getByKeyNext(previousQuery.get());
  case OperationCode::AcquirePrevious:
  case OperationCode::QueryPrevious:
    return sqlDatabase->getByKeyPrevious(previousQuery.get());
  default:
    return BtrieveError::OperationNotAllowed;
  }

  /*
  return btrieveOperationCode switch
  {
      EnumBtrieveOperationCodes.AcquireEqual => GetByKeyEqual(currentQuery),
      EnumBtrieveOperationCodes.QueryEqual => GetByKeyEqual(currentQuery),

      EnumBtrieveOperationCodes.AcquireFirst => GetByKeyFirst(currentQuery),
      EnumBtrieveOperationCodes.QueryFirst => GetByKeyFirst(currentQuery),

      EnumBtrieveOperationCodes.AcquireLast => GetByKeyLast(currentQuery),
      EnumBtrieveOperationCodes.QueryLast => GetByKeyLast(currentQuery),

      EnumBtrieveOperationCodes.AcquireGreater => GetByKeyGreater(currentQuery,
  ">"), EnumBtrieveOperationCodes.QueryGreater => GetByKeyGreater(currentQuery,
  ">"), EnumBtrieveOperationCodes.AcquireGreaterOrEqual =>
  GetByKeyGreater(currentQuery, ">="),
      EnumBtrieveOperationCodes.QueryGreaterOrEqual =>
  GetByKeyGreater(currentQuery, ">="),

      EnumBtrieveOperationCodes.AcquireLess => GetByKeyLess(currentQuery, "<"),
      EnumBtrieveOperationCodes.QueryLess => GetByKeyLess(currentQuery, "<"),
      EnumBtrieveOperationCodes.AcquireLessOrEqual => GetByKeyLess(currentQuery,
  "<="), EnumBtrieveOperationCodes.QueryLessOrEqual =>
  GetByKeyLess(currentQuery, "<="),

      EnumBtrieveOperationCodes.AcquireNext => GetByKeyNext(currentQuery),
      EnumBtrieveOperationCodes.QueryNext => GetByKeyNext(currentQuery),

      EnumBtrieveOperationCodes.AcquirePrevious =>
  GetByKeyPrevious(currentQuery), EnumBtrieveOperationCodes.QueryPrevious =>
  GetByKeyPrevious(currentQuery),

      _ => throw new Exception($"Unsupported Operation Code:
  {
    btrieveOperationCode}")
  };
  */
}

} // namespace btrieve