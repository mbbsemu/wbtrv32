#include "BtrieveDriver.h"

#include <sys/stat.h>
#include <sys/types.h>

#include <ctime>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <vector>

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

static inline bool fileExists(const wchar_t *filename) {
#ifdef WIN32
  return GetFileAttributes(filename) != INVALID_FILE_ATTRIBUTES;
#else
  struct stat stbuf;
  return stat(toStdString(filename).c_str(), &stbuf) == 0;
#endif
}

static std::basic_string<wchar_t> toUpper(
    const std::basic_string<wchar_t> &value) {
  std::basic_string<wchar_t> ret(value);
  for (size_t i = 0; i < ret.size(); ++i) {
    ret[i] = toupper(ret[i]);
  }
  return ret;
}

// Size and last write time of path, plus a hash of its contents when withHash.
static std::optional<SourceFingerprint> fingerprint(
    const std::filesystem::path &path, bool withHash) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  if (ec) {
    return std::nullopt;
  }
  const auto modified = std::filesystem::last_write_time(path, ec);
  if (ec) {
    return std::nullopt;
  }

  SourceFingerprint ret{
      static_cast<int64_t>(size),
      static_cast<int64_t>(modified.time_since_epoch().count()), 0};
  if (withHash) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      return std::nullopt;
    }
    // FNV-1a
    uint64_t hash = 0xcbf29ce484222325ull;
    std::vector<char> buffer(64 * 1024);
    while (file) {
      file.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
      const size_t read = static_cast<size_t>(file.gcount());
      for (size_t i = 0; i < read; ++i) {
        hash ^= static_cast<uint8_t>(buffer[i]);
        hash *= 0x100000001b3ull;
      }
    }
    if (!file.eof()) {
      return std::nullopt;
    }
    ret.hash = static_cast<int64_t>(hash);
  }
  return ret;
}

static std::string formatLocalTime(const char *format) {
  const time_t now = time(nullptr);
  struct tm local;
#ifdef _WIN32
  localtime_s(&local, &now);
#else
  localtime_r(&now, &local);
#endif
  char buf[32];
  strftime(buf, sizeof(buf), format, &local);
  return buf;
}

#ifdef _WIN32
static const wchar_t kArchiveFolder[] = _TEXT("CONVERTED DATABASES");
#else
static const wchar_t kArchiveFolder[] = _TEXT(".converted");
#endif

std::filesystem::path archiveFolder(const std::filesystem::path &datPath) {
  return datPath.parent_path() / kArchiveFolder;
}

// Appends a line to "CONVERTED DATABASES\CONVERSIONS.LOG" beside the .DAT, so
// the sysop can see what was converted, when, and where old copies went.
static void logConversion(const std::filesystem::path &datPath,
                          const std::string &line) {
  std::error_code ec;
  const std::filesystem::path folder = archiveFolder(datPath);
  std::filesystem::create_directories(folder, ec);
  std::ofstream log(folder / "CONVERSIONS.LOG", std::ios::app);
  log << formatLocalTime("%Y-%m-%d %H:%M:%S") << "  " << line << "\n";
}

// Moves the WAL files that belong to database from beside it to beside to.
static void moveWalFiles(const std::filesystem::path &database,
                         const std::filesystem::path &to) {
  for (const char *suffix : {"-wal", "-shm"}) {
    std::filesystem::path from = database;
    from += suffix;
    std::filesystem::path dest = to;
    dest += suffix;
    std::error_code ec;
    if (std::filesystem::exists(from, ec)) {
      std::filesystem::rename(from, dest, ec);
    }
  }
}

// Moves dbPath (with any WAL files) into a new dated folder under
// "CONVERTED DATABASES" beside the .DAT. Returns where it went, or an empty
// path if it could not be moved (e.g. another process has it open), in which
// case it is left untouched.
static std::filesystem::path retireDb(const std::filesystem::path &datPath,
                                      const std::filesystem::path &dbPath) {
  const std::filesystem::path folder =
      archiveFolder(datPath) / formatLocalTime("%Y%m%d-%H%M%S");
  std::error_code ec;
  std::filesystem::create_directories(folder, ec);
  if (ec) {
    return std::filesystem::path();
  }

  std::filesystem::path target = folder / dbPath.filename();
  for (int i = 2; std::filesystem::exists(target, ec); ++i) {
    target = folder / (dbPath.stem().wstring() + _TEXT("-") +
                       std::to_wstring(i) + dbPath.extension().wstring());
  }

  std::filesystem::rename(dbPath, target, ec);
  if (ec) {
    return std::filesystem::path();
  }
  moveWalFiles(dbPath, target);
  return target;
}

static std::string displayName(const std::filesystem::path &path) {
  return toStdString(path.filename());
}

static std::string statusText(BtrieveError error) {
  return "status " + std::to_string(static_cast<int>(error));
}

std::filesystem::path retireDatabase(const std::filesystem::path &datPath,
                                     const std::filesystem::path &dbPath,
                                     const std::string &reason) {
  const std::filesystem::path oldDb = retireDb(datPath, dbPath);
  if (oldDb.empty()) {
    logConversion(datPath, reason + ", but " + displayName(dbPath) +
                               " could not be moved aside");
  } else {
    logConversion(datPath, reason + "; previous " + displayName(dbPath) +
                               " moved to " +
                               toStdString(oldDb.parent_path().filename()) +
                               "\\" + displayName(oldDb));
  }
  return oldDb;
}

void writeDatPlaceholder(const std::filesystem::path &datPath,
                         const std::filesystem::path &dbPath) {
  std::ofstream placeholder(datPath, std::ios::binary);
  placeholder << "This is not a Btrieve file. WBTRV32.DLL keeps the data for "
              << displayName(datPath) << " in " << displayName(dbPath)
              << ".\r\nLeave it in place: programs look for it to tell "
                 "whether the file exists.\r\n";
}

std::optional<SourceFingerprint> sourceFingerprint(
    const std::filesystem::path &datPath) {
  return fingerprint(datPath, true);
}

BtrieveError BtrieveDriver::open(const wchar_t *fileName, OpenMode openMode) {
  std::filesystem::path dbPath =
      std::filesystem::path(fileName).replace_extension(
          sqlDatabase->getFileExtension());

  openedFilename = fileName;

  bool dbExists = fileExists(toWideString(dbPath).c_str());
  if (!dbExists) {
    // failed to find db, let's uppercase and try again
    std::filesystem::path dbPathUpper = dbPath;
    dbPathUpper.replace_extension(toUpper(sqlDatabase->getFileExtension()));
    dbExists = fileExists(toWideString(dbPathUpper).c_str());
    if (dbExists) {
      dbPath = dbPathUpper;
    }
  }

  // Once a .DB exists it is the live copy of the data. The .DAT is only read
  // to build the .DB; it is never written, so its timestamp says nothing about
  // which copy is current. (A touched .DAT used to trigger a reconversion that
  // silently discarded everything written to the .DB.) What does mean
  // something is a .DAT whose contents differ from the one the .DB was
  // converted from: a module upgrade or a restore put it there, and under
  // stock Btrieve it would have replaced the data. That one is converted,
  // after the old .DB is moved aside into "CONVERTED DATABASES" so nothing is
  // lost. To rebuild from an unchanged .DAT, delete the .DB.

  BtrieveError error;

  if (!dbExists && !fileExists(fileName)) {
    // neither exists; nothing to convert or log
    error = convert(fileName, dbPath, openMode);
  } else if (!dbExists) {
    error = convert(fileName, dbPath, openMode);
    logConversion(
        fileName,
        error == BtrieveError::Success
            ? displayName(fileName) + " converted to " + displayName(dbPath) +
                  " (" + std::to_string(sqlDatabase->getRecordCount()) +
                  " records)"
            : displayName(fileName) + " could not be converted (" +
                  statusText(error) + ")");
  } else {
    error = sqlDatabase->open(toWideString(dbPath).c_str(), openMode);

    // only a .DAT can be a source; a .DB opened by its own name has none
    const bool hasSource =
        toUpper(std::filesystem::path(fileName).extension().wstring()) !=
            toUpper(dbPath.extension().wstring()) &&
        fileExists(fileName);
    if (error == BtrieveError::Success && trackSourceFile && hasSource) {
      error = refreshFromChangedSource(fileName, dbPath, openMode);
    }
  }

  if (error == BtrieveError::Success) {
    // No record is current after Open, and the physical next record is the
    // first one, so Step Next returns it. Update, Delete and Get Position
    // fail until a record is read.
    sqlDatabase->setPosition(0);
    sqlDatabase->clearCurrentRecord();
  }

  return error;
}

// Called with the existing .DB open; leaves open the database to use.
BtrieveError BtrieveDriver::refreshFromChangedSource(
    const wchar_t *fileName, const std::filesystem::path &dbPath,
    OpenMode openMode) {
  const bool writable = openMode != OpenMode::ReadOnly;
  const std::optional<SourceFingerprint> recorded =
      sqlDatabase->getSourceFingerprint();

  if (!recorded) {
    // converted before fingerprints were kept: adopt the .DAT as it is now
    if (writable) {
      if (auto current = fingerprint(fileName, true)) {
        sqlDatabase->setSourceFingerprint(*current);
      }
    }
    return BtrieveError::Success;
  }

  std::optional<SourceFingerprint> current = fingerprint(fileName, false);
  if (!current || (current->size == recorded->size &&
                   current->modified == recorded->modified)) {
    return BtrieveError::Success;
  }

  current = fingerprint(fileName, true);
  if (!current) {
    return BtrieveError::Success;
  }
  if (current->size == recorded->size && current->hash == recorded->hash) {
    // same contents, only touched or copied
    if (writable) {
      sqlDatabase->setSourceFingerprint(*current);
    }
    return BtrieveError::Success;
  }

  // a different .DAT
  sqlDatabase->close();
  const std::filesystem::path oldDb = retireDb(fileName, dbPath);
  if (oldDb.empty()) {
    logConversion(fileName, displayName(fileName) + " has changed, but " +
                                displayName(dbPath) +
                                " could not be moved aside; still using it");
    return sqlDatabase->open(toWideString(dbPath).c_str(), openMode);
  }

  BtrieveError error;
  try {
    error = convert(fileName, dbPath, openMode);
  } catch (...) {
    error = BtrieveError::IOError;
  }

  const std::string moved = "previous " + displayName(dbPath) + " moved to " +
                            toStdString(oldDb.parent_path().filename()) +
                            "\\" + displayName(oldDb);
  if (error == BtrieveError::Success) {
    logConversion(fileName,
                  displayName(fileName) + " has changed and was converted (" +
                      std::to_string(sqlDatabase->getRecordCount()) +
                      " records); " + moved);
    return error;
  }

  // the new .DAT is unusable: put the old .DB back
  sqlDatabase->close();
  std::error_code ec;
  std::filesystem::remove(dbPath, ec);
  for (const char *suffix : {"-wal", "-shm"}) {
    std::filesystem::path walFile = dbPath;
    walFile += suffix;
    std::filesystem::remove(walFile, ec);
  }
  std::filesystem::rename(oldDb, dbPath, ec);
  moveWalFiles(oldDb, dbPath);
  logConversion(fileName, displayName(fileName) +
                              " has changed but could not be converted (" +
                              statusText(error) + "); kept the previous " +
                              displayName(dbPath));
  return sqlDatabase->open(toWideString(dbPath).c_str(), openMode);
}

BtrieveError BtrieveDriver::convert(const wchar_t *fileName,
                                    const std::filesystem::path &dbPath,
                                    OpenMode openMode) {
  BtrieveDatabase btrieveDatabase;
  std::unique_ptr<RecordLoader> recordLoader;
  BtrieveError error = btrieveDatabase.parseDatabase(
      fileName,
      [this, &dbPath, &btrieveDatabase, &recordLoader]() {
        recordLoader =
            sqlDatabase->create(toWideString(dbPath).c_str(), btrieveDatabase);
        return true;
      },
      [&recordLoader](const std::basic_string_view<uint8_t> record) {
        return recordLoader->onRecordLoaded(record);
      },
      [&recordLoader]() { recordLoader->onRecordsComplete(); });

  if (error == BtrieveError::Success && trackSourceFile) {
    if (auto source = fingerprint(fileName, true)) {
      sqlDatabase->setSourceFingerprint(*source);
    }
  }

  // if newly created and they want read-only, close and reopen
  if (openMode == OpenMode::ReadOnly) {
    sqlDatabase->close();
    error = sqlDatabase->open(toWideString(dbPath).c_str(), openMode);
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
  // record locks are not supported, so a lock bias (+100..+400) is ignored
  if (operationCode >= 100 && operationCode < 500) {
    operationCode = static_cast<OperationCode>(operationCode % 100);
  }

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
      // Step First and Step Last destroy logical currency
      previousQuery.reset();
      return sqlDatabase->stepFirst();
    case OperationCode::StepLast:
    case OperationCode::StepLast + 100:
    case OperationCode::StepLast + 200:
    case OperationCode::StepLast + 300:
    case OperationCode::StepLast + 400:
      previousQuery.reset();
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

    // possibly truncate the length of keyData, since some clients just specify
    // the max of 255 when the key is much smaller
    int exceededSize = keyData.size() - key->getLength();
    if (exceededSize > 0) {
      keyData.remove_suffix(exceededSize);
    }

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
