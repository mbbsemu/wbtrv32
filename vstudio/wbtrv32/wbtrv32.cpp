// wbtrv32.cpp : Defines the exported functions for the DLL.
//
#include "wbtrv32.h"

#include <algorithm>
#include <filesystem>

#include "btrieve/BtrieveDriver.h"
#include "btrieve/ErrorCode.h"
#include "btrieve/OperationCode.h"
#include "btrieve/SqliteDatabase.h"
#include "btrieve/Text.h"
#include "framework.h"

// Define this to pop up a messagebox when wbtrv32.dll is loaded, to allow you
// to attach a debugger on startup
//
// #define DEBUG_ATTACH

// Define this to log errors to wbtrv32.log
#define LOG_TO_FILE

#ifdef DEBUG_ATTACH
#include "Psapi.h"
#endif

using namespace btrieve;

static std::unordered_map<std::basic_string<wchar_t>,
                          std::shared_ptr<BtrieveDriver>>
    _openFiles;

#ifdef LOG_TO_FILE
static std::unique_ptr<FILE, decltype(&fclose)> _logFile(nullptr, &fclose);
#endif

void wbtrv32::processAttach() {
#ifdef DEBUG_ATTACH
  {
    wchar_t buf[MAX_PATH];

    GetProcessImageFileName(GetCurrentProcess(), buf, ARRAYSIZE(buf));
    MessageBox(NULL, TEXT("Attach now"), buf, MB_OK);
  }
#endif

#ifdef LOG_TO_FILE
#ifdef WIN32
  _logFile.reset(_wfsopen(L"wbtrv32.log", L"ab", _SH_DENYWR));
#else
  _logFile.reset(fopen("/tmp/wbtrv32.log", "ab"));
#endif  // WIN32
#endif  // LOG_TO_FILE
}

void wbtrv32::processDetach() {
#ifdef LOG_TO_FILE
  _logFile.reset(nullptr);
#endif
}

static BtrieveDriver *getOpenDatabase(LPVOID lpPositioningBlock) {
  wchar_t guidStr[64];

  if (lpPositioningBlock == nullptr) {
    return nullptr;
  }

  StringFromGUID2(*reinterpret_cast<GUID *>(lpPositioningBlock), guidStr,
                  ARRAYSIZE(guidStr));

  auto iterator = _openFiles.find(std::basic_string<wchar_t>(guidStr));
  if (iterator == _openFiles.end()) {
    return nullptr;
  }
  return iterator->second.get();
}

struct BtrieveCommand {
  OperationCode operation;

  LPVOID lpPositionBlock;

  LPVOID lpDataBuffer;
  LPDWORD lpdwDataBufferLength;

  LPVOID lpKeyBuffer;
  uint8_t lpKeyBufferLength;

  int8_t keyNumber;
};

static void debug(const BtrieveCommand &command, const char *format, ...) {
  BtrieveDriver *driver = getOpenDatabase(command.lpPositionBlock);
  char buf[256];
  int len;

  if (driver != nullptr) {
    len = snprintf(
        buf, sizeof(buf), "[%s]: ",
        btrieve::toStdString(driver->getOpenedFilename().c_str()).c_str());

#ifdef WIN32
    OutputDebugStringA(buf);
#endif

#ifdef LOG_TO_FILE
    if (_logFile) {
      fwrite(buf, 1, len, _logFile.get());
    }
#endif
  }

  va_list args;
  va_start(args, format);
  len = vsnprintf(buf, sizeof(buf) - 2, format, args);
  va_end(args);

  // append cr/lf
  buf[len] = '\r';
  buf[len + 1] = '\n';
  buf[len + 2] = 0;
#ifdef WIN32
  OutputDebugStringA(buf);
#endif

#ifdef LOG_TO_FILE
  if (_logFile) {
    fwrite(buf, 1, len + 2, _logFile.get());
    fflush(_logFile.get());
  }
#endif
}

static void AddToOpenFiles(BtrieveCommand &command,
                           std::shared_ptr<BtrieveDriver> driver) {
  // add to my list of open files
  GUID guid;
  CoCreateGuid(&guid);

  wchar_t guidStr[64];
  StringFromGUID2(guid, guidStr, ARRAYSIZE(guidStr));

  _openFiles.emplace(
      std::make_pair(std::basic_string<wchar_t>(guidStr), driver));

  // write the GUID in the pos block for other calls
  memset(command.lpPositionBlock, 0, POSBLOCK_LENGTH);
  memcpy(command.lpPositionBlock, &guid, sizeof(GUID));
}

static BtrieveError Open(BtrieveCommand &command) {
  wchar_t fullPathFileName[MAX_PATH];

  const char *lpszFilename =
      reinterpret_cast<const char *>(command.lpKeyBuffer);
  auto openMode = static_cast<OpenMode>(command.keyNumber);

  debug(command, "Attempting to open %s with openMode %d", lpszFilename,
        openMode);

  GetFullPathName(btrieve::toWideString(lpszFilename).c_str(),
                  ARRAYSIZE(fullPathFileName), fullPathFileName, nullptr);

  // see if we've already opened this file
  for (auto iterator = _openFiles.begin(); iterator != _openFiles.end();
       ++iterator) {
    if (!_wcsicmp(iterator->second->getOpenedFilename().c_str(),
                  fullPathFileName)) {
      // already got one? let's reuse it
      AddToOpenFiles(command, iterator->second);
      return BtrieveError::Success;
    }
  }

  std::shared_ptr<BtrieveDriver> driver =
      std::make_shared<BtrieveDriver>(new SqliteDatabase());

  BtrieveError error = driver->open(fullPathFileName, openMode);
  if (error != BtrieveError::Success) {
    return error;
  }

  AddToOpenFiles(command, driver);

  return BtrieveError::Success;
}

static BtrieveError Close(BtrieveCommand &command) {
  wchar_t guidStr[64];
  StringFromGUID2(*reinterpret_cast<GUID *>(command.lpPositionBlock), guidStr,
                  ARRAYSIZE(guidStr));

  if (_openFiles.find(guidStr) == _openFiles.end()) {
    return BtrieveError::FileNotOpen;
  }

  _openFiles.erase(guidStr);
  memset(command.lpPositionBlock, 0, POSBLOCK_LENGTH);
  return BtrieveError::Success;
}

static BtrieveError Stat(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return BtrieveError::FileNotOpen;
  }

  const bool includeFileVersion = (command.keyNumber == -1);

  if (command.lpKeyBuffer && command.lpKeyBufferLength > 0) {
    // in a sane world I would zero all this memory out, but wgserver.exe
    // crashes
    *reinterpret_cast<uint8_t *>(command.lpKeyBuffer) = 0;
  }

  size_t totalKeysIncludingSegmentedKeys = 0;
  for (const auto &key : btrieveDriver->getKeys()) {
    totalKeysIncludingSegmentedKeys += key.getSegments().size();
  }

  const unsigned int requiredSize = static_cast<unsigned int>(
      sizeof(wbtrv32::FILESPEC) +
      (totalKeysIncludingSegmentedKeys * sizeof(wbtrv32::KEYSPEC)));
  if (*command.lpdwDataBufferLength < requiredSize) {
    return BtrieveError::DataBufferLengthOverrun;
  }

  // in a sane world I would zero all the memory out in command.lpDataBuffer
  // that was passed to me, but wgserver.exe crashes

  *command.lpdwDataBufferLength = requiredSize;

  const wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(command.lpDataBuffer);
  lpFileSpec->logicalFixedRecordLength = btrieveDriver->getRecordLength();
  lpFileSpec->pageSize = 4096;  // doesn't matter, not needed for sqlite
  lpFileSpec->numberOfKeys = static_cast<uint8_t>(
      btrieveDriver->getKeys()
          .size());  // note: this is not totalKeysIncludingSegmentedKeys
  lpFileSpec->fileVersion =
      includeFileVersion ? 0x60 : 0;  // emulate btrieve 6.0

  lpFileSpec->recordCount = btrieveDriver->getRecordCount();
  lpFileSpec->fileFlags = btrieveDriver->isVariableLengthRecords() ? 1 : 0;
  lpFileSpec->numExtraPointers = 0;
  lpFileSpec->physicalPageSize =
      0;  // only set for compressed, which we don't do
  lpFileSpec->preallocatedPages = 0;

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  uint8_t keyNumber = 0;
  for (const auto &key : btrieveDriver->getKeys()) {
    for (const auto &segment : key.getSegments()) {
      lpKeySpec->position = segment.getPosition();
      lpKeySpec->length = segment.getLength();
      lpKeySpec->attributes = segment.getAttributes();
      lpKeySpec->uniqueKeys = btrieveDriver->getRecordCount();
      lpKeySpec->extendedDataType = segment.getDataType();
      lpKeySpec->nullValue = segment.getNullValue();
      lpKeySpec->reserved = 0;
      lpKeySpec->number = keyNumber;
      lpKeySpec->acsNumber = 0;
      ++lpKeySpec;
    }

    ++keyNumber;
  }

  return BtrieveError::Success;
}

static BtrieveError Delete(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return BtrieveError::FileNotOpen;
  }

  return btrieveDriver->performOperation(-1, std::basic_string_view<uint8_t>(),
                                         OperationCode::Delete);
}

static BtrieveError Step(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return BtrieveError::FileNotOpen;
  }

  auto oldPosition = btrieveDriver->getPosition();
  auto result = btrieveDriver->performOperation(
      -1, std::basic_string_view<uint8_t>(), command.operation);
  if (result != BtrieveError::Success) {
    return result;
  }

  auto record = btrieveDriver->getRecord();
  if (!record.first) {
    btrieveDriver->setPosition(oldPosition);
    return BtrieveError::IOError;
  }

  if (*command.lpdwDataBufferLength < record.second.getData().size()) {
    btrieveDriver->setPosition(oldPosition);
    return BtrieveError::DataBufferLengthOverrun;
  }

  *command.lpdwDataBufferLength =
      static_cast<DWORD>(record.second.getData().size());
  memcpy(command.lpDataBuffer, record.second.getData().data(),
         record.second.getData().size());

  return BtrieveError::Success;
}

static BtrieveError GetPosition(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return BtrieveError::FileNotOpen;
  }

  if (*command.lpdwDataBufferLength < sizeof(uint32_t)) {
    return BtrieveError::DataBufferLengthOverrun;
  }

  *command.lpdwDataBufferLength = sizeof(uint32_t);
  *reinterpret_cast<uint32_t *>(command.lpDataBuffer) =
      btrieveDriver->getPosition();

  return BtrieveError::Success;
}

static BtrieveError GetDirectRecord(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return BtrieveError::FileNotOpen;
  }

  if (*command.lpdwDataBufferLength < 4) {
    return BtrieveError::DataBufferLengthOverrun;
  }

  uint32_t position = *reinterpret_cast<uint32_t *>(command.lpDataBuffer);

  auto record = btrieveDriver->getRecord(position);
  if (!record.first) {
    return BtrieveError::InvalidRecordAddress;
  }

  if (*command.lpdwDataBufferLength < record.second.getData().size()) {
    return BtrieveError::DataBufferLengthOverrun;
  }

  if (command.keyNumber >= 0) {
    if (static_cast<uint32_t>(command.keyNumber) >=
        btrieveDriver->getKeys().size()) {
      return BtrieveError::InvalidKeyNumber;
    }

    const auto &key = btrieveDriver->getKeys().at(command.keyNumber);
    if (command.lpKeyBufferLength < key.getLength()) {
      return BtrieveError::KeyBufferTooShort;
    }

    auto error =
        btrieveDriver->logicalCurrencySeek(command.keyNumber, position);
    if (error != BtrieveError::Success) {
      return error;
    }

    auto keyBytes =
        btrieveDriver->getKeys()
            .at(command.keyNumber)
            .extractKeyDataFromRecord(std::basic_string_view<uint8_t>(
                record.second.getData().data(),
                record.second.getData().size()));

    memcpy(command.lpKeyBuffer, keyBytes.data(), keyBytes.size());
  }

  memcpy(command.lpDataBuffer, record.second.getData().data(),
         record.second.getData().size());
  return BtrieveError::Success;
}

static BtrieveError Query(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return BtrieveError::FileNotOpen;
  }

  std::basic_string_view<uint8_t> keyData;

  if (requiresKey(command.operation)) {
    if (static_cast<uint32_t>(command.keyNumber) >=
        btrieveDriver->getKeys().size()) {
      return BtrieveError::InvalidKeyNumber;
    } else if (command.lpKeyBufferLength <
               btrieveDriver->getKeys().at(command.keyNumber).getLength()) {
      return BtrieveError::KeyBufferTooShort;
    }

    keyData = std::basic_string_view<uint8_t>(
        reinterpret_cast<uint8_t *>(command.lpKeyBuffer),
        command.lpKeyBufferLength);
  }

  BtrieveError error = btrieveDriver->performOperation(
      command.keyNumber, keyData, command.operation);

  if (error != BtrieveError::Success) {
    return error;
  }

  auto record = btrieveDriver->getRecord();
  if (!record.first) {
    return requiresKey(command.operation) ? BtrieveError::KeyValueNotFound
                                          : BtrieveError::EndOfFile;
  }

  // always copy the key back to the client
  auto keyDataVector =
      btrieveDriver->getKeys()
          .at(command.keyNumber)
          .extractKeyDataFromRecord(std::basic_string_view<uint8_t>(
              record.second.getData().data(), record.second.getData().size()));

  memcpy(command.lpKeyBuffer, keyDataVector.data(), keyDataVector.size());

  if (acquiresData(command.operation)) {
    if (*command.lpdwDataBufferLength < record.second.getData().size()) {
      return BtrieveError::DataBufferLengthOverrun;
    }

    memcpy(command.lpDataBuffer, record.second.getData().data(),
           record.second.getData().size());
  }

  return BtrieveError::Success;
}

static BtrieveError Upsert(
    BtrieveCommand &command,
    std::function<std::pair<BtrieveError, unsigned int>(
        BtrieveDriver *, std::basic_string_view<uint8_t> record)>
        upsertFunction) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return BtrieveError::FileNotOpen;
  }

  if (command.keyNumber >= 0) {
    if (static_cast<uint32_t>(command.keyNumber) >=
        btrieveDriver->getKeys().size()) {
      return BtrieveError::InvalidKeyNumber;
    } else if (command.lpKeyBufferLength <
               btrieveDriver->getKeys().at(command.keyNumber).getLength()) {
      return BtrieveError::KeyBufferTooShort;
    }
  }

  auto record = std::basic_string_view<uint8_t>(
      reinterpret_cast<uint8_t *>(command.lpDataBuffer),
      *command.lpdwDataBufferLength);

  auto insertedPosition = upsertFunction(btrieveDriver, record);

  if (insertedPosition.first != BtrieveError::Success) {
    return insertedPosition.first;
  }

  if (command.keyNumber < 0) {
    return BtrieveError::Success;
  }

  auto ret = btrieveDriver->logicalCurrencySeek(command.keyNumber,
                                                insertedPosition.second);

  if (ret == BtrieveError::Success) {
    // copy the key back to the client
    auto keyDataVector = btrieveDriver->getKeys()
                             .at(command.keyNumber)
                             .extractKeyDataFromRecord(record);

    memcpy(command.lpKeyBuffer, keyDataVector.data(), keyDataVector.size());
  }
  return ret;
}

static BtrieveError Stop(const BtrieveCommand &command) {
  _openFiles.clear();

  return BtrieveError::Success;
}

static BtrieveError Create(BtrieveCommand &command) {
  const char *lpszFileName =
      reinterpret_cast<const char *>(command.lpKeyBuffer);
  wchar_t fullPathFileName[MAX_PATH];
  SqliteDatabase sql;

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(command.lpDataBuffer);
  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);

  GetFullPathName(toWideString(lpszFileName).c_str(),
                  ARRAYSIZE(fullPathFileName), fullPathFileName, nullptr);

  std::filesystem::path dbPath = std::filesystem::path(fullPathFileName)
                                     .replace_extension(sql.getFileExtension());

  if (command.keyNumber == -1) {
    if (FileExists(fullPathFileName) ||
        FileExists(toWideString(dbPath).c_str())) {
      return BtrieveError::FileAlreadyExists;
    }
  }

  std::vector<wbtrv32::LPACSCREATEDATA> clientProvidedAcs;
  uint8_t numberOfAcs = 0;
  // find and categorize all the key data before populating everything
  for (uint16_t i = 0; i < lpFileSpec->numberOfKeys; ++i, ++lpKeySpec) {
  check_acs:
    if (lpKeySpec->attributes & NumberedACS) {
      numberOfAcs =
          std::max(numberOfAcs, static_cast<uint8_t>(lpKeySpec->acsNumber + 1));
    }

    if (lpKeySpec->attributes & SegmentedKey) {
      ++lpKeySpec;
      goto check_acs;
    }
  }
  // at this point we are pointing at the ACS data, so store it now
  wbtrv32::LPACSCREATEDATA lpAcsCreateData =
      reinterpret_cast<wbtrv32::LPACSCREATEDATA>(lpKeySpec);
  while (numberOfAcs--) {
    if (lpAcsCreateData->header != 0xAC) {
      return BtrieveError::InvalidACS;
    }

    clientProvidedAcs.push_back(lpAcsCreateData++);
  }

  lpKeySpec = reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  std::vector<Key> keys;
  for (uint16_t i = 0; i < lpFileSpec->numberOfKeys; ++i, ++lpKeySpec) {
    char acsName[9];
    std::vector<char> acs;
    std::vector<KeyDefinition> keyDefinitions;

  process_key:
    if (lpKeySpec->attributes & NumberedACS) {
      wbtrv32::LPACSCREATEDATA lpAcsCreateData =
          clientProvidedAcs[lpKeySpec->acsNumber];

      memcpy(acsName, lpAcsCreateData->name, 8);
      acsName[8] = 0;

      acs.resize(ACS_LENGTH);
      memcpy(acs.data(), lpAcsCreateData->acs, ACS_LENGTH);
    } else {
      acsName[0] = 0;
    }

    KeyDefinition keyDefinition(
        i, static_cast<uint16_t>(lpKeySpec->length),
        static_cast<uint16_t>(lpKeySpec->position) - 1,
        static_cast<KeyDataType>(lpKeySpec->extendedDataType),
        lpKeySpec->attributes, lpKeySpec->attributes & SegmentedKey,
        lpKeySpec->attributes & SegmentedKey ? i : 0, 0, lpKeySpec->nullValue,
        acsName, acs);

    keyDefinitions.push_back(keyDefinition);

    if (lpKeySpec->attributes & SegmentedKey) {
      ++lpKeySpec;
      goto process_key;
    }

    keys.push_back(Key(keyDefinitions.data(), keyDefinitions.size()));
  }

  RecordType recordType = RecordType::Fixed;
  if (lpFileSpec->fileFlags & 2) {
    recordType = RecordType::VariableTruncated;
  } else if (lpFileSpec->fileFlags & 1) {
    recordType = RecordType::Variable;
  }

  BtrieveDatabase database(keys, static_cast<uint16_t>(lpFileSpec->pageSize), 0,
                           lpFileSpec->logicalFixedRecordLength,
                           lpFileSpec->logicalFixedRecordLength, 0, 0,
                           recordType, true, 0);

  sql.create(toWideString(dbPath).c_str(), database);
  return BtrieveError::Success;
}

#define CASE_WITH_RECORD_LOCK(a)   \
  case a:                          \
  case a##_SingleWaitRecordLock:   \
  case a##_SingleNoWaitRecordLock: \
  case a##_MultipleWaitRecordLock: \
  case a##_MultipleNoWaitRecordLock

// clang-format off
static BtrieveError handle(BtrieveCommand &command) {
  switch (command.operation) {
    case OperationCode::Open:
      return ::Open(command);
    case OperationCode::Close:
      return ::Close(command);
    case OperationCode::Stat:
      return ::Stat(command);
    case OperationCode::Delete:
      return ::Delete(command);
    CASE_WITH_RECORD_LOCK(OperationCode::StepFirst):
    CASE_WITH_RECORD_LOCK(OperationCode::StepLast):
    CASE_WITH_RECORD_LOCK(OperationCode::StepNext):
    CASE_WITH_RECORD_LOCK(OperationCode::StepPrevious):
      return ::Step(command);
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireFirst):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireLast):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireNext):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquirePrevious):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireGreater):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireGreaterOrEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireLess):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireLessOrEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryFirst):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryLast):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryNext):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryPrevious):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryGreater):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryGreaterOrEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryLess):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryLessOrEqual):
      return ::Query(command);
    case OperationCode::GetPosition:
      return ::GetPosition(command);
    CASE_WITH_RECORD_LOCK(OperationCode::GetDirectChunkOrRecord):
      return ::GetDirectRecord(command);
    case OperationCode::Update:
      return ::Upsert(command, [](BtrieveDriver *driver,
                                  std::basic_string_view<uint8_t> record) {
        auto position = driver->getPosition();
        return std::make_pair(driver->updateRecord(position, record), position);
      });
    case OperationCode::Insert:
      return ::Upsert(command, [](BtrieveDriver *driver,
                                  std::basic_string_view<uint8_t> record) {
        return driver->insertRecord(record);
      });
    case OperationCode::Stop:
      return ::Stop(command);
    case OperationCode::Create:
      return ::Create(command);
    default:
      return BtrieveError::InvalidOperation;
  }
}
// clang-format on

extern "C" int __stdcall BTRCALL(WORD wOperation, LPVOID lpPositionBlock,
                                 LPVOID lpDataBuffer,
                                 LPDWORD lpdwDataBufferLength,
                                 LPVOID lpKeyBuffer, BYTE bKeyLength,
                                 CHAR sbKeyNumber) {
  BtrieveCommand btrieveCommand;
  BtrieveError error;

  btrieveCommand.operation = static_cast<OperationCode>(wOperation);
  btrieveCommand.lpPositionBlock = lpPositionBlock;
  btrieveCommand.lpDataBuffer = lpDataBuffer;
  btrieveCommand.lpdwDataBufferLength = lpdwDataBufferLength;
  btrieveCommand.lpKeyBuffer = lpKeyBuffer;
  btrieveCommand.lpKeyBufferLength = bKeyLength;
  btrieveCommand.keyNumber = sbKeyNumber;

  try {
    error = handle(btrieveCommand);
  } catch (const BtrieveException &ex) {
    // make sure we don't leak the exception back to our caller
    error = ex.getError();
  }

  if (error != BtrieveError::Success) {
    debug(btrieveCommand, "handled %s [key %d], returned %s",
          toString(btrieveCommand.operation), btrieveCommand.keyNumber,
          errorToString(error));
  }

  return error;
}
