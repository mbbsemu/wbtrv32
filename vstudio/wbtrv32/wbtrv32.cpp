// wbtrv32.cpp : Defines the exported functions for the DLL.
//
#include "wbtrv32.h"

#include <filesystem>

#include "btrieve\BtrieveDriver.h"
#include "btrieve\ErrorCode.h"
#include "btrieve\OperationCode.h"
#include "btrieve\SqliteDatabase.h"
#include "combaseapi.h"
#include "framework.h"

using namespace btrieve;

static std::unordered_map<std::wstring, std::shared_ptr<BtrieveDriver>>
    _openFiles;

static void debug(const char *format, ...) {
  char buf[256];

  va_list args;
  va_start(args, format);
  int len = vsnprintf(buf, sizeof(buf), format, args);
  va_end(args);

  // append cr/lf
  buf[len] = '\r';
  buf[len + 1] = '\n';
  buf[len + 2] = 0;

  OutputDebugStringA(buf);
}

enum BtrieveOpenMode {
  Normal = 0,
  Accelerated = -1,
  ReadOnly = -2,
  VerifyWriteOperations = -3,
  ExclusiveAccess = -4
};

struct BtrieveCommand {
  OperationCode operation;

  LPVOID lpPositionBlock;

  LPVOID lpDataBuffer;
  LPDWORD lpdwDataBufferLength;

  LPVOID lpKeyBuffer;
  unsigned char lpKeyBufferLength;

  char keyNumber;
};

static void AddToOpenFiles(BtrieveCommand &command,
                           std::shared_ptr<BtrieveDriver> driver) {
  // add to my list of open files
  GUID guid;
  CoCreateGuid(&guid);

  WCHAR guidStr[64];
  StringFromGUID2(guid, guidStr, ARRAYSIZE(guidStr));

  _openFiles.emplace(std::make_pair(std::wstring(guidStr), driver));

  // write the GUID in the pos block for other calls
  memset(command.lpPositionBlock, 0, POSBLOCK_LENGTH);
  memcpy(command.lpPositionBlock, &guid, sizeof(UUID));
}

static BtrieveError Open(BtrieveCommand &command) {
  tchar fileName[MAX_PATH];
  tchar fullPathFileName[MAX_PATH];
  size_t unused;

  const char *lpszFilename =
      reinterpret_cast<const char *>(command.lpKeyBuffer);
  auto openMode = static_cast<BtrieveOpenMode>(command.keyNumber);

  debug("Attempting to open %s with openMode %d", lpszFilename, openMode);

  mbstowcs_s(&unused, fileName, ARRAYSIZE(fileName), lpszFilename, _TRUNCATE);

  GetFullPathName(fileName, ARRAYSIZE(fullPathFileName), fullPathFileName,
                  nullptr);

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

  try {
    BtrieveError error = driver->open(fullPathFileName);
    if (error != BtrieveError::Success) {
      return error;
    }

    AddToOpenFiles(command, driver);

    return BtrieveError::Success;
  } catch (const BtrieveException &ex) {
    return BtrieveError::FileNotFound;
  }
}

static BtrieveDriver *getOpenDatabase(LPVOID lpPositioningBlock) {
  WCHAR guidStr[64];
  StringFromGUID2(*reinterpret_cast<GUID *>(lpPositioningBlock), guidStr,
                  ARRAYSIZE(guidStr));

  auto iterator = _openFiles.find(std::wstring(guidStr));
  if (iterator == _openFiles.end()) {
    return nullptr;
  }
  return iterator->second.get();
}

static BtrieveError Close(BtrieveCommand &command) {
  WCHAR guidStr[64];
  StringFromGUID2(*reinterpret_cast<GUID *>(command.lpPositionBlock), guidStr,
                  ARRAYSIZE(guidStr));

  if (!_openFiles.contains(guidStr)) {
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
      lpKeySpec->uniqueKeys =
          btrieveDriver->getRecordCount();  // TODO do I need to calculate this?
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

  // TODO can this be InvalidPositioning, say on an empty file?
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

typedef struct _tagACSCREATEDATA {
  char header;    // should be 0xAC
  char name[8];   // not necessarily null terminated
  char acs[256];  // the table itself
} ACSCREATEDATA, *LPACSCREATEDATA;

static_assert(sizeof(ACSCREATEDATA) == 265);

static BtrieveError Create(BtrieveCommand &command) {
  const char *lpszFileName =
      reinterpret_cast<const char *>(command.lpKeyBuffer);
  tchar fileName[MAX_PATH];
  tchar fullPathFileName[MAX_PATH];
  SqliteDatabase sql;

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(command.lpDataBuffer);
  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  size_t unused;

  mbstowcs_s(&unused, fileName, ARRAYSIZE(fileName), lpszFileName, _TRUNCATE);

  GetFullPathName(fileName, ARRAYSIZE(fullPathFileName), fullPathFileName,
                  nullptr);

  std::filesystem::path dbPath = std::filesystem::path(fullPathFileName)
                                     .replace_extension(sql.getFileExtension());

  if (command.keyNumber == -1) {
    if (GetFileAttributes(fullPathFileName) != 0xFFFFFFFF ||
        GetFileAttributes(dbPath.c_str()) != 0xFFFFFFFF) {
      return BtrieveError::FileAlreadyExists;
    }
  }

  std::vector<Key> keys;
  for (uint16_t i = 0; i < lpFileSpec->numberOfKeys; ++i) {
    char acsName[9];
    std::vector<char> acs;

    if (lpKeySpec->attributes & NumberedACS) {
      LPACSCREATEDATA lpAcsCreateData = reinterpret_cast<LPACSCREATEDATA>(
          reinterpret_cast<uint8_t *>(command.lpDataBuffer) +
          *command.lpdwDataBufferLength - sizeof(ACSCREATEDATA));

      if (lpAcsCreateData->header != 0xAC) {
        return BtrieveError::InvalidACS;
      }

      memcpy(acsName, lpAcsCreateData->name, 8);
      acsName[8] = 0;

      acs.resize(ACS_LENGTH);
      memcpy(acs.data(), lpAcsCreateData->acs, ACS_LENGTH);
    } else {
      acsName[0] = 0;
    }

    // TODO figure out segment stuff later
    KeyDefinition keyDefinition(
        i, static_cast<uint16_t>(lpKeySpec->length),
        static_cast<uint16_t>(lpKeySpec->position) - 1,
        static_cast<KeyDataType>(lpKeySpec->extendedDataType),
        lpKeySpec->attributes, false, 0, 0, lpKeySpec->nullValue, acsName, acs);

    keys.push_back(Key(&keyDefinition, 1));
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

  sql.create(dbPath.c_str(), database);
  return BtrieveError::Success;
}

static BtrieveError handle(BtrieveCommand &command) {
  BtrieveError error;

  switch (command.operation) {
    case OperationCode::Open:
      error = ::Open(command);
      break;
    case OperationCode::Close:
      error = ::Close(command);
      break;
    case OperationCode::Stat:
      error = ::Stat(command);
      break;
    case OperationCode::Delete:
      error = ::Delete(command);
      break;
    case OperationCode::StepFirst:
    case OperationCode::StepFirst + 100:
    case OperationCode::StepFirst + 200:
    case OperationCode::StepFirst + 300:
    case OperationCode::StepFirst + 400:
    case OperationCode::StepLast:
    case OperationCode::StepLast + 100:
    case OperationCode::StepLast + 200:
    case OperationCode::StepLast + 300:
    case OperationCode::StepLast + 400:
    case OperationCode::StepNext:
    case OperationCode::StepNext + 100:
    case OperationCode::StepNext + 200:
    case OperationCode::StepNext + 300:
    case OperationCode::StepNext + 400:
    case OperationCode::StepPrevious:
    case OperationCode::StepPrevious + 100:
    case OperationCode::StepPrevious + 200:
    case OperationCode::StepPrevious + 300:
    case OperationCode::StepPrevious + 400:
      error = ::Step(command);
      break;
    case OperationCode::AcquireFirst:
    case OperationCode::AcquireLast:
    case OperationCode::AcquireNext:
    case OperationCode::AcquirePrevious:
    case OperationCode::AcquireEqual:
    case OperationCode::AcquireGreater:
    case OperationCode::AcquireGreaterOrEqual:
    case OperationCode::AcquireLess:
    case OperationCode::AcquireLessOrEqual:
    case OperationCode::QueryFirst:
    case OperationCode::QueryLast:
    case OperationCode::QueryNext:
    case OperationCode::QueryPrevious:
    case OperationCode::QueryEqual:
    case OperationCode::QueryGreater:
    case OperationCode::QueryGreaterOrEqual:
    case OperationCode::QueryLess:
    case OperationCode::QueryLessOrEqual:
      // TODO don't forget all +100-400 for these queries as well
      error = ::Query(command);
      break;
    case OperationCode::GetPosition:
      error = ::GetPosition(command);
      break;
    case OperationCode::GetDirectChunkOrRecord:
    case OperationCode::GetDirectChunkOrRecord + 100:
    case OperationCode::GetDirectChunkOrRecord + 200:
    case OperationCode::GetDirectChunkOrRecord + 300:
    case OperationCode::GetDirectChunkOrRecord + 400:
      error = ::GetDirectRecord(command);
      break;
    case OperationCode::Update:
      error = ::Upsert(command, [](BtrieveDriver *driver,
                                   std::basic_string_view<uint8_t> record) {
        auto position = driver->getPosition();
        return std::make_pair(driver->updateRecord(position, record), position);
      });
      break;
    case OperationCode::Insert:
      error = ::Upsert(command, [](BtrieveDriver *driver,
                                   std::basic_string_view<uint8_t> record) {
        return driver->insertRecord(record);
      });
      break;
    case OperationCode::Stop:
      error = ::Stop(command);
      break;
    case OperationCode::Create:
      error = ::Create(command);
      break;
    default:
      error = BtrieveError::InvalidOperation;
      break;
  }

  debug("handled %d, returned %d", command.operation, error);

  return error;
}

extern "C" int __stdcall BTRCALL(WORD wOperation, LPVOID lpPositionBlock,
                                 LPVOID lpDataBuffer,
                                 LPDWORD lpdwDataBufferLength,
                                 LPVOID lpKeyBuffer, BYTE bKeyLength,
                                 CHAR sbKeyNumber) {
  // TODO change to initializer
  BtrieveCommand btrieveCommand;
  btrieveCommand.operation = static_cast<OperationCode>(wOperation);
  btrieveCommand.lpPositionBlock = lpPositionBlock;
  btrieveCommand.lpDataBuffer = lpDataBuffer;
  btrieveCommand.lpdwDataBufferLength = lpdwDataBufferLength;
  btrieveCommand.lpKeyBuffer = lpKeyBuffer;
  btrieveCommand.lpKeyBufferLength = bKeyLength;
  btrieveCommand.keyNumber = sbKeyNumber;

  return handle(btrieveCommand);
}
