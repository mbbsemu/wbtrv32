// wbtrv32.cpp : Defines the exported functions for the DLL.
//
#include "wbtrv32.h"

#include "btrieve\BtrieveDriver.h"
#include "btrieve\ErrorCode.h"
#include "btrieve\OperationCode.h"
#include "btrieve\SqliteDatabase.h"
#include "combaseapi.h"
#include "framework.h"

static std::unordered_map<std::wstring, std::shared_ptr<btrieve::BtrieveDriver>>
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
  btrieve::OperationCode operation;

  LPVOID lpPositionBlock;

  LPVOID lpDataBuffer;
  LPDWORD lpdwDataBufferLength;

  LPVOID lpKeyBuffer;
  unsigned char lpKeyBufferLength;

  char keyNumber;
};

static void AddToOpenFiles(BtrieveCommand &command,
                           std::shared_ptr<btrieve::BtrieveDriver> driver) {
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

static btrieve::BtrieveError Open(BtrieveCommand &command) {
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
      return btrieve::BtrieveError::Success;
    }
  }

  std::shared_ptr<btrieve::BtrieveDriver> driver =
      std::make_shared<btrieve::BtrieveDriver>(new btrieve::SqliteDatabase());

  try {
    btrieve::BtrieveError error = driver->open(fullPathFileName);
    if (error != btrieve::BtrieveError::Success) {
      return error;
    }

    AddToOpenFiles(command, driver);

    return btrieve::BtrieveError::Success;
  } catch (const btrieve::BtrieveException &ex) {
    return btrieve::BtrieveError::FileNotFound;
  }
}

static btrieve::BtrieveDriver *getOpenDatabase(LPVOID lpPositioningBlock) {
  WCHAR guidStr[64];
  StringFromGUID2(*reinterpret_cast<GUID *>(lpPositioningBlock), guidStr,
                  ARRAYSIZE(guidStr));

  auto iterator = _openFiles.find(std::wstring(guidStr));
  if (iterator == _openFiles.end()) {
    return nullptr;
  }
  return iterator->second.get();
}

static btrieve::BtrieveError Close(BtrieveCommand &command) {
  WCHAR guidStr[64];
  StringFromGUID2(*reinterpret_cast<GUID *>(command.lpPositionBlock), guidStr,
                  ARRAYSIZE(guidStr));

  if (!_openFiles.contains(guidStr)) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  _openFiles.erase(guidStr);
  memset(command.lpPositionBlock, 0, POSBLOCK_LENGTH);
  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError Stat(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
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
    return btrieve::BtrieveError::DataBufferLengthOverrun;
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

  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError Delete(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  return btrieveDriver->performOperation(-1, std::basic_string_view<uint8_t>(),
                                         btrieve::OperationCode::Delete);
}

static btrieve::BtrieveError Step(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  auto oldPosition = btrieveDriver->getPosition();
  auto result = btrieveDriver->performOperation(
      -1, std::basic_string_view<uint8_t>(), command.operation);
  if (result != btrieve::BtrieveError::Success) {
    return result;
  }

  auto record = btrieveDriver->getRecord();
  if (!record.first) {
    btrieveDriver->setPosition(oldPosition);
    return btrieve::BtrieveError::IOError;
  }

  if (*command.lpdwDataBufferLength < record.second.getData().size()) {
    btrieveDriver->setPosition(oldPosition);
    return btrieve::BtrieveError::DataBufferLengthOverrun;
  }

  *command.lpdwDataBufferLength =
      static_cast<DWORD>(record.second.getData().size());
  memcpy(command.lpDataBuffer, record.second.getData().data(),
         record.second.getData().size());

  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError GetPosition(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  if (*command.lpdwDataBufferLength < sizeof(uint32_t)) {
    return btrieve::BtrieveError::DataBufferLengthOverrun;
  }

  *command.lpdwDataBufferLength = sizeof(uint32_t);
  *reinterpret_cast<uint32_t *>(command.lpDataBuffer) =
      btrieveDriver->getPosition();

  // TODO can this be InvalidPositioning, say on an empty file?
  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError GetDirectRecord(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  if (*command.lpdwDataBufferLength < 4) {
    return btrieve::BtrieveError::DataBufferLengthOverrun;
  }

  uint32_t position = *reinterpret_cast<uint32_t *>(command.lpDataBuffer);

  auto record = btrieveDriver->getRecord(position);
  if (!record.first) {
    return btrieve::BtrieveError::InvalidRecordAddress;
  }

  if (*command.lpdwDataBufferLength < record.second.getData().size()) {
    return btrieve::BtrieveError::DataBufferLengthOverrun;
  }

  if (command.keyNumber >= 0) {
    if (static_cast<uint32_t>(command.keyNumber) >=
        btrieveDriver->getKeys().size()) {
      return btrieve::BtrieveError::InvalidKeyNumber;
    }

    const auto &key = btrieveDriver->getKeys().at(command.keyNumber);
    if (command.lpKeyBufferLength < key.getLength()) {
      return btrieve::BtrieveError::KeyBufferTooShort;
    }

    auto error =
        btrieveDriver->logicalCurrencySeek(command.keyNumber, position);
    if (error != btrieve::BtrieveError::Success) {
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
  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError Query(BtrieveCommand &command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  std::basic_string_view<uint8_t> keyData;

  if (btrieve::requiresKey(command.operation)) {
    if (static_cast<uint32_t>(command.keyNumber) >=
        btrieveDriver->getKeys().size()) {
      return btrieve::BtrieveError::InvalidKeyNumber;
    } else if (command.lpKeyBufferLength <
               btrieveDriver->getKeys().at(command.keyNumber).getLength()) {
      return btrieve::BtrieveError::KeyBufferTooShort;
    }

    keyData = std::basic_string_view<uint8_t>(
        reinterpret_cast<uint8_t *>(command.lpKeyBuffer),
        command.lpKeyBufferLength);
  }

  btrieve::BtrieveError error = btrieveDriver->performOperation(
      command.keyNumber, keyData, command.operation);

  if (error != btrieve::BtrieveError::Success) {
    return error;
  }

  auto record = btrieveDriver->getRecord();
  if (!record.first) {
    return btrieve::requiresKey(command.operation)
               ? btrieve::BtrieveError::KeyValueNotFound
               : btrieve::BtrieveError::EndOfFile;
  }

  // always copy the key back to the client
  auto keyDataVector =
      btrieveDriver->getKeys()
          .at(command.keyNumber)
          .extractKeyDataFromRecord(std::basic_string_view<uint8_t>(
              record.second.getData().data(), record.second.getData().size()));

  memcpy(command.lpKeyBuffer, keyDataVector.data(), keyDataVector.size());

  if (btrieve::acquiresData(command.operation)) {
    if (*command.lpdwDataBufferLength < record.second.getData().size()) {
      return btrieve::BtrieveError::DataBufferLengthOverrun;
    }

    memcpy(command.lpDataBuffer, record.second.getData().data(),
           record.second.getData().size());
  }

  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError Upsert(
    BtrieveCommand &command,
    std::function<std::pair<btrieve::BtrieveError, unsigned int>(
        btrieve::BtrieveDriver *, std::basic_string_view<uint8_t> record)>
        upsertFunction) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  if (command.keyNumber >= 0) {
    if (static_cast<uint32_t>(command.keyNumber) >=
        btrieveDriver->getKeys().size()) {
      return btrieve::BtrieveError::InvalidKeyNumber;
    } else if (command.lpKeyBufferLength <
               btrieveDriver->getKeys().at(command.keyNumber).getLength()) {
      return btrieve::BtrieveError::KeyBufferTooShort;
    }
  }

  auto record = std::basic_string_view<uint8_t>(
      reinterpret_cast<uint8_t *>(command.lpDataBuffer),
      *command.lpdwDataBufferLength);

  auto insertedPosition = upsertFunction(btrieveDriver, record);

  if (insertedPosition.first != btrieve::BtrieveError::Success) {
    return insertedPosition.first;
  }

  if (command.keyNumber < 0) {
    return btrieve::BtrieveError::Success;
  }

  auto ret = btrieveDriver->logicalCurrencySeek(command.keyNumber,
                                                insertedPosition.second);

  if (ret == btrieve::BtrieveError::Success) {
    // copy the key back to the client
    auto keyDataVector = btrieveDriver->getKeys()
                             .at(command.keyNumber)
                             .extractKeyDataFromRecord(record);

    memcpy(command.lpKeyBuffer, keyDataVector.data(), keyDataVector.size());
  }
  return ret;
}

static btrieve::BtrieveError Stop(const BtrieveCommand &command) {
  _openFiles.clear();

  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError handle(BtrieveCommand &command) {
  btrieve::BtrieveError error;

  switch (command.operation) {
    case btrieve::OperationCode::Open:
      error = Open(command);
      break;
    case btrieve::OperationCode::Close:
      error = Close(command);
      break;
    case btrieve::OperationCode::Stat:
      error = Stat(command);
      break;
    case btrieve::OperationCode::Delete:
      error = Delete(command);
      break;
    case btrieve::OperationCode::StepFirst:
    case btrieve::OperationCode::StepFirst + 100:
    case btrieve::OperationCode::StepFirst + 200:
    case btrieve::OperationCode::StepFirst + 300:
    case btrieve::OperationCode::StepFirst + 400:
    case btrieve::OperationCode::StepLast:
    case btrieve::OperationCode::StepLast + 100:
    case btrieve::OperationCode::StepLast + 200:
    case btrieve::OperationCode::StepLast + 300:
    case btrieve::OperationCode::StepLast + 400:
    case btrieve::OperationCode::StepNext:
    case btrieve::OperationCode::StepNext + 100:
    case btrieve::OperationCode::StepNext + 200:
    case btrieve::OperationCode::StepNext + 300:
    case btrieve::OperationCode::StepNext + 400:
    case btrieve::OperationCode::StepPrevious:
    case btrieve::OperationCode::StepPrevious + 100:
    case btrieve::OperationCode::StepPrevious + 200:
    case btrieve::OperationCode::StepPrevious + 300:
    case btrieve::OperationCode::StepPrevious + 400:
      error = Step(command);
      break;
    case btrieve::OperationCode::AcquireFirst:
    case btrieve::OperationCode::AcquireLast:
    case btrieve::OperationCode::AcquireNext:
    case btrieve::OperationCode::AcquirePrevious:
    case btrieve::OperationCode::AcquireEqual:
    case btrieve::OperationCode::AcquireGreater:
    case btrieve::OperationCode::AcquireGreaterOrEqual:
    case btrieve::OperationCode::AcquireLess:
    case btrieve::OperationCode::AcquireLessOrEqual:
    case btrieve::OperationCode::QueryFirst:
    case btrieve::OperationCode::QueryLast:
    case btrieve::OperationCode::QueryNext:
    case btrieve::OperationCode::QueryPrevious:
    case btrieve::OperationCode::QueryEqual:
    case btrieve::OperationCode::QueryGreater:
    case btrieve::OperationCode::QueryGreaterOrEqual:
    case btrieve::OperationCode::QueryLess:
    case btrieve::OperationCode::QueryLessOrEqual:
      // TODO don't forget all +100-400 for these queries as well
      error = Query(command);
      break;
    case btrieve::OperationCode::GetPosition:
      error = GetPosition(command);
      break;
    case btrieve::OperationCode::GetDirectChunkOrRecord:
    case btrieve::OperationCode::GetDirectChunkOrRecord + 100:
    case btrieve::OperationCode::GetDirectChunkOrRecord + 200:
    case btrieve::OperationCode::GetDirectChunkOrRecord + 300:
    case btrieve::OperationCode::GetDirectChunkOrRecord + 400:
      error = GetDirectRecord(command);
      break;
    case btrieve::OperationCode::Update:
      // TODO update logical currency
      error = Upsert(command, [](btrieve::BtrieveDriver *driver,
                                 std::basic_string_view<uint8_t> record) {
        auto position = driver->getPosition();
        return std::make_pair(driver->updateRecord(position, record), position);
      });
      break;
    case btrieve::OperationCode::Insert:
      // TODO update logical currency
      error = Upsert(command, [](btrieve::BtrieveDriver *driver,
                                 std::basic_string_view<uint8_t> record) {
        return driver->insertRecord(record);
      });
      break;
    case btrieve::OperationCode::Stop:
      error = Stop(command);
      break;
    default:
      error = btrieve::BtrieveError::InvalidOperation;
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
  btrieveCommand.operation = static_cast<btrieve::OperationCode>(wOperation);
  btrieveCommand.lpPositionBlock = lpPositionBlock;
  btrieveCommand.lpDataBuffer = lpDataBuffer;
  btrieveCommand.lpdwDataBufferLength = lpdwDataBufferLength;
  btrieveCommand.lpKeyBuffer = lpKeyBuffer;
  btrieveCommand.lpKeyBufferLength = bKeyLength;
  btrieveCommand.keyNumber = sbKeyNumber;

  return handle(btrieveCommand);
}
