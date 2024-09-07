// wbtrv32.cpp : Defines the exported functions for the DLL.
//
#include "btrieve\BtrieveDriver.h"
#include "btrieve\ErrorCode.h"
#include "btrieve\OperationCode.h"
#include "btrieve\SqliteDatabase.h"
#include "combaseapi.h"
#include "framework.h"
#include "wbtrv32.h"

static std::unordered_map<std::wstring, std::unique_ptr<btrieve::BtrieveDriver>>
    _openFiles;

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

static btrieve::BtrieveError Open(BtrieveCommand &command) {
  tchar fileName[MAX_PATH];
  size_t unused;

  const char *lpszFilename =
      reinterpret_cast<const char *>(command.lpKeyBuffer);
  auto openMode = static_cast<BtrieveOpenMode>(command.keyNumber);

  btrieve::BtrieveDriver *db =
      new btrieve::BtrieveDriver(new btrieve::SqliteDatabase());

  mbstowcs_s(&unused, fileName, ARRAYSIZE(fileName), lpszFilename, _TRUNCATE);

  try {
    db->open(fileName);

    // add to my list of open files
    GUID guid;
    CoCreateGuid(&guid);

    WCHAR guidStr[64];
    StringFromGUID2(guid, guidStr, ARRAYSIZE(guidStr));

    _openFiles.emplace(std::make_pair(std::wstring(guidStr), db));
    db = nullptr;

    // write the GUID in the pos block for other calls
    memcpy(command.lpPositionBlock, &guid, sizeof(UUID));

    return btrieve::BtrieveError::Success;
  } catch (const btrieve::BtrieveException &unused) {
    if (db != nullptr) {
      delete db;
    }
    return btrieve::BtrieveError::FileNotFound;
  }
}

static btrieve::BtrieveDriver* getOpenDatabase(LPVOID lpPositioningBlock) {
  WCHAR guidStr[64];
  StringFromGUID2(*reinterpret_cast<GUID*>(lpPositioningBlock), guidStr,
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

static btrieve::BtrieveError Stat(BtrieveCommand& command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  if (command.lpKeyBuffer && command.lpKeyBufferLength > 0) {
    *reinterpret_cast<uint8_t*>(command.lpKeyBuffer) = 0;
  }

  unsigned int requiredSize = sizeof(wbtrv32::FILESPEC) + (btrieveDriver->getKeys().size() * sizeof(wbtrv32::KEYSPEC));
  if (*command.lpdwDataBufferLength < requiredSize) {
    return btrieve::BtrieveError::DataBufferLengthOverrun;
  }
  
  *command.lpdwDataBufferLength = requiredSize;

  wbtrv32::LPFILESPEC lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(command.lpDataBuffer);
  lpFileSpec->logicalFixedRecordLength = btrieveDriver->getRecordLength();
  lpFileSpec->pageSize = 512; // doesn't matter, not needed for sqlite
  lpFileSpec->numberOfKeys = btrieveDriver->getKeys().size();
  lpFileSpec->fileVersion = 0x60; // this is 6.0
  lpFileSpec->recordCount = btrieveDriver->getRecordCount();
  lpFileSpec->fileFlags = btrieveDriver->isVariableLengthRecords() ? 1 : 0;
  lpFileSpec->numExtraPointers = 0;
  lpFileSpec->physicalPageSize = 1; // in 512-byte blocks
  lpFileSpec->preallocatedPages = 0;

  wbtrv32::LPKEYSPEC lpKeySpec = reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  for (auto key : btrieveDriver->getKeys()) {
    lpKeySpec->position = key.getPrimarySegment().getOffset();
    lpKeySpec->length = key.getLength();
    lpKeySpec->attributes = key.getPrimarySegment().getAttributes();
    lpKeySpec->uniqueKeys = 0;
    lpKeySpec->extendedDataType = key.getPrimarySegment().getDataType();
    lpKeySpec->nullValue = key.getPrimarySegment().getNullValue();
    lpKeySpec->reserved = 0;
    lpKeySpec->number = 0;
    lpKeySpec->acsNumber = key.getACS() != nullptr ? 1 : 0;
    ++lpKeySpec;
  }
  
  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError Delete(BtrieveCommand& command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  return btrieveDriver->performOperation(-1, std::basic_string_view<uint8_t>(), btrieve::OperationCode::Delete);
}

static btrieve::BtrieveError Step(BtrieveCommand& command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  auto oldPosition = btrieveDriver->getPosition();
  auto result = btrieveDriver->performOperation(-1, std::basic_string_view<uint8_t>(), command.operation);
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

  *command.lpdwDataBufferLength = record.second.getData().size();
  memcpy(command.lpDataBuffer, record.second.getData().data(), record.second.getData().size());

  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError GetPosition(BtrieveCommand& command) {
  auto btrieveDriver = getOpenDatabase(command.lpPositionBlock);
  if (btrieveDriver == nullptr) {
    return btrieve::BtrieveError::FileNotOpen;
  }
  
  if (*command.lpdwDataBufferLength < sizeof(uint32_t)) {
    return btrieve::BtrieveError::DataBufferLengthOverrun;
  }

  *command.lpdwDataBufferLength = sizeof(uint32_t);
  *reinterpret_cast<uint32_t*>(command.lpDataBuffer) = btrieveDriver->getPosition();
  
  // TODO can this be InvalidPositioning, say on an empty file?
  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError handle(BtrieveCommand &command) {
  switch (command.operation) {
  case btrieve::OperationCode::Open:
    return Open(command);
  case btrieve::OperationCode::Close:
    return Close(command);
  case btrieve::OperationCode::Stat:
    return Stat(command);
  case btrieve::OperationCode::Delete:
    return Delete(command);
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
    return Step(command);
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
    // return Query(command);
  case btrieve::OperationCode::GetPosition:
    return GetPosition(command);
  case btrieve::OperationCode::GetDirectChunkOrRecord:
    // return GetDirectRecord(command);
  case btrieve::OperationCode::Update:
    // return Update(command);
  case btrieve::OperationCode::Insert:
    // return Insert(command);
  default:
    return btrieve::BtrieveError::InvalidOperation;
  }
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
