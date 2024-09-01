// wbtrv32.cpp : Defines the exported functions for the DLL.
//
#include "btrieve\BtrieveDriver.h"
#include "btrieve\ErrorCode.h"
#include "btrieve\OperationCode.h"
#include "btrieve\SqliteDatabase.h"
#include "combaseapi.h"
#include "framework.h"

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

static btrieve::BtrieveError Close(BtrieveCommand &command) {
  WCHAR guidStr[64];
  StringFromGUID2(*reinterpret_cast<GUID *>(command.lpPositionBlock), guidStr,
                  ARRAYSIZE(guidStr));

  if (!_openFiles.contains(guidStr)) {
    return btrieve::BtrieveError::FileNotOpen;
  }

  _openFiles.erase(guidStr);
  return btrieve::BtrieveError::Success;
}

static btrieve::BtrieveError handle(BtrieveCommand &command) {
  switch (command.operation) {
  case btrieve::OperationCode::Open:
    return Open(command);
  case btrieve::OperationCode::Close:
    return Close(command);
  case btrieve::OperationCode::Stat:
    // return Stat(command);
  case btrieve::OperationCode::Delete:
    // return Delete(command);
  case btrieve::OperationCode::StepFirst:
  case btrieve::OperationCode::StepLast:
  case btrieve::OperationCode::StepNext:
  case btrieve::OperationCode::StepPrevious:
    // return Step(command);
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
    // return GetPosition(command);
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
