#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#include <stdarg.h>
#include <tchar.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include "..\..\btrieve\OperationCode.h"

using namespace btrieve;

typedef struct _tagMYPOSBLOCK {
  char authoritative[128];
  char comparative[128];
  char filename[256];
} MYPOSBLOCK, *LPMYPOSBLOCK;

typedef struct _tagCLIENTPOSBLOCK {
  uint64_t magic;
  LPMYPOSBLOCK lpMyPosBlock;
} CLIENTPOSBLOCK, *LPCLIENTPOSBLOCK;

const uint64_t magicValue = 0xDEADBEEFABCD1234;

typedef int(__stdcall* BTRCALLPTR)(WORD wOperation, LPVOID lpPositionBlock,
                                   LPVOID lpDataBuffer,
                                   LPDWORD lpdwDataBufferLength,
                                   LPVOID lpKeyBuffer, BYTE bKeyLength,
                                   CHAR sbKeyNumber);

static HMODULE hAuthoritative;
static BTRCALLPTR authoritative;
static HMODULE hComparative;
static BTRCALLPTR comparative;

/*struct container_hash {
  std::size_t operator()(std::vector<BYTE> const& c) const {
    int hashCode = 1;
    for (E e : this) {
      hashCode = 31 * hashCode + (e == null ? 0 : e.hashCode());
    }
    return hashCode;
  }
};*/

// std::unordered_map<std::vector<BYTE>, std::vector<BYTE>, container_hash>
//    posBlockMap;

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call,
                      LPVOID lpReserved) {
  switch (ul_reason_for_call) {
    case DLL_PROCESS_ATTACH:
      MessageBox(NULL, _T("Attach"), _T("Caption"), MB_OK);

      hAuthoritative = LoadLibrary(_T("authoritative\\wbtrv32.dll"));
      hComparative = LoadLibrary(_T("comparative\\wbtrv32.dll"));
      if (hAuthoritative == NULL || hComparative == NULL) {
        return FALSE;
      }

      authoritative = reinterpret_cast<BTRCALLPTR>(
          GetProcAddress(hAuthoritative, "BTRCALL"));
      comparative =
          reinterpret_cast<BTRCALLPTR>(GetProcAddress(hComparative, "BTRCALL"));
      if (authoritative == nullptr || hComparative == nullptr) {
        return FALSE;
      }
      break;
    case DLL_PROCESS_DETACH:
      if (hAuthoritative != NULL) {
        FreeLibrary(hAuthoritative);
        hAuthoritative = NULL;
      }
      if (hComparative != NULL) {
        FreeLibrary(hComparative);
        hComparative = NULL;
      }
      break;
    case DLL_THREAD_ATTACH:
    case DLL_THREAD_DETACH:
      break;
  }
  return TRUE;
}

static void debug(LPCTSTR lpszFormat, ...) {
  TCHAR buf[256];
  va_list args;

  va_start(args, lpszFormat);
  _vstprintf_s(buf, ARRAYSIZE(buf), lpszFormat, args);
  va_end(args);

  OutputDebugString(buf);
}

extern "C" int __stdcall BTRCALL(WORD wOperation, LPVOID lpPositionBlock,
                                 LPVOID lpDataBuffer,
                                 LPDWORD lpdwDataBufferLength,
                                 LPVOID lpKeyBuffer, BYTE bKeyLength,
                                 CHAR sbKeyNumber) {
  LPMYPOSBLOCK posBlock = nullptr;
  LPCLIENTPOSBLOCK clientPosBlock = nullptr;

  switch (wOperation) {
    case OperationCode::Open:
    case OperationCode::Create:
      posBlock = new MYPOSBLOCK;
      memset(posBlock, 0, sizeof(MYPOSBLOCK));
      strcpy_s(posBlock->filename, reinterpret_cast<const char*>(lpKeyBuffer));

      memset(lpPositionBlock, 0, 128);
      clientPosBlock = reinterpret_cast<LPCLIENTPOSBLOCK>(lpPositionBlock);
      clientPosBlock->magic = magicValue;
      clientPosBlock->lpMyPosBlock = posBlock;
      break;
    default:
      if (lpPositionBlock != nullptr) {
        clientPosBlock = reinterpret_cast<LPCLIENTPOSBLOCK>(lpPositionBlock);
        /*if (clientPosBlock->magic != magicValue) {
          DebugBreak();
        }*/
        posBlock = clientPosBlock->lpMyPosBlock;
      }
  }

  debug(_T("Operation code 0x%X: file: %S\r\n"), wOperation,
        posBlock != nullptr ? posBlock->filename : "null");

  if (wOperation == OperationCode::AcquireGreater && posBlock != nullptr &&
      !strcmp(posBlock->filename, "galfill2.dat")) {
    debug(_T("Attempting to query"));
  }

  DWORD comparativeDataBufferLength =
      lpdwDataBufferLength ? *lpdwDataBufferLength : 0;

  std::unique_ptr<BYTE> comparativeDataBuffer(
      lpDataBuffer != nullptr && comparativeDataBufferLength > 0
          ? new BYTE[comparativeDataBufferLength]
          : nullptr);
  if (lpDataBuffer && comparativeDataBuffer) {
    memcpy(comparativeDataBuffer.get(), lpDataBuffer,
           comparativeDataBufferLength);
  }

  std::unique_ptr<BYTE> comparativeKeyBuffer(
      lpKeyBuffer != nullptr && bKeyLength > 0 ? new BYTE[bKeyLength]
                                               : nullptr);
  if (lpKeyBuffer && comparativeKeyBuffer) {
    memcpy(comparativeKeyBuffer.get(), lpKeyBuffer, bKeyLength);
  }

  // if open, we need to have the comparative side go first since authoritative
  // will open it exclusively
  int responseAuthoritative;
  int responseComparative;
  if (wOperation == OperationCode::Open) {
    responseComparative = comparative(
        wOperation, posBlock == nullptr ? nullptr : posBlock->comparative,
        comparativeDataBuffer.get(),
        lpdwDataBufferLength == nullptr ? nullptr
                                        : &comparativeDataBufferLength,
        comparativeKeyBuffer.get(), bKeyLength, sbKeyNumber);
  }

  responseAuthoritative = authoritative(
      wOperation, posBlock == nullptr ? nullptr : posBlock->authoritative,
      lpDataBuffer, lpdwDataBufferLength, lpKeyBuffer, bKeyLength, sbKeyNumber);

  if (wOperation != OperationCode::Open) {
    responseComparative = comparative(
        wOperation, posBlock == nullptr ? nullptr : posBlock->comparative,
        comparativeDataBuffer.get(),
        lpdwDataBufferLength == nullptr ? nullptr
                                        : &comparativeDataBufferLength,
        comparativeKeyBuffer.get(), bKeyLength, sbKeyNumber);
  }

  if (wOperation != OperationCode::GetDirectChunkOrRecord &&
      responseAuthoritative != responseComparative) {
    debug(_T("Received response mismatch! [%S] 0x%X vs 0x%X\r\n"),
          posBlock != nullptr && *posBlock->filename ? posBlock->filename
                                                     : "null",
          responseAuthoritative, responseComparative);
  }

  switch (wOperation) {
    case OperationCode::Close:
      memset(lpPositionBlock, 0, 128);
      delete posBlock;
      break;
  }

  return responseAuthoritative;
}
