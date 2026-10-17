// 26.10.13.4B 06:53PM
// Parity shim: installed as WBTRV32.DLL, it forwards every BTRCALL to two
// real implementations and reports where they disagree.
//
//   <shim dir>\WBTRV32.DLL                 this shim
//   <shim dir>\authoritative\WBTRV32.DLL   reference Btrieve (its result is
//                                          what the caller sees)
//   <shim dir>\comparative\WBTRV32.DLL     implementation under test
//
// Results go to <shim dir>\SHIM.LOG: every mismatch, plus a per-operation
// summary at unload. Create <shim dir>\SHIM.TRACE to also log every call.
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#include <stdarg.h>
#include <share.h>
#include <stdio.h>
#include <string.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>

#include "..\..\btrieve\OperationCode.h"

#pragma comment(lib, "version.lib")

using namespace btrieve;

namespace {

typedef struct _tagMYPOSBLOCK {
  char authoritative[128];
  char comparative[128];
  char filename[256];
  // authoritative physical position -> comparative physical position, learned
  // from GetPosition so GetDirect can be replayed against the comparative side
  std::unordered_map<DWORD, DWORD> positions;
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

struct OpStats {
  unsigned long calls = 0;
  unsigned long statusMismatches = 0;
  unsigned long dataMismatches = 0;
  unsigned long unmappedPositions = 0;
};

HMODULE hAuthoritative;
BTRCALLPTR authoritative;
HMODULE hComparative;
BTRCALLPTR comparative;

CRITICAL_SECTION lock;
FILE* logFile;
bool trace;
std::map<WORD, OpStats>* stats;

const char* opName(WORD op) {
  switch (op) {
    case Open: return "Open";
    case Close: return "Close";
    case Insert: return "Insert";
    case Update: return "Update";
    case Delete: return "Delete";
    case AcquireEqual: return "GetEqual";
    case AcquireNext: return "GetNext";
    case AcquirePrevious: return "GetPrevious";
    case AcquireGreater: return "GetGreater";
    case AcquireGreaterOrEqual: return "GetGreaterOrEqual";
    case AcquireLess: return "GetLess";
    case AcquireLessOrEqual: return "GetLessOrEqual";
    case AcquireFirst: return "GetFirst";
    case AcquireLast: return "GetLast";
    case Create: return "Create";
    case Stat: return "Stat";
    case Extend: return "Extend";
    case 19: return "BeginTransaction";
    case 20: return "EndTransaction";
    case 21: return "AbortTransaction";
    case GetPosition: return "GetPosition";
    case GetDirectChunkOrRecord: return "GetDirect";
    case StepNext: return "StepNext";
    case Stop: return "Stop";
    case Unlock: return "Unlock";
    case SetOwner: return "SetOwner";
    case StepFirst: return "StepFirst";
    case StepLast: return "StepLast";
    case StepPrevious: return "StepPrevious";
    case StepNextExtended: return "StepNextExtended";
    case StepPreviousExtended: return "StepPreviousExtended";
    case QueryEqual: return "QueryEqual";
    case QueryNext: return "QueryNext";
    case QueryPrevious: return "QueryPrevious";
    case QueryGreater: return "QueryGreater";
    case QueryGreaterOrEqual: return "QueryGreaterOrEqual";
    case QueryLess: return "QueryLess";
    case QueryLessOrEqual: return "QueryLessOrEqual";
    case QueryFirst: return "QueryFirst";
    case QueryLast: return "QueryLast";
    default: return "?";
  }
}

// operations whose data buffer holds a record on success
bool returnsRecord(WORD op) {
  switch (op) {
    case AcquireEqual:
    case AcquireNext:
    case AcquirePrevious:
    case AcquireGreater:
    case AcquireGreaterOrEqual:
    case AcquireLess:
    case AcquireLessOrEqual:
    case AcquireFirst:
    case AcquireLast:
    case GetDirectChunkOrRecord:
    case StepFirst:
    case StepLast:
    case StepNext:
    case StepPrevious:
      return true;
    default:
      return false;
  }
}

void logLine(const char* format, ...) {
  if (logFile == nullptr) {
    return;
  }
  SYSTEMTIME now;
  GetLocalTime(&now);
  fprintf(logFile, "%02u:%02u:%02u.%03u ", now.wHour, now.wMinute,
          now.wSecond, now.wMilliseconds);
  va_list args;
  va_start(args, format);
  vfprintf(logFile, format, args);
  va_end(args);
  fputc('\n', logFile);
  fflush(logFile);
}

void hexLine(const char* label, const BYTE* data, DWORD offset, DWORD length) {
  char buf[16 * 3 + 1];
  DWORD count = 0;
  for (DWORD i = offset; i < length && count < 16; ++i, ++count) {
    sprintf_s(buf + count * 3, sizeof(buf) - count * 3, "%02X ",
              static_cast<unsigned>(data[i]));
  }
  buf[count * 3] = '\0';
  logLine("    %s @%lu: %s", label, offset, buf);
}

std::string fileVersion(const char* path) {
  DWORD handle = 0;
  DWORD size = GetFileVersionInfoSizeA(path, &handle);
  if (size == 0) {
    return "no version info";
  }
  std::unique_ptr<BYTE[]> info(new BYTE[size]);
  VS_FIXEDFILEINFO* fixed = nullptr;
  UINT fixedLength = 0;
  if (!GetFileVersionInfoA(path, 0, size, info.get()) ||
      !VerQueryValueA(info.get(), "\\", reinterpret_cast<LPVOID*>(&fixed),
                      &fixedLength) ||
      fixed == nullptr) {
    return "no version info";
  }
  char buf[64];
  sprintf_s(buf, sizeof(buf), "%u.%u.%u.%u", HIWORD(fixed->dwFileVersionMS),
            LOWORD(fixed->dwFileVersionMS), HIWORD(fixed->dwFileVersionLS),
            LOWORD(fixed->dwFileVersionLS));
  return buf;
}

HMODULE loadImplementation(const std::string& path, BTRCALLPTR* entry) {
  // altered search path: the DLL's own directory is searched first for its
  // dependencies, then the usual order (which includes the board directory,
  // where the stock Btrieve engine files live)
  HMODULE module =
      LoadLibraryExA(path.c_str(), NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
  if (module == NULL) {
    logLine("FATAL: cannot load %s (error %lu)", path.c_str(), GetLastError());
    return NULL;
  }
  *entry = reinterpret_cast<BTRCALLPTR>(GetProcAddress(module, "BTRCALL"));
  if (*entry == nullptr) {
    logLine("FATAL: %s has no BTRCALL export", path.c_str());
    return NULL;
  }
  logLine("loaded %s (version %s)", path.c_str(), fileVersion(path.c_str()).c_str());
  return module;
}

bool attach(HMODULE self) {
  InitializeCriticalSection(&lock);
  stats = new std::map<WORD, OpStats>();

  char selfPath[MAX_PATH];
  if (GetModuleFileNameA(self, selfPath, MAX_PATH) == 0) {
    return false;
  }
  std::string dir(selfPath);
  dir.erase(dir.find_last_of("\\/") + 1);

  // shared, so the log can be read (and other processes can append) while
  // the host is still running
  logFile = _fsopen((dir + "SHIM.LOG").c_str(), "a", _SH_DENYNO);
  trace = GetFileAttributesA((dir + "SHIM.TRACE").c_str()) !=
          INVALID_FILE_ATTRIBUTES;
  logLine("==== WBTRV32 parity shim (built " __DATE__ " " __TIME__
          ") started, trace %s",
          trace ? "ON" : "off");

  hAuthoritative =
      loadImplementation(dir + "authoritative\\WBTRV32.DLL", &authoritative);
  // stock Btrieve writes the .DATs here, so they never match what the .DBs
  // were converted from; stop the comparative side reconverting on every open
  SetEnvironmentVariableA("WBTRV32_DAT_TRACKING", "0");
  hComparative =
      loadImplementation(dir + "comparative\\WBTRV32.DLL", &comparative);
  return hAuthoritative != NULL && hComparative != NULL;
}

void detach(bool processTerminating) {
  if (stats != nullptr && logFile != nullptr) {
    logLine("==== summary (op: calls / status mismatches / data mismatches / "
            "unmapped GetDirect positions)");
    for (const auto& entry : *stats) {
      const OpStats& s = entry.second;
      logLine("  %3u %-20s %8lu / %lu / %lu / %lu", entry.first,
              opName(entry.first % 100),
              s.calls, s.statusMismatches, s.dataMismatches,
              s.unmappedPositions);
    }
  }
  // on process exit the other DLLs may already be gone; only unload them on
  // an explicit FreeLibrary of this shim
  if (!processTerminating) {
    if (hAuthoritative != NULL) {
      FreeLibrary(hAuthoritative);
      hAuthoritative = NULL;
    }
    if (hComparative != NULL) {
      FreeLibrary(hComparative);
      hComparative = NULL;
    }
  }
  if (logFile != nullptr) {
    fclose(logFile);
    logFile = nullptr;
  }
}

// Callers may declare a buffer longer than they own: the engine always passes
// key length 255, and real Btrieve never reads a Get's data buffer, so an
// oversized data length goes unnoticed there. Copy defensively, zero-filling
// whatever cannot be read, and return how many bytes were readable.
DWORD safeCopy(BYTE* dst, const void* src, DWORD length) {
  __try {
    memcpy(dst, src, length);
    return length;
  } __except (EXCEPTION_EXECUTE_HANDLER) {
    for (DWORD i = 0; i < length; ++i) {
      __try {
        dst[i] = static_cast<const BYTE*>(src)[i];
      } __except (EXCEPTION_EXECUTE_HANDLER) {
        memset(dst + i, 0, length - i);
        return i;
      }
    }
    return length;
  }
}

#pragma pack(push, 1)
struct StatFileSpec {
  WORD recordLength;
  WORD pageSize;
  BYTE numberOfKeys;
  BYTE fileVersion;
  DWORD recordCount;
  WORD fileFlags;
  BYTE numExtraPointers;
  BYTE physicalPageSize;
  WORD preallocatedPages;
};
struct StatKeySpec {
  WORD position;
  WORD length;
  WORD attributes;
  DWORD uniqueKeys;
  BYTE extendedDataType;
  BYTE nullValue;
  WORD reserved;
  BYTE number;
  BYTE acsNumber;
};
#pragma pack(pop)

// Compares the Stat fields The Major BBS reads (DFAAPI.C dfaCountRec,
// dfaRecLen, clckln): record count and length, number of keys, and each
// segment's position, length, flags and type. Page size, unique-key counts
// and trailing ACS tables are implementation details and are not compared.
void compareStat(const char* filename, const BYTE* auth, DWORD authLength,
                 const BYTE* comp, DWORD compLength, DWORD bufferLength,
                 OpStats& opStats) {
  const auto* a = reinterpret_cast<const StatFileSpec*>(auth);
  const auto* c = reinterpret_cast<const StatFileSpec*>(comp);
  char diff[512] = "";
  auto note = [&](const char* format, auto... args) {
    size_t used = strlen(diff);
    sprintf_s(diff + used, sizeof(diff) - used, format, args...);
  };
  if (a->recordLength != c->recordLength) {
    note(" reclen %u/%u", a->recordLength, c->recordLength);
  }
  if (a->numberOfKeys != c->numberOfKeys) {
    note(" keys %u/%u", a->numberOfKeys, c->numberOfKeys);
  }
  if (a->recordCount != c->recordCount) {
    note(" records %lu/%lu", a->recordCount, c->recordCount);
  }
  if ((a->fileFlags & 1) != (c->fileFlags & 1)) {
    note(" variable %u/%u", a->fileFlags & 1, c->fileFlags & 1);
  }
  // segments follow the file spec; stop at the first that is not in both
  const DWORD available = min(min(authLength, compLength), bufferLength);
  const auto* ak = reinterpret_cast<const StatKeySpec*>(a + 1);
  const auto* ck = reinterpret_cast<const StatKeySpec*>(c + 1);
  unsigned keys = 0;
  for (unsigned i = 0; keys < a->numberOfKeys &&
                       sizeof(StatFileSpec) + (i + 1) * sizeof(StatKeySpec) <=
                           available;
       ++i) {
    // attributes: compare the bits that change meaning (segmented, dups,
    // modifiable, binary, null, descending, extended type)
    const WORD mask = 0x015F;
    if (ak[i].position != ck[i].position || ak[i].length != ck[i].length ||
        (ak[i].attributes & mask) != (ck[i].attributes & mask) ||
        ak[i].extendedDataType != ck[i].extendedDataType) {
      note(" seg%u pos %u/%u len %u/%u attr %04X/%04X type %u/%u", i,
           ak[i].position, ck[i].position, ak[i].length, ck[i].length,
           ak[i].attributes, ck[i].attributes, ak[i].extendedDataType,
           ck[i].extendedDataType);
    }
    if (!(ak[i].attributes & 0x10)) {
      ++keys;
    }
  }
  if (*diff) {
    ++opStats.dataMismatches;
    logLine("STATINFO [%s]%s", filename, diff);
  }
}

}  // namespace

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call,
                      LPVOID lpReserved) {
  switch (ul_reason_for_call) {
    case DLL_PROCESS_ATTACH:
      DisableThreadLibraryCalls(hModule);
      return attach(hModule) ? TRUE : FALSE;
    case DLL_PROCESS_DETACH:
      detach(lpReserved != nullptr);
      break;
  }
  return TRUE;
}

extern "C" int __stdcall BTRCALL(WORD wOperation, LPVOID lpPositionBlock,
                                 LPVOID lpDataBuffer,
                                 LPDWORD lpdwDataBufferLength,
                                 LPVOID lpKeyBuffer, BYTE bKeyLength,
                                 CHAR sbKeyNumber) {
  EnterCriticalSection(&lock);

  const WORD op = wOperation % 100;  // strip record-lock bias
  LPMYPOSBLOCK posBlock = nullptr;
  LPCLIENTPOSBLOCK clientPosBlock = nullptr;

  if (op == Open || op == Create) {
    posBlock = new MYPOSBLOCK();
    if (lpKeyBuffer != nullptr) {
      strncpy_s(posBlock->filename, reinterpret_cast<const char*>(lpKeyBuffer),
                _TRUNCATE);
    }
  } else if (lpPositionBlock != nullptr) {
    clientPosBlock = reinterpret_cast<LPCLIENTPOSBLOCK>(lpPositionBlock);
    if (clientPosBlock->magic == magicValue) {
      posBlock = clientPosBlock->lpMyPosBlock;
    }
  }
  const char* filename =
      posBlock != nullptr && *posBlock->filename ? posBlock->filename : "-";

  // the comparative side gets private copies of every in/out buffer
  // Btrieve uses only the low word of the data length; The Major BBS leaves
  // garbage in the high word (DFAAPI.C btvu), so never size anything by it
  const DWORD inLength =
      lpdwDataBufferLength ? (*lpdwDataBufferLength & 0xFFFF) : 0;
  DWORD comparativeLength = inLength;
  std::unique_ptr<BYTE[]> comparativeData(
      lpDataBuffer != nullptr && inLength > 0 ? new (std::nothrow)
                                                    BYTE[inLength]
                                              : nullptr);
  if (comparativeData) {
    const DWORD readable =
        safeCopy(comparativeData.get(), lpDataBuffer, inLength);
    if (readable < inLength) {
      logLine("SHORTBUF %s (op %u, key %d) [%s] data length %lu declared, "
              "only %lu readable",
              opName(op), wOperation, sbKeyNumber, filename, inLength,
              readable);
    }
  }
  std::unique_ptr<BYTE[]> comparativeKey(
      lpKeyBuffer != nullptr && bKeyLength > 0 ? new BYTE[bKeyLength]
                                               : nullptr);
  if (comparativeKey) {
    safeCopy(comparativeKey.get(), lpKeyBuffer, bKeyLength);
  }

  // GetDirect takes a physical position, which differs between the two
  // implementations: translate it through the map built by GetPosition
  bool skipComparative = false;
  OpStats& opStats = (*stats)[wOperation];
  ++opStats.calls;
  if (op == GetDirectChunkOrRecord && posBlock != nullptr &&
      comparativeData && inLength >= sizeof(DWORD)) {
    DWORD position;
    memcpy(&position, comparativeData.get(), sizeof(DWORD));
    auto it = posBlock->positions.find(position);
    if (it != posBlock->positions.end()) {
      memcpy(comparativeData.get(), &it->second, sizeof(DWORD));
    } else {
      skipComparative = true;
      ++opStats.unmappedPositions;
      logLine("UNMAPPED %s [%s] position %lu not seen via GetPosition; "
              "comparative call skipped",
              opName(op), filename, position);
    }
  }

  auto callComparative = [&]() {
    return comparative(
        wOperation, posBlock == nullptr ? nullptr : posBlock->comparative,
        comparativeData.get(),
        lpdwDataBufferLength == nullptr ? nullptr : &comparativeLength,
        comparativeKey.get(), bKeyLength, sbKeyNumber);
  };
  auto callAuthoritative = [&]() {
    return authoritative(
        wOperation, posBlock == nullptr ? nullptr : posBlock->authoritative,
        lpDataBuffer, lpdwDataBufferLength, lpKeyBuffer, bKeyLength,
        sbKeyNumber);
  };

  // on Open the comparative side goes first: it may convert the .DAT, which
  // the authoritative side would otherwise hold open
  int responseAuthoritative;
  int responseComparative = -1;
  if (op == Open) {
    responseComparative = callComparative();
    responseAuthoritative = callAuthoritative();
  } else {
    responseAuthoritative = callAuthoritative();
    if (!skipComparative) {
      responseComparative = callComparative();
    }
  }

  if (trace) {
    logLine("%-20s op %3u key %2d [%s] auth %d comp %d len %lu/%lu",
            opName(op), wOperation, sbKeyNumber, filename,
            responseAuthoritative, responseComparative,
            lpdwDataBufferLength ? *lpdwDataBufferLength : 0,
            comparativeLength);
  }

  if (!skipComparative) {
    if (responseAuthoritative != responseComparative) {
      ++opStats.statusMismatches;
      logLine("STATUS   %s (op %u, key %d) [%s] authoritative %d, "
              "comparative %d",
              opName(op), wOperation, sbKeyNumber, filename,
              responseAuthoritative, responseComparative);
    } else if (responseAuthoritative == 0 && op == GetPosition &&
               posBlock != nullptr && comparativeData &&
               inLength >= sizeof(DWORD)) {
      DWORD a, c;
      memcpy(&a, lpDataBuffer, sizeof(DWORD));
      memcpy(&c, comparativeData.get(), sizeof(DWORD));
      posBlock->positions[a] = c;
    } else if (responseAuthoritative == 0 && op == Stat && comparativeData &&
               inLength >= sizeof(StatFileSpec)) {
      compareStat(filename, static_cast<const BYTE*>(lpDataBuffer),
                  *lpdwDataBufferLength, comparativeData.get(),
                  comparativeLength, inLength, opStats);
    } else if (responseAuthoritative == 0 && returnsRecord(op) &&
               lpdwDataBufferLength != nullptr && comparativeData) {
      const DWORD authLength = *lpdwDataBufferLength;
      const BYTE* authData = static_cast<const BYTE*>(lpDataBuffer);
      DWORD diff = MAXDWORD;
      const DWORD common = min(min(authLength, comparativeLength), inLength);
      for (DWORD i = 0; i < common; ++i) {
        if (authData[i] != comparativeData[i]) {
          diff = i;
          break;
        }
      }
      if (authLength != comparativeLength || diff != MAXDWORD) {
        ++opStats.dataMismatches;
        logLine("DATA     %s (op %u, key %d) [%s] length %lu vs %lu, "
                "first difference at %ld",
                opName(op), wOperation, sbKeyNumber, filename, authLength,
                comparativeLength, diff == MAXDWORD ? -1L : (long)diff);
        const DWORD from = diff == MAXDWORD ? 0 : diff & ~15UL;
        hexLine("authoritative", authData, from, min(authLength, inLength));
        hexLine("comparative  ", comparativeData.get(), from,
                min(comparativeLength, inLength));
      }
    }
  }

  if (op == Open || op == Create) {
    if (op == Open && responseAuthoritative != 0) {
      // the caller never sees this file as open: release the comparative side
      if (responseComparative == 0) {
        DWORD zero = 0;
        comparative(Close, posBlock->comparative, nullptr, &zero, nullptr, 0,
                    0);
      }
      delete posBlock;
    } else if (op == Create) {
      // Create leaves nothing open; the caller opens the file afterwards
      delete posBlock;
    } else {
      memset(lpPositionBlock, 0, 128);
      clientPosBlock = reinterpret_cast<LPCLIENTPOSBLOCK>(lpPositionBlock);
      clientPosBlock->magic = magicValue;
      clientPosBlock->lpMyPosBlock = posBlock;
    }
  } else if (op == Close && posBlock != nullptr && responseAuthoritative == 0) {
    memset(lpPositionBlock, 0, 128);
    delete posBlock;
  }

  LeaveCriticalSection(&lock);
  return responseAuthoritative;
}
