#ifndef __FRAMEWORK_H_
#define __FRAMEWORK_H_

namespace btrieve {
bool FileExists(const wchar_t *file);
}

#ifdef WIN32

#define WIN32_LEAN_AND_MEAN             // Exclude rarely-used stuff from Windows headers
// Windows Header Files
#include <windows.h>
#include "combaseapi.h"

#else

#include "btrieve/Text.h"
#include <errno.h>
#include <dlfcn.h>

typedef void *LPVOID;
typedef const void *LPCVOID;

typedef uint16_t WORD;
typedef uint32_t DWORD;

typedef int8_t CHAR;
typedef uint8_t BYTE;

typedef DWORD *LPDWORD;

typedef void *HANDLE;
typedef void *HMODULE;

typedef struct _tagGUID {
  uint32_t a;
  uint32_t b;
  uint32_t c;
  uint32_t d;
} GUID, *LPGUID;

#define ARRAYSIZE(a) (sizeof(a) / sizeof(a[0]))
#define MAX_PATH 255

static void StringFromGUID2(const GUID &guid, wchar_t *guidStr, size_t guidNumberOfCharacters) {
  char *tmp = reinterpret_cast<char*>(alloca(guidNumberOfCharacters));
  snprintf(tmp, guidNumberOfCharacters, "%x_%x_%x_%x", guid.a, guid.b, guid.c, guid.d);
  // just in case
  tmp[guidNumberOfCharacters - 1] = 0;

  mbstowcs(guidStr, tmp, guidNumberOfCharacters);
}

static void CoCreateGuid(GUID *guid) {
  for (size_t i = 0; i < sizeof(GUID); ++i) {
    reinterpret_cast<unsigned char*>(guid)[i] = rand() & 0xFF;
  }
}

static HMODULE LoadLibrary(const wchar_t *path) {
  return dlopen(btrieve::toStdString(path).c_str(), RTLD_LAZY);
}

static void FreeLibrary(HMODULE hModule) {
  dlclose(hModule);
}

static void *GetProcAddress(HMODULE hModule, const char *symbol) {
  return dlsym(hModule, symbol);
}

#define _TRUNCATE -1

template <size_t size>
static error_t mbstowcs_s(
   size_t *pReturnValue,
   wchar_t (&wcstr)[size],
   const char *mbstr,
   size_t count
) {
  size_t wordsWritten = mbstowcs(wcstr, mbstr, count);
  wcstr[size - 1] = 0;
  if (count < size) {
    wcstr[count] = 0;
  }
  return 0;
}

static DWORD GetFullPathName(
  const wchar_t *lpFileName,
  DWORD nBufferLength,
  wchar_t *lpBuffer,
  wchar_t **lpFilePart) {
  char *tmp = reinterpret_cast<char*>(alloca(nBufferLength));
  realpath(btrieve::toStdString(lpFileName).c_str(), tmp);

  // TODO
  if (lpFilePart != nullptr) {
    *lpFilePart = lpBuffer;
  }

  // TODO, use wcscpy_n or whatever
  wcscpy(lpBuffer, btrieve::toWideString(tmp).c_str());

  return wcslen(lpBuffer);
}

#define wcsicmp wcscasecmp
#define __stdcall

#endif // #ifdef WIN32

#endif // #ifndef _FRAMEWORK_H_