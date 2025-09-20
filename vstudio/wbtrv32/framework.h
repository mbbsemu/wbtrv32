#ifndef __FRAMEWORK_H_
#define __FRAMEWORK_H_

namespace btrieve {
bool FileExists(const wchar_t *file);
}

#ifdef WIN32

#define WIN32_LEAN_AND_MEAN  // Exclude rarely-used stuff from Windows headers
// Windows Header Files
#define NOMINMAX
#include <windows.h>

#include "combaseapi.h"

#else

#include <dlfcn.h>
#include <errno.h>

#include "btrieve/Text.h"

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

void StringFromGUID2(const GUID &guid, wchar_t *guidStr,
                     size_t guidNumberOfCharacters);

void CoCreateGuid(GUID *guid);

HMODULE LoadLibrary(const wchar_t *path);

void FreeLibrary(HMODULE hModule);

void *GetProcAddress(HMODULE hModule, const char *symbol);

#define _TRUNCATE -1

template <size_t size>
error_t mbstowcs_s(size_t *pReturnValue, wchar_t (&wcstr)[size],
                   const char *mbstr, size_t count);

DWORD GetFullPathName(const wchar_t *lpFileName, DWORD nBufferLength,
                      wchar_t *lpBuffer, wchar_t **lpFilePart);

#define _wcsicmp wcscasecmp
#define _rmdir rmdir
#define _unlink unlink

#define __stdcall

#endif  // #ifdef WIN32

#endif  // #ifndef _FRAMEWORK_H_
