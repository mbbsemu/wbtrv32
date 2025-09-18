#include "framework.h"

#include "btrieve/Text.h"

#ifdef WIN32

#else

#include <fcntl.h>
#include <sys/stat.h>

namespace btrieve {
bool FileExists(const wchar_t *filepath) {
  struct stat st;
  return (stat(toStdString(filepath).c_str(), &st) == 0);
}
}  // namespace btrieve

void StringFromGUID2(const GUID &guid, wchar_t *guidStr,
                     size_t guidNumberOfCharacters) {
  char *tmp = reinterpret_cast<char *>(alloca(guidNumberOfCharacters));
  snprintf(tmp, guidNumberOfCharacters, "%x_%x_%x_%x", guid.a, guid.b, guid.c,
           guid.d);
  // just in case
  tmp[guidNumberOfCharacters - 1] = 0;

  mbstowcs(guidStr, tmp, guidNumberOfCharacters);
}

void CoCreateGuid(GUID *guid) {
  for (size_t i = 0; i < sizeof(GUID); ++i) {
    reinterpret_cast<unsigned char *>(guid)[i] = rand() & 0xFF;
  }
}

HMODULE LoadLibrary(const wchar_t *path) {
  return dlopen(btrieve::toStdString(path).c_str(), RTLD_LAZY);
}

void FreeLibrary(HMODULE hModule) { dlclose(hModule); }

void *GetProcAddress(HMODULE hModule, const char *symbol) {
  return dlsym(hModule, symbol);
}

template <size_t size>
error_t mbstowcs_s(size_t *pReturnValue, wchar_t (&wcstr)[size],
                   const char *mbstr, size_t count) {
  size_t wordsWritten = mbstowcs(wcstr, mbstr, count);
  wcstr[size - 1] = 0;
  if (count < size) {
    wcstr[count] = 0;
  }
  return 0;
}

DWORD GetFullPathName(const wchar_t *lpFileName, DWORD nBufferLength,
                      wchar_t *lpBuffer, wchar_t **lpFilePart) {
  char *tmp = reinterpret_cast<char *>(alloca(nBufferLength));
  realpath(btrieve::toStdString(lpFileName).c_str(), tmp);

  // TODO
  if (lpFilePart != nullptr) {
    *lpFilePart = lpBuffer;
  }

  // TODO, use wcscpy_n or whatever
  wcscpy(lpBuffer, btrieve::toWideString(tmp).c_str());

  return wcslen(lpBuffer);
}

#endif
