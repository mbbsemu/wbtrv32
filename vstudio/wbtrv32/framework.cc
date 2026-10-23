#include "framework.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>

#include "btrieve/Text.h"

int wbtrv32::readSetting(const char *name, int defaultValue, int minimum,
                         int maximum) {
  std::string environmentName = "WBTRV32_";
  for (const char *c = name; *c != 0; ++c) {
    environmentName +=
        static_cast<char>(toupper(static_cast<unsigned char>(*c)));
  }
#ifdef WIN32
  // as in trackSourceFiles() (wbtrv32.cpp), the CRT's getenv copy may miss a
  // value set with SetEnvironmentVariable
  char fromEnvironment[16];
  const DWORD length =
      GetEnvironmentVariableA(environmentName.c_str(), fromEnvironment,
                              ARRAYSIZE(fromEnvironment));
  if (length > 0 && length < ARRAYSIZE(fromEnvironment)) {
    return std::clamp(atoi(fromEnvironment), minimum, maximum);
  }

  // GetPrivateProfileInt looks in the Windows directory unless given a path
  char iniPath[MAX_PATH];
  if (GetFullPathNameA("wbtrv32.ini", ARRAYSIZE(iniPath), iniPath, nullptr) !=
      0) {
    return std::clamp(static_cast<int>(GetPrivateProfileIntA(
                          "wbtrv32", name, defaultValue, iniPath)),
                      minimum, maximum);
  }
#else
  const char *fromEnvironment = getenv(environmentName.c_str());
  if (fromEnvironment != nullptr && *fromEnvironment != 0) {
    return std::clamp(atoi(fromEnvironment), minimum, maximum);
  }
#endif
  return defaultValue;
}

#ifdef WIN32

namespace btrieve {
bool FileExists(const wchar_t* filepath) {
  WIN32_FILE_ATTRIBUTE_DATA fileAttributeData;

  return GetFileAttributesExW(filepath, GetFileExInfoStandard,
                              &fileAttributeData) != 0;
}
}  // namespace btrieve

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
int mbstowcs_s(size_t *pReturnValue, wchar_t (&wcstr)[size],
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
