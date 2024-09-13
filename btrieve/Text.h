#ifndef __TEXT_H_
#define __TEXT_H_

#include <string>

#ifdef _WIN32

#include <stdlib.h>

typedef wchar_t tchar;

#define _TEXT(s) L##s

static std::string toStdString(const tchar *str) {
  size_t destSize;
  size_t destBufferSizeInBytes = (wcslen(str) + 1) * sizeof(tchar);
  char *destBuffer = reinterpret_cast<char *>(_alloca(destBufferSizeInBytes));

  wcstombs_s(&destSize, destBuffer, destBufferSizeInBytes, str, _TRUNCATE);
  return std::string(destBuffer);
}

#else
typedef char tchar;

#define _TEXT(s) s

static std::string toStdString(const tchar *str) { return std::string(str); }

#endif

#endif
