#include "Text.h"

namespace btrieve {

#ifdef _WIN32

std::string toStdString(const wchar_t* str) {
  size_t destSize;
  size_t destBufferSizeInBytes = (wcslen(str) + 1) * sizeof(wchar_t);
  char* destBuffer = reinterpret_cast<char*>(_alloca(destBufferSizeInBytes));

  wcstombs_s(&destSize, destBuffer, destBufferSizeInBytes, str, _TRUNCATE);
  return std::string(destBuffer);
}

std::string toStdString(const std::filesystem::path& dbPath) {
  return toStdString(dbPath.c_str());
}

std::basic_string<wchar_t> toWideString(const std::filesystem::path& dbPath) {
  return dbPath.c_str();
}

std::basic_string<wchar_t> toWideString(const char* str) {
  size_t destSize;
  size_t destSizeInChars = strlen(str) + 1;
  wchar_t* destBuffer =
      reinterpret_cast<wchar_t*>(_alloca(destSizeInChars * sizeof(wchar_t)));

  mbstowcs_s(&destSize, destBuffer, destSizeInChars, str, _TRUNCATE);
  return std::basic_string<wchar_t>(destBuffer);
}

#else

#include <stdint.h>

#include <cstring>

std::string toStdString(const wchar_t *str) {
  // mbs could be multi byte, so expand a bit
  size_t destBufferSizeInBytes = (wcslen(str) + 1) * sizeof(wchar_t);
  char *destBuffer = reinterpret_cast<char *>(alloca(destBufferSizeInBytes));

  wcstombs(destBuffer, str, destBufferSizeInBytes);
  return std::string(destBuffer);
}

std::string toStdString(const std::filesystem::path &dbPath) {
  return dbPath.c_str();
}

std::basic_string<wchar_t> toWideString(const std::filesystem::path &dbPath) {
  return toWideString(dbPath.c_str());
}

std::basic_string<wchar_t> toWideString(const char *str) {
  size_t destBufferSizeInBytes = (strlen(str) + 1) * sizeof(wchar_t);
  wchar_t *destBuffer =
      reinterpret_cast<wchar_t *>(alloca(destBufferSizeInBytes));

  mbstowcs(destBuffer, str, destBufferSizeInBytes);
  return std::basic_string<wchar_t>(destBuffer);
}

#endif
}  // namespace btrieve
