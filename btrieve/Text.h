#ifndef __TEXT_H_
#define __TEXT_H_

#include <cstring>
#include <filesystem>
#include <stdlib.h>

#define _TEXT(s) L##s

namespace btrieve {

  std::string toStdString(const wchar_t *str);

  std::basic_string<wchar_t> toWideString(const char *str);

  std::basic_string<wchar_t> toWideString(const std::filesystem::path &dbPath);
}

#endif