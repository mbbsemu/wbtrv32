#ifndef __TEXT_H_
#define __TEXT_H_

#include <stdlib.h>

#include <cstring>
#include <filesystem>

#define _TEXT(s) L##s

namespace btrieve {

std::string toStdString(const wchar_t *str);

std::string toStdString(const std::filesystem::path &dbPath);

std::basic_string<wchar_t> toWideString(const char *str);

std::basic_string<wchar_t> toWideString(const std::filesystem::path &dbPath);
}  // namespace btrieve

#endif