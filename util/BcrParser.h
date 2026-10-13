#ifndef __BCR_PARSER_H_
#define __BCR_PARSER_H_

#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "btrieve/KeyDataType.h"

// Parser for Btrieve description files (.BCR), the text format consumed by
// `butil -create`, as documented at
// https://docs.actian.com/psql/psqlv13/index.html#page/advops%2Fdescfile.htm
//
// A description file is a sequence of whitespace separated keyword=value
// elements in a fixed order: file level elements first, then one group of
// elements per key segment, then an optional alternate collating sequence
// file name. /* ... */ comments are allowed anywhere.
namespace bcr {

class BcrParseException : public std::runtime_error {
 public:
  explicit BcrParseException(const std::string &message)
      : std::runtime_error(message) {}
};

enum class NullKey { None, AllSegments, AnySegment };

struct KeySegment {
  uint16_t position = 0;  // 1-based, as written in the description file
  uint16_t length = 0;
  bool duplicates = false;
  bool modifiable = false;
  btrieve::KeyDataType type = btrieve::KeyDataType::String;
  bool descending = false;
  bool alternate = false;
  bool repeatingDuplicates = false;
  NullKey nullKey = NullKey::None;
  uint8_t nullValue = 0;
  // true if another segment of the same key follows this one
  bool segment = false;
  // ACS file named at the end of this key, if any
  std::string acsFileName;
};

struct Key {
  std::vector<KeySegment> segments;
};

struct Description {
  uint16_t recordLength = 0;
  bool variable = false;
  uint8_t dupkey = 0;
  bool truncate = false;
  bool compress = false;
  uint16_t pageSize = 0;
  uint16_t allocation = 0;
  bool keyOnly = false;
  uint8_t freeSpaceThreshold = 5;
  bool vats = false;
  bool balance = false;
  std::vector<Key> keys;
  // ACS file named at the end of the file, the default for every key with
  // alternate=y that doesn't name its own
  std::string acsFileName;
};

// Parses the text of a description file, throwing BcrParseException on any
// syntax or validation error.
Description parse(std::string_view text);

// Reads and parses a description file from disk.
Description parseFile(const std::filesystem::path &path);

// Serializes a description into the data buffer expected by the Btrieve
// Create operation: a file spec, followed by a key spec per key segment,
// followed by the alternate collating sequences referenced by those keys.
// Relative ACS file names are resolved against acsDirectory.
std::vector<uint8_t> toCreateBuffer(const Description &description,
                                    const std::filesystem::path &acsDirectory);

}  // namespace bcr

#endif
