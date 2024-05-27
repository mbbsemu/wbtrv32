#ifndef __BTRIEVE_KEY_DEF_H_
#define __BTRIEVE_KEY_DEF_H_

#include "AttributeMask.h"
#include "KeyDataType.h"
#include <cstdint>
#include <vector>

namespace btrieve {

class KeyDefinition {
public:
  uint16_t getPosition() const { return offset + 1; }

  bool requiresACS() const { return attributes & NumberedACS; }

  bool isModifiable() const { return attributes & Modifiable; }

  bool allowDuplicates() const {
    return attributes & (Duplicates | RepeatingDuplicatesKey);
  }

  bool isUnique() const { return !allowDuplicates(); }

  bool isNullable() const {
    return (attributes & (NullAllSegments | NullAnySegment)) || isString();
  }

  bool isString() const {
    return dataType == KeyDataType::String ||
           dataType == KeyDataType::Lstring ||
           dataType == KeyDataType::Zstring ||
           dataType == KeyDataType::OldAscii;
  }

  uint16_t getNumber() const { return number; }

  const uint8_t *getACS() const { return acs.data(); }

  uint16_t getLength() const { return length; }

private:
  uint16_t number;
  uint16_t length;
  uint16_t offset;
  KeyDataType dataType;
  uint16_t attributes;
  bool segment;
  uint16_t segmentOf;
  uint16_t segmentIndex;
  uint8_t nullValue;
  std::vector<uint8_t> acs;
};
} // namespace btrieve

#endif
