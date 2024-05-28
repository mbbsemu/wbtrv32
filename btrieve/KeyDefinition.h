#ifndef __BTRIEVE_KEY_DEF_H_
#define __BTRIEVE_KEY_DEF_H_

#include "AttributeMask.h"
#include "KeyDataType.h"
#include <cstdint>
#include <cstring>
#include <vector>

namespace btrieve {

class KeyDefinition {
public:
  KeyDefinition(uint16_t number, uint16_t length, uint16_t offset,
                KeyDataType dataType, uint16_t attributes, bool segment,
                uint16_t segmentOf, uint16_t segmentIndex, uint8_t nullValue,
                const char *acs)
      : number(number), length(length), offset(offset), dataType(dataType),
        attributes(attributes), segment(segment), segmentOf(segmentOf),
        segmentIndex(segmentIndex), nullValue(nullValue) {
    if (acs != nullptr) {
      memcpy(this->acs, acs, 256);
    } else {
      memset(this->acs, 0, 256);
    }
  }

  KeyDefinition(const KeyDefinition &keyDefinition)
      : number(keyDefinition.number), length(keyDefinition.length),
        offset(keyDefinition.offset), dataType(keyDefinition.dataType),
        attributes(keyDefinition.attributes), segment(keyDefinition.segment),
        segmentOf(keyDefinition.segmentOf),
        segmentIndex(keyDefinition.segmentIndex),
        nullValue(keyDefinition.nullValue) {
    if (keyDefinition.acs != nullptr) {
      memcpy(this->acs, keyDefinition.acs, 256);
    } else {
      memset(this->acs, 0, 256);
    }
  }

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

  const char *getACS() const { return acs; }

  uint16_t getOffset() const { return offset; }

  uint16_t getLength() const { return length; }

  uint8_t getNullValue() const { return nullValue; }

  KeyDataType getDataType() const { return dataType; }

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
  char acs[256];
};
} // namespace btrieve

#endif
