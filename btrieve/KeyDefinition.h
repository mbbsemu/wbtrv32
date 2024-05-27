#ifndef __BTRIEVE_KEY_DEF_H_
#define __BTRIEVE_KEY_DEF_H_

#include "AttributeMask.h"
#include "KeyDataType.h"
#include <cstdint>

namespace btrieve {

class KeyDefinition {
  {
    u16_t getPosition() { return offset + 1; }

    bool requiresACS() { return attributes & NumberedACS; }

    bool isModifiable() { return attributes & Modifiable; }

    bool allowDuplicates() {
      return attributes & (Duplicates | RepeatingDuplicatesKey);
    }

    bool isUnique() { return !allowDuplicates(); }

    bool isNullable() {
      return (attributes & (NullAllSegments | NullAnySegment)) || isString();
    }

    bool isString() { return dataType == DataType.String || dataType == DataType.Lstring ||
        dataType == DataType.Zstring || dataType == DataType.OldAscii);
    }

  private:
    u16_t number;
    u16_t length;
    u16_t offset;
    KeyDataType dataType;
    u16_t attributes;
    bool segment;
    u16_t segmentOf;
    u16_t segmentIndex;
    u8_t nullValue;
    u8_t *acs;
  }
}
