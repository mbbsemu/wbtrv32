#ifndef __BTRIEVE_KEY_DEF_H_
#define __BTRIEVE_KEY_DEF_H_

#include <cstdint>
#include <cstring>
#include <vector>

#include "AttributeMask.h"
#include "BtrieveException.h"
#include "KeyDataType.h"

#define ACS_LENGTH 256

namespace btrieve {

class KeyDefinition {
 public:
  KeyDefinition() {}

  KeyDefinition(uint16_t number_, uint16_t length_, uint16_t offset_,
                KeyDataType dataType_, uint16_t attributes_, bool segment_,
                uint16_t segmentOf_, uint16_t segmentIndex_, uint8_t nullValue_,
                const std::string &acsName_, const std::vector<char> &acs_)
      : number(number_),
        length(length_),
        offset(offset_),
        dataType(dataType_),
        attributes(attributes_),
        segment(segment_),
        segmentOf(segmentOf_),
        segmentIndex(segmentIndex_),
        nullValue(nullValue_),
        acsName(acsName_),
        acs(acs_) {
    if (requiresACS() && (acsName.empty() || acs.empty())) {
      throw BtrieveException(BtrieveError::InvalidACS,
                             "Key %d requires ACS, but none was provided",
                             number);
    }

    if (dataType_ == KeyDataType::Float && (length_ != 8 && length_ != 4)) {
      throw BtrieveException(
          BtrieveError::BadKeyLength,
          "Key was specified as a float but isn't size 4/8 bytes");
    }
  }

  KeyDefinition(const KeyDefinition &keyDefinition)
      : number(keyDefinition.number),
        length(keyDefinition.length),
        offset(keyDefinition.offset),
        dataType(keyDefinition.dataType),
        attributes(keyDefinition.attributes),
        segment(keyDefinition.segment),
        segmentOf(keyDefinition.segmentOf),
        segmentIndex(keyDefinition.segmentIndex),
        nullValue(keyDefinition.nullValue),
        acsName(keyDefinition.acsName),
        acs(keyDefinition.acs) {}

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

  const char *getACS() const {
    if (acs.empty()) {
      return nullptr;
    }

    return acs.data();
  }

  const char *getACSName() const {
    if (acsName.empty()) {
      return nullptr;
    }

    return acsName.c_str();
  }

  uint16_t getOffset() const { return offset; }

  uint16_t getLength() const { return length; }

  uint8_t getNullValue() const { return nullValue; }

  uint16_t getAttributes() const { return attributes; }

  KeyDataType getDataType() const { return dataType; }

  bool isSegment() const { return segment; }

  bool operator==(const KeyDefinition &other) const {
    return number == other.number && length == other.length &&
           offset == other.offset && dataType == other.dataType &&
           attributes == other.attributes && segment == other.segment &&
           segmentOf == other.segmentOf && nullValue == other.nullValue &&
           acsName == other.acsName && acs == other.acs;
  }

  uint16_t getSegmentIndex() const { return segmentIndex; }

  void setSegmentIndex(uint16_t segmentIndex) {
    this->segmentIndex = segmentIndex;
  }

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
  std::string acsName;
  std::vector<char> acs;
};
}  // namespace btrieve

#endif
