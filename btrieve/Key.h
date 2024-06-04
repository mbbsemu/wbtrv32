#ifndef __BTRIEVEKEY_H_
#define __BTRIEVEKEY_H_

#include "BindableValue.h"
#include "KeyDefinition.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace btrieve {

class Key {
public:
  Key() {}

  Key(const Key &key) : segments(key.segments) {}

  Key(const KeyDefinition *segments, size_t numSegments)
      : segments(segments, segments + numSegments) {
    updateSegmentIndices();
  }

  const KeyDefinition &getPrimarySegment() const { return segments[0]; }

  const std::vector<KeyDefinition> &getSegments() const { return segments; }

  uint16_t getNumber() const { return getPrimarySegment().getNumber(); }

  bool isComposite() const { return segments.size() > 1; }

  bool isModifiable() const { return getPrimarySegment().isModifiable(); }

  bool isUnique() const { return getPrimarySegment().isUnique(); }

  bool isNullable() const { return getPrimarySegment().isNullable(); }

  bool requiresACS() const {
    auto requiresACS = [&](const KeyDefinition &key) {
      return key.requiresACS();
    };
    return std::any_of(segments.begin(), segments.end(), requiresACS);
  }

  // Returns the ACS name if used, otherwise nullptr.
  const char *getACSName() const { return getPrimarySegment().getACSName(); }

  // Returns the ACS table (size 256) if used, otherwise nullptr.
  const char *getACS() const { return getPrimarySegment().getACS(); }

  unsigned int getLength() const {
    unsigned int length = 0;
    for (auto &v : segments) {
      length += v.getLength();
    }
    return length;
  }

  std::string getSqliteKeyName() const {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "key_%d", getPrimarySegment().getNumber());
    return buf;
  }

  std::string getSqliteColumnSql() const;

  std::vector<uint8_t>
  extractKeyDataFromRecord(std::basic_string_view<uint8_t> record) const;

  BindableValue
  keyDataToSqliteObject(std::basic_string_view<uint8_t> keyData) const;

  BindableValue extractKeyInRecordToSqliteObject(
      std::basic_string_view<uint8_t> record) const {
    auto keyData = extractKeyDataFromRecord(record);
    return keyDataToSqliteObject(
        std::basic_string_view<uint8_t>(keyData.data(), keyData.size()));
  }

  void addSegment(const KeyDefinition &keyDefinition) {
    segments.push_back(keyDefinition);
  }

  void updateSegmentIndices() {
    uint16_t i = 0;
    for (auto &segment : segments) {
      segment.setSegmentIndex(i++);
    }
  }

private:
  std::vector<uint8_t> applyACS(std::basic_string_view<uint8_t> keyData) const;

  std::vector<KeyDefinition> segments;
};
} // namespace btrieve

#endif
