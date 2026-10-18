#ifndef __BTRIEVEKEY_H_
#define __BTRIEVEKEY_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "BindableValue.h"
#include "KeyDefinition.h"

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

  // Whether the key opts in to null values through the null attributes. Only
  // such a key stores a null key, as NULL columns, and Btrieve leaves records
  // with a null key out of the index.
  bool hasNullAttribute() const {
    return getPrimarySegment().getAttributes() &
           (NullAllSegments | NullAnySegment);
  }

  // Composite keys are only null when they opt in through the null attributes,
  // since a composite with e.g. an empty string segment is still a real key.
  bool isNullable() const {
    if (isComposite()) {
      return hasNullAttribute();
    }
    return getPrimarySegment().isNullable();
  }

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

  // A single segment key is stored in one column named key_N. A composite key
  // stores each segment in its own typed column named key_N_S, so SQLite
  // compares segment by segment rather than memcmp'ing raw bytes, which
  // misorders little endian integers, signed values, floats and strings.
  std::vector<std::string> getSqliteColumnNames() const;

  // Column definitions for CREATE TABLE, parallel to getSqliteColumnNames().
  std::vector<std::string> getSqliteColumnDefinitions() const;

  std::vector<uint8_t> extractKeyDataFromRecord(
      std::basic_string_view<uint8_t> record) const;

  // Converts key data to one value per column, parallel to
  // getSqliteColumnNames(). A null key is all nulls.
  std::vector<BindableValue> keyDataToSqliteObjects(
      std::basic_string_view<uint8_t> keyData) const {
    return keyDataToSqliteObjects(keyData, true);
  }

  // Converts a search key for a range comparison. Unlike a stored key it's
  // never null: a partial key search leaves trailing segments low, which can
  // match the null value, and SQL comparisons never match NULL.
  std::vector<BindableValue> searchKeyDataToSqliteObjects(
      std::basic_string_view<uint8_t> keyData) const {
    return keyDataToSqliteObjects(keyData, false);
  }

  std::vector<BindableValue> extractKeyInRecordToSqliteObjects(
      std::basic_string_view<uint8_t> record) const {
    auto keyData = extractKeyDataFromRecord(record);
    return keyDataToSqliteObjects(
        std::basic_string_view<uint8_t>(keyData.data(), keyData.size()));
  }

  bool isNullKeyInRecord(std::basic_string_view<uint8_t> record) const;

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

  std::vector<BindableValue> keyDataToSqliteObjects(
      std::basic_string_view<uint8_t> keyData, bool allowNull) const;

  BindableValue singleKeyDataToSqliteObject(
      std::basic_string_view<uint8_t> keyData, bool allowNull) const;

  bool isNullCompositeKeyData(std::basic_string_view<uint8_t> keyData) const;

  std::vector<KeyDefinition> segments;
};
}  // namespace btrieve

#endif
