#include "Key.h"

#include <cassert>
#include <cstring>
#include <sstream>

namespace btrieve {

static const char *getSqliteType(const KeyDefinition &segment) {
  auto type = segment.getDataType();

  if ((type == KeyDataType::AutoInc || type == KeyDataType::Integer ||
       type == KeyDataType::Unsigned || type == KeyDataType::UnsignedBinary ||
       type == KeyDataType::OldBinary) &&
      segment.getLength() <= 8) {
    return "INTEGER";
  } else if (type == KeyDataType::Lstring || type == KeyDataType::Zstring ||
             type == KeyDataType::OldAscii) {
    return "TEXT";
  } else if (type == KeyDataType::Float) {
    return "REAL";
  }
  return "BLOB";
}

std::vector<std::string> Key::getSqliteColumnNames() const {
  if (!isComposite()) {
    return {getSqliteKeyName()};
  }

  std::vector<std::string> names;
  for (size_t i = 0; i < segments.size(); ++i) {
    names.push_back(getSqliteKeyName() + "_" + std::to_string(i));
  }
  return names;
}

std::vector<std::string> Key::getSqliteColumnDefinitions() const {
  auto names = getSqliteColumnNames();

  if (!isComposite() &&
      getPrimarySegment().getDataType() == KeyDataType::AutoInc) {
    return {names[0] + " INTEGER NOT NULL UNIQUE"};
  }

  std::vector<std::string> definitions;
  for (size_t i = 0; i < segments.size(); ++i) {
    std::stringstream sql;
    sql << names[i] << " " << getSqliteType(segments[i]);

    if (!isNullable()) {
      sql << " NOT NULL";
    }

    // a composite key's uniqueness spans all its columns, so it's enforced by
    // its index instead
    if (!isComposite() && isUnique()) {
      sql << " UNIQUE";
    }

    definitions.push_back(sql.str());
  }
  return definitions;
}

std::vector<uint8_t> Key::extractKeyDataFromRecord(
    std::basic_string_view<uint8_t> record) const {
  if (!isComposite()) {
    auto ptr = record.substr(getPrimarySegment().getOffset(),
                             getPrimarySegment().getLength());

    return std::vector<uint8_t>(ptr.data(), ptr.data() + ptr.size());
  }

  std::vector<uint8_t> composite(getLength());
  int i = 0;
  for (auto &segment : segments) {
    auto ptr = record.substr(segment.getOffset(), segment.getLength()).data();
    memcpy(composite.data() + i, ptr, segment.getLength());
    i += segment.getLength();
  }

  return composite;
}

static bool isAllSameByteValue(std::basic_string_view<uint8_t> data,
                               uint8_t value) {
  auto found = data.find_first_not_of(value);
  return found == std::string::npos;
}

static const char *createDefaultACS() {
  static uint8_t INTERNAL_DEFAULT_ACS[ACS_LENGTH];
  for (unsigned int i = 0; i < ACS_LENGTH; ++i) {
    INTERNAL_DEFAULT_ACS[i] = i;
  }
  return reinterpret_cast<const char *>(INTERNAL_DEFAULT_ACS);
}

std::vector<uint8_t> Key::applyACS(
    std::basic_string_view<uint8_t> keyData) const {
  static const char *DEFAULT_ACS = createDefaultACS();

  if (!requiresACS()) {
    return std::vector(keyData.data(), keyData.data() + keyData.size());
  }

  std::vector<uint8_t> ret(keyData.size());
  const uint8_t *src = keyData.data();
  const uint8_t *end = src + keyData.size();
  uint8_t *dst = ret.data();

  for (auto &segment : segments) {
    const auto *acs = segment.requiresACS() ? segment.getACS() : DEFAULT_ACS;
    int i;

    if (segment.getDataType() == KeyDataType::Lstring) {
      // skip the first length byte in the Lstring, so just copy verbatim
      *(dst++) = *(src++);
      i = 1;
    } else {
      i = 0;
    }

    for (; i < segment.getLength(); ++i) {
      *(dst++) = acs[*(src++)];
      if (src >= end) {
        return ret;
      }
    }
  }

  return ret;
}

static std::string extractPrefixedLengthString(
    std::basic_string_view<uint8_t> keyData) {
  size_t strlen = keyData.at(0);
  if (strlen >= keyData.size()) {
    strlen = keyData.size() - 1;
  }

  return std::string(reinterpret_cast<const char *>(keyData.data() + 1),
                     strlen);
}

static std::string extractNullTerminatedString(
    std::basic_string_view<uint8_t> keyData) {
  size_t strlen = keyData.find(static_cast<uint8_t>(0));
  if (strlen == std::basic_string_view<uint8_t>::npos) {
    strlen = keyData.size();
  }

  return std::string(reinterpret_cast<const char *>(keyData.data()), strlen);
}

bool Key::isNullKeyInRecord(std::basic_string_view<uint8_t> record) const {
  std::vector<uint8_t> keyData = extractKeyDataFromRecord(record);
  return isAllSameByteValue(
      std::basic_string_view<uint8_t>(keyData.data(), keyData.size()),
      getPrimarySegment().getNullValue());
}

static bool isBigEndian() {
  union {
    uint32_t i;
    char c[4];
  } bint = {0x01020304};

  return bint.c[0] == 1;
}

static bool bigEndian = isBigEndian();

// Converts one segment's (already ACS mapped) bytes to its SQLite value. data
// can be shorter than the segment when searching with a truncated key, in
// which case the missing bytes read as zero.
static BindableValue segmentDataToSqliteObject(
    const KeyDefinition &segment, std::basic_string_view<uint8_t> data) {
  const uint16_t length = segment.getLength();
  auto byteAt = [&](size_t i) -> uint8_t {
    return i < data.size() ? data[i] : 0;
  };

  uint64_t value = 0;
  uint8_t *subValue = reinterpret_cast<uint8_t *>(&value);

  switch (segment.getDataType()) {
    case KeyDataType::AutoInc:
    case KeyDataType::Integer:
      // extend sign bit
      if (length > 0 && (byteAt(length - 1) & 0x80)) {
        value = -1;
      }
      // fall through on purpose
    case KeyDataType::Unsigned:
    case KeyDataType::UnsignedBinary:
    case KeyDataType::OldBinary:
      if (length > 0 && length <= 8) {
        for (int i = 0; i < length; ++i) {
          if (bigEndian) {
            subValue[7 - i] = byteAt(i);
          } else {
            subValue[i] = byteAt(i);
          }
        }
        return BindableValue(value);
      } else {
        // integers with size > 8 are unsupported on sqlite, so we have to
        // convert to blobs. data is LSB, sqlite blobs compare msb (using
        // memcmp), so swap bytes prior to insert
        std::vector<uint8_t> copy(data.data(), data.data() + data.size());
        std::reverse(copy.begin(), copy.end());
        return BindableValue(copy);
      }
    case KeyDataType::Lstring:
      return BindableValue(extractPrefixedLengthString(data));
    case KeyDataType::Zstring:
    case KeyDataType::OldAscii:
      return BindableValue(extractNullTerminatedString(data));
    case KeyDataType::Float: {
      uint8_t buffer[8] = {0};
      memcpy(buffer, data.data(),
             std::min<size_t>(data.size(), sizeof(buffer)));
      switch (length) {
        case 4: {
          float f;
          memcpy(&f, buffer, sizeof(f));
          return BindableValue(static_cast<double>(f));
        }
        case 8: {
          double d;
          memcpy(&d, buffer, sizeof(d));
          return BindableValue(d);
        }
        default:
          // should never happen since we verify on db creation
          throw BtrieveException(BtrieveError::BadKeyLength,
                                 "Float key not 4/8 bytes");
      }
    }
    case KeyDataType::String:
    default:
      return BindableValue(data);
  }
}

BindableValue Key::singleKeyDataToSqliteObject(
    std::basic_string_view<uint8_t> keyData) const {
  if (isNullable() &&
      (isAllSameByteValue(
           keyData, getPrimarySegment().getNullValue()) ||  // legacy null check
       (getPrimarySegment().getDataType() ==
            KeyDataType::Zstring &&  // special handling for null strings
        keyData.size() > 0 &&
        keyData[0] == 0))) {
    return BindableValue();
  }

  std::vector<uint8_t> modifiedKeyData = applyACS(keyData);
  return segmentDataToSqliteObject(
      getPrimarySegment(), std::basic_string_view<uint8_t>(
                               modifiedKeyData.data(), modifiedKeyData.size()));
}

// NullAllSegments makes the key null when every segment is entirely its null
// value, NullAnySegment when any one is.
bool Key::isNullCompositeKeyData(
    std::basic_string_view<uint8_t> keyData) const {
  if (!isNullable()) {
    return false;
  }

  size_t nullSegments = 0;
  size_t offset = 0;
  for (auto &segment : segments) {
    if (isAllSameByteValue(keyData.substr(offset, segment.getLength()),
                           segment.getNullValue())) {
      ++nullSegments;
    }
    offset += segment.getLength();
  }

  if (getPrimarySegment().getAttributes() & NullAnySegment) {
    return nullSegments > 0;
  }
  return nullSegments == segments.size();
}

std::vector<BindableValue> Key::keyDataToSqliteObjects(
    std::basic_string_view<uint8_t> keyData) const {
  std::vector<BindableValue> values;

  if (!isComposite()) {
    values.push_back(singleKeyDataToSqliteObject(keyData));
    return values;
  }

  // search keys may be truncated, so treat missing bytes as zero
  std::vector<uint8_t> paddedKeyData(keyData.data(),
                                     keyData.data() + keyData.size());
  paddedKeyData.resize(getLength(), 0);
  std::basic_string_view<uint8_t> paddedKeyDataView(paddedKeyData.data(),
                                                    paddedKeyData.size());

  if (isNullCompositeKeyData(paddedKeyDataView)) {
    values.resize(segments.size());
    return values;
  }

  std::vector<uint8_t> modifiedKeyData = applyACS(paddedKeyDataView);
  std::basic_string_view<uint8_t> modifiedKeyDataView(modifiedKeyData.data(),
                                                      modifiedKeyData.size());
  size_t offset = 0;
  for (auto &segment : segments) {
    values.push_back(segmentDataToSqliteObject(
        segment, modifiedKeyDataView.substr(offset, segment.getLength())));
    offset += segment.getLength();
  }

  return values;
}

}  // namespace btrieve
