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

static const uint8_t *createUpperCaseCollation() {
  static uint8_t UPPER_CASE[ACS_LENGTH];
  for (unsigned int i = 0; i < ACS_LENGTH; ++i) {
    UPPER_CASE[i] = (i >= 'a' && i <= 'z') ? i - 'a' + 'A' : i;
  }
  return UPPER_CASE;
}

// The table mapping a segment's bytes to the ones it sorts by: its ACS, upper
// case for a case insensitive segment, or nullptr when it sorts by its bytes.
static const uint8_t *getCollation(const KeyDefinition &segment) {
  static const uint8_t *UPPER_CASE = createUpperCaseCollation();

  if (segment.requiresACS()) {
    return reinterpret_cast<const uint8_t *>(segment.getACS());
  } else if (segment.isCaseInsensitive()) {
    return UPPER_CASE;
  }
  return nullptr;
}

std::vector<uint8_t> Key::applyCollation(
    std::basic_string_view<uint8_t> keyData) const {
  std::vector<uint8_t> ret(keyData.data(), keyData.data() + keyData.size());

  size_t offset = 0;
  for (auto &segment : segments) {
    if (offset >= ret.size()) {
      break;
    }

    const uint8_t *collation = getCollation(segment);
    if (collation != nullptr) {
      const size_t end =
          std::min<size_t>(offset + segment.getLength(), ret.size());
      // an LSTRING's first byte is its length, which isn't collated
      size_t i = offset + (segment.getDataType() == KeyDataType::Lstring);
      for (; i < end; ++i) {
        ret[i] = collation[ret[i]];
      }
    }
    offset += segment.getLength();
  }

  return ret;
}

static std::string extractPrefixedLengthString(
    std::basic_string_view<uint8_t> keyData) {
  // a truncated search key can stop before the length
  if (keyData.empty()) {
    return "";
  }

  size_t strlen = keyData[0];
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

// Converts one segment's (already ACS mapped) bytes to its SQLite value.
// Searching with a truncated key supplies only the first supplied bytes of the
// segment, possibly none. The missing bytes read as zero, except that a signed
// integer's missing high bytes extend the sign of the last byte supplied, so
// 0xFF for a 2 byte INTEGER is -1 rather than 255. data can also be shorter
// than the segment, in which case the bytes past it are missing too.
static BindableValue segmentDataToSqliteObject(
    const KeyDefinition &segment, std::basic_string_view<uint8_t> data,
    size_t supplied) {
  const uint16_t length = segment.getLength();
  supplied = std::min<size_t>({supplied, length, data.size()});
  auto byteAt = [&](size_t i) -> uint8_t {
    return i < supplied ? data[i] : 0;
  };

  uint64_t value = 0;
  uint8_t *subValue = reinterpret_cast<uint8_t *>(&value);

  switch (segment.getDataType()) {
    case KeyDataType::AutoInc:
    case KeyDataType::Integer:
      // extend sign bit
      if (supplied > 0 && (byteAt(supplied - 1) & 0x80)) {
        value = -1;
      }
      // fall through on purpose
    case KeyDataType::Unsigned:
    case KeyDataType::UnsignedBinary:
    case KeyDataType::OldBinary:
      if (length > 0 && length <= 8) {
        for (size_t i = 0; i < supplied; ++i) {
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
    std::basic_string_view<uint8_t> keyData, bool allowNull) const {
  if (allowNull && isNullKeyData(keyData)) {
    return BindableValue();
  }

  std::vector<uint8_t> modifiedKeyData = applyCollation(keyData);
  return segmentDataToSqliteObject(
      getPrimarySegment(),
      std::basic_string_view<uint8_t>(modifiedKeyData.data(),
                                      modifiedKeyData.size()),
      keyData.size());
}

// Only a key declared nullable can be null. Otherwise an empty string is an
// ordinary value: Btrieve indexes it, and range queries must find it (SQL
// comparisons never match NULL). NullAllSegments makes the key null when every
// segment is entirely its null value, NullAnySegment when any one is. That
// holds for strings too, so a ZSTRING with bytes after its terminator isn't
// null, even though it compares equal to an empty string.
bool Key::isNullKeyData(std::basic_string_view<uint8_t> keyData) const {
  if (!hasNullAttribute()) {
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
    std::basic_string_view<uint8_t> keyData, bool allowNull) const {
  std::vector<BindableValue> values;

  if (!isComposite()) {
    values.push_back(singleKeyDataToSqliteObject(keyData, allowNull));
    return values;
  }

  // search keys may be truncated, so pad with zeros for the null check and
  // ACS, and tell each segment's conversion how many of its bytes were supplied
  std::vector<uint8_t> paddedKeyData(keyData.data(),
                                     keyData.data() + keyData.size());
  paddedKeyData.resize(getLength(), 0);
  std::basic_string_view<uint8_t> paddedKeyDataView(paddedKeyData.data(),
                                                    paddedKeyData.size());

  if (allowNull && isNullKeyData(paddedKeyDataView)) {
    values.resize(segments.size());
    return values;
  }

  std::vector<uint8_t> modifiedKeyData = applyCollation(paddedKeyDataView);
  std::basic_string_view<uint8_t> modifiedKeyDataView(modifiedKeyData.data(),
                                                      modifiedKeyData.size());
  size_t offset = 0;
  for (auto &segment : segments) {
    const size_t supplied =
        keyData.size() > offset ? keyData.size() - offset : 0;
    values.push_back(segmentDataToSqliteObject(
        segment, modifiedKeyDataView.substr(offset, segment.getLength()),
        supplied));
    offset += segment.getLength();
  }

  return values;
}

}  // namespace btrieve
