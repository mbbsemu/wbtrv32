#include "Key.h"

#include <cassert>
#include <cstring>
#include <sstream>

namespace btrieve {

std::string Key::getSqliteColumnSql() const {
  std::stringstream sql;

  if (isComposite()) {
    sql << "BLOB";
  } else {
    auto type = getPrimarySegment().getDataType();

    if (type == KeyDataType::AutoInc) {
      return "INTEGER NOT NULL UNIQUE";
    } else if ((type == KeyDataType::Integer || type == KeyDataType::Unsigned ||
                type == KeyDataType::UnsignedBinary ||
                type == KeyDataType::OldBinary) &&
               getPrimarySegment().getLength() <= 8) {
      sql << "INTEGER";
    } else if (type == KeyDataType::Lstring || type == KeyDataType::Zstring ||
               type == KeyDataType::OldAscii) {
      sql << "TEXT";
    } else if (type == KeyDataType::Float) {
      sql << "REAL";
    } else {
      sql << "BLOB";
    }
  }

  if (!isNullable()) {
    sql << " NOT NULL";
  }

  if (isUnique()) {
    sql << " UNIQUE";
  }

  return sql.str();
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
    for (int i = 0; i < segment.getLength(); ++i) {
      *(dst++) = acs[*(src++)];
      if (src >= end) {
        return ret;
      }
    }
  }

  return ret;
}

static std::string extractNullTerminatedString(
    const std::vector<uint8_t> &keyData) {
  auto found = std::find(keyData.begin(), keyData.end(), 0);
  size_t strlen;
  if (found == keyData.end()) {
    strlen = keyData.size();
  } else {
    strlen = found - keyData.begin();
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

BindableValue Key::keyDataToSqliteObject(
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

  if (isComposite()) {
    return BindableValue(modifiedKeyData);
  }

  const uint8_t *data = modifiedKeyData.data();
  uint64_t value = 0;
  uint8_t *subValue = reinterpret_cast<uint8_t *>(&value);

  switch (getPrimarySegment().getDataType()) {
    case KeyDataType::AutoInc:
    case KeyDataType::Integer:
      // extend sign bit
      if (data[getPrimarySegment().getLength() - 1] & 0x80) {
        value = -1;
      }
      // fall through on purpose
    case KeyDataType::Unsigned:
    case KeyDataType::UnsignedBinary:
    case KeyDataType::OldBinary:
      if (getPrimarySegment().getLength() > 0 &&
          getPrimarySegment().getLength() <= 8) {
        for (int i = 0; i < getPrimarySegment().getLength(); ++i) {
          if (bigEndian) {
            subValue[7 - i] = data[i];
          } else {
            subValue[i] = data[i];
          }
        }
        return BindableValue(value);
      } else {
        // integers with size > 8 are unsupported on sqlite, so we have to
        // convert to blobs. data is LSB, sqlite blobs compare msb (using
        // memcmp), so swap bytes prior to insert
        std::vector<uint8_t> copy(data, data + modifiedKeyData.size());
        std::reverse(copy.begin(), copy.end());
        return BindableValue(copy);
      }
      break;
    case KeyDataType::Lstring: // TODO handle Lstring properly
    case KeyDataType::Zstring:
    case KeyDataType::OldAscii:
      return BindableValue(extractNullTerminatedString(modifiedKeyData));
    case KeyDataType::Float:
      switch (getPrimarySegment().getLength()) {
        case 4:
          return BindableValue(static_cast<double>(
              *reinterpret_cast<float *>(modifiedKeyData.data())));
        case 8:
          return BindableValue(
              *reinterpret_cast<double *>(modifiedKeyData.data()));
        default:
          // should never happen since we verify on db creation
          throw BtrieveException(BtrieveError::BadKeyLength,
                                 "Float key not 4/8 bytes");
          break;
      }
      break;
    case KeyDataType::String:
    default:
      return BindableValue(modifiedKeyData);
  }

  return BindableValue();
}

}  // namespace btrieve
