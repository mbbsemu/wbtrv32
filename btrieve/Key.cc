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
    } else if (type == KeyDataType::String || type == KeyDataType::Lstring ||
               type == KeyDataType::Zstring || type == KeyDataType::OldAscii) {
      sql << "TEXT";
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

std::vector<uint8_t>
Key::extractKeyDataFromRecord(std::basic_string_view<uint8_t> record) const {
  if (!isComposite()) {
    auto ptr = record
                   .substr(getPrimarySegment().getOffset(),
                           getPrimarySegment().getLength())
                   .data();
    return std::vector<uint8_t>(ptr, ptr + getPrimarySegment().getLength());
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

std::vector<uint8_t>
Key::applyACS(std::basic_string_view<uint8_t> keyData) const {
  if (!requiresACS()) {
    return std::vector(keyData.data(), keyData.data() + keyData.size());
  }

  std::vector<uint8_t> dst(keyData.size());
  int offset = 0;
  for (auto &segment : segments) {
    uint8_t *dstSpan = dst.data() + offset;
    const uint8_t *key = keyData.data() + offset;
    if (segment.requiresACS()) {
      const auto acs = segment.getACS();
      for (int i = 0; i < segment.getLength(); ++i) {
        dstSpan[i] = acs[key[i]];
      }
    } else {
      // simple copy
      memcpy(dstSpan, key, segment.getLength());
    }

    offset += segment.getLength();
  }

  return dst;
}

static std::string
extractNullTerminatedString(const std::vector<uint8_t> &keyData) {
  auto found = std::find(keyData.begin(), keyData.end(), 0);
  size_t strlen;
  if (found == keyData.begin() || found == keyData.end()) {
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

BindableValue
Key::keyDataToSqliteObject(std::basic_string_view<uint8_t> keyData) const {
  if (isNullable() &&
      isAllSameByteValue(keyData, getPrimarySegment().getNullValue())) {
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
      // integers with size > 8 are unsupported on sqlite, so we have to convert
      // to blobs.
      // data is LSB, sqlite blobs compare msb (using memcmp), so swap bytes
      // prior to insert
      std::vector<uint8_t> copy(data, data + modifiedKeyData.size());
      std::reverse(copy.begin(), copy.end());
      return BindableValue(copy);
    }
    break;
  case KeyDataType::String:
  case KeyDataType::Lstring:
  case KeyDataType::Zstring:
  case KeyDataType::OldAscii:
    return BindableValue(extractNullTerminatedString(modifiedKeyData));
  default:
    return BindableValue(modifiedKeyData);
  }

  return BindableValue();
}

} // namespace btrieve