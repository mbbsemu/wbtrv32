#include "Key.h"
#include <cassert>
#include <cstring>

namespace btrieve {

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

  switch (getPrimarySegment().getDataType()) {
  case KeyDataType::AutoInc:
  case KeyDataType::Integer:
    // extend sign bit
    if (data[getPrimarySegment().getLength() - 1] & 0xF0) {
      value = -1;
    }
    // fall through on purpose
  case KeyDataType::Unsigned:
  case KeyDataType::UnsignedBinary:
  case KeyDataType::OldBinary:
    if (getPrimarySegment().getLength() > 0 &&
        getPrimarySegment().getLength() <= 8) {
      uint64_t mask = 0xFFFFFFFFFFFFFF00u;
      for (int i = 0; i < getPrimarySegment().getLength(); ++i) {
        value &= mask;
        mask = mask << 8 | ((mask & 0xFF00000000000000u) >> 56);
        value |= static_cast<uint64_t>(*(data++)) << (8 * i);
      }
      return BindableValue(static_cast<uint64_t>(value));
    } else {
      // data is LSB, sqlite blobs compare msb (using memcmp), so swap bytes
      // prior to insert
      std::vector<uint8_t> copy(data, data + keyData.size());
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