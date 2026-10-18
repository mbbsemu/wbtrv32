#include "Key.h"

#include <cassert>
#include <cstring>
#include <sstream>

namespace btrieve {

// Types stored as a little endian integer. DATE (day, month, 2 byte year) and
// TIME (hundredths, seconds, minutes, hours) are too: read that way, their
// most significant field is in the high bytes, so they compare field by field.
static bool isSignedInteger(KeyDataType type) {
  return type == KeyDataType::Integer || type == KeyDataType::AutoInc ||
         type == KeyDataType::Currency;
}

static bool isBinaryInteger(KeyDataType type) {
  return isSignedInteger(type) || type == KeyDataType::Unsigned ||
         type == KeyDataType::UnsignedBinary ||
         type == KeyDataType::OldBinary || type == KeyDataType::Date ||
         type == KeyDataType::Time || type == KeyDataType::Timestamp;
}

// The number of decimal digits, excluding the sign, in a DECIMAL, MONEY or
// NUMERIC family segment, or 0 for other types.
static size_t getDecimalDigits(const KeyDefinition &segment) {
  const size_t length = segment.getLength();
  switch (segment.getDataType()) {
    case KeyDataType::Decimal:
    case KeyDataType::Money:
      // two digits per byte, less the sign nibble
      return length > 0 ? length * 2 - 1 : 0;
    case KeyDataType::Numeric:
    case KeyDataType::NumericSA:
      // the sign is overpunched on the last digit
      return length;
    case KeyDataType::NumericSTS:
      // the sign is its own last byte
      return length > 0 ? length - 1 : 0;
    default:
      return 0;
  }
}

// Decimal values with up to this many digits fit in an INTEGER.
static const size_t MAX_INTEGER_DIGITS = 18;

static bool isBfloat(const KeyDefinition &segment) {
  return segment.getDataType() == KeyDataType::Bfloat &&
         (segment.getLength() == 4 || segment.getLength() == 8);
}

static const char *getSqliteType(const KeyDefinition &segment) {
  auto type = segment.getDataType();
  const size_t digits = getDecimalDigits(segment);

  if (type == KeyDataType::NullIndicator ||
      (isBinaryInteger(type) && segment.getLength() <= 8) ||
      isBfloat(segment) || (digits > 0 && digits <= MAX_INTEGER_DIGITS)) {
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

// A BFLOAT (Microsoft Binary Format) value as an INTEGER that sorts the same.
// The last byte is the exponent, 0 for zero, and the rest is the mantissa with
// the sign in its top bit. With the sign removed, exponent then mantissa
// orders the magnitude exactly, unlike a conversion to double, which rounds
// an 8 byte BFLOAT's 55 bit mantissa.
static int64_t bfloatToInteger(const std::vector<uint8_t> &bytes) {
  const size_t mantissaBytes = bytes.size() - 1;
  const uint8_t exponent = bytes[mantissaBytes];
  if (exponent == 0) {
    return 0;
  }

  const unsigned int mantissaBits = mantissaBytes * 8 - 1;
  uint64_t mantissa = 0;
  for (size_t i = 0; i < mantissaBytes; ++i) {
    mantissa |= static_cast<uint64_t>(bytes[i]) << (i * 8);
  }
  const bool negative = (mantissa >> mantissaBits) & 1;
  mantissa &= (static_cast<uint64_t>(1) << mantissaBits) - 1;

  const int64_t magnitude = static_cast<int64_t>(
      (static_cast<uint64_t>(exponent) << mantissaBits) | mantissa);
  return negative ? -magnitude : magnitude;
}

static uint8_t asciiDigit(uint8_t c) {
  return (c >= '0' && c <= '9') ? c - '0' : 0;
}

// Decodes a DECIMAL, MONEY or NUMERIC family value into its digits, most
// significant first, and returns whether it's negative. Bytes that aren't
// digits, such as leading spaces, read as 0, and negative zero is zero.
static bool decodeDecimal(KeyDataType type, const std::vector<uint8_t> &bytes,
                          std::vector<uint8_t> &digits) {
  bool negative = false;
  const size_t last = bytes.size() - 1;

  switch (type) {
    case KeyDataType::Decimal:
    case KeyDataType::Money:
      // packed BCD, two digits per byte, and a final sign nibble: 0xD is
      // negative, 0xF and 0xC positive
      for (size_t i = 0; i < bytes.size(); ++i) {
        digits.push_back(bytes[i] >> 4);
        if (i < last) {
          digits.push_back(bytes[i] & 0xF);
        }
      }
      negative = (bytes[last] & 0xF) == 0xD;
      break;
    case KeyDataType::Numeric:
    case KeyDataType::NumericSA: {
      // ASCII digits, the last one overpunched with the sign
      for (size_t i = 0; i < last; ++i) {
        digits.push_back(asciiDigit(bytes[i]));
      }
      const uint8_t c = bytes[last];
      uint8_t digit = asciiDigit(c);
      if (type == KeyDataType::Numeric) {
        // EBCDIC: 0 to 9 are '{' and 'A' to 'I' positive, '}' and 'J' to 'R'
        // negative, or plain digits positive
        if (c >= 'A' && c <= 'I') {
          digit = c - 'A' + 1;
        } else if (c >= 'J' && c <= 'R') {
          digit = c - 'J' + 1;
          negative = true;
        } else if (c == '}') {
          negative = true;
        }
      } else {
        // ASCII: 0 to 9 are 'P' to 'Y' positive, 'p' to 'y' negative, or
        // plain digits positive
        if (c >= 'P' && c <= 'Y') {
          digit = c - 'P';
        } else if (c >= 'p' && c <= 'y') {
          digit = c - 'p';
          negative = true;
        }
      }
      digits.push_back(digit);
      break;
    }
    case KeyDataType::NumericSTS:
      // ASCII digits, then a '+' or '-'
      for (size_t i = 0; i < last; ++i) {
        digits.push_back(asciiDigit(bytes[i]));
      }
      negative = bytes[last] == '-';
      break;
    default:
      break;
  }

  if (std::all_of(digits.begin(), digits.end(),
                  [](uint8_t digit) { return digit == 0; })) {
    negative = false;
  }
  return negative;
}

// A DECIMAL, MONEY or NUMERIC family value as a signed INTEGER of its digits
// when they fit, ignoring the implied decimal point, which is the same for
// every value of the segment. Otherwise a BLOB that memcmp orders: a sign
// byte, 0 for negative, then a byte per digit, inverted when negative so
// larger magnitudes sort first.
static BindableValue decimalToSqliteObject(const KeyDefinition &segment,
                                           const std::vector<uint8_t> &bytes) {
  std::vector<uint8_t> digits;
  const bool negative = decodeDecimal(segment.getDataType(), bytes, digits);

  if (digits.size() <= MAX_INTEGER_DIGITS) {
    int64_t value = 0;
    for (uint8_t digit : digits) {
      value = value * 10 + digit;
    }
    return BindableValue(negative ? -value : value);
  }

  std::vector<uint8_t> blob;
  blob.push_back(negative ? 0 : 1);
  for (uint8_t digit : digits) {
    blob.push_back(negative ? 0xFF - digit : digit);
  }
  return BindableValue(blob);
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

  // the segment's bytes, with any missing ones zero
  auto segmentBytes = [&]() {
    std::vector<uint8_t> bytes(length);
    for (size_t i = 0; i < length; ++i) {
      bytes[i] = byteAt(i);
    }
    return bytes;
  };

  if (isBfloat(segment)) {
    return BindableValue(bfloatToInteger(segmentBytes()));
  } else if (getDecimalDigits(segment) > 0) {
    return decimalToSqliteObject(segment, segmentBytes());
  }

  switch (segment.getDataType()) {
    case KeyDataType::NullIndicator: {
      // every true null is the same unless the segment is DISTINCT, and they
      // sort after values, or first when the segment is descending
      const uint8_t indicator = byteAt(0);
      if (indicator == 0 || segment.isDistinctNullIndicator()) {
        return BindableValue(static_cast<int64_t>(indicator));
      }
      return BindableValue(static_cast<int64_t>(1));
    }
    case KeyDataType::AutoInc:
    case KeyDataType::Integer:
    case KeyDataType::Currency:
      // extend sign bit
      if (supplied > 0 && (byteAt(supplied - 1) & 0x80)) {
        value = -1;
      }
      // fall through on purpose
    case KeyDataType::Unsigned:
    case KeyDataType::UnsignedBinary:
    case KeyDataType::OldBinary:
    case KeyDataType::Date:
    case KeyDataType::Time:
    case KeyDataType::Timestamp:
      if (length > 0 && length <= 8) {
        for (size_t i = 0; i < supplied; ++i) {
          if (bigEndian) {
            subValue[7 - i] = byteAt(i);
          } else {
            subValue[i] = byteAt(i);
          }
        }
        // SQLite's INTEGER is signed, so flip an 8 byte unsigned value's top
        // bit to keep the values from 2^63 up above the rest
        if (length == 8 && !isSignedInteger(segment.getDataType())) {
          value ^= static_cast<uint64_t>(1) << 63;
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

  // A true null's field reads as zero bytes, so true nulls sort together as
  // duplicates whatever the field holds, and a search for one finds them.
  size_t indicatorOffset = 0;
  for (size_t i = 0; i + 1 < segments.size(); ++i) {
    if (segments[i].isNullIndicator() &&
        paddedKeyData[indicatorOffset] != 0) {
      auto field = paddedKeyData.begin() + indicatorOffset +
                   segments[i].getLength();
      std::fill(field, field + segments[i + 1].getLength(), 0);
    }
    indicatorOffset += segments[i].getLength();
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
