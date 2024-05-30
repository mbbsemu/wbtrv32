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

  const char *getACS() const { return getPrimarySegment().getACS(); }

  unsigned int getLength() const {
    unsigned int length = 0;
    for (auto &v : segments) {
      length += v.getLength();
    }
    return length;
  }

  std::string getSqliteKeyName() const {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "key_%d", getPrimarySegment().getNumber());
    return buf;
  }

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

/*
/// <summary>
///      Returns true if data contains all of value.
/// </summary>
private
static bool IsAllSameByteValue(ReadOnlySpan<byte> data, byte value) {
  foreach (byte b in data)
    if (b != value)
      return false;

  return true;
}

/// <summary>
///     Returns true if the key data inside record contains all of b.
/// </summary>
public
bool KeyInRecordIsAllSameByte(ReadOnlySpan<byte> record, byte b) =
    > IsAllSameByteValue(ExtractKeyDataFromRecord(record), b);

/// <summary>
///     Returns true if the key data inside record is all zero.
/// </summary>
public
bool KeyInRecordIsAllZero(ReadOnlySpan<byte> record) =
    > KeyInRecordIsAllSameByte(record, 0);

/// <summary>
///     Returns an object that can be used for inserting into the data_t key
///     column based on the type of this key, extracting from data.
/// </summary>
public
object ExtractKeyInRecordToSqliteObject(ReadOnlySpan<byte> data) =
    > KeyDataToSqliteObject(ExtractKeyDataFromRecord(data));

private
ReadOnlySpan<byte> ApplyACS(ReadOnlySpan<byte> keyData) {
  if (!RequiresACS)
    return keyData;

  var dst = new byte[Length];
  var offset = 0;
  foreach (var segment in Segments) {
    var dstSpan = dst.AsSpan().Slice(offset, segment.Length);
    var key = keyData.Slice(offset, segment.Length);
    if (segment.RequiresACS) {
      for (var i = 0; i < segment.Length; ++i) {
        dstSpan[i] = segment.ACS[key[i]];
      }
    } else {
      // simple copy
      key.CopyTo(dstSpan);
    }

    offset += segment.Length;
  }

  return dst;
}

/// <summary>
///     Returns an object that can be used for inserting into the data_t key
///     column based on the type of this key from keyData.
/// </summary>
public
object KeyDataToSqliteObject(ReadOnlySpan<byte> keyData) {
  if (IsNullable && IsAllSameByteValue(keyData, PrimarySegment.NullValue)) {
    return DBNull.Value;
  }

  keyData = ApplyACS(keyData);

  if (IsComposite)
    return keyData.ToArray();

  switch (PrimarySegment.DataType) {
  case EnumKeyDataType.Unsigned:
  case EnumKeyDataType.UnsignedBinary:
  case EnumKeyDataType.OldBinary:
    switch (PrimarySegment.Length) {
    case 2:
      return BitConverter.ToUInt16(keyData);
    case 4:
      return BitConverter.ToUInt32(keyData);
    case 6:
      return (ulong)BitConverter.ToUInt32(keyData.Slice(0, 4)) |
             (((ulong)BitConverter.ToUInt16(keyData.Slice(4, 2))) << 32);
    case 8:
      return BitConverter.ToUInt64(keyData);
    default:
      // data is LSB, sqlite blobs compare msb (using memcmp), so swap bytes
      // prior to insert
      var copy = keyData.ToArray();
      Array.Reverse<byte>(copy);
      return copy;
    }
  case EnumKeyDataType.AutoInc:
  case EnumKeyDataType.Integer:
    switch (PrimarySegment.Length) {
    case 2:
      return BitConverter.ToInt16(keyData);
    case 4:
      return BitConverter.ToInt32(keyData);
    case 6:
      return (long)BitConverter.ToUInt32(keyData.Slice(0, 4)) |
             (((long)BitConverter.ToInt16(keyData.Slice(4, 2))) << 32);
    case 8:
      return BitConverter.ToInt64(keyData);
    default:
      throw new ArgumentException(
          $ "Bad integer key length {PrimarySegment.Length}");
    }
  case EnumKeyDataType.String:
  case EnumKeyDataType.Lstring:
  case EnumKeyDataType.Zstring:
  case EnumKeyDataType.OldAscii:
    return ExtractNullTerminatedString(keyData);
  default:
    return keyData.ToArray();
  }
}

/// <summary>
///     Returns a null terminated string from b. Length will be between 0 and
///     b.Length.
/// </summary>
public
static string ExtractNullTerminatedString(ReadOnlySpan<byte> b) {
  var strlen = b.IndexOf((byte)0);
  if (strlen <= 0)
    strlen = b.Length;

  return Encoding.ASCII.GetString(b.Slice(0, strlen));
}

/// <summary>
///     Returns the SQLite column type when creating the initial database.
/// </summary>
public
string SqliteColumnType() {
  string type;

  if (IsComposite) {
    type = "BLOB";
  } else {
    switch (PrimarySegment.DataType) {
    case EnumKeyDataType.AutoInc:
      return "INTEGER NOT NULL UNIQUE";
    case EnumKeyDataType.Integer when PrimarySegment.Length <= 8:
    case EnumKeyDataType.Unsigned when PrimarySegment.Length <= 8:
    case EnumKeyDataType.UnsignedBinary when PrimarySegment.Length <= 8:
    case EnumKeyDataType.OldBinary when PrimarySegment.Length <= 8:
      type = "INTEGER";
      break;
    case EnumKeyDataType.String:
    case EnumKeyDataType.Lstring:
    case EnumKeyDataType.Zstring:
    case EnumKeyDataType.OldAscii:
      type = "TEXT";
      break;
    default:
      type = "BLOB";
      break;
    }
  }

  if (!IsNullable) {
    type += " NOT NULL";
  }

  if (IsUnique) {
    type += " UNIQUE";
  }

  return type;
}

public
BtrieveKey() { Segments = new List<BtrieveKeyDefinition>(); }

public
BtrieveKey(BtrieveKeyDefinition keyDefinition) {
  Segments = new List<BtrieveKeyDefinition>{keyDefinition};
}
}
}
*/

#endif
