#ifndef __KEY_DATA_TYPE_H_
#define __KEY_DATA_TYPE_H_

namespace btrieve {
enum KeyDataType {
  String = 0,
  Integer = 1,
  Float = 2,
  Date = 3,
  Time = 4,
  Decimal = 5,
  Money = 6,
  Logical = 7,
  Numeric = 8,
  Bfloat = 9,
  Lstring = 0xA,
  Zstring = 0xB,
  Unsigned = 0xD,
  UnsignedBinary = 0xE,
  AutoInc = 0xF,
  NumericSTS = 0x11,  // NUMERIC with a trailing '+' or '-' sign byte
  NumericSA = 0x12,   // NUMERIC with an ASCII rather than EBCDIC sign
  Currency = 0x13,
  Timestamp = 0x14,
  OldAscii = 0x20,
  OldBinary = 0x21,  // essentially an unsigned int, like Unsigned
};
}

#endif