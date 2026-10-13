// 26.10.13.4B 06:53PM
#include "Key.h"

#include "AttributeMask.h"
#include "gtest/gtest.h"

#ifdef __linux__
#undef INSTANTIATE_TEST_CASE_P
#define INSTANTIATE_TEST_CASE_P INSTANTIATE_TEST_SUITE_P
#endif

using namespace btrieve;

namespace {

static const unsigned char DATA_NEGATIVE[] = {0xF1, 0xF2, 0xF3, 0xF4,
                                              0xF5, 0xF6, 0xF7, 0xF8};
static const unsigned char DATA_POSITIVE[] = {0x1, 0x2, 0x3, 0x4,
                                              0x5, 0x6, 0x7, 0x8};
static const char STRING_DATA[32] = "\x04Test";

TEST(Key, SingleKeySegment) {
  KeyDefinition keyDefinition(0, 10, 0, KeyDataType::String, 0, false, 0, 0, 0,
                              "", std::vector<char>());

  Key key(&keyDefinition, 1);

  EXPECT_EQ(key.getNumber(), 0);
  EXPECT_EQ(key.getLength(), 10u);
  EXPECT_EQ(key.getSqliteKeyName(), "key_0");
  EXPECT_FALSE(key.requiresACS());
}

struct ParameterizedFixtureType {
  const unsigned char *data;
  int length;
  KeyDataType type;
  uint64_t expected;
};

class ParameterizedFixture
    : public testing::TestWithParam<ParameterizedFixtureType> {};

TEST_P(ParameterizedFixture, IntegerTypeConversions) {
  const ParameterizedFixtureType &view = GetParam();

  KeyDefinition keyDefinition(0, view.length, 0, view.type, UseExtendedDataType,
                              false, 0, 0, 0, "", std::vector<char>());

  Key key(&keyDefinition, 1);

  auto actual = static_cast<uint64_t>(
      key.keyDataToSqliteObject(
             std::basic_string_view<uint8_t>(view.data, view.length))
          .getIntegerValue());

  EXPECT_EQ(actual, view.expected);
}

static std::vector<ParameterizedFixtureType> create() {
  return std::vector<ParameterizedFixtureType>{
      {DATA_NEGATIVE, 2, KeyDataType::Integer, static_cast<uint64_t>(-3343)},
      {DATA_NEGATIVE, 4, KeyDataType::Integer,
       static_cast<uint64_t>(-185339151)},
      {DATA_NEGATIVE, 6, KeyDataType::Integer,
       static_cast<uint64_t>(-9938739662095l)},
      {DATA_NEGATIVE, 8, KeyDataType::Integer,
       static_cast<uint64_t>(-506664896818842895)},
      {DATA_NEGATIVE, 2, KeyDataType::AutoInc, static_cast<uint64_t>(-3343)},
      {DATA_NEGATIVE, 4, KeyDataType::AutoInc,
       static_cast<uint64_t>(-185339151)},
      {DATA_NEGATIVE, 6, KeyDataType::AutoInc,
       static_cast<uint64_t>(-9938739662095)},
      {DATA_NEGATIVE, 8, KeyDataType::AutoInc,
       static_cast<uint64_t>(-506664896818842895)},
      {DATA_NEGATIVE, 2, KeyDataType::Unsigned, 0xF2F1},
      {DATA_NEGATIVE, 4, KeyDataType::Unsigned, 0xF4F3F2F1},
      {DATA_NEGATIVE, 6, KeyDataType::Unsigned, 0xF6F5F4F3F2F1},
      {DATA_NEGATIVE, 8, KeyDataType::Unsigned, 0xF8F7F6F5F4F3F2F1},
      {DATA_NEGATIVE, 2, KeyDataType::UnsignedBinary, 0xF2F1},
      {DATA_NEGATIVE, 4, KeyDataType::UnsignedBinary, 0xF4F3F2F1},
      {DATA_NEGATIVE, 6, KeyDataType::UnsignedBinary, 0xF6F5F4F3F2F1},
      {DATA_NEGATIVE, 8, KeyDataType::UnsignedBinary, 0xF8F7F6F5F4F3F2F1},
      {DATA_NEGATIVE, 2, KeyDataType::OldBinary, 0xF2F1},
      {DATA_NEGATIVE, 4, KeyDataType::OldBinary, 0xF4F3F2F1},
      {DATA_NEGATIVE, 6, KeyDataType::OldBinary, 0xF6F5F4F3F2F1},
      {DATA_NEGATIVE, 8, KeyDataType::OldBinary, 0xF8F7F6F5F4F3F2F1},
      {DATA_POSITIVE, 2, KeyDataType::Integer, 0x201},
      {DATA_POSITIVE, 4, KeyDataType::Integer, 0x4030201},
      {DATA_POSITIVE, 6, KeyDataType::Integer, 0x60504030201},
      {DATA_POSITIVE, 8, KeyDataType::Integer, 0x807060504030201},
      {DATA_POSITIVE, 2, KeyDataType::AutoInc, 0x201},
      {DATA_POSITIVE, 4, KeyDataType::AutoInc, 0x4030201},
      {DATA_POSITIVE, 6, KeyDataType::AutoInc, 0x60504030201},
      {DATA_POSITIVE, 8, KeyDataType::AutoInc, 0x807060504030201},
      {DATA_POSITIVE, 2, KeyDataType::UnsignedBinary, 0x201},
      {DATA_POSITIVE, 4, KeyDataType::UnsignedBinary, 0x4030201},
      {DATA_POSITIVE, 6, KeyDataType::UnsignedBinary, 0x60504030201},
      {DATA_POSITIVE, 8, KeyDataType::UnsignedBinary, 0x807060504030201},
      {DATA_POSITIVE, 2, KeyDataType::Unsigned, 0x201},
      {DATA_POSITIVE, 4, KeyDataType::Unsigned, 0x4030201},
      {DATA_POSITIVE, 6, KeyDataType::Unsigned, 0x60504030201},
      {DATA_POSITIVE, 8, KeyDataType::Unsigned, 0x807060504030201},
      {DATA_POSITIVE, 2, KeyDataType::OldBinary, 0x201},
      {DATA_POSITIVE, 4, KeyDataType::OldBinary, 0x4030201},
      {DATA_POSITIVE, 6, KeyDataType::OldBinary, 0x60504030201},
      {DATA_POSITIVE, 8, KeyDataType::OldBinary, 0x807060504030201}};
}

INSTANTIATE_TEST_CASE_P(Key, ParameterizedFixture,
                        ::testing::ValuesIn(create()));

struct ParameterizedStringFixtureType {
  int length;
  KeyDataType type;
  const char *expected;
};

class ParameterizedStringFixture
    : public testing::TestWithParam<ParameterizedStringFixtureType> {};

TEST_P(ParameterizedStringFixture, StringTypeConversions) {
  const ParameterizedStringFixtureType &view = GetParam();

  KeyDefinition keyDefinition(0, view.length, 0, view.type, UseExtendedDataType,
                              false, 0, 0, 0, "", std::vector<char>());

  Key key(&keyDefinition, 1);

  auto actual = key
                    .keyDataToSqliteObject(std::basic_string_view<uint8_t>(
                        reinterpret_cast<const uint8_t *>(STRING_DATA) +
                            (view.type == KeyDataType::Lstring ? 0 : 1),
                        view.length))
                    .getStringValue();

  EXPECT_EQ(actual, view.expected);
}

static std::vector<ParameterizedStringFixtureType> createStringData() {
  return std::vector<ParameterizedStringFixtureType>{
      {32, KeyDataType::Lstring, "Test"},  {5, KeyDataType::Lstring, "Test"},
      {4, KeyDataType::Lstring, "Tes"},    {3, KeyDataType::Lstring, "Te"},
      {2, KeyDataType::Lstring, "T"},      {1, KeyDataType::Lstring, ""},
      {32, KeyDataType::Zstring, "Test"},  {5, KeyDataType::Zstring, "Test"},
      {4, KeyDataType::Zstring, "Test"},   {3, KeyDataType::Zstring, "Tes"},
      {2, KeyDataType::Zstring, "Te"},     {1, KeyDataType::Zstring, "T"},
      {32, KeyDataType::OldAscii, "Test"}, {5, KeyDataType::OldAscii, "Test"},
      {4, KeyDataType::OldAscii, "Test"},  {3, KeyDataType::OldAscii, "Tes"},
      {2, KeyDataType::OldAscii, "Te"},    {1, KeyDataType::OldAscii, "T"}};
}

INSTANTIATE_TEST_CASE_P(Key, ParameterizedStringFixture,
                        ::testing::ValuesIn(createStringData()));

TEST(Key, SegmentIndices) {
  KeyDefinition keyDefinitions[2] = {
      KeyDefinition(0, 8, 2, KeyDataType::Integer, UseExtendedDataType, true, 0,
                    0, 0, "", std::vector<char>()),
      KeyDefinition(0, 4, 20, KeyDataType::Zstring, UseExtendedDataType, false,
                    1, 0, 0, "", std::vector<char>())};

  Key key(keyDefinitions, 2);

  EXPECT_EQ(key.getPrimarySegment().getSegmentIndex(), 0);
  EXPECT_EQ(key.getSegments()[0].getSegmentIndex(), 0);
  EXPECT_EQ(key.getSegments()[1].getSegmentIndex(), 1);
}

TEST(Key, CompositeKeyConcatentation) {
  KeyDefinition keyDefinitions[2] = {
      KeyDefinition(0, 8, 2, KeyDataType::Integer, UseExtendedDataType, true, 0,
                    0, 0, "", std::vector<char>()),
      KeyDefinition(0, 4, 20, KeyDataType::Zstring, UseExtendedDataType, false,
                    1, 0, 0, "", std::vector<char>())};

  uint8_t record[128];
  memset(record, 0xFF, sizeof(record));
  // first segment is all 0x5
  memset(record + 2, 0x5, 8);
  // second segment is just a letter
  memset(record + 20, 'T', 4);

  Key key(keyDefinitions, 2);

  auto actual = key.extractKeyInRecordToSqliteObject(
                       std::basic_string_view<uint8_t>(record, sizeof(record)))
                    .getBlobValue();

  // the integer segment is stored big-endian with its sign bit flipped so the
  // blob orders numerically; the string segment is copied as-is
  uint8_t expected[] = {0x85, 0x5, 0x5, 0x5, 0x5, 0x5,
                        0x5,  0x5, 'T', 'T', 'T', 'T'};

  EXPECT_EQ(actual,
            std::vector<uint8_t>(expected, expected + sizeof(expected)));
}

// Two-segment key shaped like The Major BBS WGSGEN2.DAT key 0: a 30-byte
// zstring (often empty) followed by a 25-byte zstring module name.
static void wgsgen2Key(KeyDefinition (&defs)[2], uint16_t attributes) {
  defs[0] = KeyDefinition(0, 30, 0, KeyDataType::Zstring,
                          attributes | SegmentedKey, true, 0, 0, 0, "",
                          std::vector<char>());
  defs[1] = KeyDefinition(0, 25, 30, KeyDataType::Zstring, attributes, true, 0,
                          1, 0, "", std::vector<char>());
}

static BindableValue wgsgen2KeyValue(const Key &key, const char *first,
                                     const char *second) {
  uint8_t record[55];
  memset(record, 0, sizeof(record));
  memcpy(record, first, strlen(first));
  memcpy(record + 30, second, strlen(second));
  return key.extractKeyInRecordToSqliteObject(
      std::basic_string_view<uint8_t>(record, sizeof(record)));
}

// An empty leading string segment is an ordinary value: the later segments
// must still take part in the key, so records sort by them as in Btrieve.
TEST(Key, CompositeKeyWithEmptyLeadingStringIsNotNull) {
  KeyDefinition defs[2];
  wgsgen2Key(defs, UseExtendedDataType | Duplicates | Modifiable);
  Key key(defs, 2);

  auto snmytime = wgsgen2KeyValue(key, "", "SNMYTIME");
  auto wormsg = wgsgen2KeyValue(key, "", "WORMSG");

  ASSERT_FALSE(snmytime.isNull());
  ASSERT_FALSE(wormsg.isNull());
  EXPECT_LT(snmytime.getBlobValue(), wormsg.getBlobValue());
}

// A composite key whose bytes are all zero is a real value unless the key is
// declared nullable.
TEST(Key, CompositeKeyAllZeroIsNotNullWithoutNullAttribute) {
  KeyDefinition defs[2];
  wgsgen2Key(defs, UseExtendedDataType | Duplicates | Modifiable);
  Key key(defs, 2);

  EXPECT_FALSE(wgsgen2KeyValue(key, "", "").isNull());
}

TEST(Key, CompositeKeyAllNullValueIsNullWithNullAttribute) {
  KeyDefinition defs[2];
  wgsgen2Key(defs, UseExtendedDataType | Duplicates | NullAllSegments);
  Key key(defs, 2);

  EXPECT_TRUE(wgsgen2KeyValue(key, "", "").isNull());
  EXPECT_FALSE(wgsgen2KeyValue(key, "", "WORMSG").isNull());
}

// Composite keys are compared as blobs, so integer segments must be encoded
// so that byte order equals numeric order (Btrieve compares them as numbers).
static BindableValue twoIntKeyValue(KeyDataType type, uint32_t first,
                                    uint32_t second) {
  KeyDefinition defs[2] = {
      KeyDefinition(0, 2, 0, type, UseExtendedDataType | SegmentedKey, true, 0,
                    0, 0, "", std::vector<char>()),
      KeyDefinition(0, 4, 2, type, UseExtendedDataType, true, 0, 1, 0, "",
                    std::vector<char>())};
  Key key(defs, 2);
  uint8_t record[6];
  memcpy(record, &first, 2);
  memcpy(record + 2, &second, 4);
  return key.extractKeyInRecordToSqliteObject(
      std::basic_string_view<uint8_t>(record, sizeof(record)));
}

TEST(Key, CompositeUnsignedSegmentsOrderNumerically) {
  for (auto type : {KeyDataType::UnsignedBinary, KeyDataType::Unsigned,
                    KeyDataType::OldBinary}) {
    // 0xFF < 0x100, but little-endian bytes FF 00 > 00 01
    EXPECT_LT(twoIntKeyValue(type, 2, 0xFF).getBlobValue(),
              twoIntKeyValue(type, 2, 0x100).getBlobValue())
        << "type " << type;
    // the first segment dominates
    EXPECT_LT(twoIntKeyValue(type, 0xFF, 0xFFFFFF).getBlobValue(),
              twoIntKeyValue(type, 0x100, 0).getBlobValue())
        << "type " << type;
  }
}

TEST(Key, CompositeSignedSegmentsOrderNumerically) {
  for (auto type : {KeyDataType::Integer, KeyDataType::AutoInc}) {
    EXPECT_LT(twoIntKeyValue(type, 2, static_cast<uint32_t>(-1)).getBlobValue(),
              twoIntKeyValue(type, 2, 1).getBlobValue())
        << "type " << type;
    EXPECT_LT(twoIntKeyValue(type, static_cast<uint32_t>(-5), 0x7FFFFFFF)
                  .getBlobValue(),
              twoIntKeyValue(type, 2, 0x80000000).getBlobValue())
        << "type " << type;
  }
}

// Like a composite key, a single string key is null only when declared
// nullable; otherwise an empty string is an ordinary, indexed value
// (The Major BBS GALALS2.DAT key 2 holds many).
TEST(Key, EmptyStringKeyIsNotNullWithoutNullAttribute) {
  for (auto type : {KeyDataType::Zstring, KeyDataType::Lstring,
                    KeyDataType::String, KeyDataType::OldAscii}) {
    KeyDefinition keyDefinition(0, 17, 0, type,
                                UseExtendedDataType | Duplicates, false, 0, 0,
                                0, "", std::vector<char>());
    Key key(&keyDefinition, 1);
    uint8_t record[17];
    memset(record, 0, sizeof(record));
    EXPECT_FALSE(key.extractKeyInRecordToSqliteObject(
                        std::basic_string_view<uint8_t>(record, sizeof(record)))
                     .isNull())
        << "type " << type;
  }
}

TEST(Key, EmptyStringKeyIsNullWithNullAttribute) {
  KeyDefinition keyDefinition(0, 17, 0, KeyDataType::Zstring,
                              UseExtendedDataType | NullAllSegments, false, 0,
                              0, 0, "", std::vector<char>());
  Key key(&keyDefinition, 1);
  uint8_t record[17];
  memset(record, 0, sizeof(record));
  EXPECT_TRUE(key.extractKeyInRecordToSqliteObject(
                     std::basic_string_view<uint8_t>(record, sizeof(record)))
                  .isNull());
}

struct ParameterizedKeyDataType {
  KeyDataType type;
};

class ParameterizedKeyDataTypeFixture
    : public testing::TestWithParam<ParameterizedKeyDataType> {};

TEST_P(ParameterizedKeyDataTypeFixture, NullValues) {
  const ParameterizedKeyDataType &view = GetParam();

  KeyDefinition keyDefinition(0, 8, 2, view.type,
                              UseExtendedDataType | NullAllSegments, true, 0, 0,
                              ' ', "", std::vector<char>());

  Key key(&keyDefinition, 1);

  uint8_t record[128];
  memset(record, 0xFF, sizeof(record));
  // first segment is all spaces i.e. null
  memset(record + 2, ' ', 8);

  auto actual = key.extractKeyInRecordToSqliteObject(
      std::basic_string_view<uint8_t>(record, sizeof(record)));

  EXPECT_TRUE(actual.isNull());
}

static std::vector<ParameterizedKeyDataType> createKeyDataTypeData() {
  return std::vector<ParameterizedKeyDataType>{
      {KeyDataType::String},         {KeyDataType::Lstring},
      {KeyDataType::Zstring},        {KeyDataType::OldAscii},
      {KeyDataType::Integer},        {KeyDataType::Unsigned},
      {KeyDataType::UnsignedBinary}, {KeyDataType::OldBinary},
      {KeyDataType::Float}};
}

INSTANTIATE_TEST_CASE_P(Key, ParameterizedKeyDataTypeFixture,
                        ::testing::ValuesIn(createKeyDataTypeData()));

static std::vector<char> upperACS() {
  std::vector<char> ret(ACS_LENGTH);
  char *acs = ret.data();

  for (int i = 0; i < ACS_LENGTH; ++i) {
    acs[i] = (char)i;
  }
  // make uppercase
  for (int i = 'a'; i <= 'z'; ++i) {
    acs[i] = toupper(acs[i]);
  }

  return ret;
}

TEST(Key, ACSReplacementSingleKey_Zstring) {
  std::vector<char> acs = upperACS();

  KeyDefinition keyDefinition(0, 8, 2, KeyDataType::Zstring,
                              UseExtendedDataType | NumberedACS, true, 0, 0, 0,
                              "acsName", acs);

  Key key(&keyDefinition, 1);

  uint8_t record[128];
  memset(record, 0xFF, sizeof(record));
  // first segment is all spaces i.e. null
  record[2] = (uint8_t)'a';
  record[3] = (uint8_t)'B';
  record[4] = (uint8_t)'t';
  record[5] = (uint8_t)'Z';
  record[6] = (uint8_t)'%';
  record[7] = 0;

  auto actual = key.extractKeyInRecordToSqliteObject(
                       std::basic_string_view<uint8_t>(record, sizeof(record)))
                    .getStringValue();

  EXPECT_EQ(actual, "ABTZ%");
}

TEST(Key, ACSReplacementSingleKey_String) {
  std::vector<char> acs = upperACS();

  KeyDefinition keyDefinition(0, 16, 2, KeyDataType::String,
                              UseExtendedDataType | NumberedACS, true, 0, 0, 0,
                              "acsName", acs);

  Key key(&keyDefinition, 1);

  uint8_t record[128];
  memset(record, 0xFF, sizeof(record));
  // first segment is all spaces i.e. null
  record[2] = (uint8_t)'a';
  record[3] = (uint8_t)'B';
  record[4] = (uint8_t)'t';
  record[5] = (uint8_t)'Z';
  record[6] = (uint8_t)'%';
  record[7] = 0;
  record[8] = 0;
  record[9] = (uint8_t)'a';
  record[10] = (uint8_t)'B';
  record[11] = (uint8_t)'t';
  record[12] = 0;

  auto actual = key.extractKeyInRecordToSqliteObject(
                       std::basic_string_view<uint8_t>(record, sizeof(record)))
                    .getBlobValue();

  EXPECT_TRUE(memcmp(actual.data(), "ABTZ%\0\0ABT", 10) == 0);
}

struct ACSReplacementMultipleKey {
  bool firstACS;
  const char *input;
  const char *expected;
};

class ParameterizedACSReplacementMultipleKeyFixture
    : public testing::TestWithParam<ACSReplacementMultipleKey> {};

TEST_P(ParameterizedACSReplacementMultipleKeyFixture,
       ACSReplacementMultipleKey) {
  const ACSReplacementMultipleKey &view = GetParam();

  std::vector<char> acs = upperACS();

  KeyDefinition keyDefinitions[2] = {
      KeyDefinition(0, 8, 2, KeyDataType::Zstring,
                    UseExtendedDataType | (view.firstACS ? NumberedACS : 0),
                    true, 0, 0, 0, "test", acs),
      KeyDefinition(0, 8, 10, KeyDataType::Zstring,
                    UseExtendedDataType | NumberedACS, false, 1, 0, 0, "test",
                    acs)};

  Key key(keyDefinitions, 2);

  uint8_t record[128];
  memset(record, 0, sizeof(record));
  memcpy(record + 2, view.input, strlen(view.input));

  auto actual = key.extractKeyInRecordToSqliteObject(
                       std::basic_string_view<uint8_t>(record, sizeof(record)))
                    .getBlobValue();

  auto keyChars = reinterpret_cast<const char *>(actual.data());
  EXPECT_EQ(std::string(keyChars, std::min(actual.size(), strlen(keyChars))),
            view.expected);
}

static std::vector<ACSReplacementMultipleKey>
createACSReplacementMultipleKey() {
  return std::vector<ACSReplacementMultipleKey>{
      {true, "b", "B"},
      {true, "test", "TEST"},
      {true, "1234567890", "1234567890"},
      {true, "test1234test4321", "TEST1234TEST4321"},
      {false, "b", "b"},
      {false, "test", "test"},
      {false, "1234567890", "1234567890"},
      {false, "test1234test4321", "test1234TEST4321"}};
}

INSTANTIATE_TEST_CASE_P(Key, ParameterizedACSReplacementMultipleKeyFixture,
                        ::testing::ValuesIn(createACSReplacementMultipleKey()));

TEST(Key, FloatKeys) {
  KeyDefinition keyDefinition(0, sizeof(float), 0, KeyDataType::Float,
                              UseExtendedDataType, false, 0, 0, 0, "",
                              std::vector<char>());

  Key key(&keyDefinition, 1);

  float value = 2.435f;

  auto actual = key
                    .keyDataToSqliteObject(std::basic_string_view<uint8_t>(
                        reinterpret_cast<uint8_t *>(&value), sizeof(value)))
                    .getDoubleValue();

  EXPECT_EQ(actual, value);
}

TEST(Key, DoubleKeys) {
  KeyDefinition keyDefinition(0, sizeof(double), 0, KeyDataType::Float,
                              UseExtendedDataType, false, 0, 0, 0, "",
                              std::vector<char>());

  Key key(&keyDefinition, 1);

  double value = 2.435f;

  auto actual = key
                    .keyDataToSqliteObject(std::basic_string_view<uint8_t>(
                        reinterpret_cast<uint8_t *>(&value), sizeof(value)))
                    .getDoubleValue();

  EXPECT_EQ(actual, value);
}

}  // namespace
