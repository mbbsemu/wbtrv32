#include "AttributeMask.h"
#include "Key.h"
#include "gtest/gtest.h"

using namespace btrieve;

namespace {

static const unsigned char DATA_NEGATIVE[] = {0xF1, 0xF2, 0xF3, 0xF4,
                                              0xF5, 0xF6, 0xF7, 0xF8};
static const unsigned char DATA_POSITIVE[] = {0x1, 0x2, 0x3, 0x4,
                                              0x5, 0x6, 0x7, 0x8};
static const char STRING_DATA[32] = "Test";

TEST(Key, SingleKeySegment) {
  KeyDefinition keyDefinition(0, 10, 0, KeyDataType::String, 0, false, 0, 0, 0,
                              NULL);

  Key key(&keyDefinition, 1);

  EXPECT_EQ(key.getNumber(), 0);
  EXPECT_EQ(key.getLength(), 10);
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
                              false, 0, 0, 0, NULL);

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
                              false, 0, 0, 0, NULL);

  Key key(&keyDefinition, 1);

  auto actual =
      key
          .keyDataToSqliteObject(std::basic_string_view<uint8_t>(
              reinterpret_cast<const uint8_t *>(STRING_DATA), view.length))
          .getStringValue();

  EXPECT_EQ(actual, view.expected);
}

static std::vector<ParameterizedStringFixtureType> createStringData() {
  return std::vector<ParameterizedStringFixtureType>{
      {32, KeyDataType::String, "Test"},   {5, KeyDataType::String, "Test"},
      {4, KeyDataType::String, "Test"},    {3, KeyDataType::String, "Tes"},
      {2, KeyDataType::String, "Te"},      {1, KeyDataType::String, "T"},
      {32, KeyDataType::Lstring, "Test"},  {5, KeyDataType::Lstring, "Test"},
      {4, KeyDataType::Lstring, "Test"},   {3, KeyDataType::Lstring, "Tes"},
      {2, KeyDataType::Lstring, "Te"},     {1, KeyDataType::Lstring, "T"},
      {32, KeyDataType::Zstring, "Test"},  {5, KeyDataType::Zstring, "Test"},
      {4, KeyDataType::Zstring, "Test"},   {3, KeyDataType::Zstring, "Tes"},
      {2, KeyDataType::Zstring, "Te"},     {1, KeyDataType::Zstring, "T"},
      {32, KeyDataType::OldAscii, "Test"}, {5, KeyDataType::OldAscii, "Test"},
      {4, KeyDataType::OldAscii, "Test"},  {3, KeyDataType::OldAscii, "Tes"},
      {2, KeyDataType::OldAscii, "Te"},    {1, KeyDataType::OldAscii, "T"}};
}

INSTANTIATE_TEST_CASE_P(Key, ParameterizedStringFixture,
                        ::testing::ValuesIn(createStringData()));

TEST(Key, CompositeKeyConcatentation) {
  KeyDefinition keyDefinitions[2] = {
      KeyDefinition(0, 8, 2, KeyDataType::Integer, UseExtendedDataType, true, 0,
                    0, 0, NULL),
      KeyDefinition(0, 4, 20, KeyDataType::Zstring, UseExtendedDataType, false,
                    1, 0, 0, NULL)};

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

  uint8_t expected[] = {0x5, 0x5, 0x5, 0x5, 0x5, 0x5,
                        0x5, 0x5, 'T', 'T', 'T', 'T'};

  EXPECT_EQ(actual,
            std::vector<uint8_t>(expected, expected + sizeof(expected)));
}

} // namespace
