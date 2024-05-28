#include "AttributeMask.h"
#include "Key.h"
#include "gtest/gtest.h"

using namespace btrieve;

namespace {

static const unsigned char DATA_NEGATIVE[] = {0xF1, 0xF2, 0xF3, 0xF4,
                                              0xF5, 0xF6, 0xF7, 0xF8};
static const unsigned char DATA_POSITIVE[] = {0x1, 0x2, 0x3, 0x4,
                                              0x5, 0x6, 0x7, 0x8};

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
  int length;
  KeyDataType type;
  uint64_t expected;
};

class ParameterizedFixture
    : public testing::TestWithParam<ParameterizedFixtureType> {};

TEST_P(ParameterizedFixture, TestNegativeIntegerConversions) {
  const ParameterizedFixtureType &view = GetParam();

  KeyDefinition keyDefinition(0, view.length, 0, view.type, UseExtendedDataType,
                              false, 0, 0, 0, NULL);

  Key key(&keyDefinition, 1);

  auto actual = static_cast<uint64_t>(
      key.keyDataToSqliteObject(
             std::basic_string_view<uint8_t>(DATA_NEGATIVE, view.length))
          .getIntegerValue());

  actual = static_cast<uint64_t>(
      key.keyDataToSqliteObject(
             std::basic_string_view<uint8_t>(DATA_NEGATIVE, view.length))
          .getIntegerValue());

  EXPECT_EQ(actual, view.expected);
}

static std::vector<ParameterizedFixtureType> create() {
  return std::vector<ParameterizedFixtureType>{
      {2, KeyDataType::Integer, static_cast<uint64_t>(-3343)},
      {4, KeyDataType::Integer, static_cast<uint64_t>(-185339151)},
      {6, KeyDataType::Integer, static_cast<uint64_t>(-9938739662095l)},
      {8, KeyDataType::Integer, static_cast<uint64_t>(-506664896818842895)},
      {2, KeyDataType::AutoInc, static_cast<uint64_t>(-3343)},
      {4, KeyDataType::AutoInc, static_cast<uint64_t>(-185339151)},
      {6, KeyDataType::AutoInc, static_cast<uint64_t>(-9938739662095)},
      {8, KeyDataType::AutoInc, static_cast<uint64_t>(-506664896818842895)},
      {2, KeyDataType::Unsigned, 0xF2F1},
      {4, KeyDataType::Unsigned, 0xF4F3F2F1},
      {6, KeyDataType::Unsigned, 0xF6F5F4F3F2F1},
      {8, KeyDataType::Unsigned, 0xF8F7F6F5F4F3F2F1},
      {2, KeyDataType::UnsignedBinary, 0xF2F1},
      {4, KeyDataType::UnsignedBinary, 0xF4F3F2F1},
      {6, KeyDataType::UnsignedBinary, 0xF6F5F4F3F2F1},
      {8, KeyDataType::UnsignedBinary, 0xF8F7F6F5F4F3F2F1},
      {2, KeyDataType::OldBinary, 0xF2F1},
      {4, KeyDataType::OldBinary, 0xF4F3F2F1},
      {6, KeyDataType::OldBinary, 0xF6F5F4F3F2F1},
      {8, KeyDataType::OldBinary, 0xF8F7F6F5F4F3F2F1}};
}

INSTANTIATE_TEST_CASE_P(Key, ParameterizedFixture,
                        ::testing::ValuesIn(create()));

} // namespace
