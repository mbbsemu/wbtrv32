#include "SqliteUtil.h"

#include "AttributeMask.h"
#include "gtest/gtest.h"

using namespace btrieve;

namespace {

// A key with an INTEGER segment per attributes entry, each 2 bytes.
static Key createKey(std::initializer_list<uint16_t> attributes) {
  std::vector<KeyDefinition> segments;
  uint16_t i = 0;
  for (uint16_t attribute : attributes) {
    segments.push_back(KeyDefinition(1, 2, i * 2, KeyDataType::Integer,
                                     UseExtendedDataType | attribute, true, 1,
                                     i, 0, "", std::vector<char>()));
    ++i;
  }
  return Key(segments.data(), segments.size());
}

TEST(SqliteUtil, AscendingKey) {
  Key key = createKey({0, 0});

  EXPECT_EQ(sqliteKeyOrder(key, false), "key_1_0 ASC, key_1_1 ASC");
  EXPECT_EQ(sqliteKeyOrder(key, true), "key_1_0 DESC, key_1_1 DESC");
  EXPECT_EQ(sqliteKeyIndexColumns(key), "key_1_0, key_1_1");
  EXPECT_EQ(sqliteKeyComparison(key, ">"),
            "(key_1_0, key_1_1) > (@value0, @value1)");
  EXPECT_EQ(sqliteKeyComparison(key, "<="),
            "(key_1_0, key_1_1) <= (@value0, @value1)");
}

TEST(SqliteUtil, SingleDescendingKey) {
  Key key = createKey({DescendingKeySegment});

  EXPECT_EQ(sqliteKeyOrder(key, false), "key_1 DESC");
  EXPECT_EQ(sqliteKeyOrder(key, true), "key_1 ASC");
  EXPECT_EQ(sqliteKeyIndexColumns(key), "key_1 DESC");
  EXPECT_EQ(sqliteKeyComparison(key, ">"), "key_1 < @value0");
  EXPECT_EQ(sqliteKeyComparison(key, ">="), "key_1 <= @value0");
  EXPECT_EQ(sqliteKeyComparison(key, "<"), "key_1 > @value0");
  EXPECT_EQ(sqliteKeyComparison(key, "<="), "key_1 >= @value0");
}

TEST(SqliteUtil, AllDescendingCompositeKey) {
  Key key = createKey({DescendingKeySegment, DescendingKeySegment});

  EXPECT_EQ(sqliteKeyOrder(key, false), "key_1_0 DESC, key_1_1 DESC");
  EXPECT_EQ(sqliteKeyIndexColumns(key), "key_1_0 DESC, key_1_1 DESC");
  EXPECT_EQ(sqliteKeyComparison(key, ">="),
            "(key_1_0, key_1_1) <= (@value0, @value1)");
}

TEST(SqliteUtil, MixedDirectionKey) {
  Key key = createKey({0, DescendingKeySegment, 0});

  EXPECT_EQ(sqliteKeyOrder(key, false),
            "key_1_0 ASC, key_1_1 DESC, key_1_2 ASC");
  EXPECT_EQ(sqliteKeyOrder(key, true),
            "key_1_0 DESC, key_1_1 ASC, key_1_2 DESC");
  EXPECT_EQ(sqliteKeyIndexColumns(key), "key_1_0, key_1_1 DESC, key_1_2");
  EXPECT_EQ(sqliteKeyComparison(key, ">"),
            "key_1_0 >= @value0 AND (key_1_0 > @value0 OR (key_1_0 = @value0 "
            "AND (key_1_1 < @value1 OR (key_1_1 = @value1 AND key_1_2 > "
            "@value2))))");
  EXPECT_EQ(sqliteKeyComparison(key, ">="),
            "key_1_0 >= @value0 AND (key_1_0 > @value0 OR (key_1_0 = @value0 "
            "AND (key_1_1 < @value1 OR (key_1_1 = @value1 AND key_1_2 >= "
            "@value2))))");
  EXPECT_EQ(sqliteKeyComparison(key, "<"),
            "key_1_0 <= @value0 AND (key_1_0 < @value0 OR (key_1_0 = @value0 "
            "AND (key_1_1 > @value1 OR (key_1_1 = @value1 AND key_1_2 < "
            "@value2))))");
}

TEST(SqliteUtil, MixedDirectionKeyLeadingDescending) {
  Key key = createKey({DescendingKeySegment, 0});

  EXPECT_EQ(sqliteKeyComparison(key, "<="),
            "key_1_0 >= @value0 AND (key_1_0 > @value0 OR (key_1_0 = @value0 "
            "AND key_1_1 <= @value1))");
}

}  // namespace
