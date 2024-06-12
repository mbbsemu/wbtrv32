#include "LRUCache.h"
#include "gtest/gtest.h"
#include <memory>

using namespace btrieve;

TEST(LRUCache, SingleInsertionAndGet) {
  LRUCache<std::string, std::string> test(5);

  test.cache("hello", "my guy");

  std::shared_ptr<std::string> value(test.get("hello"));
  ASSERT_TRUE(static_cast<bool>(value));
  ASSERT_STREQ(value->c_str(), "my guy");

  ASSERT_EQ(test.size(), 1);

  ASSERT_EQ(test.get("not a key"), nullptr);
}

TEST(LRUCache, MultiInsertionWithSameKey) {
  LRUCache<std::string, std::string> test(5);

  test.cache("hello", "my guy");
  test.cache("hello", "my guy2");
  test.cache("hello", "my guy3");
  test.cache("hello", "my guy4");
  test.cache("hello", "my guy5");
  test.cache("hello", "my guy6");
  test.cache("hello", "my guy7");
  test.cache("hello", "my guy8");

  std::shared_ptr<std::string> value = test.get("hello");
  ASSERT_TRUE(static_cast<bool>(value));
  ASSERT_STREQ(value->c_str(), "my guy8");

  ASSERT_EQ(test.size(), 1);
}

TEST(LRUCache, MultiInsertion) {
  LRUCache<std::string, std::string> test(5);

  test.cache("hello", "my guy");
  test.cache("hello2", "my guy2");
  test.cache("hello3", "my guy3");
  test.cache("hello4", "my guy4");
  test.cache("hello5", "my guy5");
  test.cache("hello6", "my guy6");
  test.cache("hello7", "my guy7");
  test.cache("hello8", "my guy8");
  test.cache("hello8", "my guy8");

  ASSERT_EQ(test.get("hello"), nullptr);
  ASSERT_EQ(test.get("hello2"), nullptr);
  ASSERT_EQ(test.get("hello3"), nullptr);

  ASSERT_EQ(test.size(), 5);

  for (int i = 0; i < 5; ++i) {
    char helloStr[32];
    char myGuyStr[32];

    sprintf(helloStr, "hello%d", 4 + i);
    sprintf(myGuyStr, "my guy%d", 4 + i);

    std::shared_ptr<std::string> value(test.get(helloStr));
    ASSERT_NE(value, nullptr);
    ASSERT_STREQ(value->c_str(), myGuyStr);

    ASSERT_EQ(test.size(), 5);
  }
}