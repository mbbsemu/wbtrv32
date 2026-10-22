#ifndef __TEST_BASE_H_
#define __TEST_BASE_H_

#include <memory>
#include <string>

#include "Text.h"
#include "gtest/gtest.h"

class TempPath {
 public:
  TempPath() = default;

  ~TempPath();

  bool create();

  std::string getTempPath();

  std::basic_string<wchar_t> copyToTempPath(const char *filePath);

 private:
  void deleteAllFiles(const char *filePath);

  std::string tempFolder;
};

class TestBase : public ::testing::Test {
 protected:
  std::unique_ptr<TempPath> tempPath;

 public:
  virtual ~TestBase() = default;

  virtual void SetUp() {
    tempPath = std::make_unique<TempPath>();
    ASSERT_TRUE(tempPath->create());
  }

  virtual void TearDown() { tempPath.reset(); }
};

#endif
