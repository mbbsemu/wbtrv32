#ifndef __TEST_BASE_H_
#define __TEST_BASE_H_

#include "gtest/gtest.h"
#include "Text.h"

class TempPath {
public:
  TempPath() = default;

  ~TempPath();

  bool create();

  std::string getTempPath();

  std::basic_string<tchar> copyToTempPath(const char* filePath);

private:
  void deleteAllFiles(const char* filePath);

  std::string tempFolder;
};

class TestBase : public ::testing::Test {
protected:
  TempPath* tempPath;

public:
  TestBase() = default;

  virtual ~TestBase() = default;

  virtual void SetUp() {
    tempPath = new TempPath();
    ASSERT_TRUE(tempPath->create());
  }

  virtual void TearDown() { delete tempPath; }
};

#endif