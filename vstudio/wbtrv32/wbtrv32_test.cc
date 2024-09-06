#include "../../btrieve/ErrorCode.h"
#include "../../btrieve/TestBase.h"
#include "../../btrieve/KeyDataType.h"
#include "../../btrieve/AttributeMask.h"
#include "wbtrv32.h"
#include "btrieve/OperationCode.h"
#include "gtest/gtest.h"
#include <memory>
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>

typedef int(__stdcall* BTRCALL)(WORD wOperation, LPVOID lpPositionBlock,
  LPVOID lpDataBuffer,
  LPDWORD lpdwDataBufferLength,
  LPVOID lpKeyBuffer, BYTE bKeyLength,
  CHAR sbKeyNumber);

class wbtrv32Test : public TestBase {
public:
  wbtrv32Test() : TestBase(), dll(nullptr, &FreeLibrary) {}

  virtual void SetUp() override {
    TestBase::SetUp();

    dll.reset(LoadLibrary(TEXT("wbtrv32.dll")));

    btrcall =
      reinterpret_cast<BTRCALL>(GetProcAddress(dll.get(), "BTRCALL"));
  }
protected:
  std::unique_ptr<std::remove_pointer<HMODULE>::type, decltype(&FreeLibrary)>
    dll;
  BTRCALL btrcall;
};

TEST_F(wbtrv32Test, LoadsAndUnloadsDll) {
  ASSERT_NE(dll.get(), nullptr);
  ASSERT_NE(btrcall, nullptr);
}

TEST_F(wbtrv32Test, CannotCloseUnopenedDatabase) {
  unsigned char posBlock[128];
  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::FileNotOpen);
}

TEST_F(wbtrv32Test, LoadsAndClosesDatabase) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  unsigned char posBlock[128];
  DWORD dwDataBufferLength = 0;

  char filename[MAX_PATH] = "assets/MBBSEMU.DB";
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength, filename, -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::FileNotOpen);
}

TEST_F(wbtrv32Test, CannotStatUnopenedDatabase) {
  unsigned char posBlock[128];
  unsigned char buffer[256];
  char fileName[MAX_PATH] = "test";
  DWORD dwDataBufferLength = sizeof(buffer);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
    &dwDataBufferLength, fileName, ARRAYSIZE(fileName), 0),
    btrieve::BtrieveError::FileNotOpen);
}

TEST_F(wbtrv32Test, StatsDatabase) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  unsigned char posBlock[128];
  unsigned char buffer[80];
  char fileName[MAX_PATH] = "test";
  DWORD dwDataBufferLength = sizeof(buffer);

  char filename[MAX_PATH] = "assets/MBBSEMU.DB";
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
    &dwDataBufferLength, filename, -1, 0),
    btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
    &dwDataBufferLength, fileName, sizeof(fileName), 0),
    btrieve::BtrieveError::Success);

  ASSERT_EQ(dwDataBufferLength, 80);
  // we don't support file name, so just 0 it out
  ASSERT_EQ(*fileName, 0);

  wbtrv32::LPFILESPEC lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  EXPECT_EQ(lpFileSpec->logicalFixedRecordLength, 74);
  EXPECT_EQ(lpFileSpec->pageSize, 512);
  EXPECT_EQ(lpFileSpec->numberOfKeys, 4);
  EXPECT_EQ(lpFileSpec->fileVersion, 0x60);
  EXPECT_EQ(lpFileSpec->recordCount, 4);
  EXPECT_EQ(lpFileSpec->fileFlags, 0);
  EXPECT_EQ(lpFileSpec->numExtraPointers, 0);
  EXPECT_EQ(lpFileSpec->physicalPageSize, 1);
  EXPECT_EQ(lpFileSpec->preallocatedPages, 0);

  wbtrv32::LPKEYSPEC lpKeySpec = reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  EXPECT_EQ(lpKeySpec->position, 2);
  EXPECT_EQ(lpKeySpec->length, 32);
  EXPECT_EQ(lpKeySpec->attributes, UseExtendedDataType | Duplicates);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 0);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Zstring);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 0);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 34);
  EXPECT_EQ(lpKeySpec->length, 4);
  EXPECT_EQ(lpKeySpec->attributes, UseExtendedDataType | Modifiable);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 0);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Integer);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 0);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 38);
  EXPECT_EQ(lpKeySpec->length, 32);
  EXPECT_EQ(lpKeySpec->attributes, UseExtendedDataType | Modifiable | Duplicates);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 0);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Zstring);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 0);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 70);
  EXPECT_EQ(lpKeySpec->length, 4);
  EXPECT_EQ(lpKeySpec->attributes, UseExtendedDataType);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 0);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::AutoInc);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 0);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);
}

TEST_F(wbtrv32Test, StatsTooSmallBuffer) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  unsigned char posBlock[128];
  unsigned char buffer[80];
  char fileName[MAX_PATH] = "test";
  DWORD dwDataBufferLength;

  char filename[MAX_PATH] = "assets/MBBSEMU.DB";
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
    &dwDataBufferLength, filename, -1, 0),
    btrieve::BtrieveError::Success);

  for (DWORD i = 0; i < 80; ++i) {
    dwDataBufferLength = i;
    ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
      &dwDataBufferLength, fileName, sizeof(fileName), 0),
      btrieve::BtrieveError::DataBufferLengthOverrun);
  }
}

TEST_F(wbtrv32Test, Delete) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  unsigned char posBlock[128];
  unsigned char buffer[80];
  DWORD dwDataBufferLength = sizeof(buffer);

  char filename[MAX_PATH] = "assets/MBBSEMU.DB";
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
    &dwDataBufferLength, filename, -1, 0),
    btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
    &dwDataBufferLength, nullptr, 0, 0),
    btrieve::BtrieveError::Success);

  wbtrv32::LPFILESPEC lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  ASSERT_EQ(lpFileSpec->recordCount, 4);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Delete, posBlock, nullptr, nullptr, nullptr, 0, 0), btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
    &dwDataBufferLength, nullptr, 0, 0),
    btrieve::BtrieveError::Success);

  lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  ASSERT_EQ(lpFileSpec->recordCount, 3);
}