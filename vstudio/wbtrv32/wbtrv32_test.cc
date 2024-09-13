#include "wbtrv32.h"

#include <memory>

#include "../../btrieve/AttributeMask.h"
#include "../../btrieve/ErrorCode.h"
#include "../../btrieve/KeyDataType.h"
#include "../../btrieve/TestBase.h"
#include "btrieve/OperationCode.h"
#include "gtest/gtest.h"
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>

typedef int(__stdcall* BTRCALL)(WORD wOperation, LPVOID lpPositionBlock,
                                LPVOID lpDataBuffer,
                                LPDWORD lpdwDataBufferLength,
                                LPVOID lpKeyBuffer, BYTE bKeyLength,
                                CHAR sbKeyNumber);

class wbtrv32Test : public TestBase {
 public:
  wbtrv32Test() : TestBase(), dll(nullptr, &FreeLibrary) {
    memset(posBlock, 0, sizeof(posBlock));
  }

  virtual void SetUp() override {
    TestBase::SetUp();

    dll.reset(LoadLibrary(TEXT("wbtrv32.dll")));

    btrcall = reinterpret_cast<BTRCALL>(GetProcAddress(dll.get(), "BTRCALL"));
  }

  virtual void TearDown() override {
    for (int i = 0; i < sizeof(posBlock); ++i) {
      if (posBlock[i]) {
        btrcall(btrieve::OperationCode::Close, posBlock, nullptr, 0, nullptr, 0,
                0);
      }
    }

    TestBase::TearDown();
  }

 protected:
  std::unique_ptr<std::remove_pointer<HMODULE>::type, decltype(&FreeLibrary)>
      dll;
  BTRCALL btrcall;
  uint8_t posBlock[POSBLOCK_LENGTH];
};

TEST_F(wbtrv32Test, LoadsAndUnloadsDll) {
  ASSERT_NE(dll.get(), nullptr);
  ASSERT_NE(btrcall, nullptr);
}

TEST_F(wbtrv32Test, CannotCloseUnopenedDatabase) {
  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::FileNotOpen);
}

TEST_F(wbtrv32Test, LoadsAndClosesDatabase) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::FileNotOpen);
}

TEST_F(wbtrv32Test, CannotStatUnopenedDatabase) {
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

  unsigned char buffer[80];
  char fileName[MAX_PATH] = "test";
  DWORD dwDataBufferLength = sizeof(buffer);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, fileName, sizeof(fileName), 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(dwDataBufferLength, 80);
  // we don't support file name, so just 0 it out
  ASSERT_EQ(*fileName, 0);

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  EXPECT_EQ(lpFileSpec->logicalFixedRecordLength, 74);
  EXPECT_EQ(lpFileSpec->pageSize, 512);
  EXPECT_EQ(lpFileSpec->numberOfKeys, 4);
  EXPECT_EQ(lpFileSpec->fileVersion, 0x60);
  EXPECT_EQ(lpFileSpec->recordCount, 4);
  EXPECT_EQ(lpFileSpec->fileFlags, 0);
  EXPECT_EQ(lpFileSpec->numExtraPointers, 0);
  EXPECT_EQ(lpFileSpec->physicalPageSize, 1);
  EXPECT_EQ(lpFileSpec->preallocatedPages, 0);

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
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
  EXPECT_EQ(lpKeySpec->attributes,
            UseExtendedDataType | Modifiable | Duplicates);
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

  unsigned char buffer[80];
  char fileName[MAX_PATH] = "test";
  DWORD dwDataBufferLength;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  for (DWORD i = 0; i < 80; ++i) {
    char fileName[MAX_PATH];

    dwDataBufferLength = i;

    ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                      &dwDataBufferLength, fileName, sizeof(fileName), 0),
              btrieve::BtrieveError::DataBufferLengthOverrun);
  }
}

TEST_F(wbtrv32Test, Delete) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  unsigned char buffer[80];
  DWORD dwDataBufferLength = sizeof(buffer);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  ASSERT_EQ(lpFileSpec->recordCount, 4);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Delete, posBlock, nullptr, nullptr,
                    nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  ASSERT_EQ(lpFileSpec->recordCount, 3);
}

#pragma pack(push, 1)
typedef struct _tagRECORD {
  uint16_t unused;
  char string1[32];
  int32_t int1;
  char string2[32];
  int32_t int2;
} RECORD, *LPRECORD;
#pragma pack(pop)

static_assert(sizeof(RECORD) == 74);

/* Data layout as follows:

sqlite> select * from data_t;
    id          data        key_0       key_1       key_2       key_3
    ----------  ----------  ----------  ----------  ----------  ----------
    1                       Sysop       3444        3444        1
    2                       Sysop       7776        7776        2
    3                       Sysop       1052234073  StringValu  3
    4                       Sysop       -615634567  stringValu  4
*/
TEST_F(wbtrv32Test, StepFirst) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::StepFirst, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 3444);
  ASSERT_STREQ(record.string2, "3444");
  ASSERT_EQ(record.int2, 1);

  dwDataBufferLength = sizeof(uint32_t);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 1);

  ASSERT_EQ(btrcall(btrieve::OperationCode::StepPrevious, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::InvalidPositioning);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepNext, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 7776);
  ASSERT_STREQ(record.string2, "7776");
  ASSERT_EQ(record.int2, 2);

  dwDataBufferLength = sizeof(uint32_t);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 2);
}

TEST_F(wbtrv32Test, StepDataUnderrun) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  const unsigned int codesToTest[] = {
      btrieve::OperationCode::StepFirst,
      btrieve::OperationCode::StepFirst + 100,
      btrieve::OperationCode::StepFirst + 200,
      btrieve::OperationCode::StepFirst + 300,
      btrieve::OperationCode::StepFirst + 400,
      btrieve::OperationCode::StepLast,
      btrieve::OperationCode::StepLast + 100,
      btrieve::OperationCode::StepLast + 200,
      btrieve::OperationCode::StepLast + 300,
      btrieve::OperationCode::StepLast + 400,
      btrieve::OperationCode::StepNext,
      btrieve::OperationCode::StepNext + 100,
      btrieve::OperationCode::StepNext + 200,
      btrieve::OperationCode::StepNext + 300,
      btrieve::OperationCode::StepNext + 400,
      // not testing StepPrevious, since by default we're at beginning so
      // StepPrevious will return InvalidPositioning and not go into the
      // DataBufferLengthOverrun
  };

  for (int i = 0; i < ARRAYSIZE(codesToTest); ++i) {
    DWORD dwDataBufferLength = sizeof(record) - 1;
    ASSERT_EQ(btrcall(codesToTest[i], posBlock, &record, &dwDataBufferLength,
                      nullptr, 0, 0),
              btrieve::BtrieveError::DataBufferLengthOverrun);
  }
}

TEST_F(wbtrv32Test, StepLast) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::StepLast, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, -615634567);
  ASSERT_STREQ(record.string2, "stringValue");
  ASSERT_EQ(record.int2, 4);

  dwDataBufferLength = sizeof(uint32_t);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 4);

  ASSERT_EQ(btrcall(btrieve::OperationCode::StepNext, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::InvalidPositioning);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepPrevious, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 1052234073);
  ASSERT_STREQ(record.string2, "StringValue");
  ASSERT_EQ(record.int2, 3);

  dwDataBufferLength = sizeof(uint32_t);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 3);
}

TEST_F(wbtrv32Test, GetDirectNoKeys) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  *reinterpret_cast<uint32_t*>(&record) = 1;  // get record 1
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                    &record, &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 3444);
  ASSERT_STREQ(record.string2, "3444");
  ASSERT_EQ(record.int2, 1);
}

TEST_F(wbtrv32Test, GetDirectNoKeysBadPositioning) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  unsigned int badPositions[] = {0, 5};

  for (int i = 0; i < ARRAYSIZE(badPositions); ++i) {
    *reinterpret_cast<uint32_t*>(&record) = badPositions[i];
    ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                      &record, &dwDataBufferLength, nullptr, 0, -1),
              btrieve::BtrieveError::InvalidRecordAddress);
  }
}

TEST_F(wbtrv32Test, GetDirectNoKeysBufferOverrun) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  for (int i = 0; i < sizeof(record) - 1; ++i) {
    dwDataBufferLength = i;
    *reinterpret_cast<uint32_t*>(&record) = 1;  // get record 1
    ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                      &record, &dwDataBufferLength, nullptr, 0, -1),
              btrieve::BtrieveError::DataBufferLengthOverrun);
  }
}

TEST_F(wbtrv32Test, GetDirectWithKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  uint32_t returnedKey;

  *reinterpret_cast<uint32_t*>(&record) = 4;  // get record 4
  ASSERT_EQ(
      btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock, &record,
              &dwDataBufferLength, &returnedKey, sizeof(returnedKey), 1),
      btrieve::BtrieveError::Success);

  ASSERT_EQ(returnedKey, -615634567);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, -615634567);
  ASSERT_STREQ(record.string2, "stringValue");
  ASSERT_EQ(record.int2, 4);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireNext, posBlock, &record,
                    &dwDataBufferLength, &returnedKey, sizeof(returnedKey), 1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(returnedKey, 3444);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 3444);
  ASSERT_STREQ(record.string2, "3444");
  ASSERT_EQ(record.int2, 1);
}

TEST_F(wbtrv32Test, GetDirectWithKeyDataBufferOverrun) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record) - 1;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  uint32_t returnedKey;

  *reinterpret_cast<uint32_t*>(&record) = 4;  // get record 4
  ASSERT_EQ(
      btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock, &record,
              &dwDataBufferLength, &returnedKey, sizeof(returnedKey), 1),
      btrieve::BtrieveError::DataBufferLengthOverrun);
}

TEST_F(wbtrv32Test, GetDirectWithKey_KeyBufferTooShort) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  uint32_t returnedKey;

  *reinterpret_cast<uint32_t*>(&record) = 4;  // get record 4
  ASSERT_EQ(
      btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock, &record,
              &dwDataBufferLength, &returnedKey, sizeof(returnedKey) - 1, 1),
      btrieve::BtrieveError::KeyBufferTooShort);
}

TEST_F(wbtrv32Test, Query) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  uint32_t keyToSearch = 4000;
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireGreater, posBlock, &record,
                    &dwDataBufferLength, &keyToSearch, sizeof(keyToSearch), 1),
            btrieve::BtrieveError::Success);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 7776);
  ASSERT_STREQ(record.string2, "7776");
  ASSERT_EQ(record.int2, 2);

  ASSERT_EQ(keyToSearch, 7776);

  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireNext, posBlock, &record,
                    &dwDataBufferLength, &keyToSearch, sizeof(keyToSearch), 1),
            btrieve::BtrieveError::Success);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 1052234073);
  ASSERT_STREQ(record.string2, "StringValue");
  ASSERT_EQ(record.int2, 3);

  ASSERT_EQ(keyToSearch, 1052234073);
}

TEST_F(wbtrv32Test, QueryDataBufferOverrun) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record) - 1;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  uint32_t keyToSearch = 4000;
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireGreater, posBlock, &record,
                    &dwDataBufferLength, &keyToSearch, sizeof(keyToSearch), 1),
            btrieve::BtrieveError::DataBufferLengthOverrun);
}

TEST_F(wbtrv32Test, QueryKeyBufferTooShort) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char key[32];
  DWORD dwDataBufferLength = sizeof(record) - 1;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  uint32_t keyToSearch = 4000;
  ASSERT_EQ(
      btrcall(btrieve::OperationCode::AcquireGreater, posBlock, &record,
              &dwDataBufferLength, &keyToSearch, sizeof(keyToSearch) - 1, 1),
      btrieve::BtrieveError::KeyBufferTooShort);
}

TEST_F(wbtrv32Test, InsertNoKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  record.int1 = 10000;
  record.int2 = 5;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "whatever");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 5);

  // requery just to make sure the data was inserted properly
  memset(&record, 0, sizeof(record));

  *reinterpret_cast<uint32_t*>(&record) = 5;
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                    &record, &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(record.int1, 10000);
  ASSERT_EQ(record.int2, 5);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_STREQ(record.string2, "whatever");
}

TEST_F(wbtrv32Test, InsertBreaksConstraints) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  record.int1 = 3444;
  record.int2 = 1;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "3444");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::DuplicateKeyValue);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4);
}

TEST_F(wbtrv32Test, InsertWithKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  record.int1 = -2000000000;
  record.int2 = 5;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "whatever");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dwDataBufferLength, key, sizeof(key), 1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(*reinterpret_cast<uint32_t*>(key), -2000000000);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 5);

  memset(&record, 0, sizeof(record));
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireNext, posBlock, &record,
                    &dwDataBufferLength, key, sizeof(key), 1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(*reinterpret_cast<int32_t*>(key), -615634567);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, -615634567);
  ASSERT_STREQ(record.string2, "stringValue");
  ASSERT_EQ(record.int2, 4);
}

TEST_F(wbtrv32Test, InsertWithInvalidKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  record.int1 = -2000000000;
  record.int2 = 5;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "whatever");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dwDataBufferLength, key, sizeof(key), 5),
            btrieve::BtrieveError::InvalidKeyNumber);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4);
}

TEST_F(wbtrv32Test, InsertWithKeyBufferTooShort) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  record.int1 = -2000000000;
  record.int2 = 5;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "whatever");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dwDataBufferLength, key, sizeof(uint32_t) - 1, 1),
            btrieve::BtrieveError::KeyBufferTooShort);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4);
}

TEST_F(wbtrv32Test, UpdateNoKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepLast, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  record.int1 = -7000;
  record.int2 = 4;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "stringValue");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4);

  // requery just to make sure the data was inserted properly
  memset(&record, 0, sizeof(record));

  *reinterpret_cast<uint32_t*>(&record) = 4;
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                    &record, &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(record.int1, -7000);
  ASSERT_EQ(record.int2, 4);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_STREQ(record.string2, "stringValue");
}

TEST_F(wbtrv32Test, UpdateBreaksConstraints) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepLast, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  record.int1 = -7000;
  record.int2 = 5;  // this breaks constraints
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "stringValue");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::NonModifiableKeyValue);
}

TEST_F(wbtrv32Test, UpdateWithKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepLast, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  record.int1 = -7000;
  record.int2 = 4;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "stringValue");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &record,
                    &dwDataBufferLength, key, sizeof(key), 1),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4);

  // requery just to make sure the data was inserted properly
  memset(&record, 0, sizeof(record));

  *reinterpret_cast<uint32_t*>(&record) = 4;
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                    &record, &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(record.int1, -7000);
  ASSERT_EQ(record.int2, 4);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_STREQ(record.string2, "stringValue");

  memset(&record, 0, sizeof(record));
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireNext, posBlock, &record,
                    &dwDataBufferLength, key, sizeof(key), 1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(*reinterpret_cast<int32_t*>(key), 3444);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 3444);
  ASSERT_STREQ(record.string2, "3444");
  ASSERT_EQ(record.int2, 1);
}

TEST_F(wbtrv32Test, UpdateWithInvalidKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepLast, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  record.int1 = -7000;
  record.int2 = 4;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "stringValue");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &record,
                    &dwDataBufferLength, key, sizeof(key), 4),
            btrieve::BtrieveError::InvalidKeyNumber);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4);

  // requery just to make sure the data wasn't updated
  memset(&record, 0, sizeof(record));

  *reinterpret_cast<uint32_t*>(&record) = 4;
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                    &record, &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(record.int1, -615634567);
  ASSERT_EQ(record.int2, 4);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_STREQ(record.string2, "stringValue");
}

TEST_F(wbtrv32Test, UpdateKeyBufferTooShort) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position = 0;
  char buffer[80];
  char key[32];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepLast, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  record.int1 = -7000;
  record.int2 = 4;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "stringValue");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &record,
                    &dwDataBufferLength, key, sizeof(uint32_t) - 1, 1),
            btrieve::BtrieveError::KeyBufferTooShort);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4);

  // requery just to make sure the data wasn't updated
  memset(&record, 0, sizeof(record));

  *reinterpret_cast<uint32_t*>(&record) = 4;
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                    &record, &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(record.int1, -615634567);
  ASSERT_EQ(record.int2, 4);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_STREQ(record.string2, "stringValue");
}
