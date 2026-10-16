// 26.10.15.3B 10:20PM
#include "wbtrv32.h"

#include <cstdint>
#include <filesystem>
#include <memory>

#include "../../btrieve/AttributeMask.h"
#include "../../btrieve/BtrieveDriver.h"
#include "../../btrieve/ErrorCode.h"
#include "../../btrieve/KeyDataType.h"
#include "../../btrieve/OpenMode.h"
#include "../../btrieve/SqliteDatabase.h"
#include "../../btrieve/TestBase.h"
#include "bad_data.h"
#include "btrieve/OperationCode.h"
#include "framework.h"
#include "gtest/gtest.h"

using namespace btrieve;

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

#ifdef WIN32
    dll.reset(LoadLibrary(_TEXT("wbtrv32.dll")));
#else
    dll.reset(LoadLibrary(_TEXT("vstudio/wbtrv32/wbtrv32.so")));
#endif

    btrcall = reinterpret_cast<BTRCALL>(GetProcAddress(dll.get(), "BTRCALL"));
  }

  virtual void TearDown() override {
    for (size_t i = 0; i < sizeof(posBlock); ++i) {
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

TEST_F(wbtrv32Test, LoadsSameDatabaseTwice) {
  char posBlock2[128];
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock2, nullptr,
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

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock2, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock2, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::FileNotOpen);
}

TEST_F(wbtrv32Test, LoadsAndClosesDatabaseAsReadOnly) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DAT");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, btrieve::OpenMode::ReadOnly),
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

TEST_F(wbtrv32Test, GetPosition) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  uint32_t dwPosition;
  DWORD dwDataBufferLength = sizeof(dwPosition);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &dwPosition,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(dwDataBufferLength, 4u);
  ASSERT_EQ(dwPosition, 1u);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
}

TEST_F(wbtrv32Test, QueryGreaterThanOrEqual) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBMGEPLT.DAT");
  ASSERT_FALSE(mbbsEmuDb.empty());

  char record[512];
  uint16_t keyBuffer;
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  keyBuffer = 2;
  ASSERT_EQ(
      btrcall(btrieve::OperationCode::AcquireGreaterOrEqual, posBlock, record,
              &dwDataBufferLength, &keyBuffer, sizeof(keyBuffer), 2),
      btrieve::BtrieveError::Success);

  ASSERT_EQ(dwDataBufferLength, 493u);
  ASSERT_EQ(keyBuffer, 2);

  uint32_t position = 0;
  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  memcpy(record, &position, sizeof(uint32_t));
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(
      btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock, record,
              &dwDataBufferLength, &keyBuffer, sizeof(keyBuffer), 2),
      btrieve::BtrieveError::Success);

  uint32_t expected = 2;
  uint32_t records = 0;
  // the query nexts should grab the next things started by
  // AcquireGreaterOrEqual earlier
  while (true) {
    dwDataBufferLength = sizeof(record);
    if (btrcall(btrieve::OperationCode::AcquireNext, posBlock, record,
                &dwDataBufferLength, &keyBuffer, sizeof(keyBuffer),
                2) != btrieve::BtrieveError::Success) {
      break;
    }

    ASSERT_GE(keyBuffer, expected);

    expected = keyBuffer;
    ++records;
  }

  ASSERT_EQ(records, 13u);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
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

  ASSERT_EQ(dwDataBufferLength, 80u);
  // we don't support file name, so just 0 it out
  ASSERT_EQ(*fileName, 0);

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  EXPECT_EQ(lpFileSpec->logicalFixedRecordLength, 74);
  EXPECT_EQ(lpFileSpec->pageSize, 4096);
  EXPECT_EQ(lpFileSpec->numberOfKeys, 4);
  EXPECT_EQ(lpFileSpec->fileVersion, 0);
  EXPECT_EQ(lpFileSpec->recordCount, 4u);
  EXPECT_EQ(lpFileSpec->fileFlags, 0);
  EXPECT_EQ(lpFileSpec->numExtraPointers, 0);
  EXPECT_EQ(lpFileSpec->physicalPageSize, 0);
  EXPECT_EQ(lpFileSpec->preallocatedPages, 0);

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  EXPECT_EQ(lpKeySpec->position, 3);
  EXPECT_EQ(lpKeySpec->length, 32);
  EXPECT_EQ(lpKeySpec->attributes, UseExtendedDataType | Duplicates);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 4u);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Zstring);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 0);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 35);
  EXPECT_EQ(lpKeySpec->length, 4);
  EXPECT_EQ(lpKeySpec->attributes, UseExtendedDataType | Modifiable);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 4u);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Integer);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 1);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 39);
  EXPECT_EQ(lpKeySpec->length, 32);
  EXPECT_EQ(lpKeySpec->attributes,
            UseExtendedDataType | Modifiable | Duplicates);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 4u);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Zstring);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 2);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 71);
  EXPECT_EQ(lpKeySpec->length, 4);
  EXPECT_EQ(lpKeySpec->attributes, UseExtendedDataType);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 4u);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::AutoInc);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 3);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);
}

TEST_F(wbtrv32Test, StatsV6DatabaseWithMultiSegmentKeys) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/GALTELA.DAT");
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

  ASSERT_EQ(dwDataBufferLength, 80u);
  // we don't support file name, so just 0 it out
  ASSERT_EQ(*fileName, 0);

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  EXPECT_EQ(lpFileSpec->logicalFixedRecordLength, 950);
  EXPECT_EQ(lpFileSpec->pageSize, 4096);
  EXPECT_EQ(lpFileSpec->numberOfKeys, 3);
  EXPECT_EQ(lpFileSpec->fileVersion, 0);
  EXPECT_EQ(lpFileSpec->recordCount, 73u);
  EXPECT_EQ(lpFileSpec->fileFlags, 0);
  EXPECT_EQ(lpFileSpec->numExtraPointers, 0);
  EXPECT_EQ(lpFileSpec->physicalPageSize, 0);
  EXPECT_EQ(lpFileSpec->preallocatedPages, 0);

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  EXPECT_EQ(lpKeySpec->position, 1);
  EXPECT_EQ(lpKeySpec->length, 16);
  EXPECT_EQ(lpKeySpec->attributes,
            UseExtendedDataType | SegmentedKey | NumberedACS);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 73u);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Zstring);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 0);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 17);
  EXPECT_EQ(lpKeySpec->length, 16);
  EXPECT_EQ(lpKeySpec->attributes, UseExtendedDataType | NumberedACS);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 73u);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Zstring);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 0);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 1);
  EXPECT_EQ(lpKeySpec->length, 16);
  EXPECT_EQ(lpKeySpec->attributes,
            UseExtendedDataType | NumberedACS | Duplicates);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 73u);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Zstring);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 1);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);

  ++lpKeySpec;
  EXPECT_EQ(lpKeySpec->position, 17);
  EXPECT_EQ(lpKeySpec->length, 16);
  EXPECT_EQ(lpKeySpec->attributes,
            UseExtendedDataType | NumberedACS | Duplicates);
  EXPECT_EQ(lpKeySpec->uniqueKeys, 73u);
  EXPECT_EQ(lpKeySpec->extendedDataType, btrieve::KeyDataType::Zstring);
  EXPECT_EQ(lpKeySpec->nullValue, 0);
  EXPECT_EQ(lpKeySpec->reserved, 0);
  EXPECT_EQ(lpKeySpec->number, 2);
  EXPECT_EQ(lpKeySpec->acsNumber, 0);
}

TEST_F(wbtrv32Test, StatsTooSmallBuffer) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  unsigned char buffer[80];
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
  ASSERT_EQ(lpFileSpec->recordCount, 4u);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Delete, posBlock, nullptr, nullptr,
                    nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  ASSERT_EQ(lpFileSpec->recordCount, 3u);
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
  uint32_t position;
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

  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 0x1u);

  ASSERT_EQ(btrcall(btrieve::OperationCode::StepPrevious, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::EndOfFile);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepNext, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 7776);
  ASSERT_STREQ(record.string2, "7776");
  ASSERT_EQ(record.int2, 2);

  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 2u);
}

TEST_F(wbtrv32Test, StepDataUnderrun) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
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

  for (size_t i = 0; i < ARRAYSIZE(codesToTest); ++i) {
    // a short buffer still moves to the record (as in Btrieve), so start each
    // StepNext from the first record rather than wherever the last step left
    if (codesToTest[i] % 100 == btrieve::OperationCode::StepNext) {
      DWORD fullLength = sizeof(record);
      ASSERT_EQ(btrcall(btrieve::OperationCode::StepFirst, posBlock, &record,
                        &fullLength, nullptr, 0, 0),
                btrieve::BtrieveError::Success);
    }
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
  uint32_t position;
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

  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 4u);

  ASSERT_EQ(btrcall(btrieve::OperationCode::StepNext, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::EndOfFile);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepPrevious, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 1052234073);
  ASSERT_STREQ(record.string2, "StringValue");
  ASSERT_EQ(record.int2, 3);

  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 3u);
}

TEST_F(wbtrv32Test, GetDirectNoKeys) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
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
  ASSERT_EQ(dwDataBufferLength, 74u);
}

TEST_F(wbtrv32Test, GetDirectNoKeysBadPositioning) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  unsigned int badPositions[] = {0, 5};

  for (size_t i = 0; i < ARRAYSIZE(badPositions); ++i) {
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
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  for (size_t i = 0; i < sizeof(record) - 1; ++i) {
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

  ASSERT_EQ(returnedKey, -615634567u);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, -615634567);
  ASSERT_STREQ(record.string2, "stringValue");
  ASSERT_EQ(record.int2, 4);

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireNext, posBlock, &record,
                    &dwDataBufferLength, &returnedKey, sizeof(returnedKey), 1),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(returnedKey, 3444u);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 3444);
  ASSERT_STREQ(record.string2, "3444");
  ASSERT_EQ(record.int2, 1);
}

TEST_F(wbtrv32Test, GetDirectWithKeyDataBufferOverrun) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
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

  ASSERT_EQ(keyToSearch, 7776u);

  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireNext, posBlock, &record,
                    &dwDataBufferLength, &keyToSearch, sizeof(keyToSearch), 1),
            btrieve::BtrieveError::Success);

  ASSERT_STREQ(record.string1, "Sysop");
  ASSERT_EQ(record.int1, 1052234073);
  ASSERT_STREQ(record.string2, "StringValue");
  ASSERT_EQ(record.int2, 3);

  ASSERT_EQ(keyToSearch, 1052234073u);
}

TEST_F(wbtrv32Test, QueryDataBufferOverrun) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
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
  char buffer[80];
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

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 5u);

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
  char buffer[80];
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

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4u);
}

TEST_F(wbtrv32Test, InsertWithKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
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

  ASSERT_EQ(*reinterpret_cast<uint32_t*>(key), -2000000000u);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 5u);

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

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4u);
}

TEST_F(wbtrv32Test, InsertWithKeyBufferTooShort) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
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

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4u);
}

// Regression tests for a bug where Upsert() (wbtrv32.cpp) never updated the
// driver's tracked current position after a successful Insert -- it only
// called logicalCurrencySeek(), which builds a Query object for continuing
// Step operations but doesn't touch SqlDatabase::position. A GetPosition
// call made immediately after Insert (exactly what MBBSEmu's
// BtrieveFileProcessor.Insert() does in C# to learn the new record's
// position) could therefore return a stale value left over from whatever
// Step/Query happened last, rather than the position of the record that
// was just inserted.
TEST_F(wbtrv32Test, InsertNoKeySetsPosition) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
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

  // No prior Step/Query has run on this driver, so before the fix,
  // GetPosition below would read back position's default value rather than
  // the newly inserted record's position.
  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  uint32_t position = 0xFFFFFFFF;
  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  // MBBSEMU.DB already has 4 records, so the new one lands at position 5.
  ASSERT_EQ(position, 5u);
}

TEST_F(wbtrv32Test, InsertWithKeySetsPosition) {
  // Same regression as InsertNoKeySetsPosition, but exercising the
  // keyNumber >= 0 path: logicalCurrencySeek() seeds a Query object with the
  // correct position for continuing Step/Acquire calls, but that's a
  // separate object from SqlDatabase::position -- it never updated the
  // latter either, so GetPosition had the same staleness bug regardless of
  // key number.
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
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

  uint32_t position = 0xFFFFFFFF;
  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 5u);
}

TEST_F(wbtrv32Test, InsertIntoFreshlyCreatedFileSetsPosition) {
  // Regression test for the exact scenario that produced a false "insert
  // failed" report in MBBSEmu: the very first insert into a brand new file.
  // SqlDatabase::position was also uninitialized by its constructor (fixed
  // alongside this), so on a driver that had never had a Step/Query
  // performed on it, GetPosition deterministically read back 0 -- the same
  // sentinel MBBSEmu's C# wrapper treats as insert failure -- even though
  // the record really was inserted successfully.
  unsigned char buffer[1024];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);

  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 1;
  lpFileSpec->logicalFixedRecordLength = 128;
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->fileFlags = 0;            // not variable
  lpFileSpec->physicalPageSize = 0xFF;  // in memory

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 3;
  lpKeySpec->length = 4;
  lpKeySpec->attributes = UseExtendedDataType | Duplicates;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Integer;

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, posBlock, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(
                        reinterpret_cast<LPCVOID>(toStdString(path).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  unsigned char record[128];
  memset(record, 0, sizeof(record));
  dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  uint32_t position = 0xFFFFFFFF;
  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 1u);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
}

TEST_F(wbtrv32Test, InsertNoKeyReadOnly) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  char buffer[80];
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, btrieve::OpenMode::ReadOnly),
            btrieve::BtrieveError::Success);

  record.int1 = 10000;
  record.int2 = 5;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "whatever");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::AccessDenied);

  dwDataBufferLength = sizeof(buffer);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Stat, posBlock, buffer,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4u);

  // requery just to make sure the data is readable in readonly mode
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

TEST_F(wbtrv32Test, UpdateNoKey) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  char buffer[80];
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

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4u);

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

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4u);

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

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4u);

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

  ASSERT_EQ(reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount, 4u);

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

TEST_F(wbtrv32Test, UpdateSetsPositionToUpdatedRecord) {
  // Companion regression test to the Insert*SetsPosition tests above: the
  // Update lambda passed into Upsert() computes insertedPosition as
  // {updateRecord(position, record), position} using the position that was
  // already current, so adding setPosition() there doesn't change behavior
  // -- but it does exercise that same code path to confirm GetPosition
  // still correctly reflects the record that was just updated.
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::StepFirst, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  record.int1 = -7000;
  record.int2 = 1;
  strcpy(record.string1, "Sysop");
  strcpy(record.string2, "3444");

  ASSERT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  uint32_t position = 0xFFFFFFFF;
  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(position, 1u);
}

// Schema of a real-world database that hit a bug where key_0's persisted
// SQLite column value could get out of sync with the record's actual bytes,
// permanently blocking updates with NonModifiableKeyValue even though the
// modifiable keys (key_1/key_2) were never touched:
//   Key 0: position=1   length=30 Zstring, non-modifiable
//   Key 1: position=31  length=4  Integer, modifiable
//   Key 2: position=135 length=4  Integer, modifiable
#pragma pack(push, 1)
typedef struct _tagNONMODIFIABLEKEYRECORD {
  char name[30];
  int32_t key1;
  uint8_t reserved[100];
  int32_t key2;
} NONMODIFIABLEKEYRECORD, *LPNONMODIFIABLEKEYRECORD;
#pragma pack(pop)

static_assert(sizeof(NONMODIFIABLEKEYRECORD) == 138);

TEST_F(wbtrv32Test, UpdateSucceedsWithNonAsciiNonModifiableKey) {
  unsigned char buffer[1024];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);

  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 3;
  lpFileSpec->logicalFixedRecordLength = sizeof(NONMODIFIABLEKEYRECORD);
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->fileFlags = 0;            // not variable
  lpFileSpec->physicalPageSize = 0xFF;  // in memory

  // Key 0: name, deliberately non-modifiable (no Modifiable attribute).
  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 1;
  lpKeySpec->length = 30;
  lpKeySpec->attributes = UseExtendedDataType;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Zstring;

  // Key 1: modifiable integer.
  ++lpKeySpec;
  lpKeySpec->position = 31;
  lpKeySpec->length = 4;
  lpKeySpec->attributes = UseExtendedDataType | Modifiable;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Integer;

  // Key 2: modifiable integer.
  ++lpKeySpec;
  lpKeySpec->position = 135;
  lpKeySpec->length = 4;
  lpKeySpec->attributes = UseExtendedDataType | Modifiable;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Integer;

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, posBlock, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(
                        reinterpret_cast<LPCVOID>(toStdString(path).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  // Insert a record whose non-modifiable key_0 (name) is the exact
  // non-ASCII/binary hex pattern seen in production: 29 bytes of 0xAD
  // followed by a null terminator.
  NONMODIFIABLEKEYRECORD record;
  memset(&record, 0, sizeof(record));
  memset(record.name, 0xAD, sizeof(record.name));
  record.name[sizeof(record.name) - 1] = 0;
  record.key1 = 1379942206;
  record.key2 = 42;

  DWORD dataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  // Fetch the record immediately after inserting and confirm it round-tripped
  // exactly, including the non-modifiable key_0's hex bytes.
  NONMODIFIABLEKEYRECORD fetched;
  memset(&fetched, 0, sizeof(fetched));
  DWORD fetchedLength = sizeof(fetched);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepFirst, posBlock, &fetched,
                    &fetchedLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(memcmp(fetched.name, record.name, sizeof(record.name)), 0);
  ASSERT_EQ(fetched.key1, record.key1);
  ASSERT_EQ(fetched.key2, record.key2);

  // Update key_2 to a different value. key_0 (name) is left completely
  // untouched, so this should succeed even though key_0 is non-modifiable.
  fetched.key2 = 999;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &fetched,
                    &fetchedLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  // Verify the update actually stuck, and key_0/key_1 came through
  // unmodified.
  NONMODIFIABLEKEYRECORD verify;
  memset(&verify, 0, sizeof(verify));
  DWORD verifyLength = sizeof(verify);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepFirst, posBlock, &verify,
                    &verifyLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(memcmp(verify.name, record.name, sizeof(record.name)), 0);
  ASSERT_EQ(verify.key1, record.key1);
  ASSERT_EQ(verify.key2, 999);
}

// Lstring keys are length-prefixed and can legitimately contain a 0x00 byte
// as real payload data anywhere in the middle -- unlike Zstring, it's not a
// terminator. SqlitePreparedStatement::bindParameter used to _strdup() the
// extracted key value and then tell sqlite3_bind_text() to read its full
// logical length back out of that copy; strdup() stops at the first 0x00, so
// whenever that happened before the true end of the value, sqlite3_bind_text
// read past the end of the allocation. This test's payload embeds a 0x00
// followed by more real (non-ASCII) bytes, which exercises exactly that path.
#pragma pack(push, 1)
typedef struct _tagLSTRINGRECORD {
  uint8_t lstringKey[20];  // 1 length-prefix byte + 19 payload bytes
  char filler[10];
} LSTRINGRECORD, *LPLSTRINGRECORD;
#pragma pack(pop)

static_assert(sizeof(LSTRINGRECORD) == 30);

TEST_F(wbtrv32Test, LstringKeyWithEmbeddedNullRoundTrips) {
  unsigned char buffer[1024];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 1;
  lpFileSpec->logicalFixedRecordLength = sizeof(LSTRINGRECORD);
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->fileFlags = 0;            // not variable
  lpFileSpec->physicalPageSize = 0xFF;  // in memory

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 1;
  lpKeySpec->length = sizeof(LSTRINGRECORD::lstringKey);
  lpKeySpec->attributes = UseExtendedDataType;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Lstring;

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, posBlock, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(
                        reinterpret_cast<LPCVOID>(toStdString(path).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  LSTRINGRECORD record;
  memset(&record, 0, sizeof(record));
  record.lstringKey[0] = 19;  // length prefix
  const uint8_t payload[19] = {'A',  'B',  'C', 0x00, 'D', 'E', 0x80,
                               0x81, 0xff, 'X', 'Y',  'Z', 1,   2,
                               3,    4,    5,   6,    7};
  memcpy(record.lstringKey + 1, payload, sizeof(payload));
  strcpy(record.filler, "hello");

  DWORD dataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  // Searching for the record using its exact original key bytes must find
  // it. Acquire/Query operations look records up through the persisted
  // key_0 SQLite column, unlike Step which just returns the raw record
  // bytes -- so if key_0 got corrupted on insert, this lookup fails even
  // though the record is right there.
  LSTRINGRECORD found;
  memset(&found, 0, sizeof(found));
  DWORD foundLength = sizeof(found);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireEqual, posBlock, &found,
                    &foundLength, record.lstringKey,
                    sizeof(record.lstringKey), 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(memcmp(found.lstringKey, record.lstringKey,
                   sizeof(record.lstringKey)),
            0);
  ASSERT_STREQ(found.filler, "hello");
}

// For completeness: Zstring keys truncate at the first 0x00 by design (see
// Key::extractNullTerminatedString), so an embedded null there is expected,
// not a corruption case. Whatever comes after it is never part of the
// extracted key value, so two records -- or a record and a search key --
// that agree up to the first null must compare equal even if their bytes
// diverge completely afterward.
#pragma pack(push, 1)
typedef struct _tagZSTRINGRECORD {
  char name[30];
  char filler[10];
} ZSTRINGRECORD, *LPZSTRINGRECORD;
#pragma pack(pop)

static_assert(sizeof(ZSTRINGRECORD) == 40);

TEST_F(wbtrv32Test, ZstringKeyWithEmbeddedNullTruncatesAtNull) {
  unsigned char buffer[1024];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 1;
  lpFileSpec->logicalFixedRecordLength = sizeof(ZSTRINGRECORD);
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->fileFlags = 0;            // not variable
  lpFileSpec->physicalPageSize = 0xFF;  // in memory

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 1;
  lpKeySpec->length = sizeof(ZSTRINGRECORD::name);
  lpKeySpec->attributes = UseExtendedDataType;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Zstring;

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, posBlock, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(
                        reinterpret_cast<LPCVOID>(toStdString(path).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  // "AB" followed by a null terminator, then non-zero garbage filling out
  // the rest of the 30-byte field.
  ZSTRINGRECORD record;
  memset(&record, 0, sizeof(record));
  memcpy(record.name, "AB", 2);
  record.name[2] = 0;
  memset(record.name + 3, 0xAD, sizeof(record.name) - 3);
  strcpy(record.filler, "hello");

  DWORD dataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  // Search using "AB\0" followed by a completely different garbage tail.
  // Since Zstring truncates at the first null, this must still match.
  ZSTRINGRECORD searchKey;
  memset(&searchKey, 0, sizeof(searchKey));
  memcpy(searchKey.name, "AB", 2);
  searchKey.name[2] = 0;
  memset(searchKey.name + 3, 0xff, sizeof(searchKey.name) - 3);

  ZSTRINGRECORD found;
  memset(&found, 0, sizeof(found));
  DWORD foundLength = sizeof(found);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireEqual, posBlock, &found,
                    &foundLength, searchKey.name, sizeof(searchKey.name), 0),
            btrieve::BtrieveError::Success);

  // What comes back is the actual stored record -- its tail after the null
  // is the original 0xAD bytes, not the search key's 0xff tail.
  ASSERT_EQ(memcmp(found.name, record.name, sizeof(record.name)), 0);
  ASSERT_STREQ(found.filler, "hello");
}

// Same name/filler shape as ZSTRINGRECORD, plus a second key: a modifiable
// int32 that starts at 0 and gets updated to 1.
#pragma pack(push, 1)
typedef struct _tagZSTRINGRECORDWITHINTKEY {
  char name[30];
  char filler[10];
  int32_t intKey;
} ZSTRINGRECORDWITHINTKEY, *LPZSTRINGRECORDWITHINTKEY;
#pragma pack(pop)

static_assert(sizeof(ZSTRINGRECORDWITHINTKEY) == 44);

TEST_F(wbtrv32Test, ZstringKeyWithEmbeddedNullUpdatesAfterNull) {
  unsigned char buffer[1024];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 2;
  lpFileSpec->logicalFixedRecordLength = sizeof(ZSTRINGRECORDWITHINTKEY);
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->fileFlags = 0;            // not variable
  lpFileSpec->physicalPageSize = 0xFF;  // in memory

  // Key 0: name, non-modifiable Zstring.
  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 1;
  lpKeySpec->length = sizeof(ZSTRINGRECORDWITHINTKEY::name);
  lpKeySpec->attributes = UseExtendedDataType;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Zstring;

  // Key 1: modifiable integer, right after name/filler.
  ++lpKeySpec;
  lpKeySpec->position = 41;
  lpKeySpec->length = sizeof(ZSTRINGRECORDWITHINTKEY::intKey);
  lpKeySpec->attributes = UseExtendedDataType | Modifiable;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Integer;

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, posBlock, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(
                        reinterpret_cast<LPCVOID>(toStdString(path).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  // "AB" followed by a null terminator, then non-zero garbage filling out
  // the rest of the 30-byte field. The int key starts at 0.
  ZSTRINGRECORDWITHINTKEY record;
  memset(&record, 0, sizeof(record));
  memcpy(record.name, "AB", 2);
  record.name[2] = 0;
  memset(record.name + 3, 0xAD, sizeof(record.name) - 3);
  strcpy(record.filler, "hello");
  record.intKey = 0;

  DWORD dataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  // Insert alone doesn't move currency onto the new record (see
  // InsertNoKey), so establish it before updating.
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepFirst, posBlock, &record,
                    &dataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  // Now overwrite this newly inserted record with a data structure that
  // changes the int key from 0 to 1, but keeps key_0's logical value ("AB")
  // the same -- only the junk bytes after the null terminator differ. Since
  // Zstring extraction truncates at the first null, the persisted key_0
  // value is unchanged, so this update must succeed even though key_0 has
  // no Modifiable attribute.
  ZSTRINGRECORDWITHINTKEY updatedRecord;
  memset(&updatedRecord, 0, sizeof(updatedRecord));
  memcpy(updatedRecord.name, "AB", 2);
  updatedRecord.name[2] = 0;
  memset(updatedRecord.name + 3, 0xff, sizeof(updatedRecord.name) - 3);
  strcpy(updatedRecord.filler, "world");
  updatedRecord.intKey = 1;

  DWORD updateLength = sizeof(updatedRecord);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &updatedRecord,
                    &updateLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  // Verify the update actually stuck: the int key moved from 0 to 1, filler
  // changed, and the junk tail after the null reflects the new bytes -- the
  // raw data blob is always fully overwritten even though key_0's extracted
  // value didn't change.
  ZSTRINGRECORDWITHINTKEY verify;
  memset(&verify, 0, sizeof(verify));
  DWORD verifyLength = sizeof(verify);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepFirst, posBlock, &verify,
                    &verifyLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(
      memcmp(verify.name, updatedRecord.name, sizeof(updatedRecord.name)), 0);
  ASSERT_STREQ(verify.filler, "world");
  ASSERT_EQ(verify.intKey, 1);

  // Finally, query by key_0 using yet another different junk tail after the
  // null terminator (neither the original 0xAD nor the updated 0xff). Since
  // Zstring truncates at the first null, this must still seek to the same
  // row we just updated.
  ZSTRINGRECORDWITHINTKEY searchKey;
  memset(&searchKey, 0, sizeof(searchKey));
  memcpy(searchKey.name, "AB", 2);
  searchKey.name[2] = 0;
  memset(searchKey.name + 3, 0x77, sizeof(searchKey.name) - 3);

  ZSTRINGRECORDWITHINTKEY queried;
  memset(&queried, 0, sizeof(queried));
  DWORD queriedLength = sizeof(queried);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireEqual, posBlock, &queried,
                    &queriedLength, searchKey.name, sizeof(searchKey.name),
                    0),
            btrieve::BtrieveError::Success);

  // What comes back is the actual stored record, not the search key's junk
  // tail.
  ASSERT_EQ(
      memcmp(queried.name, updatedRecord.name, sizeof(updatedRecord.name)),
      0);
  ASSERT_STREQ(queried.filler, "world");
  ASSERT_EQ(queried.intKey, 1);
}

static const uint32_t crc32_table[] = {
    0x00000000, 0x04c11db7, 0x09823b6e, 0x0d4326d9, 0x130476dc, 0x17c56b6b,
    0x1a864db2, 0x1e475005, 0x2608edb8, 0x22c9f00f, 0x2f8ad6d6, 0x2b4bcb61,
    0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd, 0x4c11db70, 0x48d0c6c7,
    0x4593e01e, 0x4152fda9, 0x5f15adac, 0x5bd4b01b, 0x569796c2, 0x52568b75,
    0x6a1936c8, 0x6ed82b7f, 0x639b0da6, 0x675a1011, 0x791d4014, 0x7ddc5da3,
    0x709f7b7a, 0x745e66cd, 0x9823b6e0, 0x9ce2ab57, 0x91a18d8e, 0x95609039,
    0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5, 0xbe2b5b58, 0xbaea46ef,
    0xb7a96036, 0xb3687d81, 0xad2f2d84, 0xa9ee3033, 0xa4ad16ea, 0xa06c0b5d,
    0xd4326d90, 0xd0f37027, 0xddb056fe, 0xd9714b49, 0xc7361b4c, 0xc3f706fb,
    0xceb42022, 0xca753d95, 0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1,
    0xe13ef6f4, 0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d, 0x34867077, 0x30476dc0,
    0x3d044b19, 0x39c556ae, 0x278206ab, 0x23431b1c, 0x2e003dc5, 0x2ac12072,
    0x128e9dcf, 0x164f8078, 0x1b0ca6a1, 0x1fcdbb16, 0x018aeb13, 0x054bf6a4,
    0x0808d07d, 0x0cc9cdca, 0x7897ab07, 0x7c56b6b0, 0x71159069, 0x75d48dde,
    0x6b93dddb, 0x6f52c06c, 0x6211e6b5, 0x66d0fb02, 0x5e9f46bf, 0x5a5e5b08,
    0x571d7dd1, 0x53dc6066, 0x4d9b3063, 0x495a2dd4, 0x44190b0d, 0x40d816ba,
    0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e, 0xbfa1b04b, 0xbb60adfc,
    0xb6238b25, 0xb2e29692, 0x8aad2b2f, 0x8e6c3698, 0x832f1041, 0x87ee0df6,
    0x99a95df3, 0x9d684044, 0x902b669d, 0x94ea7b2a, 0xe0b41de7, 0xe4750050,
    0xe9362689, 0xedf73b3e, 0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2,
    0xc6bcf05f, 0xc27dede8, 0xcf3ecb31, 0xcbffd686, 0xd5b88683, 0xd1799b34,
    0xdc3abded, 0xd8fba05a, 0x690ce0ee, 0x6dcdfd59, 0x608edb80, 0x644fc637,
    0x7a089632, 0x7ec98b85, 0x738aad5c, 0x774bb0eb, 0x4f040d56, 0x4bc510e1,
    0x46863638, 0x42472b8f, 0x5c007b8a, 0x58c1663d, 0x558240e4, 0x51435d53,
    0x251d3b9e, 0x21dc2629, 0x2c9f00f0, 0x285e1d47, 0x36194d42, 0x32d850f5,
    0x3f9b762c, 0x3b5a6b9b, 0x0315d626, 0x07d4cb91, 0x0a97ed48, 0x0e56f0ff,
    0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623, 0xf12f560e, 0xf5ee4bb9,
    0xf8ad6d60, 0xfc6c70d7, 0xe22b20d2, 0xe6ea3d65, 0xeba91bbc, 0xef68060b,
    0xd727bbb6, 0xd3e6a601, 0xdea580d8, 0xda649d6f, 0xc423cd6a, 0xc0e2d0dd,
    0xcda1f604, 0xc960ebb3, 0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7,
    0xae3afba2, 0xaafbe615, 0xa7b8c0cc, 0xa379dd7b, 0x9b3660c6, 0x9ff77d71,
    0x92b45ba8, 0x9675461f, 0x8832161a, 0x8cf30bad, 0x81b02d74, 0x857130c3,
    0x5d8a9099, 0x594b8d2e, 0x5408abf7, 0x50c9b640, 0x4e8ee645, 0x4a4ffbf2,
    0x470cdd2b, 0x43cdc09c, 0x7b827d21, 0x7f436096, 0x7200464f, 0x76c15bf8,
    0x68860bfd, 0x6c47164a, 0x61043093, 0x65c52d24, 0x119b4be9, 0x155a565e,
    0x18197087, 0x1cd86d30, 0x029f3d35, 0x065e2082, 0x0b1d065b, 0x0fdc1bec,
    0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088, 0x2497d08d, 0x2056cd3a,
    0x2d15ebe3, 0x29d4f654, 0xc5a92679, 0xc1683bce, 0xcc2b1d17, 0xc8ea00a0,
    0xd6ad50a5, 0xd26c4d12, 0xdf2f6bcb, 0xdbee767c, 0xe3a1cbc1, 0xe760d676,
    0xea23f0af, 0xeee2ed18, 0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4,
    0x89b8fd09, 0x8d79e0be, 0x803ac667, 0x84fbdbd0, 0x9abc8bd5, 0x9e7d9662,
    0x933eb0bb, 0x97ffad0c, 0xafb010b1, 0xab710d06, 0xa6322bdf, 0xa2f33668,
    0xbcb4666d, 0xb8757bda, 0xb5365d03, 0xb1f740b4};

uint32_t xcrc32(const void* buf, int len, uint32_t init) {
  const unsigned char* buffer = reinterpret_cast<const unsigned char*>(buf);
  uint32_t crc = init;
  while (len--) {
    crc = (crc << 8) ^ crc32_table[((crc >> 24) ^ *buffer) & 255];
    buffer++;
  }
  return crc;
}

TEST_F(wbtrv32Test, BadDataTest) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/WCCACMS2.DAT");
  ASSERT_FALSE(mbbsEmuDb.empty());

  unsigned char record[400];
  uint16_t key;
  DWORD dwDataBufferLength;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, nullptr,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  unsigned int i = 1;
  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireFirst, posBlock, &record,
                    &dwDataBufferLength, &key, sizeof(key), 0),
            btrieve::BtrieveError::Success);
  do {
    ASSERT_EQ(dwDataBufferLength, 400u);
    ASSERT_EQ(key, i);

    uint32_t crc = xcrc32(record, dwDataBufferLength, 0xFFFFFFFF);

    ASSERT_EQ(wbtrv32_test::badData[i].key, i);
    ASSERT_EQ(wbtrv32_test::badData[i].crc, crc);

    ++i;
  } while (btrcall(btrieve::OperationCode::AcquireNext, posBlock, &record,
                   &dwDataBufferLength, &key, sizeof(key),
                   0) == btrieve::BtrieveError::Success);
}

TEST_F(wbtrv32Test, StopClosesAllDatabases) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Stop, posBlock, nullptr, nullptr,
                    nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::FileNotOpen);
}

// The Major BBS reads a record with a lock bias, then releases it the way
// dfaUnlockCur() does: Unlock with key number -1 and the record's position
// in the data buffer. Any non-zero status here would halt the BBS.
TEST_F(wbtrv32Test, UnlockSingleRecordByPosition) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  RECORD record;
  uint32_t position;
  DWORD dwDataBufferLength = sizeof(record);

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(
      btrcall(btrieve::OperationCode::StepFirst_SingleWaitRecordLock, posBlock,
              &record, &dwDataBufferLength, nullptr, 0, 0),
      btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = sizeof(position);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Unlock, posBlock, &position,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);
}

// dfaUnlock(pos, keynum) with keynum >= 0 sends no data buffer at all.
TEST_F(wbtrv32Test, UnlockCurrentRecordByKeyNumber) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  dwDataBufferLength = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Unlock, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
}

TEST_F(wbtrv32Test, CannotUnlockUnopenedDatabase) {
  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Unlock, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::FileNotOpen);
}

TEST_F(wbtrv32Test, CreateSingleKey) {
  unsigned char buffer[1024];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);

  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 1;
  lpFileSpec->logicalFixedRecordLength = 128;
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->physicalPageSize = 4096 / 512;
  lpFileSpec->fileFlags = 0;  // not variable

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 3;
  lpKeySpec->length = 4;
  lpKeySpec->attributes = UseExtendedDataType | Duplicates;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Integer;

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, nullptr, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(
                        reinterpret_cast<LPCVOID>(toStdString(path).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  path = mbbsEmuDb;
  path /= "test.db";
  ASSERT_TRUE(FileExists(toWideString(path).c_str()));

  btrieve::BtrieveDriver driver(new btrieve::SqliteDatabase());
  ASSERT_EQ(driver.open(toWideString(path).c_str()),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(driver.getRecordCount(), 0u);
  ASSERT_EQ(driver.isVariableLengthRecords(), false);
  ASSERT_EQ(driver.getKeys().size(), 1u);

  EXPECT_EQ(driver.getKeys()[0].getPrimarySegment(),
            btrieve::KeyDefinition(0, 4, 2, btrieve::KeyDataType::Integer,
                                   Duplicates | UseExtendedDataType, false, 0,
                                   0, 0, "", std::vector<char>()));
}

TEST_F(wbtrv32Test, CreateInMemory) {
  unsigned char buffer[1024];
  uint8_t posBlock[128];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);

  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 1;
  lpFileSpec->logicalFixedRecordLength = 128;
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->fileFlags = 0;            // not variable
  lpFileSpec->physicalPageSize = 0xFF;  // indicator for in memory db

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 3;
  lpKeySpec->length = 4;
  lpKeySpec->attributes = UseExtendedDataType | Duplicates;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Integer;

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, posBlock, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(
                        reinterpret_cast<LPCVOID>(toStdString(path).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  path = mbbsEmuDb;
  path /= "test.db";
  ASSERT_FALSE(FileExists(toWideString(path).c_str()));

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr, nullptr,
                    nullptr, 0, 0),
            btrieve::BtrieveError::Success);
}

TEST_F(wbtrv32Test, CreateSingleKeyWithAcs) {
  unsigned char buffer[1024];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);

  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 1;
  lpFileSpec->logicalFixedRecordLength = 128;
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->physicalPageSize = 4096 / 512;
  lpFileSpec->fileFlags = 1;  // variable

  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 3;
  lpKeySpec->length = 16;
  lpKeySpec->attributes = UseExtendedDataType | Duplicates | NumberedACS;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Zstring;

  wbtrv32::LPACSCREATEDATA lpAcsCreateData =
      reinterpret_cast<wbtrv32::LPACSCREATEDATA>(lpKeySpec + 1);
  lpAcsCreateData->header = 0xAC;
  strcpy(lpAcsCreateData->name, "ALLCAPS");
  for (size_t i = 0; i < ARRAYSIZE(lpAcsCreateData->acs); ++i) {
    lpAcsCreateData->acs[i] = toupper(i);
  }

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpAcsCreateData + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, nullptr, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(path.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  path = mbbsEmuDb;
  path /= "test.db";
  ASSERT_TRUE(FileExists(toWideString(path).c_str()));

  btrieve::BtrieveDriver driver(new btrieve::SqliteDatabase());
  ASSERT_EQ(driver.open(toWideString(path).c_str()),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(driver.getRecordCount(), 0u);
  ASSERT_EQ(driver.isVariableLengthRecords(), true);
  ASSERT_EQ(driver.getKeys().size(), 1u);

  std::vector<char> acsVector(ACS_LENGTH);
  memcpy(acsVector.data(), lpAcsCreateData->acs, ACS_LENGTH);

  EXPECT_EQ(
      driver.getKeys()[0].getPrimarySegment(),
      btrieve::KeyDefinition(0, 16, 2, btrieve::KeyDataType::Zstring,
                             Duplicates | UseExtendedDataType | NumberedACS,
                             false, 0, 0, 0, "ALLCAPS", acsVector));
}

TEST_F(wbtrv32Test, CreateMultipleKeysWithAcs) {
  unsigned char buffer[1024];
  auto mbbsEmuDb = tempPath->getTempPath();
  std::filesystem::path path(mbbsEmuDb);
  path /= L"test.dat";

  memset(buffer, 0, sizeof(buffer));

  wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);

  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 3;
  lpFileSpec->logicalFixedRecordLength = 128;
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->physicalPageSize = 4096 / 512;
  lpFileSpec->fileFlags = 1;  // variable

  // first key with an acs
  wbtrv32::LPKEYSPEC lpKeySpec =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->number = 0;
  lpKeySpec->position = 3;
  lpKeySpec->length = 16;
  lpKeySpec->attributes = UseExtendedDataType | Duplicates | NumberedACS;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Zstring;

  // second key, which will have two segments
  ++lpKeySpec;
  lpKeySpec->number = 1;
  lpKeySpec->position = 21;
  lpKeySpec->length = 4;
  lpKeySpec->attributes = UseExtendedDataType | SegmentedKey;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Integer;

  ++lpKeySpec;
  lpKeySpec->number = 1;
  lpKeySpec->position = 25;
  lpKeySpec->length = 8;
  lpKeySpec->attributes = UseExtendedDataType;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Float;

  // third key with a second acs
  ++lpKeySpec;
  lpKeySpec->number = 2;
  lpKeySpec->position = 31;
  lpKeySpec->length = 16;
  lpKeySpec->attributes = UseExtendedDataType | NumberedACS;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Zstring;
  lpKeySpec->acsNumber = 1;

  // first acs
  wbtrv32::LPACSCREATEDATA lpAcsCreateData1 =
      reinterpret_cast<wbtrv32::LPACSCREATEDATA>(lpKeySpec + 1);
  lpAcsCreateData1->header = 0xAC;
  strcpy(lpAcsCreateData1->name, "ALLCAPS");
  for (size_t i = 0; i < ARRAYSIZE(lpAcsCreateData1->acs); ++i) {
    lpAcsCreateData1->acs[i] = toupper(i);
  }

  // second acs
  wbtrv32::LPACSCREATEDATA lpAcsCreateData2 = lpAcsCreateData1 + 1;
  lpAcsCreateData2->header = 0xAC;
  strcpy(lpAcsCreateData2->name, "LOWER");
  for (size_t i = 0; i < ARRAYSIZE(lpAcsCreateData2->acs); ++i) {
    lpAcsCreateData2->acs[i] = tolower(i);
  }

  DWORD dwDataBufferLength =
      reinterpret_cast<unsigned char*>(lpAcsCreateData2 + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, nullptr, buffer,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(path.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  path = mbbsEmuDb;
  path /= "test.db";
  ASSERT_TRUE(FileExists(toWideString(path).c_str()));

  btrieve::BtrieveDriver driver(new btrieve::SqliteDatabase());
  ASSERT_EQ(driver.open(toWideString(path).c_str()),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(driver.getRecordCount(), 0u);
  ASSERT_EQ(driver.isVariableLengthRecords(), true);
  ASSERT_EQ(driver.getKeys().size(), 3u);

  std::vector<char> acsVector(ACS_LENGTH);
  memcpy(acsVector.data(), lpAcsCreateData1->acs, ACS_LENGTH);

  EXPECT_EQ(
      driver.getKeys()[0].getPrimarySegment(),
      btrieve::KeyDefinition(0, 16, 2, btrieve::KeyDataType::Zstring,
                             Duplicates | UseExtendedDataType | NumberedACS,
                             false, 0, 0, 0, "ALLCAPS", acsVector));

  EXPECT_EQ(driver.getKeys()[1].isComposite(), true);
  EXPECT_EQ(driver.getKeys()[1].getSegments().size(), 2u);
  EXPECT_EQ(driver.getKeys()[1].getNumber(), 1);
  EXPECT_EQ(driver.getKeys()[1].getLength(), 12u);

  memcpy(acsVector.data(), lpAcsCreateData2->acs, ACS_LENGTH);
  EXPECT_EQ(driver.getKeys()[2].getPrimarySegment(),
            btrieve::KeyDefinition(2, 16, 30, btrieve::KeyDataType::Zstring,
                                   UseExtendedDataType | NumberedACS, false, 0,
                                   0, 0, "LOWER", acsVector));
}

// The engine adds a record-lock bias (+100..+400) to Get and Query
// operations (dfaAcqEQ etc. with a lock). Locks are ignored, so every biased
// operation must behave exactly like its unbiased form; any non-zero status
// would halt the BBS.
TEST_F(wbtrv32Test, RecordLockBiasIsIgnoredByKeyedOperations) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD dwDataBufferLength = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  // positioning operations, then Next/Previous which continue from them
  const btrieve::OperationCode positioning[] = {
      btrieve::OperationCode::AcquireFirst,
      btrieve::OperationCode::AcquireLast,
      btrieve::OperationCode::AcquireEqual,
      btrieve::OperationCode::AcquireGreater,
      btrieve::OperationCode::AcquireGreaterOrEqual,
      btrieve::OperationCode::AcquireLess,
      btrieve::OperationCode::AcquireLessOrEqual,
      btrieve::OperationCode::QueryFirst,
      btrieve::OperationCode::QueryLast,
      btrieve::OperationCode::QueryEqual,
      btrieve::OperationCode::QueryGreater,
      btrieve::OperationCode::QueryGreaterOrEqual,
      btrieve::OperationCode::QueryLess,
      btrieve::OperationCode::QueryLessOrEqual,
  };
  const btrieve::OperationCode continuing[] = {
      btrieve::OperationCode::AcquireNext,
      btrieve::OperationCode::AcquirePrevious,
      btrieve::OperationCode::QueryNext,
      btrieve::OperationCode::QueryPrevious,
  };

  for (auto op : positioning) {
    for (auto cont : continuing) {
      int results[2][2];
      uint32_t keys[2][2];
      for (int bias = 0; bias <= 400; bias += 100) {
        for (int pass = 0; pass < 2; ++pass) {
          // pass 0 is unbiased (the reference), pass 1 adds the lock bias
          const int add = pass == 0 ? 0 : bias;
          RECORD record;
          uint32_t key = 7776;
          dwDataBufferLength = sizeof(record);
          results[pass][0] = btrcall(op + add, posBlock, &record,
                                     &dwDataBufferLength, &key, 255, 1);
          keys[pass][0] = key;
          dwDataBufferLength = sizeof(record);
          results[pass][1] = btrcall(cont + add, posBlock, &record,
                                     &dwDataBufferLength, &key, 255, 1);
          keys[pass][1] = key;
        }
        ASSERT_EQ(results[1][0], btrieve::BtrieveError::Success)
            << "op " << op + bias;
        ASSERT_EQ(results[0][0], results[1][0]) << "op " << op + bias;
        ASSERT_EQ(keys[0][0], keys[1][0]) << "op " << op + bias;
        ASSERT_EQ(results[0][1], results[1][1])
            << "op " << op + bias << " then " << cont + bias;
        ASSERT_EQ(keys[0][1], keys[1][1])
            << "op " << op + bias << " then " << cont + bias;
      }
    }
  }
}

// A two-segment key shaped like The Major BBS WGSGEN2.DAT key 0: a 30-byte
// zstring that is often empty, then a 25-byte zstring module name.
class CompositeEmptyLeadingStringTest : public wbtrv32Test {
 protected:
  static const int RECORD_LENGTH = 60;

  BtrieveError create(const std::filesystem::path &path, bool inMemory) {
    unsigned char buffer[256];
    memset(buffer, 0, sizeof(buffer));

    auto lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
    lpFileSpec->pageSize = 4096;
    lpFileSpec->numberOfKeys = 1;
    lpFileSpec->logicalFixedRecordLength = RECORD_LENGTH;
    lpFileSpec->fileVersion = 0x60;
    lpFileSpec->physicalPageSize = inMemory ? 0xFF : 0;

    auto lpKeySpec = reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
    lpKeySpec[0].position = 1;
    lpKeySpec[0].length = 30;
    lpKeySpec[0].attributes =
        UseExtendedDataType | Duplicates | Modifiable | SegmentedKey;
    lpKeySpec[0].extendedDataType = btrieve::KeyDataType::Zstring;
    lpKeySpec[1].position = 31;
    lpKeySpec[1].length = 25;
    lpKeySpec[1].attributes = UseExtendedDataType | Duplicates | Modifiable;
    lpKeySpec[1].extendedDataType = btrieve::KeyDataType::Zstring;

    DWORD length = reinterpret_cast<unsigned char *>(lpKeySpec + 2) - buffer;
    return static_cast<BtrieveError>(
        btrcall(btrieve::OperationCode::Create, posBlock, buffer, &length,
                const_cast<char *>(toStdString(path).c_str()), -1, 0));
  }

  BtrieveError open(const std::filesystem::path &path) {
    DWORD length = 0;
    return static_cast<BtrieveError>(
        btrcall(btrieve::OperationCode::Open, posBlock, nullptr, &length,
                const_cast<char *>(toStdString(path).c_str()), -1, 0));
  }

  BtrieveError insert(const char *module) {
    unsigned char record[RECORD_LENGTH];
    memset(record, 0, sizeof(record));
    strcpy_s(reinterpret_cast<char *>(record + 30), 25, module);
    DWORD length = sizeof(record);
    return static_cast<BtrieveError>(btrcall(btrieve::OperationCode::Insert,
                                             posBlock, record, &length,
                                             nullptr, 0, -1));
  }

  // runs a keyed operation on key 0 and returns the record's module name
  std::string get(WORD operation, const char *module, BtrieveError expected) {
    unsigned char record[RECORD_LENGTH];
    unsigned char key[255];
    memset(key, 0, sizeof(key));
    strcpy_s(reinterpret_cast<char *>(key + 30), 25, module);
    DWORD length = sizeof(record);
    EXPECT_EQ(btrcall(operation, posBlock, record, &length, key, 255, 0),
              expected)
        << "op " << operation;
    return expected == BtrieveError::Success
               ? std::string(reinterpret_cast<char *>(record + 30))
               : std::string();
  }

  // An empty leading string must not hide the second segment: records sort
  // by it, and range queries starting below it find them.
  void expectOrderedBySecondSegment() {
    EXPECT_EQ(get(btrieve::OperationCode::AcquireFirst, "",
                  BtrieveError::Success),
              "SNMYTIME");
    EXPECT_EQ(
        get(btrieve::OperationCode::AcquireNext, "", BtrieveError::Success),
        "WORMSG");
    EXPECT_EQ(get(btrieve::OperationCode::AcquireGreater, "A",
                  BtrieveError::Success),
              "SNMYTIME");
    EXPECT_EQ(get(btrieve::OperationCode::AcquireEqual, "WORMSG",
                  BtrieveError::Success),
              "WORMSG");
    EXPECT_EQ(
        get(btrieve::OperationCode::AcquireLast, "", BtrieveError::Success),
        "WORMSG");
  }
};

TEST_F(CompositeEmptyLeadingStringTest, OrdersBySecondSegment) {
  std::filesystem::path path(tempPath->getTempPath());
  path /= L"gen.dat";

  ASSERT_EQ(create(path, true), BtrieveError::Success);
  // inserted out of key order on purpose
  ASSERT_EQ(insert("WORMSG"), BtrieveError::Success);
  ASSERT_EQ(insert("SNMYTIME"), BtrieveError::Success);

  expectOrderedBySecondSegment();
}

// Databases written before the fix stored these keys as NULL; opening one
// must recompute them.
TEST_F(CompositeEmptyLeadingStringTest, UpgradeRecomputesNullCompositeKeys) {
  std::filesystem::path path(tempPath->getTempPath());
  path /= L"gen.dat";
  std::filesystem::path dbPath(path);
  dbPath.replace_extension(L".db");

  ASSERT_EQ(create(path, false), BtrieveError::Success);
  ASSERT_EQ(open(path), BtrieveError::Success);
  ASSERT_EQ(insert("WORMSG"), BtrieveError::Success);
  ASSERT_EQ(insert("SNMYTIME"), BtrieveError::Success);
  DWORD length = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr, &length,
                    nullptr, 0, 0),
            BtrieveError::Success);

  // put the file back into the state a version 3 conversion left it in
  {
    sqlite3 *db;
    ASSERT_EQ(sqlite3_open_v2(toStdString(dbPath).c_str(), &db,
                              SQLITE_OPEN_READWRITE, nullptr),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(db,
                           "UPDATE data_t SET key_0 = NULL; "
                           "UPDATE metadata_t SET version = 3;",
                           nullptr, nullptr, nullptr),
              SQLITE_OK);
    sqlite3_close(db);
  }

  ASSERT_EQ(open(path), BtrieveError::Success);
  expectOrderedBySecondSegment();
}

// The Major BBS keeps the record length in a 16-bit variable and passes its
// address as the 32-bit data length (DFAAPI.C btvu: `(ULONG*)&rlen`), so the
// high word holds whatever was next to it. Btrieve 6.15 uses only the low
// word; so must we, or an Insert reads (and stores) megabytes of garbage.
TEST_F(wbtrv32Test, DataLengthUsesOnlyLowWord) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD dwDataBufferLength = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  // big enough that reading the full 32-bit length stays in bounds
  std::vector<uint8_t> buffer(0x20000);
  auto *record = reinterpret_cast<LPRECORD>(buffer.data());
  uint8_t key[255];

  // a junk high word must not make a too-small buffer look big enough
  dwDataBufferLength = 0x00010000 | 10;
  EXPECT_EQ(btrcall(btrieve::OperationCode::AcquireFirst, posBlock, record,
                    &dwDataBufferLength, key, 255, 0),
            btrieve::BtrieveError::DataBufferLengthOverrun);

  // an insert stores exactly the low-word length
  dwDataBufferLength = sizeof(RECORD);
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireFirst, posBlock, record,
                    &dwDataBufferLength, key, 255, 0),
            btrieve::BtrieveError::Success);
  strcpy_s(record->string1, sizeof(record->string1), "LowWord");
  record->int1 = 424242;
  record->int2 = 0;  // autoincrement

  dwDataBufferLength = 0x00010000 | sizeof(RECORD);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, record,
                    &dwDataBufferLength, key, 255, -1),
            btrieve::BtrieveError::Success);

  uint32_t intKey = 424242;
  dwDataBufferLength = 4096;
  ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireEqual, posBlock, record,
                    &dwDataBufferLength, &intKey, 255, 1),
            btrieve::BtrieveError::Success);
  EXPECT_EQ(dwDataBufferLength, sizeof(RECORD));
  EXPECT_STREQ(record->string1, "LowWord");
}

// Btrieve hands back the stored record on Insert, including any
// autoincrement value it assigned, so callers can read the new id from their
// own buffer (and Update the record next without "changing" that key).
TEST_F(wbtrv32Test, InsertReturnsAssignedAutoincrementValue) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD dwDataBufferLength = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                    &dwDataBufferLength,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  RECORD record;
  memset(&record, 0, sizeof(record));
  strcpy_s(record.string1, sizeof(record.string1), "AutoInc");
  record.int1 = 515151;
  record.int2 = 0;  // key 3: autoincrement, assigned on insert

  dwDataBufferLength = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);

  // the asset holds autoincrement values 1-4
  EXPECT_EQ(record.int2, 5);

  dwDataBufferLength = sizeof(record);
  EXPECT_EQ(btrcall(btrieve::OperationCode::Update, posBlock, &record,
                    &dwDataBufferLength, nullptr, 0, -1),
            btrieve::BtrieveError::Success);
}

// Shaped like The Major BBS GALFMSG2.DAT key 1: unsigned binary segments of 2
// and 4 bytes. Btrieve orders them numerically; a bytewise compare of the raw
// little-endian bytes would put 0x100 before 0xFF.
class CompositeIntegerKeyTest : public wbtrv32Test {
 protected:
#pragma pack(push, 1)
  struct Msg {
    uint16_t forum;
    uint32_t id;
    char text[10];
  };
#pragma pack(pop)

  BtrieveError create(const std::filesystem::path &path) {
    unsigned char buffer[256];
    memset(buffer, 0, sizeof(buffer));

    auto lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
    lpFileSpec->pageSize = 4096;
    lpFileSpec->numberOfKeys = 1;
    lpFileSpec->logicalFixedRecordLength = sizeof(Msg);
    lpFileSpec->fileVersion = 0x60;

    // unique and non-modifiable, so the upgrade must get past both the
    // UNIQUE constraint and the non-modifiable trigger
    auto lpKeySpec = reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
    lpKeySpec[0].position = 1;
    lpKeySpec[0].length = 2;
    lpKeySpec[0].attributes = UseExtendedDataType | SegmentedKey;
    lpKeySpec[0].extendedDataType = btrieve::KeyDataType::UnsignedBinary;
    lpKeySpec[1].position = 3;
    lpKeySpec[1].length = 4;
    lpKeySpec[1].attributes = UseExtendedDataType;
    lpKeySpec[1].extendedDataType = btrieve::KeyDataType::UnsignedBinary;

    DWORD length = reinterpret_cast<unsigned char *>(lpKeySpec + 2) - buffer;
    return static_cast<BtrieveError>(
        btrcall(btrieve::OperationCode::Create, posBlock, buffer, &length,
                const_cast<char *>(toStdString(path).c_str()), -1, 0));
  }

  BtrieveError open(const std::filesystem::path &path) {
    DWORD length = 0;
    return static_cast<BtrieveError>(
        btrcall(btrieve::OperationCode::Open, posBlock, nullptr, &length,
                const_cast<char *>(toStdString(path).c_str()), -1, 0));
  }

  BtrieveError insert(uint16_t forum, uint32_t id) {
    Msg msg = {forum, id, "x"};
    DWORD length = sizeof(msg);
    return static_cast<BtrieveError>(btrcall(
        btrieve::OperationCode::Insert, posBlock, &msg, &length, nullptr, 0, -1));
  }

  // runs a keyed operation and returns "forum:id" of the record found
  std::string get(WORD operation, uint16_t forum, uint32_t id) {
    Msg msg;
    uint8_t key[255];
    memcpy(key, &forum, 2);
    memcpy(key + 2, &id, 4);
    DWORD length = sizeof(msg);
    int status = btrcall(operation, posBlock, &msg, &length, key, 255, 0);
    if (status != 0) {
      return "status " + std::to_string(status);
    }
    return std::to_string(msg.forum) + ":" + std::to_string(msg.id);
  }

  void insertAll() {
    // deliberately out of order, with values on both sides of byte carries
    ASSERT_EQ(insert(2, 0x100), BtrieveError::Success);
    ASSERT_EQ(insert(0x100, 1), BtrieveError::Success);
    ASSERT_EQ(insert(2, 0xFF), BtrieveError::Success);
    ASSERT_EQ(insert(0xFF, 0x10000), BtrieveError::Success);
  }

  // rewrites the keys the way schema versions 3 and 4 stored them (the raw
  // record bytes), keeping the non-modifiable trigger in place
  static void downgradeToVersion4(const std::filesystem::path &dbPath) {
    sqlite3 *db;
    ASSERT_EQ(sqlite3_open_v2(toStdString(dbPath).c_str(), &db,
                              SQLITE_OPEN_READWRITE, nullptr),
              SQLITE_OK);
    std::string triggerSql;
    ASSERT_EQ(sqlite3_exec(
                  db,
                  "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND "
                  "name = 'non_modifiable'",
                  [](void *param, int, char **data, char **) {
                    *reinterpret_cast<std::string *>(param) = data[0];
                    return 0;
                  },
                  &triggerSql, nullptr),
              SQLITE_OK);
    ASSERT_FALSE(triggerSql.empty());
    ASSERT_EQ(sqlite3_exec(db,
                           "DROP TRIGGER non_modifiable; "
                           "UPDATE data_t SET key_0 = substr(data, 1, 6) || x'00'; "
                           "UPDATE data_t SET key_0 = substr(data, 1, 6); "
                           "UPDATE metadata_t SET version = 4;",
                           nullptr, nullptr, nullptr),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(db, triggerSql.c_str(), nullptr, nullptr, nullptr),
              SQLITE_OK);
    sqlite3_close(db);
  }

  void expectNumericOrder() {
    EXPECT_EQ(get(btrieve::OperationCode::AcquireFirst, 0, 0), "2:255");
    EXPECT_EQ(get(btrieve::OperationCode::AcquireNext, 0, 0), "2:256");
    EXPECT_EQ(get(btrieve::OperationCode::AcquireNext, 0, 0), "255:65536");
    EXPECT_EQ(get(btrieve::OperationCode::AcquireNext, 0, 0), "256:1");
    EXPECT_EQ(get(btrieve::OperationCode::AcquireGreater, 2, 0xFF), "2:256");
    EXPECT_EQ(get(btrieve::OperationCode::AcquireLess, 0x100, 0),
              "255:65536");
    EXPECT_EQ(get(btrieve::OperationCode::AcquireEqual, 2, 0x100), "2:256");
  }
};

TEST_F(CompositeIntegerKeyTest, OrdersNumerically) {
  std::filesystem::path path(tempPath->getTempPath());
  path /= L"msg.dat";

  ASSERT_EQ(create(path), BtrieveError::Success);
  ASSERT_EQ(open(path), BtrieveError::Success);
  insertAll();
  expectNumericOrder();
}

// Databases written before schema version 5 hold the raw little-endian key
// bytes; opening one must re-encode them.
TEST_F(CompositeIntegerKeyTest, UpgradeReencodesKeys) {
  std::filesystem::path path(tempPath->getTempPath());
  path /= L"msg.dat";
  std::filesystem::path dbPath(path);
  dbPath.replace_extension(L".db");

  ASSERT_EQ(create(path), BtrieveError::Success);
  ASSERT_EQ(open(path), BtrieveError::Success);
  insertAll();
  DWORD length = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr, &length,
                    nullptr, 0, 0),
            BtrieveError::Success);

  downgradeToVersion4(dbPath);

  ASSERT_EQ(open(path), BtrieveError::Success);
  expectNumericOrder();
}

// Re-encoding row A yields exactly row B's old bytes, so rewriting the unique
// key one row at a time would collide.
TEST_F(CompositeIntegerKeyTest, UpgradeAvoidsUniqueCollisions) {
  std::filesystem::path path(tempPath->getTempPath());
  path /= L"msg.dat";
  std::filesystem::path dbPath(path);
  dbPath.replace_extension(L".db");

  ASSERT_EQ(create(path), BtrieveError::Success);
  ASSERT_EQ(open(path), BtrieveError::Success);
  // A encodes to 01 02 01 02 03 04, B's raw bytes are 01 02 01 02 03 04
  ASSERT_EQ(insert(0x0102, 0x01020304), BtrieveError::Success);
  ASSERT_EQ(insert(0x0201, 0x04030201), BtrieveError::Success);
  DWORD length = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr, &length,
                    nullptr, 0, 0),
            BtrieveError::Success);

  downgradeToVersion4(dbPath);

  ASSERT_EQ(open(path), BtrieveError::Success);
  EXPECT_EQ(get(btrieve::OperationCode::AcquireFirst, 0, 0), "258:16909060");
  EXPECT_EQ(get(btrieve::OperationCode::AcquireNext, 0, 0), "513:67305985");
}

// Shaped like The Major BBS GALALS2.DAT key 2: a single 17-byte zstring with
// duplicates and no null attribute, where many records hold an empty string.
// Btrieve indexes those as "", so range queries must find them.
TEST_F(wbtrv32Test, EmptyStringKeysAreIndexed) {
  unsigned char buffer[256];
  memset(buffer, 0, sizeof(buffer));
  auto lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 1;
  lpFileSpec->logicalFixedRecordLength = 20;
  lpFileSpec->fileVersion = 0x60;
  lpFileSpec->physicalPageSize = 0xFF;  // in memory
  auto lpKeySpec = reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 1;
  lpKeySpec->length = 17;
  lpKeySpec->attributes = UseExtendedDataType | Duplicates | Modifiable;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Zstring;

  std::filesystem::path path(tempPath->getTempPath());
  path /= L"alias.dat";
  DWORD length = reinterpret_cast<unsigned char *>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, posBlock, buffer, &length,
                    const_cast<char *>(toStdString(path).c_str()), -1, 0),
            btrieve::BtrieveError::Success);

  for (const char *alias : {"", "HIMMEY", ""}) {
    char record[20] = {0};
    strcpy_s(record, sizeof(record), alias);
    length = sizeof(record);
    ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, record,
                      &length, nullptr, 0, -1),
              btrieve::BtrieveError::Success);
  }

  auto get = [&](WORD operation, const char *key) {
    char record[20];
    char keyBuffer[255] = {0};
    strcpy_s(keyBuffer, sizeof(keyBuffer), key);
    DWORD length = sizeof(record);
    int status =
        btrcall(operation, posBlock, record, &length, keyBuffer, 255, 0);
    return status == 0 ? "'" + std::string(record) + "'"
                       : "status " + std::to_string(status);
  };

  EXPECT_EQ(get(btrieve::OperationCode::AcquireFirst, ""), "''");
  EXPECT_EQ(get(btrieve::OperationCode::AcquireLast, ""), "'HIMMEY'");
  EXPECT_EQ(get(btrieve::OperationCode::AcquireEqual, ""), "''");
  EXPECT_EQ(get(btrieve::OperationCode::AcquireLessOrEqual, ""), "''");
  EXPECT_EQ(get(btrieve::OperationCode::AcquireGreaterOrEqual, ""), "''");
  EXPECT_EQ(get(btrieve::OperationCode::AcquireLess, "A"), "''");
  EXPECT_EQ(get(btrieve::OperationCode::AcquireGreater, ""), "'HIMMEY'");
  EXPECT_EQ(get(btrieve::OperationCode::AcquireLess, ""), "status 9");
}

// Databases written before schema version 6 stored those empty keys as NULL.
TEST_F(wbtrv32Test, UpgradeIndexesEmptyStringKeys) {
  unsigned char buffer[256];
  memset(buffer, 0, sizeof(buffer));
  auto lpFileSpec = reinterpret_cast<wbtrv32::LPFILESPEC>(buffer);
  lpFileSpec->pageSize = 4096;
  lpFileSpec->numberOfKeys = 1;
  lpFileSpec->logicalFixedRecordLength = 20;
  lpFileSpec->fileVersion = 0x60;
  auto lpKeySpec = reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);
  lpKeySpec->position = 1;
  lpKeySpec->length = 17;
  lpKeySpec->attributes = UseExtendedDataType | Duplicates | Modifiable;
  lpKeySpec->extendedDataType = btrieve::KeyDataType::Zstring;

  std::filesystem::path path(tempPath->getTempPath());
  path /= L"alias.dat";
  std::filesystem::path dbPath(path);
  dbPath.replace_extension(L".db");
  DWORD length = reinterpret_cast<unsigned char *>(lpKeySpec + 1) - buffer;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Create, posBlock, buffer, &length,
                    const_cast<char *>(toStdString(path).c_str()), -1, 0),
            btrieve::BtrieveError::Success);
  length = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, &length,
                    const_cast<char *>(toStdString(path).c_str()), -1, 0),
            btrieve::BtrieveError::Success);
  for (const char *alias : {"", "HIMMEY", ""}) {
    char record[20] = {0};
    strcpy_s(record, sizeof(record), alias);
    length = sizeof(record);
    ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, record,
                      &length, nullptr, 0, -1),
              btrieve::BtrieveError::Success);
  }
  length = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr, &length,
                    nullptr, 0, 0),
            btrieve::BtrieveError::Success);

  {
    sqlite3 *db;
    ASSERT_EQ(sqlite3_open_v2(toStdString(dbPath).c_str(), &db,
                              SQLITE_OPEN_READWRITE, nullptr),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(db,
                           "UPDATE data_t SET key_0 = NULL WHERE key_0 = ''; "
                           "UPDATE metadata_t SET version = 5;",
                           nullptr, nullptr, nullptr),
              SQLITE_OK);
    sqlite3_close(db);
  }

  length = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, &length,
                    const_cast<char *>(toStdString(path).c_str()), -1, 0),
            btrieve::BtrieveError::Success);

  char record[20];
  char key[255] = {0};
  length = sizeof(record);
  EXPECT_EQ(btrcall(btrieve::OperationCode::AcquireLessOrEqual, posBlock,
                    record, &length, key, 255, 0),
            btrieve::BtrieveError::Success);
  EXPECT_STREQ(record, "");
  length = sizeof(record);
  memset(key, 0, sizeof(key));
  EXPECT_EQ(btrcall(btrieve::OperationCode::AcquireGreaterOrEqual, posBlock,
                    record, &length, key, 255, 0),
            btrieve::BtrieveError::Success);
  EXPECT_STREQ(record, "");
}

// GetDirect makes the record it reads the current one, so a following
// GetPosition reports that record (The Major BBS forums: Insert a message,
// GetDirect an older one, GetPosition, later GetDirect + Update with that
// position -- a stale position there rewrote the inserted message).
TEST_F(wbtrv32Test, GetPositionAfterGetDirectReportsRecordRead) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD length = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, &length,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  for (int keyNumber : {0, -1}) {
    RECORD record;
    uint8_t key[255];
    uint32_t first;
    length = sizeof(record);
    ASSERT_EQ(btrcall(btrieve::OperationCode::AcquireFirst, posBlock, &record,
                      &length, key, 255, 1),
              btrieve::BtrieveError::Success);
    length = sizeof(first);
    ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &first,
                      &length, nullptr, 0, 0),
              btrieve::BtrieveError::Success);

    // an insert moves currency to the new record
    memset(&record, 0, sizeof(record));
    strcpy_s(record.string1, sizeof(record.string1), "New");
    record.int1 = 600000 + keyNumber;
    length = sizeof(record);
    ASSERT_EQ(btrcall(btrieve::OperationCode::Insert, posBlock, &record,
                      &length, key, 255, 0),
              btrieve::BtrieveError::Success);

    memcpy(&record, &first, sizeof(first));
    length = sizeof(record);
    ASSERT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord,
                      posBlock, &record, &length, key, 255,
                      static_cast<CHAR>(keyNumber)),
              btrieve::BtrieveError::Success);

    uint32_t after = 0;
    length = sizeof(after);
    ASSERT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &after,
                      &length, nullptr, 0, 0),
              btrieve::BtrieveError::Success);
    EXPECT_EQ(after, first) << "GetDirect with key " << keyNumber;
  }
}

// When the data buffer is too short (status 22), Btrieve 6.15 still makes the
// record current for Step and GetDirect, so GetPosition reports it and a
// following StepNext continues after it. The Major BBS relies on this when
// it walks WGSGEN2.DAT, whose first record can exceed its 8192-byte buffer.
TEST_F(wbtrv32Test, BufferTooShortStillPositionsOnRecord) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  DWORD length = 0;
  ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr, &length,
                    const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                        toStdString(mbbsEmuDb.c_str()).c_str())),
                    -1, 0),
            btrieve::BtrieveError::Success);

  RECORD record;
  uint32_t position;
  auto currentPosition = [&]() {
    uint32_t p = 0;
    DWORD l = sizeof(p);
    EXPECT_EQ(btrcall(btrieve::OperationCode::GetPosition, posBlock, &p, &l,
                      nullptr, 0, 0),
              btrieve::BtrieveError::Success);
    return p;
  };

  length = 10;
  EXPECT_EQ(btrcall(btrieve::OperationCode::StepFirst, posBlock, &record,
                    &length, nullptr, 0, 0),
            btrieve::BtrieveError::DataBufferLengthOverrun);
  EXPECT_EQ(currentPosition(), 1u);

  length = sizeof(record);
  ASSERT_EQ(btrcall(btrieve::OperationCode::StepNext, posBlock, &record,
                    &length, nullptr, 0, 0),
            btrieve::BtrieveError::Success);
  EXPECT_EQ(currentPosition(), 2u);

  length = 10;
  EXPECT_EQ(btrcall(btrieve::OperationCode::StepNext, posBlock, &record,
                    &length, nullptr, 0, 0),
            btrieve::BtrieveError::DataBufferLengthOverrun);
  EXPECT_EQ(currentPosition(), 3u);

  position = 1;
  memcpy(&record, &position, sizeof(position));
  length = 10;
  EXPECT_EQ(btrcall(btrieve::OperationCode::GetDirectChunkOrRecord, posBlock,
                    &record, &length, nullptr, 0, -1),
            btrieve::BtrieveError::DataBufferLengthOverrun);
  EXPECT_EQ(currentPosition(), 1u);
}

// Transactions span every file written until End or Abort. Each test closes
// what it opens and finishes its transaction, since transaction state lives
// for as long as the DLL is loaded.
class wbtrv32TransactionTest : public wbtrv32Test {
 protected:
  // a second copy of MBBSEMU.DB, for transactions across files; call it
  // before copying MBBSEMU.DB itself, which it passes through
  std::basic_string<wchar_t> copyOtherDb() {
    auto copy = tempPath->copyToTempPath("assets/MBBSEMU.DB");
    std::filesystem::path other(copy);
    other.replace_filename("MBBSEMU2.DB");
    std::filesystem::rename(copy, other);
    return other.wstring();
  }

  int open(void *block, const std::basic_string<wchar_t> &path) {
    std::string name = toStdString(path.c_str());
    return btrcall(btrieve::OperationCode::Open, block, nullptr, nullptr,
                   const_cast<char *>(name.c_str()), -1, 0);
  }

  int close(void *block) {
    return btrcall(btrieve::OperationCode::Close, block, nullptr, nullptr,
                   nullptr, 0, 0);
  }

  int operation(WORD op) {
    return btrcall(op, nullptr, nullptr, nullptr, nullptr, 0, 0);
  }

  unsigned int recordCount(void *block) {
    char buffer[80];
    DWORD length = sizeof(buffer);
    EXPECT_EQ(btrcall(btrieve::OperationCode::Stat, block, buffer, &length,
                      nullptr, 0, 0),
              btrieve::BtrieveError::Success);
    return reinterpret_cast<wbtrv32::LPFILESPEC>(buffer)->recordCount;
  }

  // the record count as a fresh open sees it
  unsigned int recordCountOnDisk(const std::basic_string<wchar_t> &path) {
    uint8_t block[POSBLOCK_LENGTH] = {};
    EXPECT_EQ(open(block, path), btrieve::BtrieveError::Success);
    unsigned int count = recordCount(block);
    EXPECT_EQ(close(block), btrieve::BtrieveError::Success);
    return count;
  }

  int deleteFirst(void *block) {
    RECORD record;
    DWORD length = sizeof(record);
    EXPECT_EQ(btrcall(btrieve::OperationCode::StepFirst, block, &record,
                      &length, nullptr, 0, 0),
              btrieve::BtrieveError::Success);
    return btrcall(btrieve::OperationCode::Delete, block, nullptr, nullptr,
                   nullptr, 0, 0);
  }

  int insert(void *block, int32_t int1) {
    RECORD record = {};
    record.int1 = int1;
    record.int2 = int1;
    strcpy(record.string1, "Sysop");
    snprintf(record.string2, sizeof(record.string2), "%d", int1);
    DWORD length = sizeof(record);
    return btrcall(btrieve::OperationCode::Insert, block, &record, &length,
                   nullptr, 0, -1);
  }
};

TEST_F(wbtrv32TransactionTest, AbortDiscardsWritesToEveryFile) {
  auto second = copyOtherDb();
  auto first = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  uint8_t block2[POSBLOCK_LENGTH] = {};
  ASSERT_EQ(open(posBlock, first), btrieve::BtrieveError::Success);
  ASSERT_EQ(open(block2, second), btrieve::BtrieveError::Success);

  // with a lock bias, as The Major BBS sends it (dfaBegTrans)
  ASSERT_EQ(operation(btrieve::OperationCode::BeginTransaction + 100),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(deleteFirst(posBlock), btrieve::BtrieveError::Success);
  ASSERT_EQ(insert(block2, 10000), btrieve::BtrieveError::Success);
  // reads in the transaction see its writes
  EXPECT_EQ(recordCount(posBlock), 3u);
  EXPECT_EQ(recordCount(block2), 5u);

  ASSERT_EQ(operation(btrieve::OperationCode::AbortTransaction),
            btrieve::BtrieveError::Success);
  EXPECT_EQ(recordCount(posBlock), 4u);
  EXPECT_EQ(recordCount(block2), 4u);

  ASSERT_EQ(close(block2), btrieve::BtrieveError::Success);
  ASSERT_EQ(close(posBlock), btrieve::BtrieveError::Success);
  EXPECT_EQ(recordCountOnDisk(first), 4u);
  EXPECT_EQ(recordCountOnDisk(second), 4u);
}

TEST_F(wbtrv32TransactionTest, EndKeepsWritesToEveryFile) {
  auto second = copyOtherDb();
  auto first = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  uint8_t block2[POSBLOCK_LENGTH] = {};
  ASSERT_EQ(open(posBlock, first), btrieve::BtrieveError::Success);
  ASSERT_EQ(open(block2, second), btrieve::BtrieveError::Success);

  ASSERT_EQ(operation(btrieve::OperationCode::BeginTransaction),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(deleteFirst(posBlock), btrieve::BtrieveError::Success);
  ASSERT_EQ(insert(block2, 10000), btrieve::BtrieveError::Success);
  ASSERT_EQ(operation(btrieve::OperationCode::EndTransaction),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(close(block2), btrieve::BtrieveError::Success);
  ASSERT_EQ(close(posBlock), btrieve::BtrieveError::Success);
  EXPECT_EQ(recordCountOnDisk(first), 3u);
  EXPECT_EQ(recordCountOnDisk(second), 5u);
}

TEST_F(wbtrv32TransactionTest, StatusCodes) {
  EXPECT_EQ(operation(btrieve::OperationCode::EndTransaction),
            btrieve::BtrieveError::TransactionNotActive);
  EXPECT_EQ(operation(btrieve::OperationCode::AbortTransaction),
            btrieve::BtrieveError::TransactionNotActive);

  ASSERT_EQ(operation(btrieve::OperationCode::BeginTransaction + 200),
            btrieve::BtrieveError::Success);
  EXPECT_EQ(operation(btrieve::OperationCode::BeginTransaction),
            btrieve::BtrieveError::TransactionIsActive);
  // a transaction with no writes
  EXPECT_EQ(operation(btrieve::OperationCode::EndTransaction),
            btrieve::BtrieveError::Success);
  EXPECT_EQ(operation(btrieve::OperationCode::EndTransaction),
            btrieve::BtrieveError::TransactionNotActive);
}

// A write that fails inside a transaction undoes only itself.
TEST_F(wbtrv32TransactionTest, FailedWriteKeepsTheRestOfTheTransaction) {
  auto db = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_EQ(open(posBlock, db), btrieve::BtrieveError::Success);

  ASSERT_EQ(operation(btrieve::OperationCode::BeginTransaction),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(insert(posBlock, 10000), btrieve::BtrieveError::Success);
  // 3444 is already in the file under a unique key
  ASSERT_EQ(insert(posBlock, 3444), btrieve::BtrieveError::DuplicateKeyValue);
  ASSERT_EQ(insert(posBlock, 10001), btrieve::BtrieveError::Success);
  ASSERT_EQ(operation(btrieve::OperationCode::EndTransaction),
            btrieve::BtrieveError::Success);

  ASSERT_EQ(close(posBlock), btrieve::BtrieveError::Success);
  EXPECT_EQ(recordCountOnDisk(db), 6u);
}

// Closing a file mid-transaction leaves its writes in the transaction, and
// reopening it then works with the same connection.
TEST_F(wbtrv32TransactionTest, CloseAndReopenDuringTransaction) {
  auto db = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_EQ(open(posBlock, db), btrieve::BtrieveError::Success);

  ASSERT_EQ(operation(btrieve::OperationCode::BeginTransaction),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(deleteFirst(posBlock), btrieve::BtrieveError::Success);
  ASSERT_EQ(close(posBlock), btrieve::BtrieveError::Success);

  ASSERT_EQ(open(posBlock, db), btrieve::BtrieveError::Success);
  EXPECT_EQ(recordCount(posBlock), 3u);
  ASSERT_EQ(insert(posBlock, 10000), btrieve::BtrieveError::Success);
  ASSERT_EQ(close(posBlock), btrieve::BtrieveError::Success);

  ASSERT_EQ(operation(btrieve::OperationCode::AbortTransaction),
            btrieve::BtrieveError::Success);
  EXPECT_EQ(recordCountOnDisk(db), 4u);

  ASSERT_EQ(open(posBlock, db), btrieve::BtrieveError::Success);
  ASSERT_EQ(operation(btrieve::OperationCode::BeginTransaction),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(deleteFirst(posBlock), btrieve::BtrieveError::Success);
  ASSERT_EQ(close(posBlock), btrieve::BtrieveError::Success);
  ASSERT_EQ(operation(btrieve::OperationCode::EndTransaction),
            btrieve::BtrieveError::Success);
  EXPECT_EQ(recordCountOnDisk(db), 3u);
}

TEST_F(wbtrv32TransactionTest, StopAbortsTransaction) {
  auto db = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_EQ(open(posBlock, db), btrieve::BtrieveError::Success);

  ASSERT_EQ(operation(btrieve::OperationCode::BeginTransaction),
            btrieve::BtrieveError::Success);
  ASSERT_EQ(deleteFirst(posBlock), btrieve::BtrieveError::Success);
  ASSERT_EQ(operation(btrieve::OperationCode::Stop),
            btrieve::BtrieveError::Success);
  memset(posBlock, 0, sizeof(posBlock));

  EXPECT_EQ(recordCountOnDisk(db), 4u);
  EXPECT_EQ(operation(btrieve::OperationCode::EndTransaction),
            btrieve::BtrieveError::TransactionNotActive);
}
