#include "../../btrieve/ErrorCode.h"
#include "../../btrieve/TestBase.h"
#include "btrieve/OperationCode.h"
#include "gtest/gtest.h"
#include <memory>
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>

class wbtrv32 : public TestBase {};

typedef int(__stdcall *BTRCALL)(WORD wOperation, LPVOID lpPositionBlock,
                                LPVOID lpDataBuffer,
                                LPDWORD lpdwDataBufferLength,
                                LPVOID lpKeyBuffer, BYTE bKeyLength,
                                CHAR sbKeyNumber);

TEST_F(wbtrv32, LoadsAndUnloadsDll) {
  std::unique_ptr<std::remove_pointer<HMODULE>::type, decltype(&FreeLibrary)>
      dll(LoadLibrary(TEXT("wbtrv32.dll")), &FreeLibrary);

  ASSERT_NE(dll.get(), nullptr);
}

TEST_F(wbtrv32, CannotCloseUnopenedDatabase) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  std::unique_ptr<std::remove_pointer<HMODULE>::type, decltype(&FreeLibrary)>
      dll(LoadLibrary(TEXT("wbtrv32.dll")), &FreeLibrary);
  ASSERT_NE(dll.get(), nullptr);

  BTRCALL btrcall =
      reinterpret_cast<BTRCALL>(GetProcAddress(dll.get(), "BTRCALL"));
  ASSERT_NE(btrcall, nullptr);

  unsigned char posBlock[128];
  DWORD dwDataBufferLength = 0;

  ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                    &dwDataBufferLength, nullptr, 0, 0),
            btrieve::BtrieveError::FileNotOpen);
}

TEST_F(wbtrv32, LoadsAndClosesDatabase) {
  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
  ASSERT_FALSE(mbbsEmuDb.empty());

  std::unique_ptr<std::remove_pointer<HMODULE>::type, decltype(&FreeLibrary)>
      dll(LoadLibrary(TEXT("wbtrv32.dll")), &FreeLibrary);
  ASSERT_NE(dll.get(), nullptr);

  BTRCALL btrcall =
      reinterpret_cast<BTRCALL>(GetProcAddress(dll.get(), "BTRCALL"));
  ASSERT_NE(btrcall, nullptr);

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