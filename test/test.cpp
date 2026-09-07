#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "btrieve/AttributeMask.h"
#include "btrieve/ErrorCode.h"
#include "btrieve/OpenMode.h"
#include "btrieve/OperationCode.h"
#include "vstudio/wbtrv32/framework.h"
#include "vstudio/wbtrv32/wbtrv32.h"

extern "C" int __stdcall BTRCALL(WORD wOperation, LPVOID lpPositionBlock,
                                 LPVOID lpDataBuffer,
                                 LPDWORD lpdwDataBufferLength,
                                 LPVOID lpKeyBuffer, BYTE bKeyLength,
                                 CHAR sbKeyNumber);

// Layout derived from the Stat output:
//   Key 0: position=1   length=30 dataType=11 (Zstring)
//   Key 1: position=31  length=4  dataType=1  (Integer)
//   Key 2: position=135 length=4  dataType=1  (Integer)
#pragma pack(push, 1)
struct Record {
  char name[30];
  int32_t key1;
  uint8_t reserved[100];  // unused bytes between key 1 and key 2
  int32_t key2;
};
#pragma pack(pop)

static_assert(sizeof(Record) == 138);

constexpr int32_t kTargetKey1 = 1379942206;

static void printNameHex(const char *label, const char name[30]) {
  printf("%s (hex): ", label);
  for (int i = 0; i < 30; ++i) {
    printf("%02x ", static_cast<unsigned char>(name[i]));
  }
  printf("\r\n");
}

static void printRecords(uint8_t *posBlock, DWORD recordLength) {
  uint8_t recordBuffer[8192];
  if (recordLength > sizeof(recordBuffer) ||
      recordLength < sizeof(Record)) {
    fprintf(stderr, "Record length %u incompatible with Record struct\r\n",
            recordLength);
    return;
  }

  bool isFirst = true;
  for (;;) {
    DWORD dataBufferLength = sizeof(recordBuffer);
    int error = BTRCALL(isFirst ? btrieve::OperationCode::StepFirst
                                : btrieve::OperationCode::StepNext,
                        posBlock, recordBuffer, &dataBufferLength, nullptr, 0,
                        0);
    isFirst = false;

    if (error == btrieve::BtrieveError::EndOfFile) {
      break;
    }
    if (error != btrieve::BtrieveError::Success) {
      fprintf(stderr, "Failed to step to next record: %s\r\n",
              btrieve::errorToString(static_cast<btrieve::BtrieveError>(error)));
      break;
    }

    Record *record = reinterpret_cast<Record *>(recordBuffer);
    printf("name=%.30s key1=%d key2=%d\r\n", record->name, record->key1,
           record->key2);

    if (record->key1 == kTargetKey1) {
      printNameHex("name before update", record->name);

      record->key2 = rand();

      int updateError = BTRCALL(btrieve::OperationCode::Update, posBlock,
                                recordBuffer, &dataBufferLength, nullptr, 0,
                                -1);
      printf("Update status: %d\r\n", updateError);

      // Re-fetch the record directly from storage (bypassing our in-memory
      // copy) to see what's actually persisted, whether or not the update
      // succeeded.
      uint32_t position;
      DWORD positionLength = sizeof(position);
      int positionError = BTRCALL(btrieve::OperationCode::GetPosition,
                                  posBlock, &position, &positionLength,
                                  nullptr, 0, 0);
      if (positionError == btrieve::BtrieveError::Success) {
        uint8_t verifyBuffer[8192];
        memcpy(verifyBuffer, &position, sizeof(position));
        DWORD verifyLength = sizeof(verifyBuffer);
        int getError = BTRCALL(btrieve::OperationCode::GetDirectChunkOrRecord,
                               posBlock, verifyBuffer, &verifyLength, nullptr,
                               0, -1);
        if (getError == btrieve::BtrieveError::Success) {
          const Record *verify =
              reinterpret_cast<const Record *>(verifyBuffer);
          printNameHex("name after update", verify->name);
        }
      }

      // Step never touches the key buffer, so it can't tell us what's
      // actually stored in the key_0 SQLite column. AcquireEqual can: it
      // looks the record up through that column's index, so searching for
      // this record using its own current name bytes will fail if key_0 is
      // stale relative to the record's real content.
      uint8_t queryKeyBuffer[sizeof(record->name)];
      memcpy(queryKeyBuffer, record->name, sizeof(queryKeyBuffer));

      uint8_t queryBuffer[8192];
      DWORD queryDataLength = sizeof(queryBuffer);
      int queryError = BTRCALL(btrieve::OperationCode::AcquireEqual, posBlock,
                               queryBuffer, &queryDataLength, queryKeyBuffer,
                               sizeof(queryKeyBuffer), 0);
      printf(
          "AcquireEqual on key_0 with this record's own name bytes: %d "
          "(Success means key_0 matches; KeyValueNotFound means it's "
          "stale)\r\n",
          queryError);

      // AcquireEqual repositions currency, so we can't keep StepNext-ing
      // from where we left off. We've already found and diagnosed the
      // record we cared about, so stop here.
      break;
    }
  }
}

static void printKeys(const wbtrv32::LPFILESPEC lpFileSpec,
                      DWORD dataBufferLength) {
  const size_t keySegmentCount =
      (dataBufferLength - sizeof(wbtrv32::FILESPEC)) / sizeof(wbtrv32::KEYSPEC);
  const wbtrv32::LPKEYSPEC lpKeySpecs =
      reinterpret_cast<wbtrv32::LPKEYSPEC>(lpFileSpec + 1);

  for (size_t i = 0; i < keySegmentCount; ++i) {
    const wbtrv32::KEYSPEC &keySpec = lpKeySpecs[i];
    printf(
        "  Key %u: position=%u length=%u dataType=%u attributes=0x%04x "
        "(duplicates=%d modifiable=%d segmented=%d descending=%d) "
        "uniqueKeys=%u nullValue=%u\r\n",
        keySpec.number, keySpec.position, keySpec.length,
        keySpec.extendedDataType, keySpec.attributes,
        (keySpec.attributes & Duplicates) != 0,
        (keySpec.attributes & Modifiable) != 0,
        (keySpec.attributes & SegmentedKey) != 0,
        (keySpec.attributes & DescendingKeySegment) != 0, keySpec.uniqueKeys,
        keySpec.nullValue);
  }
}

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <database file>\r\n", argv[0]);
    return 1;
  }

  srand(static_cast<unsigned int>(time(nullptr)));

  const char *databaseFile = argv[1];
  uint8_t posBlock[POSBLOCK_LENGTH] = {0};
  DWORD dwDataBufferLength = 0;

  int error = BTRCALL(
      btrieve::OperationCode::Open, posBlock, nullptr, &dwDataBufferLength,
      const_cast<char *>(databaseFile),
      static_cast<BYTE>(strlen(databaseFile)), btrieve::OpenMode::Normal);
  if (error != btrieve::BtrieveError::Success) {
    fprintf(stderr, "Failed to open %s: %s\r\n", databaseFile,
            btrieve::errorToString(static_cast<btrieve::BtrieveError>(error)));
    return 1;
  }

  printf("Opened %s\r\n", databaseFile);

  uint8_t statBuffer[8192];
  DWORD statBufferLength = sizeof(statBuffer);
  error = BTRCALL(btrieve::OperationCode::Stat, posBlock, statBuffer,
                  &statBufferLength, nullptr, 0, -1);
  if (error != btrieve::BtrieveError::Success) {
    fprintf(stderr, "Failed to stat %s: %s\r\n", databaseFile,
            btrieve::errorToString(static_cast<btrieve::BtrieveError>(error)));
    return 1;
  }

  const wbtrv32::LPFILESPEC lpFileSpec =
      reinterpret_cast<wbtrv32::LPFILESPEC>(statBuffer);
  printf("Record length: %u\r\n", lpFileSpec->logicalFixedRecordLength);
  printf("Record count: %u\r\n", lpFileSpec->recordCount);
  printf("Number of keys: %u\r\n", lpFileSpec->numberOfKeys);
  printKeys(lpFileSpec, statBufferLength);

  printRecords(posBlock, lpFileSpec->logicalFixedRecordLength);

  error = BTRCALL(btrieve::OperationCode::Close, posBlock, nullptr,
                  &dwDataBufferLength, nullptr, 0, 0);
  if (error != btrieve::BtrieveError::Success) {
    fprintf(stderr, "Failed to close %s: %s\r\n", databaseFile,
            btrieve::errorToString(static_cast<btrieve::BtrieveError>(error)));
    return 1;
  }

  printf("Closed %s\r\n", databaseFile);
  return 0;
}
