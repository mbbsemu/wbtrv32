#ifndef __WBTRV32_H_
#define __WBTRV32_H_

#include <stdint.h>

#define POSBLOCK_LENGTH 128

namespace wbtrv32 {
#pragma pack(push, 1)

typedef struct _tagFILESPEC {
  uint16_t logicalFixedRecordLength;
  uint16_t pageSize;
  uint8_t numberOfKeys;
  uint8_t fileVersion;  // not always set
  uint32_t recordCount;
  uint16_t fileFlags;
  uint8_t numExtraPointers;
  uint8_t physicalPageSize;
  uint16_t preallocatedPages;
} FILESPEC, *LPFILESPEC;

typedef struct _tagKEYSPEC {
  uint16_t position;
  uint16_t length;
  uint16_t attributes;
  uint32_t uniqueKeys;
  uint8_t extendedDataType;
  uint8_t nullValue;
  uint16_t reserved;
  uint8_t number;
  uint8_t acsNumber;
} KEYSPEC, *LPKEYSPEC;
#pragma pack(pop)

static_assert(sizeof(FILESPEC) == 16);
static_assert(sizeof(KEYSPEC) == 16);

void processAttach();

void processDetach();
}  // namespace wbtrv32

#endif
