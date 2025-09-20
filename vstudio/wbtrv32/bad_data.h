#ifndef __BAD_DATA_H_
#define __BAD_DATA_H_

#include <cstdint>

namespace wbtrv32_test {

typedef struct _tagBADDATA {
  uint16_t key;
  uint32_t crc;
} BADDATA;

extern const BADDATA badData[];

} // namespace wbtrv32_test

#endif