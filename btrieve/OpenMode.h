#ifndef __OPEN_MODE_H_
#define __OPEN_MODE_H_

namespace btrieve {
enum OpenMode {
  Normal = 0,
  Accelerated = -1,
  ReadOnly = -2,
  VerifyWriteOperations = -3,
  ExclusiveAccess = -4
};
}

#endif
