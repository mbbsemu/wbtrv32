#include "SqliteUtil.h"

#include "BtrieveException.h"
#include "sqlite/sqlite3.h"

namespace btrieve {
void throwException(int errorCode) {
  const char *sqlite3ErrMsg = sqlite3_errstr(errorCode);

  throw BtrieveException(BtrieveError::IOError,
                         sqlite3ErrMsg == nullptr ? "Sqlite error: [%d]"
                                                  : "Sqlite error: [%d] - [%s]",
                         errorCode, sqlite3ErrMsg);
}
}  // namespace btrieve
