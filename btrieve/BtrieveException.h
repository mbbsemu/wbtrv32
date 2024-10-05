#ifndef __FILE_EXCEPTION_H_
#define __FILE_EXCEPTION_H_

#include <stdio.h>

#include <cstdarg>
#include <string>

#include "ErrorCode.h"

namespace btrieve {
class BtrieveException {
 public:
  BtrieveException(BtrieveError error_, const char *format, ...)
      : error(error_) {
    char buf[256];

    va_list args;
    va_start(args, format);
    vsnprintf(buf, sizeof(buf), format, args);
    va_end(args);

    // just in case
    buf[sizeof(buf) - 1] = 0;

    errorMessage = buf;
  }

  BtrieveException(const BtrieveException &ex)
      : errorMessage(ex.errorMessage) {}

  const std::string &getErrorMessage() const { return errorMessage; }

  BtrieveError getError() const { return error; }

 private:
  BtrieveError error;
  std::string errorMessage;
};
}  // namespace btrieve

#endif
