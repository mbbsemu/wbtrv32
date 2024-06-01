#ifndef __FILE_EXCEPTION_H_
#define __FILE_EXCEPTION_H_

#include <cstdarg>
#include <stdio.h>

namespace btrieve {
class BtrieveException {
public:
  BtrieveException(const char *format, ...) {
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

  const std::string &getErrorMessage() { return errorMessage; }

private:
  std::string errorMessage;
};
} // namespace btrieve

#endif
