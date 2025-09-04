#include "framework.h"
#include "btrieve/Text.h"

namespace btrieve {
#ifdef WIN32

#else

#include <fcntl.h>
#include <sys/stat.h>

bool FileExists(const wchar_t *filepath) {
  struct stat st;
  return (stat(toStdString(filepath).c_str(), &st) == 0);
}

#endif
}
