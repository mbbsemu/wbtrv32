// Creates an empty wbtrv32 database from a Btrieve description (.BCR) file,
// equivalent to `butil -create <outputFile> <descriptionFile>`.
//
// The description file is translated into a Btrieve Create operation and
// handed to wbtrv32's BTRCALL, so the resulting database is exactly what an
// application issuing the same Create would get.

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

#include "btrieve/BtrieveDriver.h"
#include "btrieve/ErrorCode.h"
#include "btrieve/OperationCode.h"
#include "util/BcrParser.h"
#include "vstudio/wbtrv32/framework.h"
#include "vstudio/wbtrv32/wbtrv32.h"

extern "C" int __stdcall BTRCALL(WORD wOperation, LPVOID lpPositionBlock,
                                 LPVOID lpDataBuffer,
                                 LPDWORD lpdwDataBufferLength,
                                 LPVOID lpKeyBuffer, BYTE bKeyLength,
                                 CHAR sbKeyNumber);

static int usage(const char *program) {
  fprintf(stderr,
          "usage: %s [--overwrite] <outputFile> <descriptionFile>\n"
          "\n"
          "Creates an empty wbtrv32 database described by a Btrieve "
          "description\n"
          "(.BCR) file. The database is written next to outputFile with a "
          ".db\n"
          "extension. Fails if the file already exists unless --overwrite "
          "is given.\n",
          program);
  return 2;
}

int main(int argc, const char **argv) {
  bool overwrite = false;
  const char *outputFile = nullptr;
  const char *descriptionFile = nullptr;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--overwrite") == 0 || strcmp(argv[i], "-o") == 0) {
      overwrite = true;
    } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    } else if (outputFile == nullptr) {
      outputFile = argv[i];
    } else if (descriptionFile == nullptr) {
      descriptionFile = argv[i];
    } else {
      return usage(argv[0]);
    }
  }

  if (outputFile == nullptr || descriptionFile == nullptr) {
    return usage(argv[0]);
  }

  std::vector<uint8_t> buffer;
  try {
    std::filesystem::path descriptionPath(descriptionFile);
    bcr::Description description = bcr::parseFile(descriptionPath);
    buffer = bcr::toCreateBuffer(description, descriptionPath.parent_path());
  } catch (const bcr::BcrParseException &ex) {
    fprintf(stderr, "%s: %s\n", descriptionFile, ex.what());
    return 1;
  }

  uint8_t posBlock[POSBLOCK_LENGTH] = {};
  DWORD dwDataBufferLength = static_cast<DWORD>(buffer.size());
  // key number -1 tells Create to fail rather than replace an existing file
  int error = BTRCALL(btrieve::OperationCode::Create, posBlock, buffer.data(),
                      &dwDataBufferLength, const_cast<char *>(outputFile), 0,
                      overwrite ? 0 : -1);
  if (error != btrieve::BtrieveError::Success) {
    fprintf(stderr, "Create of %s failed: %s (%d)\n", outputFile,
            btrieve::errorToString(static_cast<btrieve::BtrieveError>(error)),
            error);
    return 1;
  }

  printf("Created %s\n",
         btrieve::sqlPathFor(outputFile, L"db").string().c_str());
  return 0;
}
