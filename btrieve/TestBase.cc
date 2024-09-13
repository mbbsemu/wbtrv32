#include "TestBase.h"
#include "BtrieveException.h"
#include <filesystem>
#include <list>
#include <string>

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#include <combaseapi.h>
#else
#include <dirent.h>
#endif

#pragma pack(push, 1)
typedef struct _taguuid {
  uint32_t a;
  uint16_t b;
  uint16_t c;
  uint16_t d;
  uint16_t e;
  uint32_t f;
} uuid;
#pragma pack(pop)

static_assert(sizeof(uuid) == 16);

bool TempPath::create() {
  char buf[64];

#ifdef WIN32
  GUID guid;
  WCHAR guidBuf[ARRAYSIZE(buf)];
  size_t unused;

  CoCreateGuid(&guid);

  StringFromGUID2(guid, guidBuf, ARRAYSIZE(guidBuf));

  wcstombs_s(&unused, buf, ARRAYSIZE(buf), guidBuf, _TRUNCATE);
#else
  uuid id;
  std::unique_ptr<FILE, decltype(&fclose)> f(fopen("/dev/random", "r"),
                                             &fclose);
  if (!f) {
    return false;
  }

  if (fread(&id, 1, sizeof(id), f.get()) != sizeof(id)) {
    return false;
  }

  snprintf(buf, sizeof(buf), "%X-%X-%X-%X-%X%X", id.a, id.b, id.c, id.d, id.e,
           id.f);
  buf[sizeof(buf) - 1] = 0;
#endif
  tempFolder = buf;

  std::string tempPath = getTempPath();
#ifdef WIN32
  int result = mkdir(tempPath.c_str());
#else
  int result = mkdir(tempPath.c_str(), 0700);
#endif

  if (result == 0) {
    return true;
  } else if (result == EEXIST) {
    deleteAllFiles(tempPath.c_str());
    return true;
  }

  return false;
}

TempPath::~TempPath() {
  auto tempPath = getTempPath();

  deleteAllFiles(tempPath.c_str());

  rmdir(tempPath.c_str());
}

std::string TempPath::getTempPath() {
  std::filesystem::path tempPath(testing::TempDir());
  tempPath /= std::filesystem::path(tempFolder.c_str());

#ifdef WIN32
  char path[MAX_PATH];
  size_t unused;
  wcstombs_s(&unused, path, tempPath.c_str(), _TRUNCATE);
  return std::string(path);
#else
  return tempPath.c_str();
#endif
}

std::basic_string<tchar> TempPath::copyToTempPath(const char *filePath) {
  const size_t bufferSize = 32 * 1024;

  std::filesystem::path destPath(getTempPath());
  destPath /= std::filesystem::path(filePath).filename();

  {
    FILE *src = fopen(filePath, "rb");
    std::unique_ptr<FILE, decltype(&fclose)> sourceFile(src, &fclose);
    if (!sourceFile) {
      throw btrieve::BtrieveException("Can't open %s\n", filePath);
    }

    std::unique_ptr<FILE, decltype(&fclose)> destFile(
#ifdef WIN32
        _wfopen(destPath.c_str(), _TEXT("wb")), &fclose);
#else
        fopen(destPath.c_str(), "wb"), &fclose);
#endif
    if (!destFile) {
      throw btrieve::BtrieveException("Can't open %s\n", destPath.c_str());
    }

    size_t numRead;
    size_t numWritten;
    std::unique_ptr<uint8_t> buffer(new uint8_t[bufferSize]);
    while ((numRead = fread(reinterpret_cast<void *>(buffer.get()), 1,
                            bufferSize, sourceFile.get())) > 0) {
      numWritten = fwrite(reinterpret_cast<void *>(buffer.get()), 1, numRead,
                          destFile.get());
      if (numWritten != numRead) {
        throw btrieve::BtrieveException("Can't write data\n");
      }
    }
  }

  return destPath;
}

void TempPath::deleteAllFiles(const char *filePath) {
  std::list<std::string> filesToUnlink;

#ifdef WIN32
  WIN32_FIND_DATAA data;
  char path[MAX_PATH];
  ZeroMemory(&data, sizeof(data));
  // windows is dumb and I have to append *.* to the path for searching
  strcpy(path, filePath);
  strcat(path, "\\*.*");
  HANDLE hFind = FindFirstFileA(path, &data);
  DWORD dwError = GetLastError();
  ASSERT_NE(hFind, INVALID_HANDLE_VALUE);
  do {
    if (strcmp(data.cFileName, ".") && strcmp(data.cFileName, "..")) {
      filesToUnlink.push_front(data.cFileName);
    }
  } while (FindNextFileA(hFind, &data));

  FindClose(hFind);
#else
  {
    struct closedir_deleter {
      void operator()(DIR *d) const { closedir(d); }
    };

    std::unique_ptr<DIR, closedir_deleter> dir(opendir(filePath));
    if (dir) {
      struct dirent *d;

      while ((d = readdir(dir.get()))) {
        if (strcmp(d->d_name, ".") && strcmp(d->d_name, "..")) {
          filesToUnlink.push_front(d->d_name);
        }
      }
    }
  }
#endif

  for (auto &fileName : filesToUnlink) {
    std::filesystem::path path(filePath);
    path /= fileName;
#ifdef WIN32
    EXPECT_EQ(_wunlink(path.c_str()), 0);
#else
    EXPECT_EQ(unlink(path.c_str()), 0);
#endif
  }
}
