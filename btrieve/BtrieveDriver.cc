#include "BtrieveDriver.h"
#include "BtrieveDatabase.h"
#include <filesystem>
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace btrieve {
BtrieveDriver::~BtrieveDriver() { close(); }

void BtrieveDriver::open(const char *fileName) {
  struct stat statbuf;
  std::filesystem::path dbPath =
      std::filesystem::path(fileName).replace_extension(
          sqlDatabase->getFileExtension());

  if (stat(dbPath.c_str(), &statbuf) == 0) {
    // create the db now
  }

  /*if (fileInfoDAT.Exists && fileInfoDB.Exists && fileInfoDAT.LastWriteTime >
     fileInfoDB.LastWriteTime)
          {
              _logger.Warn($"{fullPathDAT} is newer than {fullPathDB},
     reconverting the DAT -> DB"); File.Delete(fullPathDB); fileInfoDB = new
     FileInfo(fullPathDB);
          }*/

  BtrieveDatabase btrieveDatabase;
  std::unique_ptr<RecordLoader> recordLoader;
  btrieveDatabase.parseDatabase(
      fileName,
      [this, &dbPath, &btrieveDatabase, &recordLoader]() {
        recordLoader = sqlDatabase->create(dbPath.c_str(), btrieveDatabase);
        return true;
      },
      [&recordLoader](const std::basic_string_view<uint8_t> record) {
        return recordLoader->onRecordLoaded(record);
      },
      [&recordLoader]() { recordLoader->onRecordsComplete(); });
  // btrieveFile.LoadFile(_logger, path, loadedFileName);
  // CreateSqliteDB(fullPathDB, btrieveFile);
  // throw BtrieveException("new path is %s", dbPath.c_str());
}

void BtrieveDriver::close() {
  sqlDatabase->close();
  // release ownership and delete
  sqlDatabase.reset(nullptr);
}
} // namespace btrieve