#ifndef __RECORD_LOADER_H_
#define __RECORD_LOADER_H_

#include "Key.h"
#include <string>
#include <unordered_set>
#include <vector>

namespace btrieve {
class BtrieveDatabase {
public:
  BtrieveDatabase() {}

  BtrieveDatabase(const BtrieveDatabase &database)
      : keys(database.keys), fileName(database.fileName),
        deletedRecordOffsets(database.deletedRecordOffsets),
        pageLength(database.pageLength), pageCount(database.pageCount),
        recordLength(database.recordLength),
        physicalRecordLength(database.physicalRecordLength),
        recordCount(database.recordCount), fileLength(database.fileLength),
        keyCount(database.keyCount), logKeyPresent(database.logKeyPresent),
        variableLengthRecords(database.variableLengthRecords) {}

  BtrieveDatabase(BtrieveDatabase &&database)
      : keys(std::move(database.keys)), fileName(std::move(database.fileName)),
        deletedRecordOffsets(std::move(database.deletedRecordOffsets)),
        pageLength(database.pageLength), pageCount(database.pageCount),
        recordLength(database.recordLength),
        physicalRecordLength(database.physicalRecordLength),
        recordCount(database.recordCount), fileLength(database.fileLength),
        keyCount(database.keyCount), logKeyPresent(database.logKeyPresent),
        variableLengthRecords(database.variableLengthRecords) {}

  const std::vector<Key> &getKeys() const { return keys; }

  unsigned int getRecordLength() const { return recordLength; }

  unsigned int getPhysicalRecordLength() const { return physicalRecordLength; }

  uint16_t getPageLength() const { return pageLength; }

  const std::string &getFilename() const { return fileName; }

  unsigned int getPageCount() const { return pageCount; }

  unsigned int getRecordCount() const { return recordCount; }

  bool isLogKeyPresent() const { return logKeyPresent; }

  bool isVariableLengthRecords() const { return variableLengthRecords; }

  // throws FileException
  void parseDatabase(const std::string &fileName,
                     std::function<bool()> onMetadataLoaded,
                     std::function<bool(const std::basic_string_view<uint8_t>)>
                         onRecordLoaded);

private:
  void from(FILE *f);

  const char *validateDatabase(FILE *f, const uint8_t *firstPage);

  bool loadACS(FILE *f, char *acs);
  void loadKeyDefinitions(FILE *f, const uint8_t *firstPage, const char *acs);

  void loadRecords(FILE *f,
                   std::function<bool(const std::basic_string_view<uint8_t>)>
                       onRecordLoaded);

  bool isUnusedRecord(std::basic_string_view<uint8_t> fixedRecordData);

  void getVariableLengthData(FILE *f,
                             std::basic_string_view<uint8_t> recordData,
                             std::vector<uint8_t> &stream);

  uint32_t getFragment(std::basic_string_view<uint8_t> page, uint32_t fragment,
                       uint32_t numFragments, uint32_t &length,
                       bool &nextPointerExists);

  std::vector<Key> keys;
  std::string fileName;
  std::unordered_set<uint32_t> deletedRecordOffsets;

  uint16_t pageLength;
  unsigned int pageCount;

  unsigned int recordLength;
  unsigned int physicalRecordLength;
  unsigned int recordCount;

  unsigned int fileLength;

  unsigned int keyCount;

  bool logKeyPresent;

  bool variableLengthRecords;
};

} // namespace btrieve

#endif