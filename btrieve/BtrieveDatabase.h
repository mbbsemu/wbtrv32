#ifndef __RECORD_LOADER_H_
#define __RECORD_LOADER_H_

#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

#include "Key.h"
#include "Text.h"

namespace btrieve {

enum RecordType {
  Fixed = 0,
  Variable = 1,
  VariableTruncated = 2,
  Compressed = 3,
  UsesVAT = 4,
  CompressedVariable = 5,
};

class BtrieveDatabase {
 public:
  // Constructs an empty BtrieveDatabase.
  // Afterwards, call parseDatabase.
  BtrieveDatabase()
      : deletedRecordOffsets(0),
        pageLength(0),
        pageCount(0),
        recordLength(0),
        physicalRecordLength(0),
        recordCount(0),
        fileLength(0),
        recordType(RecordType::Fixed),
        v6(false) {}

  BtrieveDatabase(const BtrieveDatabase &database)
      : keys(database.keys),
        deletedRecordOffsets(database.deletedRecordOffsets),
        pageLength(database.pageLength),
        pageCount(database.pageCount),
        recordLength(database.recordLength),
        physicalRecordLength(database.physicalRecordLength),
        recordCount(database.recordCount),
        fileLength(database.fileLength),
        recordType(database.recordType),
        v6(database.v6) {}

  BtrieveDatabase(BtrieveDatabase &&database)
      : keys(std::move(database.keys)),
        deletedRecordOffsets(std::move(database.deletedRecordOffsets)),
        pageLength(database.pageLength),
        pageCount(database.pageCount),
        recordLength(database.recordLength),
        physicalRecordLength(database.physicalRecordLength),
        recordCount(database.recordCount),
        fileLength(database.fileLength),
        recordType(database.recordType),
        v6(database.v6) {}

  BtrieveDatabase(const std::vector<Key> &keys_, uint16_t pageLength_,
                  unsigned int pageCount_, unsigned int recordLength_,
                  unsigned int physicalRecordLength_, unsigned int recordCount_,
                  unsigned int fileLength_, RecordType recordType_, bool v6_,
                  uint16_t fcrKeyAttributeTableOffset_)
      : keys(keys_),
        pageLength(pageLength_),
        pageCount(pageCount_),
        recordLength(recordLength_),
        physicalRecordLength(physicalRecordLength_),
        recordCount(recordCount_),
        fileLength(fileLength_),
        recordType(recordType_),
        v6(v6_) {}

  // Returns the set of keys contained in this Btrieve database.
  const std::vector<Key> &getKeys() const { return keys; }

  // Returns he fixed length portion of each record in this Btrieve database
  // containing only user data.
  unsigned int getRecordLength() const { return recordLength; }

  // Returns the total record length of each record in this Btrieve database,
  // including metadata.
  unsigned int getPhysicalRecordLength() const { return physicalRecordLength; }

  // Returns the page length of the Btrieve database. Will be a multiple of 512.
  uint16_t getPageLength() const { return pageLength; }

  // Returns the total number of pages in this Btrieve database.
  unsigned int getPageCount() const { return pageCount; }

  // Returns the total number of records contained in this Btrieve database.
  unsigned int getRecordCount() const { return recordCount; }

  // Returns whether the records in this database are variable length or fixed.
  bool isVariableLengthRecords() const {
    return recordType == RecordType::Variable ||
           recordType == RecordType::VariableTruncated;
  }

  // Reads and parses the entire Btrieve DAT database.
  // Calls onMetadataLoaded when the header is read and getter methods on this
  // instance can be safely accessed. Return false to prevent reading any
  // records. Calls onRecordLoaded for each record in the database. Return false
  // to discontinue enumeration. Throws BtrieveException when a critical error
  // is encountered.
  void parseDatabase(
      const tchar *fileName, std::function<bool()> onMetadataLoaded,
      std::function<bool(const std::basic_string_view<uint8_t>)> onRecordLoaded,
      std::function<void()> onRecordsComplete = []() {});

 private:
  typedef struct _tagKEYDEFINITION_DATA {
    uint16_t fcrOffset;
    uint32_t keyAttributeTableOffset;
  } KEYDEFINITIONDATA;

  // Reads and validates the metadata from the Btrieve database identified by f.
  void from(FILE *f);

  // Validates the Btrieve database header to ensure values are
  // expected/consistent.
  KEYDEFINITIONDATA validateDatabase(FILE *f, const uint8_t *firstPage);

  // Loads the PAT and validates each page
  bool loadPAT(FILE *f, std::string &acsName, std::vector<char> &acs);

  // Loads the ACS, if present, into acs, which is expected to be at least 256
  // bytes in size. If no ACS, acsName and acs are emptied.
  bool loadACS(FILE *f, std::string &acsName, std::vector<char> &acs,
               uint32_t pageNumber);

  // Loads the key definitions into the keys member variables, given the acs
  // loaded previously from loadACS. acsName and acs could both be empty.
  void loadKeyDefinitions(FILE *f, const uint8_t *firstPage,
                          const KEYDEFINITIONDATA &keyDefinitionData,
                          const std::string &acsName,
                          const std::vector<char> &acs);

  // Enumerates all records and calls onRecordLoaded for each one.
  void loadRecords(FILE *f,
                   std::function<bool(const std::basic_string_view<uint8_t>)>
                       onRecordLoaded);

  // Determines whether the data contained in fixedRecordData is unused.
  // Unused records have a 4-byte record pointer pointing to the next available
  // record, and the rest of the bytes are all 0.
  bool isUnusedRecord(std::basic_string_view<uint8_t> fixedRecordData);

  // Reads the entirety of a variable length record from the initial recordData
  // (up to physicalRecordLength). Stores all the data inside stream.
  void getVariableLengthData(FILE *f,
                             std::basic_string_view<uint8_t> recordData,
                             std::vector<uint8_t> &stream);

  int32_t logicalPageToPhysicalOffset(FILE *f, int32_t logicalPage);

  // The list of keys defined in the Btrieve database.
  std::vector<Key> keys;
  // The list of deleted record pointers inside the Btrieve database.
  // Data that exists on these record pointers will be skipped.
  std::unordered_set<uint32_t> deletedRecordOffsets;

  // The page length of the Btrieve database. Will be a multiple of 512.
  uint16_t pageLength;
  // The total number of pages in this Btrieve database.
  unsigned int pageCount;

  // The fixed length portion of each record in this Btrieve database containing
  // only user data.
  unsigned int recordLength;
  // The total record length of each record in this Btrieve database, including
  // metadata.
  unsigned int physicalRecordLength;
  // The total number of records contained in this Btrieve database.
  unsigned int recordCount;

  // The total size in bytes of this Btrieve database.
  unsigned int fileLength;

  // The type of records contained in this Btrieve database.
  RecordType recordType;

  // Whether the database is version 6.0. Otherwise it's 5.0
  bool v6;
};

}  // namespace btrieve

#endif
