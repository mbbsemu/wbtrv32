#include "BtrieveDatabase.h"

namespace btrieve {

static inline uint16_t toUint16(const void *ptr) {
  auto p = reinterpret_cast<const uint8_t *>(ptr);
  return p[0] | p[1] << 8;
}

static inline uint32_t toUint32(const void *ptr) {
  auto p = reinterpret_cast<const uint8_t *>(ptr);
  return p[0] | p[1] << 8 | p[2] << 16 | p[3] << 24;
}

static inline uint32_t getRecordPointer(std::basic_string_view<uint8_t> data) {
  // 2 byte high word -> 2 byte low word
  return static_cast<uint32_t>(toUint16(data.data())) << 16 |
         static_cast<uint32_t>(toUint16(data.data() + 2));
}

static uint32_t getRecordPointer(FILE *f, uint32_t offset) {
  uint8_t data[4];
  fseek(f, offset, SEEK_SET);
  fread(data, 1, sizeof(data), f);
  return getRecordPointer(std::basic_string_view<uint8_t>(data, sizeof(data)));
}

static void getRecordPointerList(FILE *f, uint32_t first,
                                 std::unordered_set<uint32_t> &set) {
  while (first != 0xFFFFFFFF) {
    set.insert(first);

    first = getRecordPointer(f, first);
  }
}

static inline uint32_t
getPageFromVariableLengthRecordPointer(std::basic_string_view<uint8_t> data) {
  // high low mid, yep - it's stupid
  return (uint)data[0] << 16 | (uint)data[1] | (uint)data[2] << 8;
}

static uint16_t
getPageOffsetFromFragmentArray(std::basic_string_view<uint8_t> arrayEntry,
                               bool &nextPointerExists) {
  if (arrayEntry[0] == 0xFF && arrayEntry[1] == 0xFF) {
    nextPointerExists = false;
    return 0xFFFF;
  }

  uint16_t offset = (uint)arrayEntry[0] | ((uint)arrayEntry[1] & 0x7F) << 8;
  nextPointerExists = (arrayEntry[1] & 0x80) != 0;
  return offset;
}

static void append(std::vector<uint8_t> &vector,
                   std::basic_string_view<uint8_t> data) {
  unsigned int vectorSize = vector.size();
  vector.resize(vectorSize + data.size());
  memcpy(vector.data() + vectorSize, data.data(), data.size());
}

const char *BtrieveDatabase::validateDatabase(FILE *f,
                                              const uint8_t *firstPage) {
  if (firstPage[0] == 'F' && firstPage[1] == 'C') {
    return "Cannot import v6 Btrieve database - only v5 databases are "
           "supported for now.";
  }

  if (firstPage[0] != 0 && firstPage[1] != 0 && firstPage[2] != 0 &&
      firstPage[3] != 0) {
    return "Doesn't appear to be a v5 Btrieve database - bad header";
  }

  uint32_t versionCode = firstPage[6] << 16 | firstPage[7];
  switch (versionCode) {
  case 3:
  case 4:
  case 5:
    break;
  default:
    return "Invalid version code in v5 Btrieve database";
  }

  auto needsRecovery = (firstPage[0x22] == 0xFF && firstPage[0x23] == 0xFF);
  if (needsRecovery) {
    return "Cannot import Btrieve database since it's marked inconsistent and "
           "needs recovery.";
  }

  pageLength = toUint16(firstPage + 0x8);
  if (pageLength < 512 || (pageLength & 0x1FF) != 0) {
    return "Invalid PageLength, must be multiple of 512";
  }

  auto accelFlags = toUint16(firstPage + 0xA);
  if (accelFlags != 0) {
    return "Invalid accel flags, expected 0";
  }

  auto usrflgs = toUint16(firstPage + 0x106);
  if ((usrflgs & 0x8) != 0) {
    return "firstPage is compressed, cannot handle";
  }

  variableLengthRecords = ((usrflgs & 0x1) != 0);
  auto recordsContainVariableLength = (firstPage[0x38] == 0xFF);

  if (variableLengthRecords ^ recordsContainVariableLength) {
    return "Mismatched variable length fields";
  }

  fseek(f, 0, SEEK_END);
  long totalSize = ftell(f);

  pageCount = totalSize / pageLength - 1;

  recordCount = toUint16(firstPage + 0x1A) << 16 | toUint16(firstPage + 0x1C);

  recordLength = toUint16(firstPage + 0x16);

  physicalRecordLength = toUint16(firstPage + 0x18);

  keyCount = toUint16(firstPage + 0x14);

  return nullptr;
}

bool BtrieveDatabase::isUnusedRecord(std::basic_string_view<uint8_t> data) {
  if (data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 0) {
    // additional validation, to ensure the record pointer is valid
    uint32_t offset = getRecordPointer(data);
    // sanity check to ensure the data is valid
    if (offset < fileLength) {
      return true;
    }
  }

  return false;
}

void BtrieveDatabase::loadRecords(
    FILE *f,
    std::function<bool(const std::basic_string_view<uint8_t>)> onRecordLoaded) {
  unsigned int recordsLoaded = 0;
  uint8_t *const data = reinterpret_cast<uint8_t *>(alloca(pageLength));
  const unsigned int recordsInPage = ((pageLength - 6) / physicalRecordLength);

  fseek(f, pageLength, SEEK_SET);
  // Starting at 1, since the first page is the header
  for (unsigned int i = 1; i <= pageCount; i++) {
    // read in the entire page
    fread(data, 1, pageLength, f);
    // Verify Data Page, high bit set on byte 5 (usage count)
    if ((data[0x5] & 0x80) == 0) {
      continue;
    }

    // page data starts 6 bytes in
    unsigned int recordOffset = 6;
    for (unsigned int j = 0; j < recordsInPage;
         j++, recordOffset += physicalRecordLength) {
      if (recordsLoaded == recordCount) {
        return;
      }

      // Marked for deletion? Skip
      if (deletedRecordOffsets.count(recordOffset) > 0) {
        continue;
      }

      std::basic_string_view<uint8_t> record =
          std::basic_string_view<uint8_t>(data + recordOffset, recordLength);
      if (isUnusedRecord(record)) {
        break;
      }

      if (variableLengthRecords) {
        std::vector<uint8_t> stream(recordLength);
        memcpy(stream.data(), record.data(), record.size());

        getVariableLengthData(f,
                              std::basic_string_view<uint8_t>(
                                  data + recordOffset, physicalRecordLength),
                              stream);

        onRecordLoaded(
            std::basic_string_view<uint8_t>(stream.data(), stream.size()));
      } else {
        onRecordLoaded(record);
      }

      recordsLoaded++;
    }
  }

  if (recordsLoaded != recordCount) {
    fprintf(stderr, "Database contains %d records but only read %d!\n",
            recordCount, recordsLoaded);
  }
}

bool BtrieveDatabase::parseDatabase(
    const std::string &fileName,
    std::function<bool(const BtrieveDatabase &database)> onMetadataLoaded,
    std::function<bool(const std::basic_string_view<uint8_t>)> onRecordLoaded) {
  FILE *f = fopen(fileName.c_str(), "rb");
  if (f == nullptr) {
    fprintf(stderr, "Couldn't open %s\n", fileName.c_str());
    return false;
  }

  BtrieveDatabase database;
  bool ret = database.from(f);
  if (ret) {
    if (onMetadataLoaded(database) && database.getRecordCount() > 0) {
      database.loadRecords(f, onRecordLoaded);
    }
  } else {
    fprintf(stderr, "Couldn't load %s: %s\n", fileName.c_str(), "generic");
  }
  fclose(f);
  return ret;
}

static const uint8_t ACS_PAGE_HEADER[] = {0, 0, 1, 0, 0, 0, 0xAC};

bool BtrieveDatabase::loadACS(FILE *f, char *acs) {
  // ACS page immediately follows FCR (the first)
  char secondPage[512];
  fseek(f, pageLength, SEEK_SET);
  fread(secondPage, 1, sizeof(secondPage), f);

  if (memcmp(secondPage, ACS_PAGE_HEADER, sizeof(ACS_PAGE_HEADER))) {
    memset(acs, 0, 256);
    return false;
  }

  // read the acs data
  char acsName[10];
  memcpy(acsName, secondPage + 7, 9);
  acsName[9] = 0;

  memcpy(acs, secondPage + 0xF, 256);
  return true;
}

void BtrieveDatabase::loadKeyDefinitions(FILE *f, const uint8_t *firstPage,
                                         const char *acs) {
  unsigned int keyDefinitionBase = 0x110;
  const auto keyDefinitionLength = 0x1E;

  logKeyPresent = (firstPage[0x10C] == 1);

  keys.resize(keyCount);

  unsigned int totalKeys = keyCount;
  unsigned int currentKeyNumber = 0;
  while (currentKeyNumber < totalKeys) {
    auto data = std::basic_string_view<uint8_t>(firstPage + keyDefinitionBase,
                                                keyDefinitionLength);
    KeyDataType dataType;

    uint16_t attributes = toUint16(data.data() + 0x8);
    if (attributes & UseExtendedDataType) {
      dataType = (KeyDataType)data[0x1C];
    } else {
      dataType = (attributes & OldStyleBinary) ? KeyDataType::OldBinary
                                               : KeyDataType::OldAscii;
    }
    KeyDefinition keyDefinition(
        /* number= */ currentKeyNumber,
        /* length= */ toUint16(data.data() + 0x16),
        /* offset= */ toUint16(data.data() + 0x14), dataType, attributes,
        /* segment= */ attributes & SegmentedKey,
        /* segmentOf= */
        (attributes & SegmentedKey) ? currentKeyNumber : (ushort)0,
        /* segmentIndex= */ 0,
        /* nullValue= */ data[0x1D], acs);

    // If it's a segmented key, don't increment so the next key gets added to
    // the same ordinal as an additional segment
    if (!keyDefinition.isSegment()) {
      currentKeyNumber++;
    }

    keys[keyDefinition.getNumber()].addSegment(keyDefinition);

    keyDefinitionBase += keyDefinitionLength;
  }

  for (auto &key : keys) {
    key.updateSegmentIndices();
  }
}

bool BtrieveDatabase::from(FILE *f) {
  uint8_t firstPage[512];
  char acs[256];

  fseek(f, 0, SEEK_END);
  fileLength = ftell(f);
  fseek(f, 0, SEEK_SET);

  fread(firstPage, 1, sizeof(firstPage), f);

  if (validateDatabase(f, firstPage) != nullptr) {
    return false;
  }

  getRecordPointerList(f, getRecordPointer(f, 0x10), deletedRecordOffsets);

  loadACS(f, acs);

  loadKeyDefinitions(f, firstPage, acs);

  return true;
}

uint32_t BtrieveDatabase::getFragment(std::basic_string_view<uint8_t> page,
                                      uint32_t fragment, uint32_t numFragments,
                                      uint32_t &length,
                                      bool &nextPointerExists) {
  auto offsetPointer = pageLength - 2 * (fragment + 1);
  auto offset = getPageOffsetFromFragmentArray(page.substr(offsetPointer, 2),
                                               nextPointerExists);

  // to compute length, keep going until I read the next valid fragment and get
  // its offset then we subtract the two offets to compute length
  auto nextFragmentOffset = offsetPointer;
  uint32_t nextOffset = 0xFFFFFFFFu;
  for (unsigned int i = fragment + 1; i <= numFragments; ++i) {
    bool unused;
    // fragment array is at end of page and grows downward
    nextFragmentOffset -= 2; 
    nextOffset = getPageOffsetFromFragmentArray(
        page.substr(nextFragmentOffset, 2), unused);
    if (nextOffset == 0xFFFF) {
      continue;
    }
    // valid offset, break now
    break;
  }

  // some sanity checks
  if (nextOffset == 0xFFFFFFFF) {
    // throw new ArgumentException($"Can't find next fragment offset {fragment}
    // numFragments:{numFragments} {FileName}");
    // TODO
    return offset;
  }

  length = nextOffset - offset;
  // final sanity check
  if (offset < 0xC ||
      (offset + length) > (pageLength - 2 * (numFragments + 1))) {
    // throw new ArgumentException($"Variable data overflows page {fragment}
    // numFragments:{numFragments} {FileName}");
    // TODO
    return offset;
  }

  return offset;
}

void BtrieveDatabase::getVariableLengthData(
    FILE *f, std::basic_string_view<uint8_t> recordData,
    std::vector<uint8_t> &stream) {
  unsigned int filePosition = ftell(f);
  std::basic_string_view<uint8_t> variableData(
      recordData.data() + recordLength, physicalRecordLength - recordLength);
  auto vrecPage = getPageFromVariableLengthRecordPointer(variableData);
  auto vrecFragment = variableData[3];
  uint8_t *data = reinterpret_cast<uint8_t *>(alloca(pageLength));

  while (true) {
    // invalid page? abort and return what we have
    if (vrecPage == 0xFFFFFF && vrecFragment == 0xFF) {
      break;
    }

    // jump to that page
    auto vpage = vrecPage * pageLength;
    fseek(f, vpage, SEEK_SET);
    fread(data, 1, pageLength, f);

    auto numFragmentsInPage = toUint16(data + 0xA);
    uint32_t length;
    bool nextPointerExists;
    // grab the fragment pointer
    auto offset = getFragment(std::basic_string_view<uint8_t>(data, pageLength),
                              vrecFragment, numFragmentsInPage, length,
                              nextPointerExists);
    // now finally read the data!
    std::basic_string_view<uint8_t> variableData(data + offset, length);
    if (!nextPointerExists) {
      // read all the data and reached the end!
      append(stream, variableData);
      break;
    }

    // keep going through more pages!
    vrecPage = getPageFromVariableLengthRecordPointer(variableData);
    vrecFragment = variableData[3];

    append(stream, variableData.substr(4));
  }

  fseek(f, filePosition, SEEK_SET);
}

} // namespace btrieve