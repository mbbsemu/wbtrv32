#include "BtrieveDatabase.h"
#include "BtrieveException.h"

namespace btrieve {

static size_t fread_s(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  size_t numRead = fread(ptr, size, nmemb, stream);
  if (numRead != nmemb) {
    throw BtrieveException(
        "Failed to read all bytes, got %d, wanted %d, errno=%d", numRead, nmemb,
        errno);
  }
  return numRead;
}

static int fseek_s(FILE *stream, long offset, int whence) {
  int ret = fseek(stream, offset, whence);
  if (ret != 0) {
    throw BtrieveException(
        "Failed to seek in file to position [%d|%d], errno=%d", offset, whence,
        errno);
  }
  return ret;
}

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
  fseek_s(f, offset, SEEK_SET);
  fread_s(data, 1, sizeof(data), f);
  return getRecordPointer(std::basic_string_view<uint8_t>(data, sizeof(data)));
}

// Fills set with all record pointers reachable from first.
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

void BtrieveDatabase::validateDatabase(FILE *f, const uint8_t *firstPage) {
  if (firstPage[0] == 'F' && firstPage[1] == 'C') {
    throw BtrieveException(
        "Cannot import v6 Btrieve database - only v5 databases are "
        "supported for now.");
  }

  if (firstPage[0] != 0 && firstPage[1] != 0 && firstPage[2] != 0 &&
      firstPage[3] != 0) {
    throw BtrieveException(
        "Doesn't appear to be a v5 Btrieve database - bad header");
  }

  uint32_t versionCode = firstPage[6] << 16 | firstPage[7];
  switch (versionCode) {
  case 3:
  case 4:
  case 5:
    break;
  default:
    throw BtrieveException(
        "Invalid version code in v5 Btrieve database, got %d wanted 3/4/5",
        versionCode);
  }

  auto needsRecovery = (firstPage[0x22] == 0xFF && firstPage[0x23] == 0xFF);
  if (needsRecovery) {
    throw BtrieveException(
        "Cannot import Btrieve database since it's marked inconsistent and "
        "needs recovery.");
  }

  pageLength = toUint16(firstPage + 0x8);
  if (pageLength < 512 || (pageLength & 0x1FF) != 0) {
    throw BtrieveException(
        "Invalid PageLength, must be multiple of 512. Got %d", pageLength);
  }

  auto accelFlags = toUint16(firstPage + 0xA);
  if (accelFlags != 0) {
    throw BtrieveException("Invalid accel flags, got %d, expected 0",
                           accelFlags);
  }

  auto usrflgs = toUint16(firstPage + 0x106);
  if ((usrflgs & 0x8) != 0) {
    throw BtrieveException("firstPage is compressed, cannot handle");
  }

  variableLengthRecords = ((usrflgs & 0x1) != 0);
  auto recordsContainVariableLength = (firstPage[0x38] == 0xFF);

  if (variableLengthRecords ^ recordsContainVariableLength) {
    throw BtrieveException("Mismatched variable length fields");
  }

  fseek_s(f, 0, SEEK_END);
  long totalSize = ftell(f);

  pageCount = totalSize / pageLength - 1;

  recordCount = toUint16(firstPage + 0x1A) << 16 | toUint16(firstPage + 0x1C);

  recordLength = toUint16(firstPage + 0x16);

  physicalRecordLength = toUint16(firstPage + 0x18);

  keys.resize(toUint16(firstPage + 0x14));
}

bool BtrieveDatabase::isUnusedRecord(std::basic_string_view<uint8_t> data) {
  if (data.size() >= 4 && data.substr(4).find_first_not_of(static_cast<uint8_t>(
                              0)) == std::string_view::npos) {
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
  unsigned int pageOffset = pageLength;

  fseek_s(f, pageLength, SEEK_SET);
  // Starting at 1, since the first page is the header
  for (unsigned int i = 1; i <= pageCount; i++, pageOffset += pageLength) {
    // read in the entire page
    fread_s(data, 1, pageLength, f);
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
      if (deletedRecordOffsets.count(pageOffset + recordOffset) > 0) {
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

        if (!onRecordLoaded(std::basic_string_view<uint8_t>(stream.data(),
                                                            stream.size()))) {
          return;
        }
      } else {
        if (!onRecordLoaded(record)) {
          return;
        }
      }

      recordsLoaded++;
    }
  }

  if (recordsLoaded != recordCount) {
    fprintf(stderr, "Database contains %d records but only read %d!\n",
            recordCount, recordsLoaded);
  }
}

void BtrieveDatabase::parseDatabase(
    const char *fileName, std::function<bool()> onMetadataLoaded,
    std::function<bool(const std::basic_string_view<uint8_t>)> onRecordLoaded,
    std::function<void()> onRecordsComplete) {
  FILE *f = fopen(fileName, "rb");
  if (f == nullptr) {
    fprintf(stderr, "Couldn't open %s\n", fileName);
    return;
  }

  from(f);

  if (onMetadataLoaded() && getRecordCount() > 0) {
    loadRecords(f, onRecordLoaded);
  }

  onRecordsComplete();

  fclose(f);
}

static const uint8_t ACS_PAGE_HEADER[] = {0, 0, 1, 0, 0, 0, 0xAC};

bool BtrieveDatabase::loadACS(FILE *f, std::string &acsName,
                              std::vector<char> &acs) {
  // ACS page immediately follows FCR (the first)
  char secondPage[512];
  fseek_s(f, pageLength, SEEK_SET);
  fread_s(secondPage, 1, sizeof(secondPage), f);

  if (memcmp(secondPage, ACS_PAGE_HEADER, sizeof(ACS_PAGE_HEADER))) {
    acsName.clear();
    acs.clear();
    return false;
  }

  // read the acs data
  char acsNameBuf[10];
  memcpy(acsNameBuf, secondPage + 7, 9);
  acsNameBuf[9] = 0;
  acsName = acsNameBuf;

  acs.resize(ACS_LENGTH);
  memcpy(acs.data(), secondPage + 0xF, ACS_LENGTH);
  return true;
}

void BtrieveDatabase::loadKeyDefinitions(FILE *f, const uint8_t *firstPage,
                                         const std::string &acsName,
                                         const std::vector<char> &acs) {
  unsigned int keyDefinitionBase = 0x110;
  const auto keyDefinitionLength = 0x1E;

  unsigned int totalKeys = keys.size();
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
        /* nullValue= */ data[0x1D], acsName, acs);

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

void BtrieveDatabase::from(FILE *f) {
  uint8_t firstPage[512];
  std::string acsName;
  std::vector<char> acs;

  fseek_s(f, 0, SEEK_END);
  fileLength = ftell(f);
  fseek_s(f, 0, SEEK_SET);

  fread_s(firstPage, 1, sizeof(firstPage), f);

  validateDatabase(f, firstPage);

  getRecordPointerList(f, getRecordPointer(f, 0x10), deletedRecordOffsets);

  loadACS(f, acsName, acs);

  loadKeyDefinitions(f, firstPage, acsName, acs);
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
    throw BtrieveException(
        "Can't find next fragment offset %d, numFragments %d", nextOffset,
        numFragments);
  }

  length = nextOffset - offset;
  // final sanity check
  if (offset < 0xC ||
      (offset + length) > (pageLength - 2 * (numFragments + 1))) {
    throw BtrieveException("Variable data overflows page %d, numFragments %d",
                           offset, numFragments);
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
    fseek_s(f, vpage, SEEK_SET);
    fread_s(data, 1, pageLength, f);

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

  fseek_s(f, filePosition, SEEK_SET);
}

} // namespace btrieve