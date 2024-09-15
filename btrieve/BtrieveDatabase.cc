#include "BtrieveDatabase.h"

#include "BtrieveException.h"

namespace btrieve {

static size_t fread_s(void* ptr, size_t size, size_t nmemb, FILE* stream) {
  size_t numRead = fread(ptr, size, nmemb, stream);
  if (numRead != nmemb) {
    throw BtrieveException(
        "Failed to read all bytes, got %d, wanted %d, errno=%d", numRead, nmemb,
        errno);
  }
  return numRead;
}

static int fseek_s(FILE* stream, long offset, int whence) {
  int ret = fseek(stream, offset, whence);
  if (ret != 0) {
    throw BtrieveException(
        "Failed to seek in file to position [%d|%d], errno=%d", offset, whence,
        errno);
  }
  return ret;
}

static inline uint16_t toUint16(const void* ptr) {
  auto p = reinterpret_cast<const uint8_t*>(ptr);
  return p[0] | p[1] << 8;
}

static inline uint32_t toUint32(const void* ptr) {
  auto p = reinterpret_cast<const uint8_t*>(ptr);
  return p[0] | p[1] << 8 | p[2] << 16 | p[3] << 24;
}

static inline uint32_t getRecordPointer(std::basic_string_view<uint8_t> data) {
  // 2 byte high word -> 2 byte low word
  return static_cast<uint32_t>(toUint16(data.data())) << 16 |
         static_cast<uint32_t>(toUint16(data.data() + 2));
}

static uint32_t getRecordPointer(FILE* f, uint32_t offset) {
  uint8_t data[4];
  fseek_s(f, offset, SEEK_SET);
  fread_s(data, 1, sizeof(data), f);
  return getRecordPointer(std::basic_string_view<uint8_t>(data, sizeof(data)));
}

// Fills set with all record pointers reachable from first.
static void getRecordPointerList(FILE* f, uint32_t first,
                                 std::unordered_set<uint32_t>& set) {
  while (first != 0xFFFFFFFF) {
    set.insert(first);

    first = getRecordPointer(f, first);
  }
}

static inline uint32_t getPageFromVariableLengthRecordPointer(
    std::basic_string_view<uint8_t> data) {
  // high low mid, yep - it's stupid
  return static_cast<uint32_t>(data[0]) << 16 | static_cast<uint32_t>(data[1]) |
         static_cast<uint32_t>(data[2]) << 8;
}

static uint16_t getPageOffsetFromFragmentArray(
    std::basic_string_view<uint8_t> arrayEntry, bool& nextPointerExists) {
  if (arrayEntry[0] == 0xFF && arrayEntry[1] == 0xFF) {
    nextPointerExists = false;
    return 0xFFFF;
  }

  uint16_t offset = static_cast<uint16_t>(arrayEntry[0]) |
                    (static_cast<uint16_t>(arrayEntry[1]) & 0x7F) << 8;
  nextPointerExists = (arrayEntry[1] & 0x80) != 0;
  return offset;
}

static void append(std::vector<uint8_t>& vector,
                   std::basic_string_view<uint8_t> data) {
  size_t vectorSize = vector.size();
  vector.resize(vectorSize + data.size());
  memcpy(vector.data() + vectorSize, data.data(), data.size());
}

void BtrieveDatabase::validateDatabase(FILE* f, const uint8_t* firstPage) {
  const uint8_t* fcr = firstPage;

  v6 = fcr[0] == 'F' && fcr[1] == 'C' && fcr[2] == 0 && fcr[3] == 0;

  if (fcr[0] != 0 && fcr[1] != 0 && fcr[2] != 0 && fcr[3] != 0) {
    throw BtrieveException(
        "Doesn't appear to be a v5 Btrieve database - bad header");
  }

  pageLength = toUint16(fcr + 0x8);
  if (pageLength < 512 || (pageLength & 0x1FF) != 0) {
    throw BtrieveException(
        "Invalid PageLength, must be multiple of 512. Got %d", pageLength);
  }

  // find the valid FCR in v6
  if (v6) {
    uint8_t* wholePage = reinterpret_cast<uint8_t*>(_alloca(pageLength));
    fseek_s(f, pageLength, SEEK_SET);
    fread_s(wholePage, 1, pageLength, f);

    // check the usage count to find the active FCR
    uint32_t usageCount1 = toUint32(fcr + 4);
    uint32_t usageCount2 = toUint32(wholePage + 4);

    fcr = wholePage;

    // get the FCR based on usage counts, if first page we need to read in the
    // entire thing since pageLength might not equal 512 which we read initially
    if (usageCount1 > usageCount2) {
      fseek_s(f, 0, SEEK_SET);
      fread_s(wholePage, 1, pageLength, f);
    }
  } else {  // for v5 databases
    uint16_t versionCode = fcr[6] << 16 | fcr[7];
    switch (versionCode) {
      case 3:
      case 4:
      case 5:
        break;
      default:
        throw BtrieveException("Invalid version code, expected 3/4/5, got %d",
                               versionCode);
    }

    bool needsRecovery = (fcr[0x22] == 0xFF && fcr[0x23] == 0xFF);
    if (needsRecovery) {
      if (needsRecovery) {
        throw BtrieveException(
            "Cannot import Btrieve database since it's marked inconsistent and "
            "needs recovery.");
      }
    }
  }

  auto accelFlags = toUint16(fcr + 0xA);
  if (accelFlags != 0) {
    throw BtrieveException("Invalid accel flags, got %d, expected 0",
                           accelFlags);
  }

  auto usrflgs = toUint16(fcr + 0x106);
  if ((usrflgs & 0x8) != 0) {
    throw BtrieveException("firstPage is compressed, cannot handle");
  }

  variableLengthRecords = ((usrflgs & 0x1) != 0);
  variableLengthTruncation = ((usrflgs & 0x2) != 0);
  auto recordsContainVariableLength = (fcr[0x38] == 0xFF);

  if (variableLengthRecords ^ recordsContainVariableLength) {
    throw BtrieveException("Mismatched variable length fields");
  }

  fseek_s(f, 0, SEEK_END);
  long totalSize = ftell(f);

  pageCount = totalSize / pageLength - 1;

  recordCount = toUint16(fcr + 0x1A) << 16 | toUint16(fcr + 0x1C);

  recordLength = toUint16(fcr + 0x16);

  physicalRecordLength = toUint16(fcr + 0x18);

  keys.resize(toUint16(fcr + 0x14));

  memcpy(this->fcr, fcr, sizeof(this->fcr));
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
    FILE* f,
    std::function<bool(const std::basic_string_view<uint8_t>)> onRecordLoaded) {
  unsigned int recordsLoaded = 0;
  uint8_t* const data = reinterpret_cast<uint8_t*>(alloca(pageLength));
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
    const tchar* fileName, std::function<bool()> onMetadataLoaded,
    std::function<bool(const std::basic_string_view<uint8_t>)> onRecordLoaded,
    std::function<void()> onRecordsComplete) {
#ifdef WIN32
  FILE* f = _wfopen(fileName, _TEXT("rb"));
#else
  FILE* f = fopen(fileName, _TEXT("rb"));
#endif
  if (f == nullptr) {
#ifdef WIN32
    fwprintf(stderr, _TEXT("Couldn't open %s\n"), fileName);
#else
    fprintf(stderr, "Couldn't open %s\n", fileName);
#endif
    return;
  }

  from(f);

  if (onMetadataLoaded() && getRecordCount() > 0) {
    loadRecords(f, onRecordLoaded);
  }

  onRecordsComplete();

  fclose(f);
}

bool BtrieveDatabase::loadPAT(FILE* f, std::string& acsName,
                              std::vector<char>& acs) {
  uint8_t* pat1 = reinterpret_cast<uint8_t*>(_alloca(pageLength));
  uint8_t* pat2 = reinterpret_cast<uint8_t*>(_alloca(pageLength));

  fseek_s(f, pageLength * 2, SEEK_SET);
  fread_s(pat1, 1, pageLength, f);
  fread_s(pat2, 1, pageLength, f);

  if (pat1[0] != 'P' || pat1[1] != 'P') {
    throw BtrieveException("PAT1 table is invalid");
  }
  if (pat2[0] != 'P' || pat2[1] != 'P') {
    throw BtrieveException("PAT2 table is invalid");
  }

  // check out the usage count to find active pat1/2
  uint16_t usageCount1 = toUint16(pat1 + 4);
  uint16_t usageCount2 = toUint16(pat2 + 4);
  // scan page type code to find ACS/Index/etc pages
  uint8_t* activePat = (usageCount1 > usageCount2) ? pat1 : pat2;
  uint16_t sequenceNumber = activePat[2] << 8 | activePat[3];

  // enumerate all pages
  for (int i = 8; i < pageLength; i += 4) {
    uint8_t type = activePat[i + 1];
    int pageNumber =
        activePat[i] << 16 | activePat[i + 2] | activePat[i + 3] << 8;
    // codes are 'A' for ACS, D for fixed-length data pages, E for extra pages
    // and V for variable length pages index have high bit set
    if ((type & 0x80) != 0) {
      continue;
    }

    if (type == 'A') {
      loadACS(f, acsName, acs, pageNumber);
    }

    if (type != 0 && type != 'A' && type != 'D' && type != 'E' && type != 'V') {
      throw BtrieveException("Bad PAT entry");
    }
  }
}

bool BtrieveDatabase::loadACS(FILE* f, std::string& acsName,
                              std::vector<char>& acs, uint32_t pageNumber) {
  static const uint8_t ACS_PAGE_HEADER[] = {0, 0, 1, 0, 0, 0, 0xAC};

  char* acsPage = reinterpret_cast<char*>(_alloca(pageLength));
  fseek_s(f, pageNumber * pageLength, SEEK_SET);
  fread_s(acsPage, 1, pageLength, f);

  if (v6) {
    if (acsPage[1] != 'A' && acsPage[6] != 0xAC) {
      throw BtrieveException("Bad v6 ACS header!");
    }
  } else {
    if (memcmp(acsPage, ACS_PAGE_HEADER, sizeof(ACS_PAGE_HEADER))) {
      acsName.clear();
      acs.clear();
      return false;
    }
  }

  // read the acs data
  char acsNameBuf[10];
  memcpy(acsNameBuf, acsPage + 7, 9);
  acsNameBuf[9] = 0;
  acsName = acsNameBuf;

  acs.resize(ACS_LENGTH);
  memcpy(acs.data(), acsPage + 0xF, ACS_LENGTH);
  return true;
}

void BtrieveDatabase::loadKeyDefinitions(FILE* f, const uint8_t* firstPage,
                                         const std::string& acsName,
                                         const std::vector<char>& acs) {
  const auto keyDefinitionLength = 0x1E;

  uint8_t data[512];
  size_t totalKeys = keys.size();
  unsigned int currentKeyNumber = 0;
  std::vector<uint32_t> keyOffsets;
  keyOffsets.resize(totalKeys + 1);

  if (v6) {
    if (fcr[0x76] != totalKeys) {
      throw BtrieveException("Key number in KAT mismatches earlier key count");
    }

    uint16_t katOffset = fcr[0x78] | fcr[0x79] << 8;

    uint8_t* ptr = data;
    fseek_s(f, katOffset, SEEK_SET);
    fread_s(data, 1, sizeof(uint16_t) * totalKeys, f);

    for (int i = 0; i < totalKeys; ++i, ptr += 2) {
      keyOffsets[i] = ptr[0] | ptr[1] << 8;
    }
  } else {
    const uint32_t keyDefinitionBase = 0x110;
    for (int i = 0; i < totalKeys; ++i) {
      keyOffsets[i] = keyDefinitionBase + (i * keyDefinitionLength);
    }
  }

  uint32_t keyOffset = keyOffsets[currentKeyNumber];

  while (currentKeyNumber < totalKeys) {
    fseek_s(f, keyOffset, SEEK_SET);
    fread_s(data, 1, keyDefinitionLength, f);

    std::basic_string_view<uint8_t>(data, keyDefinitionLength);
    KeyDataType dataType;

    uint16_t attributes = toUint16(data + 0x8);
    if (attributes & UseExtendedDataType) {
      dataType = (KeyDataType)data[0x1C];
    } else {
      dataType = (attributes & OldStyleBinary) ? KeyDataType::OldBinary
                                               : KeyDataType::OldAscii;
    }
    KeyDefinition keyDefinition(
        /* number= */ currentKeyNumber,
        /* length= */ toUint16(data + 0x16),
        /* offset= */ toUint16(data + 0x14), dataType, attributes,
        /* segment= */ attributes & SegmentedKey,
        /* segmentOf= */
        (attributes & SegmentedKey) ? currentKeyNumber
                                    : static_cast<uint16_t>(0),
        /* segmentIndex= */ 0,
        /* nullValue= */ data[0x1D], acsName, acs);

    // If it's a segmented key, don't increment so the next key gets added to
    // the same ordinal as an additional segment
    if (!keyDefinition.isSegment()) {
      keyOffset = keyOffsets[++currentKeyNumber];
    } else {
      keyOffset += keyDefinitionLength;
    }

    keys[keyDefinition.getNumber()].addSegment(keyDefinition);
  }

  for (auto& key : keys) {
    key.updateSegmentIndices();
  }
}

void BtrieveDatabase::from(FILE* f) {
  uint8_t firstPage[512];
  std::string acsName;
  std::vector<char> acs;

  fseek_s(f, 0, SEEK_END);
  fileLength = ftell(f);
  fseek_s(f, 0, SEEK_SET);

  fread_s(firstPage, 1, sizeof(firstPage), f);

  validateDatabase(f, firstPage);

  if (v6) {
    loadPAT(f, acsName, acs);
  } else {
    getRecordPointerList(f, getRecordPointer(f, 0x10), deletedRecordOffsets);

    loadACS(f, acsName, acs, 1);  // acs always on first page
  }

  loadKeyDefinitions(f, firstPage, acsName, acs);
}

uint32_t BtrieveDatabase::getFragment(std::basic_string_view<uint8_t> page,
                                      uint32_t fragment, uint32_t numFragments,
                                      uint32_t& length,
                                      bool& nextPointerExists) {
  auto offsetPointer = pageLength - 2 * (fragment + 1);
  auto offset = getPageOffsetFromFragmentArray(page.substr(offsetPointer, 2),
                                               nextPointerExists);

  // to compute length, keep going until I read the next valid fragment and
  // get its offset then we subtract the two offets to compute length
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
    FILE* f, std::basic_string_view<uint8_t> recordData,
    std::vector<uint8_t>& stream) {
  unsigned int filePosition = ftell(f);
  std::basic_string_view<uint8_t> variableData(
      recordData.data() + recordLength, physicalRecordLength - recordLength);
  auto vrecPage = getPageFromVariableLengthRecordPointer(variableData);
  auto vrecFragment = variableData[3];
  uint8_t* data = reinterpret_cast<uint8_t*>(alloca(pageLength));

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

}  // namespace btrieve
