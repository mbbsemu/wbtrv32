#include "BtrieveDatabase.h"

#include "BtrieveException.h"

namespace btrieve {

static size_t fread_s(void* ptr, size_t bytes, FILE* stream) {
  size_t numRead = fread(ptr, 1, bytes, stream);
  if (numRead != bytes) {
    throw BtrieveException(
        BtrieveError::IOError,
        "Failed to read all bytes, got %d, wanted %d, errno=%d", numRead, bytes,
        errno);
  }
  return numRead;
}

static int fseek_s(FILE* stream, long offset, int whence) {
  int ret = fseek(stream, offset, whence);
  if (ret != 0) {
    throw BtrieveException(
        BtrieveError::IOError,
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
  fread_s(data, sizeof(data), f);
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

static void append(std::vector<uint8_t>& vector,
                   std::basic_string_view<uint8_t> data) {
  size_t vectorSize = vector.size();
  vector.resize(vectorSize + data.size());
  memcpy(vector.data() + vectorSize, data.data(), data.size());
}

BtrieveDatabase::FCRDATA BtrieveDatabase::validateDatabase(
    FILE* f, const uint8_t* firstPage) {
  const uint8_t* fcr = firstPage;
  FCRDATA fcrData;

  fcrData.fcrOffset = 0;

  v6 = fcr[0] == 'F' && fcr[1] == 'C' && fcr[2] == 0 && fcr[3] == 0;

  if (fcr[0] != 0 && fcr[1] != 0 && fcr[2] != 0 && fcr[3] != 0) {
    throw BtrieveException(
        BtrieveError::NotBtrieveFile,
        "Doesn't appear to be a v5 Btrieve database - bad header");
  }

  pageLength = toUint16(fcr + 0x8);
  if (pageLength < 512 || (pageLength & 0x1FF) != 0) {
    throw BtrieveException(
        BtrieveError::NotBtrieveFile,
        "Invalid PageLength, must be multiple of 512. Got %d", pageLength);
  }

  // find the valid FCR in v6
  if (v6) {
    uint8_t* wholePage = reinterpret_cast<uint8_t*>(alloca(pageLength));
    fseek_s(f, pageLength, SEEK_SET);
    fread_s(wholePage, pageLength, f);

    // check the usage count to find the active FCR
    uint32_t usageCount1 = toUint32(fcr + 4);
    uint32_t usageCount2 = toUint32(wholePage + 4);

    fcr = wholePage;

    // get the FCR based on usage counts, if first page we need to read in the
    // entire thing since pageLength might not equal 512 which we read initially
    if (usageCount1 > usageCount2) {
      fcrData.fcrOffset = 0;

      fseek_s(f, 0, SEEK_SET);
      fread_s(wholePage, pageLength, f);
    } else {
      fcrData.fcrOffset = pageLength;
    }
  } else {  // for v5 databases
    uint16_t versionCode = fcr[6] << 16 | fcr[7];
    switch (versionCode) {
      case 3:
      case 4:
      case 5:
        break;
      default:
        throw BtrieveException(BtrieveError::NotBtrieveFile,
                               "Invalid version code, expected 3/4/5, got %d",
                               versionCode);
    }

    bool needsRecovery = (fcr[0x22] == 0xFF && fcr[0x23] == 0xFF);
    if (needsRecovery) {
      if (needsRecovery) {
        throw BtrieveException(
            BtrieveError::IOError,
            "Cannot import Btrieve database since it's marked inconsistent and "
            "needs recovery.");
      }
    }
  }

  auto accelFlags = toUint16(fcr + 0xA);
  if (accelFlags != 0) {
    throw BtrieveException(BtrieveError::NotBtrieveFile,
                           "Invalid accel flags, got %d, expected 0",
                           accelFlags);
  }

  auto usrflgs = toUint16(fcr + 0x106);
  if ((usrflgs & 0x8) != 0) {
    // TODO HANDLE THIS CASE
    throw BtrieveException(BtrieveError::NotBtrieveFile,
                           "firstPage is compressed, cannot handle");
  }

  auto variableRecordFlags = fcr[0x38];

  if ((usrflgs & 0x8) && (variableRecordFlags || usrflgs & 0x1)) {
    recordType = CompressedVariable;
  } else if (v6 && usrflgs & 0x0800) {
    recordType = UsesVAT;
  } else if (usrflgs & 0x8) {
    recordType = Compressed;
  } else if (variableRecordFlags || usrflgs & 0x1) {
    if (variableRecordFlags == 0x00FD || usrflgs & 0x2) {
      recordType = VariableTruncated;
    } else {
      recordType = Variable;
    }
  } else {
    recordType = Fixed;
  }

  if (v6 && (fcr[0x76] != fcr[0x14])) {
    throw BtrieveException(BtrieveError::NotBtrieveFile,
                           "Key count and KAT key count differ!");
  }

  fseek_s(f, 0, SEEK_END);

  fileLength = ftell(f);

  pageCount = toUint16(fcr + 0x26) << 16 | toUint16(fcr + 0x28);

  recordCount = toUint16(fcr + 0x1A) << 16 | toUint16(fcr + 0x1C);

  recordLength = toUint16(fcr + 0x16);

  physicalRecordLength = toUint16(fcr + 0x18);

  keys.resize(toUint16(fcr + 0x14));

  fcrData.keyAttributeTableOffset =
      fcrData.fcrOffset + (fcr[0x78] | fcr[0x79] << 8);

  fcrData.deletedRecordPointer = getRecordPointer(
      std::basic_string_view<uint8_t>(fcr + 0x10, sizeof(uint32_t)));

  return fcrData;
}

bool BtrieveDatabase::isUnusedRecord(std::basic_string_view<uint8_t> data) {
  if (v6) {
    if (data.size() < 2) {  // will probably never happen, but yolo
      return true;
    }
    // first two bytes are usage count, which will be non-zero if used
    uint16_t usageCount = data[0] << 8 | data[1];
    return usageCount == 0;
  } else if (data.size() >= 4 &&
             data.substr(4).find_first_not_of(static_cast<uint8_t>(0)) ==
                 std::string_view::npos) {
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
    std::function<LoadRecordResult(const std::basic_string_view<uint8_t>)>
        onRecordLoaded) {
  unsigned int recordsLoaded = 0;
  uint8_t* const data = reinterpret_cast<uint8_t*>(alloca(pageLength));
  const unsigned int recordsInPage = ((pageLength - 6) / physicalRecordLength);
  unsigned int pageOffset = v6 ? pageLength : 0;

  fseek_s(f, pageLength, SEEK_SET);
  // Starting at 1, since the first page is the header
  for (unsigned int i = v6 ? 1 : 0; i < pageCount;
       i++, pageOffset += pageLength) {
    int32_t physicalOffset = logicalPageToPhysicalOffset(f, i);
    if (physicalOffset < 0) {
      continue;
    }

    // read in the entire page
    fseek_s(f, physicalOffset, SEEK_SET);
    fread_s(data, pageLength, f);
    // Verify Data Page, high bit set on byte 5 (usage count)
    if ((data[0x5] & 0x80) == 0) {
      continue;
    }

    // page data starts 6 bytes in
    unsigned int recordOffset = 6;
    for (unsigned int j = 0; j < recordsInPage;
         j++, recordOffset += physicalRecordLength) {
      // Marked for deletion? Skip
      if (deletedRecordOffsets.count(pageOffset + recordOffset) > 0) {
        continue;
      }

      std::basic_string_view<uint8_t> record =
          std::basic_string_view<uint8_t>(data + recordOffset, recordLength);
      if (isUnusedRecord(record)) {
        break;
      }

      if (v6) {
        record = std::basic_string_view<uint8_t>(data + recordOffset + 2,
                                                 recordLength);
      }

      LoadRecordResult loadRecordResult;

      if (isVariableLengthRecords()) {
        std::basic_string_view<uint8_t> physicalRecord =
            std::basic_string_view<uint8_t>(
                data + recordOffset + (v6 ? 2 : 0),
                physicalRecordLength - (v6 ? 2 : 0));

        std::vector<uint8_t> stream(record.size());
        memcpy(stream.data(), record.data(), record.size());

        getVariableLengthData(f, physicalRecord, stream);

        loadRecordResult = onRecordLoaded(
            std::basic_string_view<uint8_t>(stream.data(), stream.size()));
      } else {
        loadRecordResult = onRecordLoaded(record);
      }

      switch (loadRecordResult) {
        case LoadRecordResult::CANCEL_ENUMERATION:
          return;
        case LoadRecordResult::COUNT:
          ++recordsLoaded;
          break;
        case LoadRecordResult::SKIP_COUNT:
        default:
          break;
      }
    }

    if (recordsLoaded == recordCount) {
      goto finished_loaded;
    }
  }

finished_loaded:
  if (recordsLoaded != recordCount) {
    fprintf(stderr, "Database contains %d records but read %d!\n", recordCount,
            recordsLoaded);
  }
}

BtrieveError BtrieveDatabase::parseDatabase(
    const tchar* fileName, std::function<bool()> onMetadataLoaded,
    std::function<LoadRecordResult(const std::basic_string_view<uint8_t>)>
        onRecordLoaded,
    std::function<void()> onRecordsComplete) {
#ifdef WIN32
  FILE* f = _wfopen(fileName, _TEXT("rb"));
#else
  FILE* f = fopen(fileName, _TEXT("rb"));
#endif
  if (f == nullptr) {
#ifdef WIN32
    fwprintf(stderr, _TEXT("Couldn't open %s: %d\n"), fileName, errno);
#else
    fprintf(stderr, "Couldn't open %s: %d\n", fileName, errno);
#endif
    return BtrieveError::FileNotFound;
  }

  from(f);

  if (onMetadataLoaded() && getRecordCount() > 0) {
    loadRecords(f, onRecordLoaded);
  }

  onRecordsComplete();

  fclose(f);
  return BtrieveError::Success;
}

bool BtrieveDatabase::loadPAT(FILE* f, std::string& acsName,
                              std::vector<char>& acs) {
  uint8_t* pat1 = reinterpret_cast<uint8_t*>(alloca(pageLength * 2));
  uint8_t* pat2 = pat1 + pageLength;  // pat2 is sequentially after pat1

  fseek_s(f, pageLength * 2, SEEK_SET);  // starts on third page
  fread_s(pat1, pageLength * 2, f);

  if (pat1[0] != 'P' || pat1[1] != 'P') {
    throw BtrieveException(BtrieveError::NotBtrieveFile,
                           "PAT1 table is invalid");
  }
  if (pat2[0] != 'P' || pat2[1] != 'P') {
    throw BtrieveException(BtrieveError::NotBtrieveFile,
                           "PAT2 table is invalid");
  }

  // check out the usage count to find active pat1/2
  uint16_t usageCount1 = toUint16(pat1 + 4);
  uint16_t usageCount2 = toUint16(pat2 + 4);
  // scan page type code to find ACS/Index/etc pages
  uint8_t* activePat = (usageCount1 > usageCount2) ? pat1 : pat2;

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
      loadACSAtPhysicalOffset(f, acsName, acs, pageNumber * pageLength);
    }

    if (type != 0 && type != 'A' && type != 'D' && type != 'E' && type != 'V') {
      throw BtrieveException(BtrieveError::NotBtrieveFile, "Bad PAT entry");
    }
  }

  return true;
}

bool BtrieveDatabase::loadACS(FILE* f, std::string& acsName,
                              std::vector<char>& acs, uint32_t logicalPage) {
  int32_t physicalOffset = logicalPageToPhysicalOffset(f, logicalPage);
  if (physicalOffset < 0) {
    throw BtrieveException(BtrieveError::InvalidACS,
                           "Can't map logical page %d to physical page",
                           logicalPage);
  }

  return loadACSAtPhysicalOffset(f, acsName, acs, physicalOffset);
}

bool BtrieveDatabase::loadACSAtPhysicalOffset(FILE* f, std::string& acsName,
                                              std::vector<char>& acs,
                                              uint32_t physicalOffset) {
  static const uint8_t ACS_PAGE_HEADER[] = {0, 0, 1, 0, 0, 0, 0xAC};

  char* acsPage = reinterpret_cast<char*>(alloca(pageLength));

  fseek_s(f, physicalOffset, SEEK_SET);
  fread_s(acsPage, pageLength, f);

  if (v6) {
    if (acsPage[1] != 'A' && acsPage[6] != 0xAC) {
      throw BtrieveException(BtrieveError::NotBtrieveFile,
                             "Bad v6 ACS header!");
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
  acsNameBuf[sizeof(acsNameBuf) - 1] = 0;
  // remove trailing spaces
  for (int i = strlen(acsNameBuf) - 1; i >= 0; --i) {
    if (acsNameBuf[i] != ' ') {
      break;
    }

    acsNameBuf[i] = 0;
  }

  acsName = acsNameBuf;

  acs.resize(ACS_LENGTH);
  memcpy(acs.data(), acsPage + 0xF, ACS_LENGTH);
  return true;
}

void BtrieveDatabase::loadKeyDefinitions(FILE* f, const uint8_t* firstPage,
                                         const FCRDATA& fcrData,
                                         const std::string& acsName,
                                         const std::vector<char>& acs) {
  const auto keyDefinitionLength = 0x1E;

  uint8_t data[512];
  size_t totalKeys = keys.size();
  unsigned int currentKeyNumber = 0;
  std::vector<uint32_t> keyOffsets;
  std::string multiAcsName;
  std::vector<char> multiAcs;
  const std::string* keyAcsName = &acsName;
  const std::vector<char>* keyAcs = &acs;

  keyOffsets.resize(totalKeys + 1);

  if (v6) {
    uint8_t* ptr = data;
    fseek_s(f, fcrData.keyAttributeTableOffset, SEEK_SET);
    fread_s(data, sizeof(uint16_t) * totalKeys, f);

    for (size_t i = 0; i < totalKeys; ++i, ptr += 2) {
      keyOffsets[i] = ptr[0] | ptr[1] << 8;
    }
  } else {
    const uint32_t keyDefinitionBase = 0x110;
    for (size_t i = 0; i < totalKeys; ++i) {
      keyOffsets[i] =
          static_cast<uint32_t>(keyDefinitionBase + (i * keyDefinitionLength));
    }
  }

  uint32_t keyOffset = keyOffsets[currentKeyNumber];

  while (currentKeyNumber < totalKeys) {
    fseek_s(f, keyOffset + fcrData.fcrOffset, SEEK_SET);
    fread_s(data, keyDefinitionLength, f);

    KeyDataType dataType;

    uint16_t attributes = toUint16(data + 0x8);
    if (attributes & UseExtendedDataType) {
      dataType = (KeyDataType)data[0x1C];
    } else {
      dataType = (attributes & OldStyleBinary) ? KeyDataType::OldBinary
                                               : KeyDataType::OldAscii;
    }

    if (attributes & MultipleACS) {
      uint32_t acsLogicalPage = data[0x19] << 16 | toUint16(data + 0x1A);
      if (!loadACS(f, multiAcsName, multiAcs, acsLogicalPage)) {
        throw BtrieveException(BtrieveError::InvalidACS, "Can't load ACS");
      }

      keyAcsName = &multiAcsName;
      keyAcs = &multiAcs;
    }

    uint16_t offset = toUint16(data + 0x14);
    // v6 databases have that 2 byte usage count prefix, so account for that
    if (v6) {
      offset -= 2;
    }

    KeyDefinition keyDefinition(
        /* number= */ currentKeyNumber,
        /* length= */ toUint16(data + 0x16),
        /* offset= */ offset, dataType, attributes,
        /* segment= */ attributes & SegmentedKey,
        /* segmentOf= */
        (attributes & SegmentedKey) ? currentKeyNumber
                                    : static_cast<uint16_t>(0),
        /* segmentIndex= */ 0,
        /* nullValue= */ data[0x1D], *keyAcsName, *keyAcs);

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

  fread_s(firstPage, sizeof(firstPage), f);

  FCRDATA fcrData = validateDatabase(f, firstPage);

  if (v6) {
    loadPAT(f, acsName, acs);
  } else {
    getRecordPointerList(f, fcrData.deletedRecordPointer, deletedRecordOffsets);

    loadACS(f, acsName, acs, 1);  // acs always on first page
  }

  loadKeyDefinitions(f, firstPage, fcrData, acsName, acs);
}

#pragma pack(push, 1)
typedef struct {  // (hi << 8 ) + lo = page
  uint8_t high;
  uint8_t low;
  uint8_t mid;
  uint8_t fragment;
} VRECPTR;
#pragma pack(pop)

static inline uint8_t getVRecordFragment(VRECPTR* x) { return x->fragment; }
static inline int32_t getVRecordPage(VRECPTR* x) {
  return static_cast<int32_t>((static_cast<int32_t>(x->high) << 16) |
                              (x->mid << 8) | x->low);
}

void BtrieveDatabase::getVariableLengthData(
    FILE* f, std::basic_string_view<uint8_t> recordData,
    std::vector<uint8_t>& stream) {
  uint8_t* const data = reinterpret_cast<uint8_t*>(alloca(pageLength));
  const unsigned int filePosition = ftell(f);
  VRECPTR Vrec =
      *reinterpret_cast<const VRECPTR*>(recordData.data() + recordLength);
  const uint16_t truncatedBytes =
      toUint16(recordData.data() + recordLength + 2);
  const uint16_t* fragpp = reinterpret_cast<uint16_t*>(data);
  uint8_t fragmentNumber;
  int32_t fragmentPhysicalOffset;
  int16_t fragmentIndex;
  int16_t fragmentOffset;
  int16_t fragmentLength;
  int16_t lofs;
  while (true) {
    fragmentNumber = getVRecordFragment(&Vrec);  // for multiple frags
    if (fragmentNumber > 254) {
      break;
    }

    fragmentPhysicalOffset =
        logicalPageToPhysicalOffset(f, getVRecordPage(&Vrec));
    if (fragmentPhysicalOffset < 0) {
      break;
    }

    fseek_s(f, fragmentPhysicalOffset, SEEK_SET);  // read page into buffer
    fread_s(data, pageLength, f);

    fragmentIndex = ((pageLength - 1) >> 1) - fragmentNumber;
    fragmentOffset = fragpp[fragmentIndex] & 0x7FFF;
    for (lofs = 1; fragpp[fragmentIndex - lofs] == -1; lofs++) {
      /* all done in test! */
    }
    fragmentLength = (fragpp[fragmentIndex - lofs] & 0x7FFF) - fragmentOffset;

    if (v6 || fragpp[fragmentIndex] & 0x8000) {
      Vrec = *reinterpret_cast<VRECPTR*>(data + fragmentOffset);
      fragmentOffset += sizeof(VRECPTR);
      fragmentLength -= sizeof(VRECPTR);
    } else {
      Vrec.low = Vrec.mid = Vrec.high = Vrec.fragment = 0xFF;
    }

    append(stream, std::basic_string_view<uint8_t>(data + fragmentOffset,
                                                   fragmentLength));
  }

  if (recordType == RecordType::VariableTruncated) {
    for (uint16_t i = 0; i < truncatedBytes; ++i) {
      stream.push_back(' ');
    }
  }

  fseek_s(f, filePosition, SEEK_SET);
}

int32_t BtrieveDatabase::logicalPageToPhysicalOffset(FILE* f,
                                                     int32_t logicalPage) {
  if (!v6) {
    return logicalPage * pageLength;
  }

  // go through the PAT
  uint32_t ret = 2;
  const int32_t pagesPerPAT = (pageLength / 4u) - 2u;

  // logical page can never be higher than max physical pages
  if (static_cast<uint32_t>(logicalPage) >= pageCount) {
    return -1;
  }

  // not on the current page? if so page up
  while (logicalPage > pagesPerPAT) {
    logicalPage -= pagesPerPAT;
    ret += (pageLength / 4u);
  }

  uint8_t* pat1 = reinterpret_cast<uint8_t*>(alloca(pageLength * 2));
  uint8_t* pat2 = pat1 + pageLength;

  const uint32_t physicalOffset = ret * pageLength;
  if (physicalOffset >= (fileLength - pageLength * 2)) {
    // we overflowed, this is junk
    return -1;
  }

  // read two pages worth, for pat1 and pat2 sequentially stored
  fseek_s(f, physicalOffset, SEEK_SET);
  fread_s(pat1, pageLength * 2, f);

  // pick the one with best usage count
  if (pat1[0] != 'P' && pat1[1] != 'P' && pat2[0] != 'P' && pat2[1] != 'P') {
    throw BtrieveException(BtrieveError::NotBtrieveFile, "Not a valid PAT");
  }

  uint32_t usageCount1 = toUint32(pat1 + 4);
  uint32_t usageCount2 = toUint32(pat2 + 4);
  uint8_t* activePat = (usageCount1 > usageCount2) ? pat1 : pat2;

  uint8_t* positionInPat = activePat + (logicalPage * 4) + 4;
  int64_t physicalPage =
      (positionInPat[0] << 16) | (positionInPat[3] << 8) | positionInPat[2];

  if (physicalPage == 0xFFFFFFL || physicalPage < 0) {
    return -1;
  }

  return static_cast<int32_t>(physicalPage * pageLength);
}
}  // namespace btrieve
