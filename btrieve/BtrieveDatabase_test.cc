#include "BtrieveDatabase.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <fstream>
#include <iterator>

#include "Text.h"
#include "gtest/gtest.h"

using namespace btrieve;

TEST(BtrieveDatabase, LoadsMBBSEmuDat) {
  unsigned int recordCount = 0;
  BtrieveDatabase database;
  std::string acsName;
  std::vector<char> blankACS;

  database.parseDatabase(
      _TEXT("assets/MBBSEMU.DAT"), []() { return true; },
      [&database, &recordCount](std::basic_string_view<uint8_t> record) {
        EXPECT_EQ(record.size(), database.getRecordLength());
        ++recordCount;
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(database.getKeys().size(), 4u);
  EXPECT_EQ(database.getRecordLength(), 74u);
  EXPECT_EQ(database.getRecordCount(), 4u);
  EXPECT_EQ(database.getPhysicalRecordLength(), 90u);
  EXPECT_EQ(database.getPageLength(), 512u);
  EXPECT_EQ(database.getPageCount(), 6u);
  EXPECT_FALSE(database.isVariableLengthRecords());

  EXPECT_FALSE(database.getKeys()[0].isComposite());
  EXPECT_FALSE(database.getKeys()[1].isComposite());
  EXPECT_FALSE(database.getKeys()[2].isComposite());
  EXPECT_FALSE(database.getKeys()[3].isComposite());

  EXPECT_EQ(database.getKeys()[0].getPrimarySegment(),
            KeyDefinition(0, 32, 2, KeyDataType::Zstring,
                          Duplicates | UseExtendedDataType, false, 0, 0, 0,
                          acsName, blankACS));

  EXPECT_EQ(database.getKeys()[1].getPrimarySegment(),
            KeyDefinition(1, 4, 34, KeyDataType::Integer,
                          Modifiable | UseExtendedDataType, false, 0, 0, 0,
                          acsName, blankACS));

  EXPECT_EQ(database.getKeys()[2].getPrimarySegment(),
            KeyDefinition(2, 32, 38, KeyDataType::Zstring,
                          Duplicates | Modifiable | UseExtendedDataType, false,
                          0, 0, 0, acsName, blankACS));

  EXPECT_EQ(database.getKeys()[3].getPrimarySegment(),
            KeyDefinition(3, 4, 70, KeyDataType::AutoInc, UseExtendedDataType,
                          false, 0, 0, 0, acsName, blankACS));

  EXPECT_EQ(recordCount, database.getRecordCount());
}

typedef struct _tagPOPULATE {
  uint32_t magic;
  uint16_t key1;
  uint16_t key2;
} POPULATE;

TEST(BtrieveDatabase, LoadsVariableDat) {
  unsigned int recordCount = 0;
  BtrieveDatabase database;
  std::string acsName;
  std::vector<char> blankACS;

  database.parseDatabase(
      _TEXT("assets/VARIABLE.DAT"), []() { return true; },
      [&database, &recordCount](std::basic_string_view<uint8_t> record) {
        EXPECT_GT(record.size(), database.getRecordLength() - 1);

        const POPULATE *data =
            reinterpret_cast<const POPULATE *>(record.data());
        EXPECT_EQ(data->magic, 0xDEADBEEF);
        EXPECT_EQ(data->key1, recordCount % 64);
        EXPECT_EQ(data->key2, recordCount);
        EXPECT_EQ(record.size(), sizeof(POPULATE) + recordCount);
        for (unsigned int i = 0; i < record.size() - sizeof(POPULATE); ++i) {
          EXPECT_EQ(record.data()[sizeof(POPULATE) + i],
                    static_cast<uint8_t>(i));
        }

        ++recordCount;
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(database.getKeys().size(), 2u);
  EXPECT_EQ(database.getRecordLength(), 8u);
  EXPECT_EQ(database.getRecordCount(), 1024u);
  EXPECT_EQ(database.getPhysicalRecordLength(), 20u);
  EXPECT_EQ(database.getPageLength(), 512);
  EXPECT_EQ(database.getPageCount(), 1156u);
  EXPECT_TRUE(database.isVariableLengthRecords());

  EXPECT_FALSE(database.getKeys()[0].isComposite());
  EXPECT_FALSE(database.getKeys()[1].isComposite());

  EXPECT_EQ(database.getKeys()[0].getPrimarySegment(),
            KeyDefinition(0, 2, 4, KeyDataType::Integer,
                          Duplicates | UseExtendedDataType, false, 0, 0, 0,
                          acsName, blankACS));

  EXPECT_EQ(database.getKeys()[1].getPrimarySegment(),
            KeyDefinition(1, 2, 6, KeyDataType::Integer, UseExtendedDataType,
                          false, 0, 0, 0, acsName, blankACS));

  EXPECT_EQ(recordCount, database.getRecordCount());
}

typedef struct _tagV6VARIABLEDATA {
  const char *key;
  uint32_t recordLength;
} VARIABLEDATA;

static const VARIABLEDATA variableData[] = {
    {"AACCOUNT", 448},   {"ADLTAGGED", 450},  {"ADOORS", 448},
    {"AEMAIL", 448},     {"AEXIT", 448},      {"AEXIT2", 450},
    {"AFINGER", 450},    {"AFORUMS", 448},    {"AFTP", 450},
    {"AFTPMENU", 828},   {"AGLOBALS", 448},   {"AHELP", 448},
    {"AHOST", 450},      {"AINFO", 828},      {"ALIBRARY", 448},
    {"ANETWORK", 1080},  {"APOLLS", 448},     {"AQWK", 448},
    {"AREGISTRY", 448},  {"AREMOTE", 448},    {"ARLOGIN", 450},
    {"ATELE", 450},      {"ATELNET", 450},    {"ATOP", 2214},
    {"AWORLNK", 450},    {"AYOURSYS", 448},   {"CACCOUNT", 450},
    {"CBROADCAST", 450}, {"CCSFTP", 450},     {"CDOORS", 450},
    {"CFINGER", 450},    {"CHOST", 450},      {"CLIBRARY", 450},
    {"CMESSAGE", 450},   {"CNETWORK", 1080},  {"CPOLLS", 450},
    {"CREGISTRY", 450},  {"CREMOTE", 450},    {"CRLOGIN", 450},
    {"CTELE", 448},      {"CTELNET", 450},    {"CTOP", 1962},
    {"CVIDSYSOP", 450},  {"CWORLDLINK", 702}, {"CWORLNK", 450},
    {"CWORMSG", 450},
};

TEST(BtrieveDatabase, LoadsVariableDatV6) {
  BtrieveDatabase database;

  std::unordered_map<std::string, uint32_t> expectedData;
  for (size_t i = 0; i < sizeof(variableData) / sizeof(variableData[0]); ++i) {
    expectedData[variableData[i].key] = variableData[i].recordLength;
  }

  database.parseDatabase(
      _TEXT("assets/WGSMENU2.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 448u);

        return database.getRecordLength() == 448;
      },
      [&database, &expectedData](std::basic_string_view<uint8_t> record) {
        auto iter =
            expectedData.find(reinterpret_cast<const char *>(record.data()));

        EXPECT_TRUE(iter != expectedData.end());
        EXPECT_EQ(iter->second, record.size());

        expectedData.erase(iter);

        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(expectedData.size(), 0u);

  ASSERT_EQ(database.getRecordCount(),
            sizeof(variableData) / sizeof(variableData[0]));

  ASSERT_EQ(database.getKeys().size(), 1u);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getOffset(), 0);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getPosition(), 1);
  ASSERT_EQ(database.getKeys()[0].getPrimarySegment().getLength(), 17u);
}

// A v6 file can carry stray pages where a PAT pair is expected (seen in a
// Major BBS add-on's shipped .VIR: four junk pages ahead of PAT 31). Btrieve
// finds the PAT by its sequence number and every physical page in that PAT's
// region moves along with it, so loading must give the same records.
TEST(BtrieveDatabase, LoadsVariableDatV6WithPagesBeforePAT) {
  std::ifstream in("assets/WGSMENU2.DAT", std::ios::binary);
  std::vector<char> original((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
  ASSERT_GT(original.size(), 2u * 4096u);

  const size_t pageLength = 4096;
  std::vector<char> shifted(original.begin(),
                            original.begin() + 2 * pageLength);
  shifted.resize(shifted.size() + 4 * pageLength, static_cast<char>(0xCC));
  shifted.insert(shifted.end(), original.begin() + 2 * pageLength,
                 original.end());

  const std::filesystem::path path =
      std::filesystem::temp_directory_path() / "WGSMENU2-PATGAP.DAT";
  {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(shifted.data(), shifted.size());
  }

  std::unordered_map<std::string, uint32_t> expectedData;
  for (size_t i = 0; i < sizeof(variableData) / sizeof(variableData[0]); ++i) {
    expectedData[variableData[i].key] = variableData[i].recordLength;
  }

  BtrieveDatabase database;
  database.parseDatabase(
      path.wstring().c_str(), []() { return true; },
      [&expectedData](std::basic_string_view<uint8_t> record) {
        auto iter =
            expectedData.find(reinterpret_cast<const char *>(record.data()));
        EXPECT_TRUE(iter != expectedData.end());
        if (iter != expectedData.end()) {
          EXPECT_EQ(iter->second, record.size());
          expectedData.erase(iter);
        }
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });
  std::filesystem::remove(path);

  EXPECT_EQ(expectedData.size(), 0u);
  ASSERT_EQ(database.getKeys().size(), 1u);
}

TEST(BtrieveDatabase, LoadsFixedDatV6) {
  unsigned int recordCount = 0;
  BtrieveDatabase database;

  database.parseDatabase(
      _TEXT("assets/GALTELA.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 950u);

        return database.getRecordLength() == 950;
      },
      [&database, &recordCount](std::basic_string_view<uint8_t> record) {
        ++recordCount;

        EXPECT_EQ(record.size(), 950u);

        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(recordCount, 73u);
  ASSERT_EQ(database.getRecordCount(), 73u);

  ASSERT_EQ(database.getKeys().size(), 3u);

  ASSERT_EQ(database.getKeys()[0].getSegments().size(), 2u);
  ASSERT_EQ(database.getKeys()[0].getLength(), 32u);

  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getOffset(), 0);
  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getPosition(), 1);
  ASSERT_EQ(database.getKeys()[1].getPrimarySegment().getLength(), 16u);

  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getOffset(), 16);
  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getPosition(), 17);
  ASSERT_EQ(database.getKeys()[2].getPrimarySegment().getLength(), 16u);
}

// Every record of a Btrieve file, in the order they're loaded.
static std::vector<std::string> loadAllRecords(const std::filesystem::path &dat) {
  std::vector<std::string> records;
  BtrieveDatabase database;
  database.parseDatabase(
      toWideString(dat).c_str(), []() { return true; },
      [&records](std::basic_string_view<uint8_t> record) {
        records.emplace_back(reinterpret_cast<const char *>(record.data()),
                             record.size());
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });
  return records;
}

// Copies GALTELA.DAT (v6, 4096 byte pages) to the temp directory with a copy
// of one of its physical pages appended, after edit has changed it.
static std::filesystem::path galtelaWithExtraPage(
    const char *name, uint32_t physicalPage,
    std::function<void(std::vector<char> &)> edit) {
  const size_t pageLength = 4096;
  std::ifstream in("assets/GALTELA.DAT", std::ios::binary);
  std::vector<char> file((std::istreambuf_iterator<char>(in)),
                         std::istreambuf_iterator<char>());
  std::vector<char> page(file.begin() + physicalPage * pageLength,
                         file.begin() + (physicalPage + 1) * pageLength);
  edit(page);
  file.insert(file.end(), page.begin(), page.end());

  std::filesystem::path dat(testing::TempDir());
  dat /= name;
  std::ofstream out(dat, std::ios::binary | std::ios::trunc);
  out.write(file.data(), file.size());
  return dat;
}

// Btrieve 6 writes each new version of a page to a free physical page, so old
// copies stay behind. A copy older than the live one must not be read.
TEST(BtrieveDatabase, LoadsOnlyTheNewestCopyOfAV6DataPage) {
  const auto original = loadAllRecords("assets/GALTELA.DAT");
  ASSERT_EQ(original.size(), 73u);

  // physical page 9 holds logical page 12 at usage count 4; add a copy at
  // usage count 3 with different records
  const auto dat =
      galtelaWithExtraPage("OLDCOPY.DAT", 9, [](std::vector<char> &page) {
        ASSERT_EQ(page[2], 0x0c);
        ASSERT_EQ(page[4], 0x04);
        page[4] = 0x03;
        std::fill(page.begin() + 8, page.end(), 'X');
      });

  EXPECT_EQ(loadAllRecords(dat), original);
}

// A data page left behind for a logical page the PAT has since freed holds no
// live records.
TEST(BtrieveDatabase, IgnoresV6DataPagesThePATHasFreed) {
  const auto original = loadAllRecords("assets/GALTELA.DAT");

  // logical page 6 is unallocated in GALTELA.DAT's PAT
  const auto dat =
      galtelaWithExtraPage("FREED.DAT", 9, [](std::vector<char> &page) {
        page[2] = 0x06;
      });

  EXPECT_EQ(loadAllRecords(dat), original);
}

// Builds a v6 file laid out the way Btrieve 6.15 lays one out: the FCR pair at
// physical pages 0 and 1, PAT pair n (from 1) at physical page
// 2 + (n - 1) * pageLength / 4 mapping the pageLength / 4 - 2 pages after it,
// and the records packed into data pages, the first and last data pages of
// every PAT's region taken first. Has no keys.
static std::vector<char> buildV6File(uint16_t pageLength, uint32_t patPairs,
                                     const std::vector<std::string> &records) {
  const uint16_t recordLength = static_cast<uint16_t>(records.at(0).size());
  const uint16_t physicalRecordLength = recordLength + 2;  // + usage count
  const uint32_t pagesPerRegion = pageLength / 4u;
  const uint32_t pagesPerPAT = pagesPerRegion - 2u;
  const size_t recordsPerPage = (pageLength - 6u) / physicalRecordLength;
  const size_t dataPages =
      (records.size() + recordsPerPage - 1) / recordsPerPage;
  EXPECT_LE(dataPages, static_cast<size_t>(patPairs) * pagesPerPAT);

  std::vector<char> file(
      static_cast<size_t>(2 + patPairs * pagesPerRegion) * pageLength, 0);
  auto put16 = [&file](size_t at, uint16_t value) {
    file[at] = static_cast<char>(value & 0xFF);
    file[at + 1] = static_cast<char>(value >> 8);
  };
  auto putHighLow = [&put16](size_t at, uint32_t value) {
    put16(at, static_cast<uint16_t>(value >> 16));
    put16(at + 2, static_cast<uint16_t>(value & 0xFFFF));
  };
  // a page header or PAT entry: [high][type][low][mid] page number
  auto putPageRef = [&file](size_t at, uint32_t page, char type) {
    file[at] = static_cast<char>(page >> 16);
    file[at + 1] = type;
    file[at + 2] = static_cast<char>(page & 0xFF);
    file[at + 3] = static_cast<char>((page >> 8) & 0xFF);
  };

  // the FCR, the same in both copies
  const uint32_t pageCount = patPairs * pagesPerPAT + 1;
  for (size_t fcr = 0; fcr < 2u * pageLength; fcr += pageLength) {
    file[fcr] = 'F';
    file[fcr + 1] = 'C';
    file[fcr + 4] = 1;  // usage count
    put16(fcr + 0x8, pageLength);
    putHighLow(fcr + 0x10, 0xFFFFFFFF);  // no deleted records
    put16(fcr + 0x16, recordLength);
    put16(fcr + 0x18, physicalRecordLength);
    putHighLow(fcr + 0x1A, static_cast<uint32_t>(records.size()));
    putHighLow(fcr + 0x26, pageCount);
  }

  // both copies of each PAT; the first, with the higher usage count, is live
  for (uint32_t n = 1; n <= patPairs; ++n) {
    const size_t pat = static_cast<size_t>(2 + (n - 1) * pagesPerRegion) *
                       pageLength;
    for (size_t copy = 0; copy < 2; ++copy) {
      const size_t page = pat + copy * pageLength;
      file[page] = 'P';
      file[page + 1] = 'P';
      put16(page + 2, static_cast<uint16_t>(n));
      file[page + 4] = copy == 0 ? 1 : 0;
    }
  }

  for (size_t d = 0; d < dataPages; ++d) {
    // region d % patPairs; its first data slot, its last, its second, ...
    const uint32_t region = static_cast<uint32_t>(d % patPairs);
    const uint32_t nth = static_cast<uint32_t>(d / patPairs);
    const uint32_t slot = nth % 2 == 0 ? nth / 2 : pagesPerPAT - 1 - nth / 2;
    const uint32_t logicalPage = region * pagesPerPAT + slot + 1;
    const uint32_t physicalPage = 2 + region * pagesPerRegion + 2 + slot;

    const size_t pat = static_cast<size_t>(2 + region * pagesPerRegion) *
                       pageLength;
    putPageRef(pat + 8 + slot * 4, physicalPage, 'D');
    putPageRef(pat + pageLength + 8 + slot * 4, physicalPage, 'D');

    const size_t page = static_cast<size_t>(physicalPage) * pageLength;
    putPageRef(page, logicalPage, 'D');
    file[page + 4] = 1;  // usage count, with the data page bit
    file[page + 5] = static_cast<char>(0x80);
    for (size_t r = 0; r < recordsPerPage; ++r) {
      const size_t record = d * recordsPerPage + r;
      if (record >= records.size()) {
        break;
      }
      const size_t at = page + 6 + r * physicalRecordLength;
      file[at + 1] = 1;  // record usage count
      std::copy(records[record].begin(), records[record].end(),
                file.begin() + at + 2);
    }
  }

  return file;
}

// Btrieve 6.15, growing a v6 file past 30 PAT pairs, leaves 4096 bytes (not a
// page) ahead of PAT pair 31, so every page from there on sits 4096 bytes
// further into the file.
static void insertGapBeforePAT31(std::vector<char> &file, uint16_t pageLength) {
  const size_t gapAt = static_cast<size_t>(2 + 30 * (pageLength / 4u)) *
                       pageLength;
  ASSERT_LT(gapAt, file.size());
  file.insert(file.begin() + gapAt, 4096, static_cast<char>(0xCC));
}

static std::filesystem::path writeTempFile(const char *name,
                                           const std::vector<char> &bytes) {
  std::filesystem::path dat(testing::TempDir());
  dat /= name;
  std::ofstream out(dat, std::ios::binary | std::ios::trunc);
  out.write(bytes.data(), bytes.size());
  return dat;
}

// Expects the same records in any order, without printing thousands of them
// when they differ.
static void expectSameRecords(std::vector<std::string> actual,
                              std::vector<std::string> expected) {
  ASSERT_EQ(actual.size(), expected.size());
  std::sort(actual.begin(), actual.end());
  std::sort(expected.begin(), expected.end());
  EXPECT_TRUE(actual == expected) << "same number of records, not the same";
}

// Loads a v6 file with 32 PAT pairs and pageLength byte pages, first as built
// and then with Btrieve 6.15's gap ahead of PAT pair 31, and expects every
// record back from both.
static void expectAllRecordsAcrossGapBeforePAT31(uint16_t pageLength) {
  const uint32_t patPairs = 32;
  const uint16_t recordLength = 100;
  const size_t recordsPerPage = (pageLength - 6u) / (recordLength + 2u);
  std::vector<std::string> records;
  // the first and last data page of each PAT's region
  for (size_t i = 0; i < 2 * patPairs * recordsPerPage; ++i) {
    std::string record = "page length " + std::to_string(pageLength) +
                         " record " + std::to_string(i);
    record.resize(recordLength, static_cast<char>('A' + i % 26));
    records.push_back(record);
  }
  std::vector<char> file = buildV6File(pageLength, patPairs, records);

  const std::string name = "PAT31-" + std::to_string(pageLength);
  const auto plain = writeTempFile((name + ".DAT").c_str(), file);
  expectSameRecords(loadAllRecords(plain), records);

  insertGapBeforePAT31(file, pageLength);
  const auto gapped = writeTempFile((name + "-GAP.DAT").c_str(), file);
  BtrieveDatabase database;
  std::vector<std::string> loaded;
  database.parseDatabase(
      toWideString(gapped).c_str(), []() { return true; },
      [&loaded](std::basic_string_view<uint8_t> record) {
        loaded.emplace_back(reinterpret_cast<const char *>(record.data()),
                            record.size());
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });
  expectSameRecords(loaded, records);
  EXPECT_EQ(database.getConversionWarning(), "");

  std::filesystem::remove(plain);
  std::filesystem::remove(gapped);
}

// 4096 bytes is not a whole number of 1536 byte pages: PAT pair 31 starts
// 1024 bytes past a page boundary.
TEST(BtrieveDatabase, LoadsV6PagesPastTheGapBeforePAT31With1536BytePages) {
  expectAllRecordsAcrossGapBeforePAT31(1536);
}

// With 512 byte pages the gap is 8 whole pages.
TEST(BtrieveDatabase, LoadsV6PagesPastTheGapBeforePAT31With512BytePages) {
  expectAllRecordsAcrossGapBeforePAT31(512);
}

// MajorMUD's rooms file, WCCMP001.VIR (1536 byte pages), is the case that
// matters: once Btrieve 6.15 has rewritten it as a v6 file it has about 70
// PAT pairs, and every room past the gap before PAT 31 was lost. It is too
// big to keep in assets, so this runs only when WBTRV32_WCCMP001 names a copy
// of the file shipped in MBBSEmu's WCCMMUD 1.11p module (a v5 file), e.g.
// bazel test //btrieve:tests --test_env=WBTRV32_WCCMP001=/path/WCCMP001.VIR
TEST(BtrieveDatabase, LoadsWCCMP001RebuiltAsV6WithTheGapBeforePAT31) {
  const char *source = getenv("WBTRV32_WCCMP001");
  if (source == nullptr || !std::filesystem::exists(source)) {
    GTEST_SKIP() << "set WBTRV32_WCCMP001 to the path of WCCMP001.VIR";
  }

  const auto rooms = loadAllRecords(source);
  ASSERT_EQ(rooms.size(), 26720u);

  const uint16_t pageLength = 1536;
  const uint32_t pagesPerPAT = pageLength / 4u - 2u;
  const uint32_t patPairs =
      static_cast<uint32_t>((rooms.size() + pagesPerPAT - 1) / pagesPerPAT);
  std::vector<char> file = buildV6File(pageLength, patPairs, rooms);
  insertGapBeforePAT31(file, pageLength);
  const auto dat = writeTempFile("WCCMP001-V6.DAT", file);

  expectSameRecords(loadAllRecords(dat), rooms);
  std::filesystem::remove(dat);

  // and Btrieve 6.15's own copy of the file holds the same rooms
  expectSameRecords(loadAllRecords("assets/WCCMP001.FULL.DAT"), rooms);
}

// Loads a file Btrieve 6.15 wrote itself and expects every record the file's
// header says it holds, each recordLength bytes and no two the same.
static void expectAllRecordsOfBtrieve615File(const char *path,
                                             uint16_t pageLength,
                                             unsigned int recordLength,
                                             unsigned int recordCount) {
  BtrieveDatabase database;
  std::vector<std::string> records;
  database.parseDatabase(
      toWideString(path).c_str(), []() { return true; },
      [&records](std::basic_string_view<uint8_t> record) {
        records.emplace_back(reinterpret_cast<const char *>(record.data()),
                             record.size());
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  EXPECT_EQ(database.getPageLength(), pageLength);
  EXPECT_EQ(database.getRecordCount(), recordCount);
  EXPECT_EQ(database.getConversionWarning(), "");
  ASSERT_EQ(records.size(), recordCount);
  for (const auto &record : records) {
    ASSERT_EQ(record.size(), recordLength);
  }
  std::sort(records.begin(), records.end());
  EXPECT_EQ(std::adjacent_find(records.begin(), records.end()), records.end())
      << "a record was loaded twice";
}

// WCCMP001.FULL.DAT and WCCMSG.FULL.DAT are Btrieve 6.15's own files (the
// W32MKDE.EXE workstation engine with WBTRV32.DLL, as Nightmare Redux ships
// them): a clone of WCCMMUD 1.11p's WCCMP001.VIR and WCCMSG.VIR, with every
// record of the original inserted. Both grew past 30 PAT pairs, so both have
// the 4096-byte gap ahead of PAT pair 31. They hold the same records as the
// shipped files, in a different order.

// MajorMUD's rooms: 1536 byte pages, so everything past the gap (about
// 17.7 MB in) is off the page grid. Without reading past it only 11,226 of
// the 26,720 rooms load.
TEST(BtrieveDatabase, LoadsBtrieve615GrownRoomsFile) {
  expectAllRecordsOfBtrieve615File("assets/WCCMP001.FULL.DAT", 1536, 1528,
                                   26720);
}

// MajorMUD's messages: 512 byte pages, so the gap is 8 whole pages.
TEST(BtrieveDatabase, LoadsBtrieve615GrownMessagesFile) {
  expectAllRecordsOfBtrieve615File("assets/WCCMSG.FULL.DAT", 512, 253, 3867);
}

TEST(BtrieveDatabase, LoadsMultiAcsDatV6) {
  BtrieveDatabase database;

  database.parseDatabase(
      _TEXT("assets/MULTIACS.DAT"),
      [&database]() {
        EXPECT_EQ(database.getRecordLength(), 128u);

        return database.getRecordLength() == 128;
      },
      [](std::basic_string_view<uint8_t> record) {
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(database.getRecordCount(), 0u);

  ASSERT_EQ(database.getKeys().size(), 3u);

  ASSERT_STREQ(database.getKeys().at(0).getACSName(), "ALLCAPS");
  // key 1 has no ACS of its own, so it doesn't inherit key 0's
  ASSERT_STRNE(database.getKeys().at(1).getACSName(), "ALLCAPS");
  ASSERT_STREQ(database.getKeys().at(2).getACSName(), "LOWER");
}

// Fewer records than the file's header claims is reported, so the conversion
// log can show it where the sysop will see it.
TEST(BtrieveDatabase, WarnsWhenRecordsAreMissing) {
  {
    BtrieveDatabase database;
    database.parseDatabase(
        _TEXT("assets/MBBSEMU.DAT"), []() { return true; },
        [](std::basic_string_view<uint8_t> record) {
          return BtrieveDatabase::LoadRecordResult::COUNT;
        });
    EXPECT_EQ(database.getConversionWarning(), "");
  }

  std::filesystem::path dat(testing::TempDir());
  dat /= "MISSING.DAT";
  std::filesystem::copy_file("assets/MBBSEMU.DAT", dat,
                             std::filesystem::copy_options::overwrite_existing);
  // the record count's low word is at 0x1C in this v5 file: claim 5, not 4
  {
    std::fstream file(dat, std::ios::in | std::ios::out | std::ios::binary);
    file.seekg(0x1C);
    ASSERT_EQ(file.get(), 4);
    file.seekp(0x1C);
    file.put(5);
  }

  BtrieveDatabase database;
  database.parseDatabase(
      toWideString(dat).c_str(), []() { return true; },
      [](std::basic_string_view<uint8_t> record) {
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });
  EXPECT_EQ(database.getConversionWarning(),
            "read 4 records, but the file's header says it holds 5");
}

// NOCASE_KEY (0x400) shares a bit with NUMBERED_ACS (0x420), so a case
// insensitive key without an ACS must not be treated as having one.
TEST(BtrieveDatabase, LoadsCaseInsensitiveKeyWithoutACS) {
  std::filesystem::path dat(testing::TempDir());
  dat /= "NOCASE.DAT";
  std::filesystem::copy_file("assets/MBBSEMU.DAT", dat,
                             std::filesystem::copy_options::overwrite_existing);

  // key 0's attributes are at 0x118 in this v5 file, 0x0101 (duplicates and
  // extended type), so set the high byte to add NOCASE_KEY
  {
    std::fstream file(dat, std::ios::in | std::ios::out | std::ios::binary);
    file.seekg(0x118);
    char attributes[2];
    file.read(attributes, sizeof(attributes));
    ASSERT_EQ(attributes[0], 0x01);
    ASSERT_EQ(attributes[1], 0x01);

    file.seekp(0x119);
    file.put(0x05);
  }

  BtrieveDatabase database;
  database.parseDatabase(
      toWideString(dat).c_str(), []() { return true; },
      [](std::basic_string_view<uint8_t> record) {
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  ASSERT_EQ(database.getKeys().size(), 4u);
  const KeyDefinition &key = database.getKeys()[0].getPrimarySegment();
  EXPECT_EQ(key.getAttributes(), 0x0501);
  EXPECT_TRUE(key.isCaseInsensitive());
  EXPECT_FALSE(key.requiresACS());
  EXPECT_EQ(key.getACS(), nullptr);
  EXPECT_EQ(database.getRecordCount(), 4u);
}

// A Btrieve v5 key table has one 30-byte entry per segment, not per key, so
// every key after a multi-segment key starts further along than its number
// suggests. Expected values are what Btrieve 6.15's Stat reports for this
// file (an empty data file from a Major BBS add-on).
TEST(BtrieveDatabase, LoadsV5KeysAfterSegmentedKeys) {
  BtrieveDatabase database;
  database.parseDatabase(
      _TEXT("assets/V5SEGKEY.DAT"), []() { return true; },
      [](std::basic_string_view<uint8_t> record) {
        return BtrieveDatabase::LoadRecordResult::COUNT;
      });

  struct Segment {
    uint16_t offset;
    uint16_t length;
    KeyDataType type;
  };
  const std::vector<std::vector<Segment>> expected = {
      {{0, 4, KeyDataType::UnsignedBinary}},
      {{4, 30, KeyDataType::Zstring}},
      {{34, 30, KeyDataType::Zstring}},
      {{4, 30, KeyDataType::Zstring}, {0, 4, KeyDataType::UnsignedBinary}},
      {{34, 30, KeyDataType::Zstring}, {0, 4, KeyDataType::UnsignedBinary}},
      {{34, 30, KeyDataType::Zstring},
       {64, 1, KeyDataType::String},
       {0, 4, KeyDataType::UnsignedBinary}},
  };

  ASSERT_EQ(database.getKeys().size(), expected.size());
  EXPECT_EQ(database.getRecordLength(), 216u);
  for (size_t k = 0; k < expected.size(); ++k) {
    const auto &segments = database.getKeys()[k].getSegments();
    ASSERT_EQ(segments.size(), expected[k].size()) << "key " << k;
    for (size_t s = 0; s < segments.size(); ++s) {
      EXPECT_EQ(segments[s].getOffset(), expected[k][s].offset)
          << "key " << k << " segment " << s;
      EXPECT_EQ(segments[s].getLength(), expected[k][s].length)
          << "key " << k << " segment " << s;
      EXPECT_EQ(segments[s].getDataType(), expected[k][s].type)
          << "key " << k << " segment " << s;
    }
  }
}
