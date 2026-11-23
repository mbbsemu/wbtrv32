#include "BtrieveDatabase.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
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

// A v6 file for buildV6File to write, laid out the way Btrieve 6.x lays one
// out: the FCR pair at physical pages 0 and 1, then PAT pair n (from 0) at
// physical page 2 + n * pageLength / 4, mapping pageLength / 4 - 2 logical
// pages, and the pages after it up to the next pair its region.
struct V6File {
  uint16_t pageLength;
  // the PAT pairs written to the file
  uint32_t patPairs;
  // fixed-length records, packed into the data pages in turn
  std::vector<std::string> records;
  struct Page {
    uint32_t logicalPage;
    uint32_t physicalPage;
  };
  std::vector<Page> dataPages;
  // An alternate collating sequence: a page of its own, used by a key of its
  // own (key i uses acs[i], whose table is acsTable(i)).
  struct ACS {
    std::string name;
    Page page;
  };
  std::vector<ACS> acs;
  // the FCR's page count; 0 for one past the last logical page the PAT pairs
  // map
  uint32_t pageCount = 0;

  uint32_t pagesPerRegion() const { return pageLength / 4u; }
  uint32_t pagesPerPAT() const { return pagesPerRegion() - 2u; }
  size_t recordsPerPage() const {
    return (pageLength - 6u) / (records.at(0).size() + 2u);
  }
  // the page slot (from 0) of PAT pair region (from 0) maps, and where that
  // slot's page is, after the PAT pair
  Page page(uint32_t region, uint32_t slot) const {
    return Page{region * pagesPerPAT() + slot + 1,
                2 + region * pagesPerRegion() + 2 + slot};
  }
};

// Key i's ACS: case-insensitive, with the letters rotated by i so each key's
// differs. Byte 0 maps to itself, as in real ACSs: it follows the 8-byte name.
static std::vector<uint8_t> acsTable(size_t key) {
  std::vector<uint8_t> table(256);
  for (size_t b = 0; b < table.size(); ++b) {
    table[b] = static_cast<uint8_t>(b);
    if (isalpha(static_cast<int>(b))) {
      table[b] = static_cast<uint8_t>('A' + (toupper(static_cast<int>(b)) -
                                             'A' + key) % 26);
    }
  }
  return table;
}

// Writes spec as Btrieve would: its records, and for each ACS a key over 10
// bytes of the record from offset 10 * i. No gap before PAT pair 31: see
// insertGapBeforePAT31.
static std::vector<char> buildV6File(const V6File &spec) {
  const uint16_t pageLength = spec.pageLength;
  const uint16_t recordLength =
      static_cast<uint16_t>(spec.records.at(0).size());
  const uint16_t physicalRecordLength = recordLength + 2;  // + usage count
  const size_t recordsPerPage = spec.recordsPerPage();
  EXPECT_GE(spec.dataPages.size() * recordsPerPage, spec.records.size());
  EXPECT_LE(spec.acs.size() * 10u, recordLength);

  std::vector<char> file(
      static_cast<size_t>(2 + spec.patPairs * spec.pagesPerRegion()) *
          pageLength,
      0);
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
  auto pageAt = [pageLength](uint32_t physicalPage) {
    return static_cast<size_t>(physicalPage) * pageLength;
  };
  // both copies of the PAT entry for page, the first copy live
  auto putPATEntry = [&](V6File::Page page, char type) {
    const uint32_t region = (page.logicalPage - 1) / spec.pagesPerPAT();
    const uint32_t slot = (page.logicalPage - 1) % spec.pagesPerPAT();
    const size_t pat = pageAt(2 + region * spec.pagesPerRegion());
    putPageRef(pat + 8 + slot * 4, page.physicalPage, type);
    putPageRef(pat + pageLength + 8 + slot * 4, page.physicalPage, type);
  };

  // the FCR, the same in both copies
  const uint32_t pageCount = spec.pageCount != 0
                                 ? spec.pageCount
                                 : spec.patPairs * spec.pagesPerPAT() + 1;
  const uint16_t keys = static_cast<uint16_t>(spec.acs.size());
  const size_t kat = 0x180;            // the key attribute table
  const size_t keyDefinitions = 0x1A0;  // 0x1E bytes each
  for (size_t fcr = 0; fcr < 2u * pageLength; fcr += pageLength) {
    file[fcr] = 'F';
    file[fcr + 1] = 'C';
    file[fcr + 4] = 1;  // usage count
    put16(fcr + 0x8, pageLength);
    putHighLow(fcr + 0x10, 0xFFFFFFFF);  // no deleted records
    put16(fcr + 0x14, keys);
    put16(fcr + 0x16, recordLength);
    put16(fcr + 0x18, physicalRecordLength);
    putHighLow(fcr + 0x1A, static_cast<uint32_t>(spec.records.size()));
    putHighLow(fcr + 0x26, pageCount);
    file[fcr + 0x76] = static_cast<char>(keys);
    put16(fcr + 0x78, static_cast<uint16_t>(kat));
    for (uint16_t k = 0; k < keys; ++k) {
      const size_t definition = keyDefinitions + k * 0x1E;
      put16(fcr + kat + k * 2, static_cast<uint16_t>(definition));
      put16(fcr + definition + 0x8,
            Duplicates | MultipleACS | UseExtendedDataType);
      put16(fcr + definition + 0x14, 10 * k + 2);  // + the usage count
      put16(fcr + definition + 0x16, 10);
      const uint32_t acsPage = spec.acs[k].page.logicalPage;
      file[fcr + definition + 0x19] = static_cast<char>(acsPage >> 16);
      put16(fcr + definition + 0x1A, static_cast<uint16_t>(acsPage & 0xFFFF));
      file[fcr + definition + 0x1C] = static_cast<char>(KeyDataType::String);
    }
  }

  for (uint32_t n = 0; n < spec.patPairs; ++n) {
    const size_t pat = pageAt(2 + n * spec.pagesPerRegion());
    for (size_t copy = 0; copy < 2; ++copy) {
      const size_t page = pat + copy * pageLength;
      file[page] = 'P';
      file[page + 1] = 'P';
      put16(page + 2, static_cast<uint16_t>(n + 1));
      file[page + 4] = copy == 0 ? 1 : 0;  // usage count
    }
  }

  for (size_t k = 0; k < spec.acs.size(); ++k) {
    const auto &acs = spec.acs[k];
    putPATEntry(acs.page, 'A');
    const size_t page = pageAt(acs.page.physicalPage);
    putPageRef(page, acs.page.logicalPage, 'A');
    file[page + 6] = static_cast<char>(0xAC);
    std::string name = acs.name;
    name.resize(8, ' ');
    std::copy(name.begin(), name.end(), file.begin() + page + 7);
    const auto table = acsTable(k);
    std::copy(table.begin(), table.end(), file.begin() + page + 0xF);
  }

  for (size_t d = 0; d < spec.dataPages.size(); ++d) {
    const auto &dataPage = spec.dataPages[d];
    putPATEntry(dataPage, 'D');
    const size_t page = pageAt(dataPage.physicalPage);
    putPageRef(page, dataPage.logicalPage, 'D');
    file[page + 4] = 1;  // usage count, with the data page bit
    file[page + 5] = static_cast<char>(0x80);
    for (size_t r = 0; r < recordsPerPage; ++r) {
      const size_t record = d * recordsPerPage + r;
      if (record >= spec.records.size()) {
        break;
      }
      const size_t at = page + 6 + r * physicalRecordLength;
      file[at + 1] = 1;  // record usage count
      std::copy(spec.records[record].begin(), spec.records[record].end(),
                file.begin() + at + 2);
    }
  }

  return file;
}

// count distinct 100-byte records
static std::vector<std::string> numberedRecords(uint16_t pageLength,
                                                size_t count) {
  std::vector<std::string> records;
  for (size_t i = 0; i < count; ++i) {
    std::string record = "page length " + std::to_string(pageLength) +
                         " record " + std::to_string(i);
    record.resize(100, static_cast<char>('A' + i % 26));
    records.push_back(record);
  }
  return records;
}

// A V6File with patPairs PAT pairs and pageLength byte pages, holding count
// 100-byte records on pages spread over every PAT's region: the first data
// slot of each region, then the last, then the second, ...
static V6File spreadOverPATs(uint16_t pageLength, uint32_t patPairs,
                             size_t count) {
  V6File spec{pageLength, patPairs, numberedRecords(pageLength, count)};
  const size_t dataPages =
      (count + spec.recordsPerPage() - 1) / spec.recordsPerPage();
  EXPECT_LE(dataPages, static_cast<size_t>(patPairs) * spec.pagesPerPAT());
  for (size_t d = 0; d < dataPages; ++d) {
    const uint32_t region = static_cast<uint32_t>(d % patPairs);
    const uint32_t nth = static_cast<uint32_t>(d / patPairs);
    const uint32_t slot =
        nth % 2 == 0 ? nth / 2 : spec.pagesPerPAT() - 1 - nth / 2;
    spec.dataPages.push_back(spec.page(region, slot));
  }
  return spec;
}

// Btrieve 6.x, growing a file past 30 PAT pairs, leaves 4096 bytes (not a
// page) ahead of PAT pair 31, so every page from there on sits 4096 bytes
// further into the file.
static void insertGapBeforePAT31(std::vector<char> &file, uint16_t pageLength) {
  const size_t gapAt = static_cast<size_t>(2 + 30 * (pageLength / 4u)) *
                       pageLength;
  ASSERT_LT(gapAt, file.size());
  file.insert(file.begin() + gapAt, 4096, static_cast<char>(0xCC));
}

static std::filesystem::path writeTempFile(const std::string &name,
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

// Loads spec, built, first as is and then with the gap ahead of PAT pair 31,
// and expects every record and every key's ACS back from both.
static void expectAllOfV6FileAcrossGapBeforePAT31(const V6File &spec,
                                                  const std::string &name) {
  std::vector<char> file = buildV6File(spec);
  for (const bool gap : {false, true}) {
    SCOPED_TRACE(gap ? "with the gap" : "without the gap");
    if (gap) {
      insertGapBeforePAT31(file, spec.pageLength);
    }
    const auto dat = writeTempFile(name + (gap ? "-GAP.DAT" : ".DAT"), file);

    BtrieveDatabase database;
    std::vector<std::string> loaded;
    database.parseDatabase(
        toWideString(dat).c_str(), []() { return true; },
        [&loaded](std::basic_string_view<uint8_t> record) {
          loaded.emplace_back(reinterpret_cast<const char *>(record.data()),
                              record.size());
          return BtrieveDatabase::LoadRecordResult::COUNT;
        });
    std::filesystem::remove(dat);

    expectSameRecords(loaded, spec.records);
    EXPECT_EQ(database.getConversionWarning(), "");
    ASSERT_EQ(database.getKeys().size(), spec.acs.size());
    for (size_t k = 0; k < spec.acs.size(); ++k) {
      const Key &key = database.getKeys()[k];
      EXPECT_STREQ(key.getACSName(), spec.acs[k].name.c_str()) << "key " << k;
      const auto table = acsTable(k);
      ASSERT_NE(key.getACS(), nullptr) << "key " << k;
      EXPECT_EQ(memcmp(key.getACS(), table.data(), table.size()), 0)
          << "key " << k;
    }
  }
}

// 32 PAT pairs, records on the first and last data page of each PAT's region
static void expectRecordsAcrossGapBeforePAT31(uint16_t pageLength) {
  V6File probe{pageLength, 32, {std::string(100, ' ')}};
  const V6File spec =
      spreadOverPATs(pageLength, 32, 2 * 32 * probe.recordsPerPage());
  expectAllOfV6FileAcrossGapBeforePAT31(
      spec, "PAT31-" + std::to_string(pageLength));
}

// 4096 bytes is not a whole number of 1536 byte pages: PAT pair 31 starts
// 1024 bytes past a page boundary.
TEST(BtrieveDatabase, LoadsV6PagesPastTheGapBeforePAT31With1536BytePages) {
  expectRecordsAcrossGapBeforePAT31(1536);
}

// With 512 byte pages the gap is 8 whole pages.
TEST(BtrieveDatabase, LoadsV6PagesPastTheGapBeforePAT31With512BytePages) {
  expectRecordsAcrossGapBeforePAT31(512);
}

// Pages other than data and variable pages, such as ACS pages, are found
// through the PAT, which can point from one PAT's region into another's: past
// the gap, back across it, and within the regions past it.
static void expectACSPagesAcrossGapBeforePAT31(uint16_t pageLength) {
  V6File spec = spreadOverPATs(pageLength, 32, 64);
  spec.acs = {
      // a logical page PAT pair 1 maps, on a page past the gap
      {"UPWARD",
       {spec.page(0, 10).logicalPage, spec.page(31, 10).physicalPage}},
      // one PAT pair 32 maps, on a page before the gap
      {"DOWNWARD",
       {spec.page(31, 11).logicalPage, spec.page(29, 10).physicalPage}},
      // one PAT pair 31 maps, on a page in PAT pair 32's region
      {"PAST", {spec.page(30, 10).logicalPage, spec.page(31, 11).physicalPage}},
  };
  expectAllOfV6FileAcrossGapBeforePAT31(
      spec, "PAT31-ACS-" + std::to_string(pageLength));
}

TEST(BtrieveDatabase, LoadsV6ACSPagesAcrossTheGapBeforePAT31With1536BytePages) {
  expectACSPagesAcrossGapBeforePAT31(1536);
}

TEST(BtrieveDatabase, LoadsV6ACSPagesAcrossTheGapBeforePAT31With512BytePages) {
  expectACSPagesAcrossGapBeforePAT31(512);
}

// The FCR's page count covers logical pages, but shadow paging puts their
// current copies on any free physical page, so the file can run into physical
// regions past the last PAT pair the page count implies, each still headed by
// a PAT pair. Here the page count implies 30 PAT pairs, and pages that the
// last two of them map, and the ACS, sit past the gap, in the regions of PAT
// pairs 31 and 32.
static void expectPagesInRegionsPastThePageCount(uint16_t pageLength) {
  V6File spec{pageLength, 32, {std::string(100, ' ')}};
  spec.pageCount = 30 * spec.pagesPerPAT() + 1;
  for (uint32_t region = 0; region < 30; ++region) {
    const uint32_t physicalRegion = region < 28 ? region : region + 2;
    spec.dataPages.push_back({spec.page(region, 0).logicalPage,
                              spec.page(physicalRegion, 0).physicalPage});
  }
  spec.records = numberedRecords(
      pageLength, spec.dataPages.size() * spec.recordsPerPage());
  spec.acs = {{"SPILLED",
               {spec.page(0, 10).logicalPage, spec.page(31, 10).physicalPage}}};
  expectAllOfV6FileAcrossGapBeforePAT31(
      spec, "PAT31-SPILL-" + std::to_string(pageLength));
}

TEST(BtrieveDatabase, LoadsV6PagesBeyondThePageCountWith1536BytePages) {
  expectPagesInRegionsPastThePageCount(1536);
}

TEST(BtrieveDatabase, LoadsV6PagesBeyondThePageCountWith512BytePages) {
  expectPagesInRegionsPastThePageCount(512);
}

// Stray bytes ahead of a PAT pair needn't be whole pages: page lengths are
// multiples of 512, so 512 stray bytes can move a pair of 1536-byte pages off
// the page grid as the gap before PAT pair 31 does.
TEST(BtrieveDatabase, LoadsV6PagesPastStrayBytesThatArentAWholePage) {
  const uint16_t pageLength = 1536;
  const V6File spec = spreadOverPATs(pageLength, 8, 2 * 8 * 14);
  std::vector<char> file = buildV6File(spec);
  // ahead of PAT pair 5
  file.insert(file.begin() + (2 + 4 * spec.pagesPerRegion()) * pageLength, 512,
              static_cast<char>(0xCC));
  const auto dat = writeTempFile("STRAY512.DAT", file);
  expectSameRecords(loadAllRecords(dat), spec.records);
  std::filesystem::remove(dat);
}

// MajorMUD's rooms file, WCCMP001.VIR (1536 byte pages), is the case that
// matters: once Btrieve 6.15 has rewritten it as a v6 file it has about 70
// PAT pairs, and every room past the gap before PAT 31 was lost. The shipped
// file is 44 MB and not in assets, so this runs only when WBTRV32_WCCMP001
// names a copy of it from MBBSEmu's WCCMMUD 1.11p module (a v5 file), e.g.
// bazel test //btrieve:tests --test_env=WBTRV32_WCCMP001=/path/WCCMP001.VIR
TEST(BtrieveDatabase, LoadsWCCMP001RebuiltAsV6WithTheGapBeforePAT31) {
  const char *source = getenv("WBTRV32_WCCMP001");
  if (source == nullptr || !std::filesystem::exists(source)) {
#ifdef GTEST_SKIP
    GTEST_SKIP() << "set WBTRV32_WCCMP001 to the path of WCCMP001.VIR";
#else
    // googletest before 1.10, as the MSBuild build uses, has no GTEST_SKIP
    return;
#endif
  }

  const auto rooms = loadAllRecords(source);
  ASSERT_EQ(rooms.size(), 26720u);

  const uint16_t pageLength = 1536;
  V6File spec{pageLength, 0, rooms};
  spec.patPairs = static_cast<uint32_t>(
      (rooms.size() + spec.pagesPerPAT() - 1) / spec.pagesPerPAT());
  for (uint32_t d = 0; d < rooms.size(); ++d) {
    spec.dataPages.push_back(spec.page(d / spec.pagesPerPAT(),
                                       d % spec.pagesPerPAT()));
  }
  std::vector<char> file = buildV6File(spec);
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
