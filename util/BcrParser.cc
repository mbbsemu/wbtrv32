#include "util/BcrParser.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <sstream>

#include "btrieve/AttributeMask.h"
#include "vstudio/wbtrv32/wbtrv32.h"

namespace bcr {

namespace {

struct Element {
  std::string keyword;  // lowercased
  std::string value;
  int line;
};

std::string toLower(std::string_view str) {
  std::string ret(str);
  std::transform(ret.begin(), ret.end(), ret.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return ret;
}

[[noreturn]] void fail(int line, const std::string &message) {
  std::ostringstream stream;
  if (line > 0) {
    stream << "line " << line << ": ";
  }
  stream << message;
  throw BcrParseException(stream.str());
}

// Splits the description into keyword=value elements, dropping comments. A
// bare word with no '=' is appended to the previous element's value so that
// multi-word values like "type=unsigned binary" survive tokenization.
std::vector<Element> tokenize(std::string_view text) {
  std::vector<Element> elements;
  int line = 1;
  size_t i = 0;

  while (i < text.size()) {
    char c = text[i];
    if (c == '\n') {
      ++line;
      ++i;
      continue;
    }
    if (std::isspace(static_cast<unsigned char>(c)) || c == '\x1A') {
      // \x1A is the DOS end-of-file marker some editors append
      ++i;
      continue;
    }
    if (text.compare(i, 2, "/*") == 0) {
      const int commentLine = line;
      size_t end = text.find("*/", i + 2);
      if (end == std::string_view::npos) {
        fail(commentLine, "unterminated comment");
      }
      line += static_cast<int>(
          std::count(text.begin() + i, text.begin() + end, '\n'));
      i = end + 2;
      continue;
    }

    size_t start = i;
    while (i < text.size() &&
           !std::isspace(static_cast<unsigned char>(text[i])) &&
           text.compare(i, 2, "/*") != 0) {
      ++i;
    }
    std::string_view token = text.substr(start, i - start);

    size_t equals = token.find('=');
    if (equals == std::string_view::npos) {
      if (elements.empty()) {
        fail(line, "expected keyword=value but found '" + std::string(token) +
                       "'");
      }
      elements.back().value += ' ';
      elements.back().value += token;
      continue;
    }
    if (equals == 0) {
      fail(line, "missing keyword before '=' in '" + std::string(token) + "'");
    }

    elements.push_back(Element{toLower(token.substr(0, equals)),
                               std::string(token.substr(equals + 1)), line});
  }

  return elements;
}

// Walks the element list in the fixed order mandated by the description file
// format.
class Reader {
 public:
  explicit Reader(std::vector<Element> elements_)
      : elements(std::move(elements_)) {}

  bool atEnd() const { return index >= elements.size(); }

  // Consumes the next element if its keyword is one of the given names.
  const Element *optional(std::initializer_list<const char *> keywords) {
    if (atEnd()) {
      return nullptr;
    }
    for (const char *keyword : keywords) {
      if (elements[index].keyword == keyword) {
        return &elements[index++];
      }
    }
    return nullptr;
  }

  const Element &required(std::initializer_list<const char *> keywords) {
    const Element *element = optional(keywords);
    if (element != nullptr) {
      return *element;
    }

    std::string expected = std::string("'") + *keywords.begin() + "='";
    if (atEnd()) {
      fail(lastLine(), "expected " + expected + " but reached end of file");
    }
    const Element &next = elements[index];
    fail(next.line, "expected " + expected + " but found '" + next.keyword +
                        "=' (elements must appear in the documented order)");
  }

  const Element &peek() const { return elements[index]; }

 private:
  int lastLine() const { return elements.empty() ? 0 : elements.back().line; }

  std::vector<Element> elements;
  size_t index = 0;
};

unsigned long parseNumber(const Element &element, unsigned long min,
                          unsigned long max) {
  const std::string &value = element.value;
  if (value.empty() ||
      !std::all_of(value.begin(), value.end(),
                   [](unsigned char c) { return std::isdigit(c); })) {
    fail(element.line, "'" + element.keyword + "' must be a number, not '" +
                           value + "'");
  }
  unsigned long number = std::strtoul(value.c_str(), nullptr, 10);
  if (number < min || number > max) {
    fail(element.line, "'" + element.keyword + "=" + value +
                           "' is out of range (" + std::to_string(min) + "-" +
                           std::to_string(max) + ")");
  }
  return number;
}

bool parseYesNo(const Element &element) {
  std::string value = toLower(element.value);
  if (value == "y" || value == "yes") {
    return true;
  }
  if (value == "n" || value == "no") {
    return false;
  }
  fail(element.line,
       "'" + element.keyword + "' must be y or n, not '" + element.value + "'");
}

uint8_t parseHexByte(const Element &element) {
  std::string value = toLower(element.value);
  if (value.rfind("0x", 0) == 0) {
    value = value.substr(2);
  }
  if (value.empty() || value.size() > 2 ||
      !std::all_of(value.begin(), value.end(),
                   [](unsigned char c) { return std::isxdigit(c); })) {
    fail(element.line, "'" + element.keyword +
                           "' must be a one byte hex value, not '" +
                           element.value + "'");
  }
  return static_cast<uint8_t>(std::strtoul(value.c_str(), nullptr, 16));
}

struct KeyTypeName {
  const char *name;
  btrieve::KeyDataType type;
};

// Only the key types that wbtrv32 knows how to collate.
const KeyTypeName keyTypeNames[] = {
    {"string", btrieve::KeyDataType::String},
    {"integer", btrieve::KeyDataType::Integer},
    {"float", btrieve::KeyDataType::Float},
    {"date", btrieve::KeyDataType::Date},
    {"time", btrieve::KeyDataType::Time},
    {"decimal", btrieve::KeyDataType::Decimal},
    {"money", btrieve::KeyDataType::Money},
    {"logical", btrieve::KeyDataType::Logical},
    {"numeric", btrieve::KeyDataType::Numeric},
    {"bfloat", btrieve::KeyDataType::Bfloat},
    {"lstring", btrieve::KeyDataType::Lstring},
    {"zstring", btrieve::KeyDataType::Zstring},
    {"unsigned binary", btrieve::KeyDataType::UnsignedBinary},
    {"autoincrement", btrieve::KeyDataType::AutoInc},
};

// Accepts the full type name or any unambiguous abbreviation of at least
// three letters (the documented short form is the first three letters).
btrieve::KeyDataType parseKeyType(const Element &element) {
  std::string value = toLower(element.value);
  // allow "unsigned_binary" and "unsigned-binary" spellings too
  std::replace(value.begin(), value.end(), '_', ' ');
  std::replace(value.begin(), value.end(), '-', ' ');

  for (const auto &keyType : keyTypeNames) {
    if (value == keyType.name) {
      return keyType.type;
    }
  }

  const KeyTypeName *match = nullptr;
  if (value.size() >= 3) {
    for (const auto &keyType : keyTypeNames) {
      if (std::string_view(keyType.name).substr(0, value.size()) == value) {
        if (match != nullptr) {
          fail(element.line, "ambiguous key type '" + element.value + "'");
        }
        match = &keyType;
      }
    }
  }
  if (match == nullptr) {
    fail(element.line, "unsupported key type '" + element.value + "'");
  }
  return match->type;
}

bool isStringType(btrieve::KeyDataType type) {
  return type == btrieve::KeyDataType::String ||
         type == btrieve::KeyDataType::Lstring ||
         type == btrieve::KeyDataType::Zstring;
}

bool isValidPageSize(unsigned long pageSize) {
  return (pageSize >= 512 && pageSize <= 4096 && pageSize % 512 == 0) ||
         pageSize == 8192 || pageSize == 16384;
}

std::string parseAcsName(const Element &element) {
  if (element.keyword == "isr") {
    fail(element.line, "ISR tables are not supported, use an ACS file");
  }
  if (element.value.empty()) {
    fail(element.line, "missing ACS file name");
  }
  return element.value;
}

KeySegment parseSegment(Reader &reader, const Description &description,
                        size_t keyNumber, bool firstSegment) {
  KeySegment segment;

  if (firstSegment) {
    if (const Element *keynum = reader.optional({"keynum"})) {
      if (parseNumber(*keynum, 0, 118) != keyNumber) {
        fail(keynum->line,
             "non-sequential key numbers are not supported, expected keynum=" +
                 std::to_string(keyNumber));
      }
    }
  }

  const Element &position = reader.required({"position"});
  segment.position = static_cast<uint16_t>(
      parseNumber(position, 1, description.recordLength));

  const Element &length = reader.required({"length"});
  segment.length = static_cast<uint16_t>(parseNumber(length, 1, 255));
  if (segment.position + segment.length - 1 > description.recordLength) {
    fail(length.line, "key segment at position " +
                          std::to_string(segment.position) + " with length " +
                          std::to_string(segment.length) +
                          " extends past the record length of " +
                          std::to_string(description.recordLength));
  }

  segment.duplicates = parseYesNo(reader.required({"duplicates"}));
  segment.modifiable = parseYesNo(reader.required({"modifiable"}));

  const Element &type = reader.required({"type"});
  segment.type = parseKeyType(type);
  if (segment.type == btrieve::KeyDataType::Float && segment.length != 4 &&
      segment.length != 8) {
    fail(type.line, "float keys must be 4 or 8 bytes long");
  }

  if (const Element *descending = reader.optional({"descending"})) {
    segment.descending = parseYesNo(*descending);
  }

  const Element &alternate = reader.required({"alternate"});
  segment.alternate = parseYesNo(alternate);
  if (segment.alternate && !isStringType(segment.type)) {
    fail(alternate.line,
         "alternate=y only applies to string, lstring and zstring keys");
  }

  if (const Element *caseInsensitive = reader.optional({"caseinsensitive"})) {
    if (parseYesNo(*caseInsensitive)) {
      fail(caseInsensitive->line,
           "caseinsensitive=y is not supported, use an ACS file instead");
    }
  }

  if (const Element *repeatdup = reader.optional({"repeatdup"})) {
    segment.repeatingDuplicates = parseYesNo(*repeatdup);
    if (segment.repeatingDuplicates && !segment.duplicates) {
      fail(repeatdup->line, "repeatdup=y requires duplicates=y");
    }
  }

  // "nullkey" is the current keyword; older butil versions used "null"
  if (const Element *nullKey = reader.optional({"nullkey", "null"})) {
    std::string value = toLower(nullKey->value);
    if (value == "allsegs" || value == "y" || value == "yes") {
      segment.nullKey = NullKey::AllSegments;
    } else if (value == "anyseg") {
      segment.nullKey = NullKey::AnySegment;
    } else if (value == "n" || value == "no") {
      segment.nullKey = NullKey::None;
    } else {
      fail(nullKey->line, "'" + nullKey->keyword +
                              "' must be allsegs, anyseg or n, not '" +
                              nullKey->value + "'");
    }

    const Element *value_ = reader.optional({"value"});
    if (value_ != nullptr) {
      segment.nullValue = parseHexByte(*value_);
    } else if (segment.nullKey != NullKey::None) {
      fail(nullKey->line, "'" + nullKey->keyword + "' requires 'value='");
    }
  }

  segment.segment = parseYesNo(reader.required({"segment"}));
  return segment;
}

// An ACS file is the same 265 byte block the Create operation expects: an
// 0xAC header byte, an 8 byte name, and the 256 byte collating table.
std::vector<char> loadAcs(const std::filesystem::path &path) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    throw BcrParseException("unable to open ACS file " + path.string());
  }

  std::vector<char> data((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());

  if (data.size() != sizeof(wbtrv32::ACSCREATEDATA) ||
      static_cast<uint8_t>(data[0]) != 0xAC) {
    throw BcrParseException(
        "invalid ACS file " + path.string() + ": expected " +
        std::to_string(sizeof(wbtrv32::ACSCREATEDATA)) +
        " bytes starting with 0xAC");
  }

  return data;
}

}  // namespace

Description parse(std::string_view text) {
  Reader reader(tokenize(text));
  Description description;

  if (reader.atEnd()) {
    fail(0, "description file is empty");
  }

  description.recordLength =
      static_cast<uint16_t>(parseNumber(reader.required({"record"}), 4, 65535));

  description.variable = parseYesNo(reader.required({"variable"}));

  if (const Element *dupkey = reader.optional({"dupkey"})) {
    description.dupkey = static_cast<uint8_t>(parseNumber(*dupkey, 0, 119));
  }

  if (const Element *truncate = reader.optional({"truncate"})) {
    description.truncate = parseYesNo(*truncate);
    if (description.truncate && !description.variable) {
      fail(truncate->line, "truncate=y requires variable=y");
    }
  }

  if (const Element *compress = reader.optional({"compress"})) {
    description.compress = parseYesNo(*compress);
  }

  const Element &keyCount = reader.required({"key"});
  description.keys.resize(parseNumber(keyCount, 0, 119));

  const Element &page = reader.required({"page"});
  unsigned long pageSize = parseNumber(page, 512, 16384);
  if (!isValidPageSize(pageSize)) {
    fail(page.line, "invalid page size " + page.value +
                        ", must be a multiple of 512 up to 4096, 8192, or "
                        "16384");
  }
  description.pageSize = static_cast<uint16_t>(pageSize);

  if (const Element *allocation = reader.optional({"allocation"})) {
    description.allocation =
        static_cast<uint16_t>(parseNumber(*allocation, 1, 65535));
  }

  if (const Element *replace = reader.optional({"replace"})) {
    parseYesNo(*replace);  // obsolete, validated and ignored
  }

  if (const Element *data = reader.optional({"data"})) {
    description.keyOnly = !parseYesNo(*data);
  }

  if (const Element *fthreshold = reader.optional({"fthreshold"})) {
    unsigned long threshold = parseNumber(*fthreshold, 5, 30);
    if (threshold != 5 && threshold != 10 && threshold != 20 &&
        threshold != 30) {
      fail(fthreshold->line, "fthreshold must be 5, 10, 20 or 30");
    }
    description.freeSpaceThreshold = static_cast<uint8_t>(threshold);
  }

  if (const Element *vats = reader.optional({"vats", "huge"})) {
    description.vats = parseYesNo(*vats);
  }

  if (const Element *balance = reader.optional({"balance"})) {
    description.balance = parseYesNo(*balance);
  }

  if (const Element *usekeynum = reader.optional({"usekeynum"})) {
    parseYesNo(*usekeynum);
  }

  // these only tune the physical file layout, which doesn't apply to sqlite
  reader.optional({"sysdataonrecord"});
  if (const Element *pagecompress = reader.optional({"pagecompress"})) {
    parseYesNo(*pagecompress);
  }

  for (size_t keyNumber = 0; keyNumber < description.keys.size();
       ++keyNumber) {
    Key &key = description.keys[keyNumber];
    do {
      key.segments.push_back(
          parseSegment(reader, description, keyNumber, key.segments.empty()));
    } while (key.segments.back().segment);

    // An ACS name following the last key is the file level default, which
    // is how a single ACS shared by every key is normally written.
    if (keyNumber + 1 < description.keys.size()) {
      if (const Element *name = reader.optional({"name", "isr"})) {
        key.segments.back().acsFileName = parseAcsName(*name);
      }
    }
  }

  if (const Element *name = reader.optional({"name", "isr"})) {
    description.acsFileName = parseAcsName(*name);
  }

  if (!reader.atEnd()) {
    const Element &next = reader.peek();
    fail(next.line, "unexpected '" + next.keyword + "=' after " +
                        std::to_string(description.keys.size()) +
                        " key(s) were defined, check the key= count and "
                        "element order");
  }

  return description;
}

Description parseFile(const std::filesystem::path &path) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    throw BcrParseException("unable to open " + path.string());
  }

  std::string text((std::istreambuf_iterator<char>(file)),
                   std::istreambuf_iterator<char>());
  return parse(text);
}

std::vector<uint8_t> toCreateBuffer(const Description &description,
                                    const std::filesystem::path &acsDirectory) {
  // Resolve which ACS each key uses, numbering distinct files in order of
  // first use.
  std::vector<std::filesystem::path> acsFiles;
  std::vector<int> keyAcsNumbers;
  for (size_t keyNumber = 0; keyNumber < description.keys.size();
       ++keyNumber) {
    const Key &key = description.keys[keyNumber];
    bool usesAcs = std::any_of(key.segments.begin(), key.segments.end(),
                               [](const KeySegment &s) { return s.alternate; });
    if (!usesAcs) {
      keyAcsNumbers.push_back(-1);
      continue;
    }

    std::string fileName = key.segments.back().acsFileName;
    if (fileName.empty()) {
      fileName = description.acsFileName;
    }
    if (fileName.empty()) {
      throw BcrParseException("key " + std::to_string(keyNumber) +
                              " uses alternate=y but no ACS file was named");
    }

    std::filesystem::path path(fileName);
    if (path.is_relative()) {
      path = acsDirectory / path;
    }
    path = path.lexically_normal();

    auto existing = std::find(acsFiles.begin(), acsFiles.end(), path);
    keyAcsNumbers.push_back(static_cast<int>(existing - acsFiles.begin()));
    if (existing == acsFiles.end()) {
      acsFiles.push_back(path);
    }
  }

  const uint16_t acsAttribute =
      acsFiles.size() > 1 ? MultipleACS : NumberedACS;

  size_t segmentCount = 0;
  for (const Key &key : description.keys) {
    segmentCount += key.segments.size();
  }

  std::vector<uint8_t> buffer(sizeof(wbtrv32::FILESPEC) +
                              segmentCount * sizeof(wbtrv32::KEYSPEC) +
                              acsFiles.size() *
                                  sizeof(wbtrv32::ACSCREATEDATA));

  wbtrv32::FILESPEC fileSpec = {};
  fileSpec.logicalFixedRecordLength = description.recordLength;
  fileSpec.pageSize = description.pageSize;
  fileSpec.numberOfKeys = static_cast<uint8_t>(description.keys.size());
  fileSpec.numExtraPointers = description.dupkey;
  fileSpec.preallocatedPages = description.allocation;
  fileSpec.fileFlags = (description.variable ? 0x1 : 0) |
                       (description.truncate ? 0x2 : 0) |
                       (description.allocation ? 0x4 : 0) |
                       (description.compress ? 0x8 : 0) |
                       (description.keyOnly ? 0x10 : 0) |
                       (description.balance ? 0x20 : 0) |
                       (description.vats ? 0x800 : 0);
  switch (description.freeSpaceThreshold) {
    case 10:
      fileSpec.fileFlags |= 0x40;
      break;
    case 20:
      fileSpec.fileFlags |= 0x80;
      break;
    case 30:
      fileSpec.fileFlags |= 0xC0;
      break;
  }

  uint8_t *ptr = buffer.data();
  memcpy(ptr, &fileSpec, sizeof(fileSpec));
  ptr += sizeof(fileSpec);

  for (size_t keyNumber = 0; keyNumber < description.keys.size();
       ++keyNumber) {
    for (const KeySegment &segment : description.keys[keyNumber].segments) {
      wbtrv32::KEYSPEC keySpec = {};
      keySpec.position = segment.position;
      keySpec.length = segment.length;
      keySpec.extendedDataType = static_cast<uint8_t>(segment.type);
      keySpec.nullValue = segment.nullValue;
      keySpec.number = static_cast<uint8_t>(keyNumber);
      keySpec.attributes =
          UseExtendedDataType | (segment.duplicates ? Duplicates : 0) |
          (segment.modifiable ? Modifiable : 0) |
          (segment.segment ? SegmentedKey : 0) |
          (segment.descending ? DescendingKeySegment : 0) |
          (segment.repeatingDuplicates ? RepeatingDuplicatesKey : 0) |
          (segment.nullKey == NullKey::AllSegments ? NullAllSegments : 0) |
          (segment.nullKey == NullKey::AnySegment ? NullAnySegment : 0);
      if (segment.alternate) {
        keySpec.attributes |= acsAttribute;
        keySpec.acsNumber = static_cast<uint8_t>(keyAcsNumbers[keyNumber]);
      }

      memcpy(ptr, &keySpec, sizeof(keySpec));
      ptr += sizeof(keySpec);
    }
  }

  for (const auto &acsFile : acsFiles) {
    std::vector<char> acs = loadAcs(acsFile);
    memcpy(ptr, acs.data(), acs.size());
    ptr += acs.size();
  }

  return buffer;
}

}  // namespace bcr
