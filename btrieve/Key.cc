#include "Key.h"
#include <cstring>

namespace btrieve {

std::vector<uint8_t>
Key::extractKeyDataFromRecord(std::basic_string_view<uint8_t> record) const {
  if (!isComposite()) {
    auto ptr = record
                   .substr(getPrimarySegment().getOffset(),
                           getPrimarySegment().getLength())
                   .data();
    return std::vector<uint8_t>(ptr, ptr + getPrimarySegment().getLength());
  }

  std::vector<uint8_t> composite(getLength());
  int i = 0;
  for (auto &segment : segments) {
    auto ptr = record.substr(segment.getOffset(), segment.getLength()).data();
    memcpy(composite.data() + i, ptr, segment.getLength());
    i += segment.getLength();
  }

  return composite;
}
} // namespace btrieve