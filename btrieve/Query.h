#ifndef __QUERY_H_
#define __QUERY_H_

#include "Key.h"
#include "Reader.h"
#include <cstdint>
#include <memory>
#include <vector>

namespace btrieve {

enum CursorDirection { Seek, Forward, Reverse };

class Query {
public:
  Query(unsigned int position_, const Key *key_,
        std::basic_string_view<uint8_t> keyData_)
      : position(position_), cursorDirection(CursorDirection::Seek), key(key_),
        keyData(keyData_.data(), keyData_.data() + keyData_.size()) {}

  virtual ~Query() = default;

  CursorDirection getCursorDirection() const { return cursorDirection; }

  void setCursorDirection(CursorDirection cursorDirection) {
    this->cursorDirection = cursorDirection;
  }

  const Key *getKey() const { return key; }

  const unsigned int getPosition() const { return position; }

  std::basic_string_view<uint8_t> getKeyData() const {
    return std::basic_string_view<uint8_t>(keyData.data(), keyData.size());
  }

  virtual std::pair<bool, Record> next(CursorDirection cursorDirection) = 0;

protected:
  unsigned int position;
  CursorDirection cursorDirection;
  const Key *key;
  std::vector<uint8_t> keyData;
};
} // namespace btrieve
#endif