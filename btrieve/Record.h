#ifndef __RECORD__H_
#define __RECORD__H_

#include <cstdint>
#include <string_view>
#include <vector>

namespace btrieve {
class Record {
public:
  Record() : position(-1) {}

  Record(unsigned int position_, std::basic_string_view<uint8_t> data_)
      : position(position_), data(data_.data(), data_.data() + data_.size()) {}

  Record(unsigned int position_, std::vector<uint8_t> data_)
      : position(position_), data(data_) {}

  Record(const Record &record) : position(record.position), data(record.data) {}

  Record(Record &&record)
      : position(record.position), data(std::move(record.data)) {}

  unsigned int getPosition() const { return position; }

  const std::vector<uint8_t> &getData() const { return data; }

  Record &operator=(Record &&other) {
    position = other.position;
    data = std::move(other.data);
    return *this;
  }

private:
  unsigned int position;
  std::vector<uint8_t> data;
};
} // namespace btrieve
#endif