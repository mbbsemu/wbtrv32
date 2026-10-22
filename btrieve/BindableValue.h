#ifndef __BINDABLE_VALUE_H_
#define __BINDABLE_VALUE_H_

#include <stdint.h>

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "ByteStringViewTraits.h"

namespace btrieve {
class BindableValue {
 public:
  // in the order of the alternatives in value
  enum Type { Null, Integer, Double, Text, Blob };

  BindableValue() = default;

  BindableValue(int32_t value) : value(int64_t{value}) {}

  BindableValue(uint32_t value) : value(int64_t{value}) {}

  BindableValue(int64_t value) : value(value) {}

  BindableValue(uint64_t value) : value(static_cast<int64_t>(value)) {}

  BindableValue(double value) : value(value) {}

  BindableValue(const std::vector<uint8_t> &data) : value(data) {}

  BindableValue(const std::basic_string_view<uint8_t> data)
      : value(std::vector<uint8_t>(data.begin(), data.end())) {}

  BindableValue(const char *str) {
    if (str != nullptr) {
      value = std::string(str);
    }
  }

  BindableValue(const std::string_view data) : value(std::string(data)) {}

  BindableValue(const BindableValue &) = default;
  BindableValue &operator=(const BindableValue &) = default;

  // a moved-from value is Null
  BindableValue(BindableValue &&other) noexcept
      : value(std::exchange(other.value, std::monostate())) {}

  BindableValue &operator=(BindableValue &&other) noexcept {
    value = std::exchange(other.value, std::monostate());
    return *this;
  }

  Type getType() const { return static_cast<Type>(value.index()); }

  int64_t getIntegerValue() const { return std::get<int64_t>(value); }

  double getDoubleValue() const { return std::get<double>(value); }

  const std::string &getStringValue() const {
    return std::get<std::string>(value);
  }

  const std::vector<uint8_t> &getBlobValue() const {
    return std::get<std::vector<uint8_t>>(value);
  }

  bool isNull() const { return getType() == Type::Null; }

 private:
  std::variant<std::monostate, int64_t, double, std::string,
               std::vector<uint8_t>>
      value;
};
}  // namespace btrieve

#endif
