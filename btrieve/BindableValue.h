#ifndef __BINDABLE_VALUE_H_
#define __BINDABLE_VALUE_H_

#include "sqlite3.h"
#include <string>
#include <vector>

namespace btrieve {
class BindableValue {
private:
  enum Type { Null, Integer, Double, Text, Blob };

public:
  BindableValue() : type(Type::Null) {}

  BindableValue(sqlite3_int64 value) : type(Type::Integer), int_value(value) {}

  BindableValue(const std::vector<uint8_t> &data)
      : type(Type::Blob), blob_value(new std::vector<uint8_t>(data)) {}

  BindableValue(const std::basic_string_view<uint8_t> data)
      : type(Type::Blob), blob_value(new std::vector<uint8_t>(
                              data.data(), data.data() + data.size())) {}

  BindableValue(const std::string_view data)
      : type(Type::Text), text_value(new std::string(data)) {}
  // deep copy
  BindableValue(const BindableValue &value) : type(value.type) {
    // TODO
  }

  // move copy
  BindableValue(BindableValue &&value) : type(value.type) {
    switch (value.type) {
    case Type::Null:
      break;
    case Type::Integer:
      int_value = value.int_value;
      value.int_value = -1;
      break;
    case Type::Double:
      double_value = value.double_value;
      value.double_value = -1;
      break;
    case Type::Text:
      text_value = value.text_value;
      value.text_value = NULL;
      break;
    case Type::Blob:
      blob_value = value.blob_value;
      value.blob_value = NULL;
    }

    value.type = Type::Null;
  }

  ~BindableValue() {
    if (type == Type::Blob && blob_value != nullptr) {
      delete blob_value;
    } else if (type == Type::Text && text_value != nullptr) {
      delete text_value;
    }
  }

  Type getType() const { return type; }

  sqlite3_int64 getIntegerValue() const { return int_value; }

  const std::string &getStringValue() const { return *text_value; }

  const std::vector<uint8_t> &getBlobValue() const { return *blob_value; }

  bool isNull() { return getType() == Type::Null; }

private:
  Type type;
  union {
    sqlite3_int64 int_value;
    double double_value;
    std::string *text_value;
    std::vector<uint8_t> *blob_value;
  };
};
} // namespace btrieve

#endif