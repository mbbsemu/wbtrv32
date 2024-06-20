#ifndef __READER_H_
#define __READER_H_

namespace btrieve {
class Reader {
public:
  virtual ~Reader() = default;

  virtual bool read() = 0;
  virtual int getInt32(unsigned int columnOrdinal) const = 0;
  virtual int64_t getInt64(unsigned int columnOrdinal) const = 0;
  virtual bool getBoolean(unsigned int columnOrdinal) const = 0;
  virtual bool isDBNull(unsigned int columnOrdinal) const = 0;
  virtual std::string getString(unsigned int columnOrdinal) const = 0;
  virtual std::vector<uint8_t> getBlob(unsigned int columnOrdinal) const = 0;
  virtual BindableValue getBindableValue(unsigned int columnOrdinal) const = 0;
};
} // namespace btrieve
#endif
