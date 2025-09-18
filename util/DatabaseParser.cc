#include "btrieve/BtrieveDatabase.h"
#include "btrieve/BtrieveException.h"
#include "btrieve/Text.h"

int main(int argc, const char **argv) {
  btrieve::BtrieveDatabase database;

  for (int i = 1; i < argc; ++i) {
    printf("Opening %s\n", argv[i]);

    try {
      uint recordCount = 0;
      database.parseDatabase(
          btrieve::toWideString(argv[i]).c_str(), []() { return true; },
          [&recordCount](const std::basic_string_view<uint8_t> record) {
            ++recordCount;
            return btrieve::BtrieveDatabase::LoadRecordResult::COUNT;
          });

      printf("Successfully read all %d records from %s\n", recordCount,
             argv[i]);

      for (auto &key : database.getKeys()) {
        for (auto &segment : key.getSegments()) {
          printf("Key datatype %d\n", segment.getDataType());
        }
      }
    } catch (btrieve::BtrieveException &ex) {
      fprintf(stderr, "Error while parsing %s: %s\n", argv[i],
              ex.getErrorMessage().c_str());
    }
  }
}
