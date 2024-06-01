#include "btrieve/BtrieveDatabase.h"
#include "btrieve/BtrieveException.h"

int main(int argc, const char **argv) {
  btrieve::BtrieveDatabase database;

  for (int i = 1; i < argc; ++i) {
    printf("Opening %s\n", argv[i]);

    try {
      uint recordCount = 0;
      database.parseDatabase(
          argv[i], []() { return true; },
          [&recordCount](const std::basic_string_view<uint8_t> record) {
            ++recordCount;
            return true;
          });

      printf("Successfully read all %d records from %s\n", recordCount,
             argv[i]);
    } catch (btrieve::BtrieveException &ex) {
      fprintf(stderr, "Error while parsing %s: %s\n", argv[i],
              ex.getErrorMessage().c_str());
    }
  }

  // /home/tcj/mbbs/ELWICLIB.DAT
}