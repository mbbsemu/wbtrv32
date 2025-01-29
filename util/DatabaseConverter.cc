#include "btrieve/BtrieveDriver.h"
#include "btrieve/BtrieveException.h"
#include "btrieve/SqliteDatabase.h"
#include "btrieve/Text.h"

int main(int argc, const char **argv) {

  for (int i = 1; i < argc; ++i) {
    printf("Opening %s\n", argv[i]);

    btrieve::BtrieveDriver driver(new btrieve::SqliteDatabase());
    try {
      driver.open(btrieve::toWideString(argv[i]).c_str());
      printf("Successfully opened %s\n", argv[i]);
    } catch (btrieve::BtrieveException &ex) {
      fprintf(stderr, "Error while parsing %s: %s\n", argv[i],
              ex.getErrorMessage().c_str());
    }
  }
}