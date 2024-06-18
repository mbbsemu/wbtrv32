#include "btrieve/BtrieveDriver.h"
#include "btrieve/BtrieveException.h"
#include "btrieve/SqliteDatabase.h"

int main(int argc, const char **argv) {

  for (int i = 1; i < argc; ++i) {
    printf("Opening %s\n", argv[i]);

    btrieve::BtrieveDriver driver(new btrieve::SqliteDatabase());
    try {
      driver.open(argv[i]);
      printf("Successfully opened %s\n", argv[i]);
    } catch (btrieve::BtrieveException &ex) {
      fprintf(stderr, "Error while parsing %s: %s\n", argv[i],
              ex.getErrorMessage().c_str());
    }
  }
}