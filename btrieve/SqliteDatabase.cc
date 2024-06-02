#include "SqliteDatabase.h"

namespace btrieve {
// Opens a Btrieve database as a sql backed file. Will convert a legacy file
// in place if required. Throws a BtrieveException if something fails.
void SqliteDatabase::open(const char *fileName) {}

void SqliteDatabase::create(const char *fileName,
                            const BtrieveDatabase &database) {}

// Closes an opened database.
void SqliteDatabase::close() {}
} // namespace btrieve