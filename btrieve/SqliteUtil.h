#ifndef __SQLITE_UTIL_H_
#define __SQLITE_UTIL_H_

#include <string>
#include <vector>

#include "BindableValue.h"
#include "Key.h"

namespace btrieve {
void throwException(int errorCode);

// The key's columns joined by commas, each followed by suffix, e.g.
// "key_1_0 DESC, key_1_1 DESC".
std::string sqliteKeyColumns(const Key &key, const char *suffix = "");

// Compares the key's columns against parameters @value0..@valueN-1 with op.
// Composite keys use a row value, e.g. "(key_1_0, key_1_1) > (@value0,
// @value1)", which orders segment by segment and can seek the key's index.
std::string sqliteKeyComparison(const Key &key, const char *op);

// Matches each of the key's columns against values, e.g. "key_1_0 = @value0
// AND key_1_1 = @value1". Null values are matched with IS NULL and aren't
// parameters, so bind only the non-null values, in order, starting at 1.
std::string sqliteKeyEquals(const Key &key,
                            const std::vector<BindableValue> &values);
}  // namespace btrieve
#endif
