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

// ORDER BY terms for the key's index order, each column in its segment's
// direction, or the opposite when reverse, e.g. "key_1_0 ASC, key_1_1 DESC".
std::string sqliteKeyOrder(const Key &key, bool reverse);

// The key's columns for CREATE INDEX, with DESC after descending segments.
std::string sqliteKeyIndexColumns(const Key &key);

// Compares the key's columns against parameters @value0..@valueN-1 with op
// (>, >=, < or <=) in the key's index order, so on a descending segment >
// means a lower value. When every segment sorts the same way it's a row value,
// e.g. "(key_1_0, key_1_1) > (@value0, @value1)", which orders segment by
// segment and can seek the key's index. Mixed directions are compared segment
// by segment instead.
std::string sqliteKeyComparison(const Key &key, const char *op);

// Matches each of the key's columns against parameters @value0..@valueN-1,
// e.g. "key_1_0 = @value0 AND key_1_1 = @value1".
std::string sqliteKeyEquals(const Key &key);

// A condition that the key has no true null, e.g. "key_1_0 = 0", one term
// per null indicator segment.
std::string sqliteKeyWithoutTrueNulls(const Key &key);

// A WHERE clause, with a leading space, that leaves out records whose key is
// null, since Btrieve doesn't index them. Empty for a key that can't be null.
std::string sqliteKeyIndexedWhere(const Key &key);
}  // namespace btrieve
#endif
