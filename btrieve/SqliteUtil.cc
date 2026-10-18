#include "SqliteUtil.h"

#include <algorithm>

#include "BtrieveException.h"
#include "sqlite/sqlite3.h"

namespace btrieve {
void throwException(int errorCode) {
  const char *sqlite3ErrMsg = sqlite3_errstr(errorCode);

  throw BtrieveException(BtrieveError::IOError,
                         sqlite3ErrMsg == nullptr ? "Sqlite error: [%d]"
                                                  : "Sqlite error: [%d] - [%s]",
                         errorCode, sqlite3ErrMsg);
}
}  // namespace btrieve

namespace btrieve {
static std::string join(const std::vector<std::string> &values,
                        const char *separator) {
  std::string ret;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      ret += separator;
    }
    ret += values[i];
  }
  return ret;
}

std::string sqliteKeyColumns(const Key &key, const char *suffix) {
  std::vector<std::string> columns = key.getSqliteColumnNames();
  for (auto &column : columns) {
    column += suffix;
  }
  return join(columns, ", ");
}

std::string sqliteKeyOrder(const Key &key, bool reverse) {
  std::vector<std::string> columns = key.getSqliteColumnNames();
  for (size_t i = 0; i < columns.size(); ++i) {
    const bool descending = key.getSegments()[i].isDescending() != reverse;
    columns[i] += descending ? " DESC" : " ASC";
  }
  return join(columns, ", ");
}

std::string sqliteKeyIndexColumns(const Key &key) {
  std::vector<std::string> columns = key.getSqliteColumnNames();
  for (size_t i = 0; i < columns.size(); ++i) {
    if (key.getSegments()[i].isDescending()) {
      columns[i] += " DESC";
    }
  }
  return join(columns, ", ");
}

// The SQL operator comparing a descending column's values for op, which is in
// index order.
static std::string flipComparison(const std::string &op) {
  if (op == ">") return "<";
  if (op == ">=") return "<=";
  if (op == "<") return ">";
  if (op == "<=") return ">=";
  throw BtrieveException(BtrieveError::InvalidOperation,
                         "Unknown key comparison %s", op.c_str());
}

std::string sqliteKeyComparison(const Key &key, const char *op) {
  const std::vector<std::string> columns = key.getSqliteColumnNames();
  const auto &segments = key.getSegments();
  std::vector<std::string> parameters;
  for (size_t i = 0; i < segments.size(); ++i) {
    parameters.push_back("@value" + std::to_string(i));
  }

  auto isDescending = [](const KeyDefinition &segment) {
    return segment.isDescending();
  };
  const bool allDescending =
      std::all_of(segments.begin(), segments.end(), isDescending);

  // every column sorts the same way, so a single row value compares them all
  if (allDescending ||
      std::none_of(segments.begin(), segments.end(), isDescending)) {
    const std::string sqlOp = allDescending ? flipComparison(op) : op;
    if (!key.isComposite()) {
      return columns[0] + " " + sqlOp + " " + parameters[0];
    }
    return "(" + join(columns, ", ") + ") " + sqlOp + " (" +
           join(parameters, ", ") + ")";
  }

  // Directions are mixed, so compare segment by segment, e.g. for > with the
  // second segment descending:
  //   key_1_0 >= @value0 AND (key_1_0 > @value0 OR (key_1_0 = @value0 AND
  //   key_1_1 < @value1))
  // The leading bound is redundant, but lets SQLite seek the key's index.
  const std::string strictOp = std::string(op).substr(0, 1);
  auto columnOp = [&](size_t i, const std::string &op) {
    return segments[i].isDescending() ? flipComparison(op) : op;
  };

  const size_t last = columns.size() - 1;
  std::string expression =
      columns[last] + " " + columnOp(last, op) + " " + parameters[last];
  for (size_t i = last; i-- > 0;) {
    expression = "(" + columns[i] + " " + columnOp(i, strictOp) + " " +
                 parameters[i] + " OR (" + columns[i] + " = " + parameters[i] +
                 " AND " + expression + "))";
  }

  return columns[0] + " " + columnOp(0, strictOp + "=") + " " +
         parameters[0] + " AND " + expression;
}

std::string sqliteKeyEquals(const Key &key) {
  std::vector<std::string> columns = key.getSqliteColumnNames();
  for (size_t i = 0; i < columns.size(); ++i) {
    columns[i] += " = @value" + std::to_string(i);
  }
  return join(columns, " AND ");
}

std::string sqliteKeyWithoutTrueNulls(const Key &key) {
  std::vector<std::string> columns = key.getSqliteColumnNames();
  std::vector<std::string> terms;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (key.getSegments()[i].isNullIndicator()) {
      terms.push_back(columns[i] + " = 0");
    }
  }
  return join(terms, " AND ");
}

std::string sqliteKeyIndexedWhere(const Key &key) {
  if (!key.hasNullAttribute()) {
    return "";
  }

  // a null key is NULL in every column
  return " WHERE " + key.getSqliteColumnNames()[0] + " IS NOT NULL";
}
}  // namespace btrieve
