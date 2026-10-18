#include "SqliteUtil.h"

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

std::string sqliteKeyComparison(const Key &key, const char *op) {
  std::vector<std::string> parameters;
  for (size_t i = 0; i < key.getSegments().size(); ++i) {
    parameters.push_back("@value" + std::to_string(i));
  }

  if (!key.isComposite()) {
    return sqliteKeyColumns(key) + " " + op + " " + parameters[0];
  }

  return "(" + sqliteKeyColumns(key) + ") " + op + " (" +
         join(parameters, ", ") + ")";
}

std::string sqliteKeyEquals(const Key &key) {
  std::vector<std::string> columns = key.getSqliteColumnNames();
  for (size_t i = 0; i < columns.size(); ++i) {
    columns[i] += " = @value" + std::to_string(i);
  }
  return join(columns, " AND ");
}

std::string sqliteKeyIndexedWhere(const Key &key) {
  if (!key.hasNullAttribute()) {
    return "";
  }

  // a null key is NULL in every column
  return " WHERE " + key.getSqliteColumnNames()[0] + " IS NOT NULL";
}
}  // namespace btrieve
