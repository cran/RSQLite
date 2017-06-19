#ifndef RSQLITE_SQLITEDATAFRAME_H
#define RSQLITE_SQLITEDATAFRAME_H


#include "sqlite3.h"
#include <boost/container/stable_vector.hpp>
#include "ColumnDataType.h"

class SqliteColumn;

class SqliteDataFrame {
  sqlite3_stmt* stmt;
  const int n_max;
  int i;
  boost::container::stable_vector<SqliteColumn> data;
  std::vector<std::string> names;

public:
  SqliteDataFrame(sqlite3_stmt* stmt, std::vector<std::string> names, const int n_max_, const std::vector<DATA_TYPE>& types);
  ~SqliteDataFrame();

public:
  void set_col_values();
  bool advance();

  List get_data(std::vector<DATA_TYPE>& types);
  size_t get_ncols() const;

private:
  void finalize_cols();
};


#endif //RSQLITE_SQLITEDATAFRAME_H
