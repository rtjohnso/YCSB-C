//
//  splinter_db.h
//  YCSB-C
//

#ifndef YCSB_C_SPLINTER_DB_H_
#define YCSB_C_SPLINTER_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include "core/properties.h"

extern "C" {
#include "splinterdb/splinterdb.h"
}

using std::cout;
using std::endl;

namespace ycsbc {

class SplinterDB : public DB {
public:
  SplinterDB(utils::Properties &props, bool preloaded);
  ~SplinterDB();

  void Init();
  void Close();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key);

private:
  splinter_config           splinter_cfg;
  data_config               data_cfg;
  splinter_handle          *spl;
};

} // ycsbc

#endif // YCSB_C_SPLINTER_DB_H_

