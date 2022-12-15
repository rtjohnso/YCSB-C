//
//  splinter_db.h
//  YCSB-C
//

#ifndef YCSB_C_CLASSIC_SPLINTER_DB_H_
#define YCSB_C_CLASSIC_SPLINTER_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include "core/properties.h"

extern "C" {
#include "splinterdb/kvstore.h"
}

using std::cout;
using std::endl;

namespace ycsbc {

class ClassicSplinterDB : public DB {
public:
  ClassicSplinterDB(utils::Properties &props, bool preloaded);
  ~ClassicSplinterDB();

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
  data_config    data_cfg;
  kvstore_config splinter_cfg;
  kvstore       *spl;
};

} // ycsbc

#endif // YCSB_C_CLASSIC_SPLINTER_DB_H_
