//
//  rocks_db.cc
//  YCSB-C
//
//  Created by Rob Johnson on 3/20/2022.
//  Copyright (c) 2022 VMware.
//

#include "db/rocks_db.h"
#include <string>
#include <vector>
#include <rocksdb/convenience.h>
#include <rocksdb/utilities/options_util.h>

using std::string;
using std::vector;

namespace ycsbc {

void RocksDB::InitializeOptions(utils::Properties &props)
{
  const std::map<std::string, std::string> &m = (const std::map<std::string, std::string> &)props;

  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;

  if (m.count("rocksdb.config_file")) {
    assert(LoadOptionsFromFile(m.at("rocksdb.config_file"), rocksdb::Env::Default(), &options, &cf_descs) == rocksdb::Status::OK());
  }

  std::unordered_map<std::string, std::string> options_map;
  for (auto tuple : m) {
    if (tuple.first.find("rocksdb.options.") == 0) {
      auto key = tuple.first.substr(strlen("rocksdb.options."), std::string::npos);
      options_map[key] = tuple.second;

    } else if (tuple.first == "rocksdb.write_options.sync") {
      long int sync = props.GetIntProperty("rocksdb.write_options.sync");
      woptions.sync = sync;
    } else if (tuple.first == "rocksdb.write_options.disableWAL") {
      long int disableWAL = props.GetIntProperty("rocksdb.write_options.disableWAL");
      woptions.disableWAL = disableWAL;
    } else if (tuple.first == "rocksdb.config_file") {
      // ignore it here -- loaded above
    } else if (tuple.first == "rocksdb.database_filename") {
      // ignore it, used in constructor
    } else if (tuple.first.find("rocksdb.") == 0) {
      std::cout << "Unknown rocksdb config option " << tuple.first << std::endl;
      assert(0);
    }
  }
  assert(GetDBOptionsFromMap(options, options_map, &options, false, false) == rocksdb::Status::OK());

  // Matrixkv options
  if (props.GetIntProperty("matrixkv.use_nvm_module")) {
    auto nvm_setup = new rocksdb::NvmSetup();
    nvm_setup->use_nvm_module = true;
    assert(props.HasProperty("matrixkv.pmem_path"));
    nvm_setup->pmem_path = props.GetProperty("matrixkv.pmem_path");

    if (props.HasProperty("matrixkv.level0_column_compaction_trigger_size"))
      nvm_setup->Level0_column_compaction_trigger_size = props.GetIntProperty("matrixkv.level0_column_compaction_trigger_size");

    if (props.HasProperty("matrixkv.level0_column_compaction_slowdown_size"))
      nvm_setup->Level0_column_compaction_slowdown_size = props.GetIntProperty("matrixkv.level0_column_compaction_slowdown_size");

    if (props.HasProperty("matrixkv.level0_column_compaction_stop_size"))
      nvm_setup->Level0_column_compaction_stop_size = props.GetIntProperty("matrixkv.level0_column_compaction_stop_size");

    options.nvm_setup.reset(nvm_setup);
  }

}

RocksDB::RocksDB(utils::Properties &props, bool preloaded)
{
  InitializeOptions(props);
  std::string database_filename = props.GetProperty("rocksdb.database_filename");
  options.create_if_missing = !preloaded;
  options.error_if_exists = !preloaded;
  rocksdb::Status status = rocksdb::DB::Open(options, database_filename, &db);
  assert(status.ok());
}

RocksDB::~RocksDB()
{
  delete db;
}

void RocksDB::Init()
{
}

void RocksDB::Close()
{
}

int RocksDB::Read(const string &table,
                     const string &key,
                     const vector<string> *fields,
                     vector<KVPair> &result)
{
  string value;
  rocksdb::Status status = db->Get(roptions, rocksdb::Slice(key), &value);
  assert(status.ok() || status.IsNotFound()); // TODO is it expected we're querying non-existing keys?
  return DB::kOK;
}

int RocksDB::Scan(const string &table,
                  const string &key, int len,
                  const vector<string> *fields,
                  vector<vector<KVPair>> &result)
{
  rocksdb::Iterator* it = db->NewIterator(roptions);
  int i = 0;
  for (it->Seek(key); i < len && it->Valid(); it->Next()) {
    i++;
  }
  delete it;
  return DB::kOK;
}

int RocksDB::Update(const string &table,
                    const string &key,
                    vector<KVPair> &values)
{
  return Insert(table, key, values);
}

int RocksDB::Insert(const string &table, const string &key, vector<KVPair> &values)
{
  assert(values.size() == 1);
  rocksdb::Status status = db->Put(woptions, rocksdb::Slice(key), rocksdb::Slice(values[0].second));
  assert(status.ok());
  return DB::kOK;
}

int RocksDB::Delete(const string &table, const string &key)
{
  rocksdb::Status status = db->Delete(woptions, rocksdb::Slice(key));
  assert(status.ok());
  return DB::kOK;
}

} // ycsbc


// Might want this for later:
//
//
//     inline void sync(bool /*fullSync*/) {
//         static struct rocksdb::FlushOptions foptions = rocksdb::FlushOptions();
//         rocksdb::Status status = db.Flush(foptions);
//         assert(status.ok());
//     }
