//
//  splinter_db.cc
//  YCSB-C
//
//  Created by Rob Johnson on 3/20/2022.
//  Copyright (c) 2022 VMware.
//

#include "db/splinter_db.h"
extern "C" {
#include "splinterdb/default_data_config.h"
}

#include <string>
#include <vector>

using std::string;
using std::vector;

namespace ycsbc {

SplinterDB::SplinterDB(utils::Properties &props, bool preloaded) {
  uint64_t max_key_size = props.GetIntProperty("splinterdb.max_key_size");

  // Init data_cfg
  default_data_config_init(max_key_size, &data_cfg);

  // Init splinter_cfg
  splinter_cfg.page_size = props.GetIntProperty("splinterdb.page_size");
  splinter_cfg.extent_size = props.GetIntProperty("splinterdb.extent_size");
  splinter_cfg.fanout = props.GetIntProperty("splinterdb.fanout");
  splinter_cfg.use_stats = props.GetIntProperty("splinterdb.use_stats");
  splinter_cfg.reclaim_threshold = props.GetIntProperty("splinterdb.reclaim_threshold");

  // Use defaults
  uint64               max_pivot_keys;          // hard limit on number of pivot keys
  uint64               max_tuples_per_node;
  uint64               max_branches_per_node;
  uint64               target_leaf_tuples;      // make leaves this big when splitting
  uint64               reclaim_threshold;       // start reclaming space when free space < threshold

  memtable_config      mt_cfg;
  btree_config         btree_cfg; // point message btree config
  btree_config         range_btree_cfg; // range delete btree config
  routing_config       index_filter_cfg;
  routing_config       leaf_filter_cfg;

  // Init d
  data_config         *data_cfg; // point message data config

  bool                 use_log;
  log_config          *log_cfg;

  splinter_cfg.filename                 = props.GetProperty("splinterdb.filename").c_str();
  splinter_cfg.cache_size               = props.GetIntProperty("splinterdb.cache_size_mb") * 1024 *1024;
  splinter_cfg.disk_size                = props.GetIntProperty("splinterdb.disk_size_gb") * 1024 * 1024 * 1024;
  splinter_cfg.data_cfg                 = &data_cfg;
  splinter_cfg.heap_handle              = NULL;
  splinter_cfg.heap_id                  = NULL;
  splinter_cfg.page_size                = props.GetIntProperty("splinterdb.page_size");
  splinter_cfg.extent_size              = props.GetIntProperty("splinterdb.extent_size");
  splinter_cfg.io_flags                 = props.GetIntProperty("splinterdb.io_flags");
  splinter_cfg.io_perms                 = props.GetIntProperty("splinterdb.io_perms");
  splinter_cfg.io_async_queue_depth     = props.GetIntProperty("splinterdb.io_async_queue_depth");
  splinter_cfg.cache_use_stats          = props.GetIntProperty("splinterdb.cache_use_stats");
  splinter_cfg.cache_logfile            = props.GetProperty("splinterdb.cache_logfile").c_str();
  splinter_cfg.btree_rough_count_height = props.GetIntProperty("splinterdb.btree_rough_count_height");
  splinter_cfg.filter_remainder_size    = props.GetIntProperty("splinterdb.filter_remainder_size");
  splinter_cfg.filter_index_size        = props.GetIntProperty("splinterdb.filter_index_size");
  splinter_cfg.use_log                  = props.GetIntProperty("splinterdb.use_log");
  splinter_cfg.memtable_capacity        = props.GetIntProperty("splinterdb.memtable_capacity");
  splinter_cfg.max_branches_per_node    = props.GetIntProperty("splinterdb.max_branches_per_node");

  if (preloaded) {
    assert(!splinterdb_open(&splinterdb_cfg, &spl));
  } else {
    assert(!splinterdb_create(&splinterdb_cfg, &spl));
  }
}

SplinterDB::~SplinterDB()
{
  splinterdb_close(&spl);
}

void SplinterDB::Init()
{
  splinterdb_register_thread(spl);
}

void SplinterDB::Close()
{
  splinterdb_deregister_thread(spl);
}

int SplinterDB::Read(const string &table,
                     const string &key,
                     const vector<string> *fields,
                     vector<KVPair> &result) {
  splinterdb_lookup_result  lookup_result;
  splinterdb_lookup_result_init(spl, &lookup_result, 0, NULL);
  slice key_slice = slice_create(key.size(), key.c_str());
  //cout << "lookup " << key << endl;
  assert(!splinterdb_lookup(spl, key_slice, &lookup_result));
  if (!splinterdb_lookup_found(&lookup_result)) {
    cout << "FAILED lookup " << key << endl;
    assert(0);
  }
  //cout << "done lookup " << key << endl;
  splinterdb_lookup_result_deinit(&lookup_result);
  return DB::kOK;
}

int SplinterDB::Scan(const string &table,
                     const string &key, int len,
                     const vector<string> *fields,
                     vector<vector<KVPair>> &result) {
  assert(fields == NULL);

  slice key_slice = slice_create(key.size(), key.c_str());

  splinterdb_iterator *itor;
  assert(!splinterdb_iterator_init(spl, &itor, key_slice));
  for (int i = 0; i < len; i++) {
    if (!splinterdb_iterator_valid(itor)) {
      break;
    }
    slice key, val;
    splinterdb_iterator_get_current(itor, &key, &val);
    splinterdb_iterator_next(itor);
  }
  assert(!splinterdb_iterator_status(itor));
  splinterdb_iterator_deinit(itor);

  return DB::kOK;
}

int SplinterDB::Update(const string &table,
                       const string &key,
                       vector<KVPair> &values) {
  return Insert(table, key, values);
}

int SplinterDB::Insert(const string &table, const string &key, vector<KVPair> &values) {
  assert(values.size() == 1);

  std::string val = values[0].second;
  slice key_slice = slice_create(key.size(), key.c_str());
  slice val_slice = slice_create(val.size(), val.c_str());
  //cout << "insert " << key << endl;
  assert(!splinterdb_insert(spl, key_slice, val_slice));
  //cout << "done insert " << key << endl;

  return DB::kOK;
}

int SplinterDB::Delete(const string &table, const string &key) {
  slice key_slice = slice_create(key.size(), key.c_str());
  assert(!splinterdb_delete(spl, key_slice));

  return DB::kOK;
}

} // ycsbc
