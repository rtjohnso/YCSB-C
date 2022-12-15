//
//  splinter_db.cc
//  YCSB-C
//
//  Created by Rob Johnson on 3/20/2022.
//  Copyright (c) 2022 VMware.
//

#include "db/classic_splinterdb.h"
#include "xxhash.h"
extern "C" {
#include "splinterdb/data.h"
}

#include <string>
#include <vector>

using std::string;
using std::vector;

namespace ycsbc {


static int key_compare(const data_config *cfg,
                       const void *key1,
                       const void *key2)
{
  return memcmp(key1, key2, cfg->key_size);
}

static uint32 key_hash(const void* input, size_t length, uint32 seed)
{
  return XXH32(input, length, seed);
}

static message_type message_class(const data_config *cfg,
                     const void *raw_message)
{
  char *charmsg = (char *)raw_message;
  return charmsg[0] == 0 ? MESSAGE_TYPE_DELETE : MESSAGE_TYPE_INSERT;
}

static void merge_tuples(const data_config *cfg,
                        const void *key,
                        const void *old_raw_message,
                        void *new_raw_message)
{
}

static void merge_tuple_final(const data_config *cfg,
                       const void *key,
                       void *oldest_raw_message)
{
}

static void key_or_message_to_str(const data_config *cfg,
                           const void *key_or_message,
                           char *str,
                           size_t max_len)
{
  if (message_class(cfg, key_or_message) == MESSAGE_TYPE_DELETE) {
    snprintf(str, max_len, "<DELETE>");
  } else {
    snprintf(str, max_len, "%s", (const char *)key_or_message);
  }
}

data_config data_config_template = {
  .key_size                          = 0, // To be filled in
  .message_size                      = 0, // To be filled in
  .min_key                           = { 0 },
  .max_key                           = { 127 },
  .key_compare                       = key_compare,
  .key_hash                          = key_hash,
  .message_class                     = message_class,
  .merge_tuples                      = merge_tuples,
  .merge_tuples_final                = merge_tuple_final,
  .clobber_message_with_range_delete = NULL,
  .key_to_string                     = key_or_message_to_str,
  .message_to_string                 = key_or_message_to_str
};

ClassicSplinterDB::ClassicSplinterDB(utils::Properties &props, bool preloaded) {
  data_cfg = data_config_template;
  data_cfg.key_size = props.GetIntProperty("splinterdb.max_key_size");
  data_cfg.message_size = props.GetIntProperty("splinterdb.max_value_size");

  splinter_cfg.filename                 = props.GetProperty("splinterdb.filename").c_str();
  splinter_cfg.cache_size               = props.GetIntProperty("splinterdb.cache_size_mb") * 1024 *1024;
  splinter_cfg.disk_size                = props.GetIntProperty("splinterdb.disk_size_gb") * 1024 * 1024 * 1024;
  splinter_cfg.data_cfg                 = data_cfg;
  splinter_cfg.heap_handle              = NULL;
  splinter_cfg.heap_id                  = NULL;
  // splinter_cfg.page_size                = props.GetIntProperty("splinterdb.page_size");
  // splinter_cfg.extent_size              = props.GetIntProperty("splinterdb.extent_size");
  // splinter_cfg.io_flags                 = props.GetIntProperty("splinterdb.io_flags");
  // splinter_cfg.io_perms                 = props.GetIntProperty("splinterdb.io_perms");
  // splinter_cfg.io_async_queue_depth     = props.GetIntProperty("splinterdb.io_async_queue_depth");
  // splinter_cfg.cache_use_stats          = props.GetIntProperty("splinterdb.cache_use_stats");
  // splinter_cfg.cache_logfile            = props.GetProperty("splinterdb.cache_logfile").c_str();
  // splinter_cfg.btree_rough_count_height = props.GetIntProperty("splinterdb.btree_rough_count_height");
  // splinter_cfg.filter_remainder_size    = props.GetIntProperty("splinterdb.filter_remainder_size");
  // splinter_cfg.filter_index_size        = props.GetIntProperty("splinterdb.filter_index_size");
  // splinter_cfg.use_log                  = props.GetIntProperty("splinterdb.use_log");
  // splinter_cfg.memtable_capacity        = props.GetIntProperty("splinterdb.memtable_capacity");
  // splinter_cfg.max_branches_per_node    = props.GetIntProperty("splinterdb.max_branches_per_node");

  if (preloaded) {
    assert(FALSE);
  } else {
    assert(!kvstore_init(&splinter_cfg, &spl));
  }
}

ClassicSplinterDB::~ClassicSplinterDB()
{
  kvstore_deinit(spl);
}

void ClassicSplinterDB::Init()
{
  kvstore_register_thread(spl);
}

void ClassicSplinterDB::Close()
{
  //kvstore_deregister_thread(spl);
}

int ClassicSplinterDB::Read(const string &table,
                     const string &key,
                     const vector<string> *fields,
                     vector<KVPair> &result) {
  char lookup_result[MAX_MESSAGE_SIZE];
  bool found;

  assert(!kvstore_lookup(spl, key.c_str(), lookup_result, &found));
  if (!found) {
    cout << "FAILED lookup " << key << endl;
    assert(0);
  }
  return DB::kOK;
}

int ClassicSplinterDB::Scan(const string &table,
                     const string &key, int len,
                     const vector<string> *fields,
                     vector<vector<KVPair>> &result) {
  assert(FALSE);
  // assert(fields == NULL);

  // slice key_slice = slice_create(key.size(), key.c_str());

  // splinterdb_iterator *itor;
  // assert(!splinterdb_iterator_init(spl, &itor, key_slice));
  // for (int i = 0; i < len; i++) {
  //   if (!splinterdb_iterator_valid(itor)) {
  //     break;
  //   }
  //   slice key, val;
  //   splinterdb_iterator_get_current(itor, &key, &val);
  //   splinterdb_iterator_next(itor);
  // }
  // assert(!splinterdb_iterator_status(itor));
  // splinterdb_iterator_deinit(itor);

  return DB::kOK;
}

int ClassicSplinterDB::Update(const string &table,
                       const string &key,
                       vector<KVPair> &values) {
  return Insert(table, key, values);
}

int ClassicSplinterDB::Insert(const string &table, const string &key, vector<KVPair> &values) {
  assert(values.size() == 1);

  std::string val = values[0].second;
  assert(!kvstore_insert(spl, key.c_str(), val.c_str()));

  return DB::kOK;
}

int ClassicSplinterDB::Delete(const string &table, const string &key) {
  static const char null_message[MAX_MESSAGE_SIZE] = { 0 };
  assert(!kvstore_insert(spl, key.c_str(), null_message));

  return DB::kOK;
}

} // ycsbc
