//
//  rocks_db.cc
//  YCSB-C
//
//  Created by Rob Johnson on 3/20/2022.
//  Copyright (c) 2022 VMware.
//

#include "db/transaction_rocks_db.h"
#include <rocksdb/convenience.h>
#include <rocksdb/utilities/options_util.h>
#include <string>
#include <vector>

using std::string;
using std::vector;

namespace ycsbc {

void TransactionRocksDB::InitializeOptions(utils::Properties &props) {
  const std::map<std::string, std::string> &m =
      (const std::map<std::string, std::string> &)props;
  rocksdb::ConfigOptions copts;

  if (m.count("rocksdb.config_file")) {
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
    assert(LoadOptionsFromFile(copts, m.at("rocksdb.config_file"), &options,
                               &cf_descs) == rocksdb::Status::OK());
  }

  std::unordered_map<std::string, std::string> options_map;
  for (auto tuple : m) {
    if (tuple.first.find("rocksdb.options.") == 0) {
      auto key =
          tuple.first.substr(strlen("rocksdb.options."), std::string::npos);
      options_map[key] = tuple.second;

    } else if (tuple.first == "rocksdb.write_options.sync") {
      long int sync = props.GetIntProperty("rocksdb.write_options.sync");
      woptions.sync = sync;
    } else if (tuple.first == "rocksdb.write_options.disableWAL") {
      long int disableWAL =
          props.GetIntProperty("rocksdb.write_options.disableWAL");
      woptions.disableWAL = disableWAL;
    } else if (tuple.first == "rocksdb.config_file") {
      // ignore it here -- loaded above
    } else if (tuple.first == "rocksdb.database_filename") {
      // ignore it, used in constructor
    } else if (tuple.first ==
               "rocksdb.txndb_options.transaction_lock_timeout") {
      long transaction_lock_timeout = props.GetIntProperty(
          "rocksdb.txndb_options.transaction_lock_timeout");
      txndb_options.transaction_lock_timeout = transaction_lock_timeout;
    } else if (tuple.first.find("rocksdb.") == 0) {
      std::cout << "Unknown rocksdb config option " << tuple.first << std::endl;
      assert(0);
    }
  }
  rocksdb::Options new_options;
  assert(GetDBOptionsFromMap(copts, options, options_map, &new_options) ==
         rocksdb::Status::OK());
  options = new_options;
}

TransactionRocksDB::TransactionRocksDB(utils::Properties &props,
                                       bool preloaded) {
  InitializeOptions(props);
  std::string database_filename =
      props.GetProperty("rocksdb.database_filename");
  options.create_if_missing = !preloaded;
  options.error_if_exists = !preloaded;
  rocksdb::Status status = rocksdb::TransactionDB::Open(options, txndb_options,
                                                        database_filename, &db);
  assert(status.ok());
}

TransactionRocksDB::~TransactionRocksDB() { delete db; }

void TransactionRocksDB::Init() {}

void TransactionRocksDB::Close() {}

void TransactionRocksDB::Begin(Transaction **txn) {
  *txn = new RocksDBTransaction();
  ((RocksDBTransaction *)*txn)->handle = db->BeginTransaction(woptions);
}

int TransactionRocksDB::Commit(Transaction **txn) {
  rocksdb::Transaction *txn_handle = ((RocksDBTransaction *)*txn)->handle;
  rocksdb::Status s = txn_handle->Commit();
  delete txn_handle;
  delete *txn;
  *txn = NULL;

  if (s.ok()) {
    return DB::kOK;
  }

  if (s.IsAborted() || s.IsTimedOut()) {
    return DB::kErrorConflict;
  }

  // FIXME: this error type might not be correct
  return DB::kErrorNotSupport;
}

int TransactionRocksDB::Read(Transaction *txn, const std::string &table,
                             const std::string &key,
                             const std::vector<std::string> *fields,
                             std::vector<KVPair> &result) {
  assert(txn != NULL);
  string value;

  rocksdb::Transaction *txn_handle = ((RocksDBTransaction *)txn)->handle;
  rocksdb::Status status =
      txn_handle->GetForUpdate(roptions, rocksdb::Slice(key), &value);
  assert(status.ok() || status.IsNotFound()); // TODO is it expected we're
                                              // querying non-existing keys?
  return DB::kOK;
}

int TransactionRocksDB::Scan(Transaction *txn, const std::string &table,
                             const std::string &key, int len,
                             const std::vector<std::string> *fields,
                             std::vector<std::vector<KVPair>> &result) {
  return DB::kErrorNotSupport;
  // rocksdb::Iterator* it = db->NewIterator(roptions);
  // int i = 0;
  // for (it->Seek(key); i < len && it->Valid(); it->Next()) {
  //   i++;
  // }
  // delete it;
  // return DB::kOK;
}

int TransactionRocksDB::Update(Transaction *txn, const std::string &table,
                               const std::string &key,
                               std::vector<KVPair> &values) {
  return Insert(txn, table, key, values);
}

int TransactionRocksDB::Insert(Transaction *txn, const std::string &table,
                               const std::string &key,
                               std::vector<KVPair> &values) {
  assert(txn != NULL);
  assert(values.size() == 1);

  rocksdb::Transaction *txn_handle = ((RocksDBTransaction *)txn)->handle;
  rocksdb::Status status =
      txn_handle->Put(rocksdb::Slice(key), rocksdb::Slice(values[0].second));
  assert(status.ok());
  return DB::kOK;
}

int TransactionRocksDB::Delete(Transaction *txn, const std::string &table,
                               const std::string &key) {
  assert(txn != NULL);
  rocksdb::Transaction *txn_handle = ((RocksDBTransaction *)txn)->handle;
  rocksdb::Status status = txn_handle->Delete(rocksdb::Slice(key));
  assert(status.ok());
  return DB::kOK;
}

} // namespace ycsbc

// Might want this for later:
//
//
//     inline void sync(bool /*fullSync*/) {
//         static struct rocksdb::FlushOptions foptions =
//         rocksdb::FlushOptions(); rocksdb::Status status = db.Flush(foptions);
//         assert(status.ok());
//     }
