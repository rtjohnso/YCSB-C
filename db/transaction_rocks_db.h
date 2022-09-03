//
//  rocks_db.h
//  YCSB-C
//

#ifndef YCSB_C_TRANSACTION_ROCKS_DB_H_
#define YCSB_C_TRANSACTION_ROCKS_DB_H_

#include "core/db.h"
#include "core/properties.h"
#include "core/transaction.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
#include <iostream>
#include <string>

using std::cout;
using std::endl;

namespace ycsbc {

class TransactionRocksDB : public DB {
public:
  TransactionRocksDB(utils::Properties &props, bool preloaded);
  ~TransactionRocksDB();

  void Init();
  void Close();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    return DB::kErrorNotSupport;
  }

  int Scan(const std::string &table, const std::string &key, int len,
           const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    return DB::kErrorNotSupport;
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return DB::kErrorNotSupport;
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return DB::kErrorNotSupport;
  }

  int Delete(const std::string &table, const std::string &key) {
    return DB::kErrorNotSupport;
  }

  void Begin(Transaction **txn);

  int Commit(Transaction **txn);

  int Read(Transaction *txn, const std::string &table, const std::string &key,
           const std::vector<std::string> *fields, std::vector<KVPair> &result);

  int Scan(Transaction *txn, const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(Transaction *txn, const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(Transaction *txn, const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(Transaction *txn, const std::string &table,
             const std::string &key);

private:
  void InitializeOptions(utils::Properties &props);

  rocksdb::TransactionDB *db;
  rocksdb::Options options;
  rocksdb::ReadOptions roptions;
  rocksdb::WriteOptions woptions;
  rocksdb::TransactionDBOptions txndb_options;
};

class OptimisticTransactionRocksDB : public DB {
public:
  OptimisticTransactionRocksDB(utils::Properties &props, bool preloaded);
  ~OptimisticTransactionRocksDB();

  void Init();
  void Close();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    return DB::kErrorNotSupport;
  }

  int Scan(const std::string &table, const std::string &key, int len,
           const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    return DB::kErrorNotSupport;
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return DB::kErrorNotSupport;
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return DB::kErrorNotSupport;
  }

  int Delete(const std::string &table, const std::string &key) {
    return DB::kErrorNotSupport;
  }

  void Begin(Transaction **txn);

  int Commit(Transaction **txn);

  int Read(Transaction *txn, const std::string &table, const std::string &key,
           const std::vector<std::string> *fields, std::vector<KVPair> &result);

  int Scan(Transaction *txn, const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(Transaction *txn, const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(Transaction *txn, const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(Transaction *txn, const std::string &table,
             const std::string &key);

private:
  void InitializeOptions(utils::Properties &props);

  rocksdb::OptimisticTransactionDB *db;
  rocksdb::Options options;
  rocksdb::ReadOptions roptions;
  rocksdb::WriteOptions woptions;
};

class RocksDBTransaction : public Transaction {
private:
  rocksdb::Transaction *handle;

  friend TransactionRocksDB;
  friend OptimisticTransactionRocksDB;
};

} // namespace ycsbc

#endif // YCSB_C_TRANSACTION_ROCKS_DB_H_
