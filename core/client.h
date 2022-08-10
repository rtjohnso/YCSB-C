//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {
    workload_.InitKeyBuffer(key);
    workload_.InitPairs(pairs);
  }
  
  virtual bool DoInsert();
  virtual bool DoTransaction();
  
  virtual ~Client() { }
  
 protected:
  
  virtual int TransactionRead(Transaction *txn);
  virtual int TransactionReadModifyWrite(Transaction *txn);
  virtual int TransactionScan(Transaction *txn);
  virtual int TransactionUpdate(Transaction *txn);
  virtual int TransactionInsert(Transaction *txn);
  
  DB &db_;
  CoreWorkload &workload_;
  std::string key;
  std::vector<DB::KVPair> pairs;
};

inline bool Client::DoInsert() {
  workload_.NextSequenceKey(key);
  workload_.UpdateValues(pairs);
  int status = -1;
  Transaction *txn = NULL;
  do {
    db_.Begin(&txn);
    status = db_.Insert(txn, workload_.NextTable(), key, pairs);
  } while (db_.Commit(&txn) == DB::kErrorConflict);
  return (status == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  Transaction *txn = NULL;
  do {
    db_.Begin(&txn);

    for (int i = 0; i < workload_.ops_per_transaction(); ++i) {
      switch (workload_.NextOperation()) {
      case READ:
	status = TransactionRead(txn);
	break;
      case UPDATE:
	status = TransactionUpdate(txn);
	break;
      case INSERT:
	status = TransactionInsert(txn);
	break;
      case SCAN:
	status = TransactionScan(txn);
	break;
      case READMODIFYWRITE:
	status = TransactionReadModifyWrite(txn);
	break;
      default:
	throw utils::Exception("Operation request is not recognized!");
      }
      assert(status >= 0);
    }
  } while (db_.Commit(&txn) == DB::kErrorConflict);
  return (status == DB::kOK);
}

inline int Client::TransactionRead(Transaction *txn) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(txn, table, key, &fields, result);
  } else {
    return db_.Read(txn, table, key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite(Transaction *txn) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(txn, table, key, &fields, result);
  } else {
    db_.Read(txn, table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(txn, table, key, values);
}

inline int Client::TransactionScan(Transaction *txn) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(txn, table, key, len, &fields, result);
  } else {
    return db_.Scan(txn, table, key, len, NULL, result);
  }
}

inline int Client::TransactionUpdate(Transaction *txn) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(txn, table, key, values);
}

inline int Client::TransactionInsert(Transaction *txn) {
  const std::string &table = workload_.NextTable();
  workload_.NextSequenceKey(key);
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(txn, table, key, values);
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
