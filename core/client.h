//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include "core_workload.h"
#include "db.h"
#include "transaction.h"
#include "utils.h"
#include <string>

namespace ycsbc {

class Client {
public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {
    workload_.InitKeyBuffer(key);
    workload_.InitPairs(pairs);
  }

  virtual bool DoInsert();
  virtual bool DoTransaction();

  virtual ~Client() {}

protected:
  virtual int TransactionRead(Transaction *txn);
  virtual int TransactionReadModifyWrite(Transaction *txn);
  virtual int TransactionScan(Transaction *txn);
  virtual int TransactionUpdate(Transaction *txn);
  virtual int TransactionInsert(Transaction *txn);

  virtual int TransactionReadRetry(Transaction *txn, TransactionOperation &top);
  virtual int TransactionReadModifyWriteRetry(Transaction *txn,
                                              TransactionOperation &top);
  virtual int TransactionScanRetry(Transaction *txn, TransactionOperation &top);
  virtual int TransactionUpdateRetry(Transaction *txn,
                                     TransactionOperation &top);
  virtual int TransactionInsertRetry(Transaction *txn,
                                     TransactionOperation &top);

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

  db_.Begin(&txn);

  txn->SetTransactionOperationsSize(workload_.ops_per_transaction());
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

  bool need_retry = db_.Commit(&txn) == DB::kErrorConflict;

  while (need_retry) {
    db_.Begin(&txn);

    for (unsigned long i = 0; i < txn->GetTransactionOperationsSize(); ++i) {
      TransactionOperation &top = txn->GetOperation(i);
      switch (top.op) {
      case READ:
        status = TransactionReadRetry(txn, top);
        break;
      case UPDATE:
        status = TransactionUpdateRetry(txn, top);
        break;
      case INSERT:
        status = TransactionInsertRetry(txn, top);
        break;
      case SCAN:
        status = TransactionScanRetry(txn, top);
        break;
      case READMODIFYWRITE:
        status = TransactionReadModifyWriteRetry(txn, top);
        break;
      default:
        throw utils::Exception("Operation request is not recognized!");
      }
      assert(status >= 0);
    }

    need_retry = db_.Commit(&txn) == DB::kErrorConflict;
  }

  return (status == DB::kOK);
}

inline int Client::TransactionRead(Transaction *txn) {
  TransactionOperation &top = txn->GetNextOperation();
  top.op = READ;
  top.table = workload_.NextTable();
  top.key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(txn, top.table, top.key, &fields, result);
  } else {
    return db_.Read(txn, top.table, top.key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite(Transaction *txn) {
  TransactionOperation &top = txn->GetNextOperation();
  top.op = READMODIFYWRITE;
  top.table = workload_.NextTable();
  top.key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(txn, top.table, top.key, &fields, result);
  } else {
    db_.Read(txn, top.table, top.key, NULL, result);
  }

  if (workload_.write_all_fields()) {
    workload_.BuildValues(top.values);
  } else {
    workload_.BuildUpdate(top.values);
  }
  return db_.Update(txn, top.table, top.key, top.values);
}

inline int Client::TransactionScan(Transaction *txn) {
  TransactionOperation &top = txn->GetNextOperation();
  top.op = SCAN;
  top.table = workload_.NextTable();
  top.key = workload_.NextTransactionKey();
  top.len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(txn, top.table, top.key, top.len, &fields, result);
  } else {
    return db_.Scan(txn, top.table, top.key, top.len, NULL, result);
  }
}

inline int Client::TransactionUpdate(Transaction *txn) {
  TransactionOperation &top = txn->GetNextOperation();
  top.op = UPDATE;
  top.table = workload_.NextTable();
  top.key = workload_.NextTransactionKey();
  if (workload_.write_all_fields()) {
    workload_.BuildValues(top.values);
  } else {
    workload_.BuildUpdate(top.values);
  }
  return db_.Update(txn, top.table, top.key, top.values);
}

inline int Client::TransactionInsert(Transaction *txn) {
  TransactionOperation &top = txn->GetNextOperation();
  top.op = INSERT;
  top.table = workload_.NextTable();
  workload_.NextSequenceKey(key);
  top.key = key;
  workload_.BuildValues(top.values);

  return db_.Insert(txn, top.table, top.key, top.values);
}

inline int Client::TransactionReadRetry(Transaction *txn,
                                        TransactionOperation &top) {
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(txn, top.table, top.key, &fields, result);
  } else {
    return db_.Read(txn, top.table, top.key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWriteRetry(Transaction *txn,
                                                   TransactionOperation &top) {
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(txn, top.table, top.key, &fields, result);
  } else {
    db_.Read(txn, top.table, top.key, NULL, result);
  }

  return db_.Update(txn, top.table, top.key, top.values);
}

inline int Client::TransactionScanRetry(Transaction *txn,
                                        TransactionOperation &top) {
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(txn, top.table, top.key, top.len, &fields, result);
  } else {
    return db_.Scan(txn, top.table, top.key, top.len, NULL, result);
  }
}

inline int Client::TransactionUpdateRetry(Transaction *txn,
                                          TransactionOperation &top) {
  return db_.Update(txn, top.table, top.key, top.values);
}

inline int Client::TransactionInsertRetry(Transaction *txn,
                                          TransactionOperation &top) {
  return db_.Insert(txn, top.table, top.key, top.values);
}

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
