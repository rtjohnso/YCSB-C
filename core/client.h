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
#include <atomic>
#include <string>

namespace ycsbc {

class Client {
public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {
    workload_.InitKeyBuffer(key);
    workload_.InitPairs(pairs);

    abort_cnt = 0;
  }

  virtual bool DoInsert();
  virtual bool DoTransaction();

  virtual ~Client() { Client::total_abort_cnt += abort_cnt; }

  static std::atomic<unsigned long> total_abort_cnt;

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

  unsigned long abort_cnt;
};

inline bool Client::DoInsert() {
  workload_.NextSequenceKey(key);
  workload_.UpdateValues(pairs);
  int status = -1;
  Transaction *txn = NULL;
  db_.Begin(&txn);
  status = db_.Insert(txn, workload_.NextTable(), key, pairs);
  db_.Commit(&txn);
  return (status == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  Transaction *txn = NULL;

  db_.Begin(&txn);

  if (txn != NULL) {
    txn->ReadyToRecordOperations(workload_.ops_per_transaction());
  }
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

    if (status == DB::kErrorConflict) {
      txn->SetAborted(true);
    }
  }

  bool need_retry = db_.Commit(&txn) == DB::kErrorConflict;

  while (need_retry) {
    ++abort_cnt;

    db_.Begin(&txn);

    txn->SetAborted(false);

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

      if (status == DB::kErrorConflict) {
        txn->SetAborted(true);
        break;
      }
    }

    need_retry = db_.Commit(&txn) == DB::kErrorConflict;
  }

  return true;
}

inline int Client::TransactionRead(Transaction *txn) {
  if (txn != NULL) {
    TransactionOperation &top = txn->GetNextOperation();
    top.op = READ;
    top.table = workload_.NextTable();
    top.key = workload_.NextTransactionKey();

    if (txn->IsAborted()) {
      return DB::kErrorConflict;
    }

    std::vector<DB::KVPair> result;
    if (!workload_.read_all_fields()) {
      std::vector<std::string> fields;
      fields.push_back("field" + workload_.NextFieldName());
      return db_.Read(txn, top.table, top.key, &fields, result);
    } else {
      return db_.Read(txn, top.table, top.key, NULL, result);
    }
  }

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
  if (txn != NULL) {
    TransactionOperation &top = txn->GetNextOperation();
    top.op = READMODIFYWRITE;
    top.table = workload_.NextTable();
    top.key = workload_.NextTransactionKey();

    if (workload_.write_all_fields()) {
      workload_.BuildValues(top.values);
    } else {
      workload_.BuildUpdate(top.values);
    }

    if (txn->IsAborted()) {
      return DB::kErrorConflict;
    }

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
  if (txn != NULL) {
    TransactionOperation &top = txn->GetNextOperation();
    top.op = SCAN;
    top.table = workload_.NextTable();
    top.key = workload_.NextTransactionKey();
    top.len = workload_.NextScanLength();

    if (txn->IsAborted()) {
      return DB::kErrorConflict;
    }

    std::vector<std::vector<DB::KVPair>> result;
    if (!workload_.read_all_fields()) {
      std::vector<std::string> fields;
      fields.push_back("field" + workload_.NextFieldName());
      return db_.Scan(txn, top.table, top.key, top.len, &fields, result);
    } else {
      return db_.Scan(txn, top.table, top.key, top.len, NULL, result);
    }
  }

  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  uint64_t len = workload_.NextScanLength();

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
  if (txn != NULL) {
    TransactionOperation &top = txn->GetNextOperation();
    top.op = UPDATE;
    top.table = workload_.NextTable();
    top.key = workload_.NextTransactionKey();
    if (workload_.write_all_fields()) {
      workload_.BuildValues(top.values);
    } else {
      workload_.BuildUpdate(top.values);
    }

    if (txn->IsAborted()) {
      return DB::kErrorConflict;
    }

    return db_.Update(txn, top.table, top.key, top.values);
  }

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
  if (txn != NULL) {
    TransactionOperation &top = txn->GetNextOperation();
    top.op = INSERT;
    top.table = workload_.NextTable();
    workload_.NextSequenceKey(key);
    top.key = key;
    workload_.BuildValues(top.values);

    if (txn->IsAborted()) {
      return DB::kErrorConflict;
    }

    return db_.Insert(txn, top.table, top.key, top.values);
  }

  const std::string &table = workload_.NextTable();
  workload_.NextSequenceKey(key);
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);

  return db_.Insert(txn, table, key, values);
}

inline int Client::TransactionReadRetry(Transaction *txn,
                                        TransactionOperation &top) {
  assert(txn != NULL);

  if (txn->IsAborted()) {
    return DB::kErrorConflict;
  }

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
  assert(txn != NULL);

  if (txn->IsAborted()) {
    return DB::kErrorConflict;
  }

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
  assert(txn != NULL);

  if (txn->IsAborted()) {
    return DB::kErrorConflict;
  }

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
  assert(txn != NULL);

  if (txn->IsAborted()) {
    return DB::kErrorConflict;
  }

  return db_.Update(txn, top.table, top.key, top.values);
}

inline int Client::TransactionInsertRetry(Transaction *txn,
                                          TransactionOperation &top) {
  assert(txn != NULL);

  if (txn->IsAborted()) {
    return DB::kErrorConflict;
  }

  return db_.Insert(txn, top.table, top.key, top.values);
}

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
