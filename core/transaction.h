#ifndef YCSB_C_TRANSACTION_H_
#define YCSB_C_TRANSACTION_H_

#include <vector>

#include "core_workload.h"
#include "db.h"

namespace ycsbc {

struct TransactionOperation {
  enum Operation op;
  std::string table;
  std::string key;
  int len;
  std::vector<DB::KVPair> values;
};

class Transaction {
public:
  Transaction() : next_op(0), is_aborted(false){};

  virtual ~Transaction(){};

  void ReadyToRecordOperations(unsigned long size) {
    ops.resize(size);
    next_op = 0;
  };

  unsigned long GetTransactionOperationsSize() { return ops.size(); };

  TransactionOperation &GetNextOperation() { return ops[next_op++]; }

  TransactionOperation &GetOperation(unsigned long i) { return ops[i]; }

  void SetAborted(bool aborted) { is_aborted = aborted; };

  bool IsAborted() { return is_aborted; };

protected:
  std::vector<TransactionOperation> ops;
  unsigned long next_op;

  bool is_aborted;
};

} // namespace ycsbc

#endif // YCSB_C_TRANSACTION_H_
