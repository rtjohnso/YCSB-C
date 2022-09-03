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
  Transaction() { next_op = 0; };

  void SetTransactionOperationsSize(unsigned long size) { ops.resize(size); };

  unsigned long GetTransactionOperationsSize() { return ops.size(); };

  TransactionOperation &GetNextOperation() { return ops[next_op++]; }

  TransactionOperation &GetOperation(unsigned long i) { return ops[i]; }

private:
  std::vector<TransactionOperation> ops;
  unsigned long next_op;
};

} // namespace ycsbc

#endif // YCSB_C_TRANSACTION_H_
