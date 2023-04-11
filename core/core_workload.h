//
//  core_workload.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <vector>
#include <string>
#include "db.h"
#include "properties.h"
#include "generator.h"
#include "discrete_generator.h"
#include "counter_generator.h"
#include "batched_counter_generator.h"
#include "utils.h"

namespace ycsbc {

enum Operation {
  INSERT,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE
};

class CoreWorkload {
 public:
  /// 
  /// The name of the database table to run queries against.
  ///
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;
  
  /// 
  /// The name of the property for the number of fields in a record.
  ///
  static const std::string FIELD_COUNT_PROPERTY;
  static const std::string FIELD_COUNT_DEFAULT;
  
  /// 
  /// The name of the property for the field length distribution.
  /// Options are "uniform", "zipfian" (favoring short records), and "constant".
  ///
  static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;
  
  /// 
  /// The name of the property for the length of a field in bytes.
  ///
  static const std::string FIELD_LENGTH_PROPERTY;
  static const std::string FIELD_LENGTH_DEFAULT;
  
  /// 
  /// The name of the property for deciding whether to read one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string READ_ALL_FIELDS_PROPERTY;
  static const std::string READ_ALL_FIELDS_DEFAULT;

  /// 
  /// The name of the property for deciding whether to write one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string WRITE_ALL_FIELDS_PROPERTY;
  static const std::string WRITE_ALL_FIELDS_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of read transactions.
  ///
  static const std::string READ_PROPORTION_PROPERTY;
  static const std::string READ_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of update transactions.
  ///
  static const std::string UPDATE_PROPORTION_PROPERTY;
  static const std::string UPDATE_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of insert transactions.
  ///
  static const std::string INSERT_PROPORTION_PROPERTY;
  static const std::string INSERT_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of scan transactions.
  ///
  static const std::string SCAN_PROPORTION_PROPERTY;
  static const std::string SCAN_PROPORTION_DEFAULT;
  
  ///
  /// The name of the property for the proportion of
  /// read-modify-write transactions.
  ///
  static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
  static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string REQUEST_DISTRIBUTION_DEFAULT;
  
  ///
  /// The name of the property for adding zero padding to record numbers in order to match 
  /// string sort order. Controls the number of 0s to left pad with.
  ///
  static const std::string ZERO_PADDING_PROPERTY;
  static const std::string ZERO_PADDING_DEFAULT;

  /// 
  /// The name of the property for the max scan length (number of records).
  ///
  static const std::string MAX_SCAN_LENGTH_PROPERTY;
  static const std::string MAX_SCAN_LENGTH_DEFAULT;
  
  /// 
  /// The name of the property for the scan length distribution.
  /// Options are "uniform" and "zipfian" (favoring short scans).
  ///
  static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

  /// 
  /// The name of the property for the order to insert records.
  /// Options are "ordered" or "hashed".
  ///
  static const std::string INSERT_ORDER_PROPERTY;
  static const std::string INSERT_ORDER_DEFAULT;

  static const std::string INSERT_START_PROPERTY;
  static const std::string INSERT_START_DEFAULT;
  
  static const std::string RECORD_COUNT_PROPERTY;
  static const std::string OPERATION_COUNT_PROPERTY;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void InitLoadWorkload(const utils::Properties &p, unsigned int nthreads, unsigned int this_thread, BatchedCounterGenerator *key_generator);
  virtual void InitRunWorkload(const utils::Properties &p, unsigned int nthreads, unsigned int this_thread);

  void InitKeyBuffer(std::string &buffer);

  virtual void InitPairs(std::vector<ycsbc::DB::KVPair> &values);
  virtual void BuildValues(std::vector<ycsbc::DB::KVPair> &values);
  virtual void UpdateValues(std::vector<ycsbc::DB::KVPair> &values);
  virtual void BuildUpdate(std::vector<ycsbc::DB::KVPair> &update);
  
  virtual std::string NextTable() { return table_name_; }
  virtual bool NextSequenceKey(std::string &buffer, uint64_t limit); /// Used for loading data
  virtual std::string NextTransactionKey(); /// Used for transactions
  virtual Operation NextOperation() { return op_chooser_.Next(); }
  virtual std::string NextFieldName();
  virtual size_t NextScanLength() { return scan_len_chooser_->Next(); }
  
  bool read_all_fields() const { return read_all_fields_; }
  bool write_all_fields() const { return write_all_fields_; }
  uint64_t record_count() const { return record_count_; }

  CoreWorkload() :
      generator_(),
      field_count_(0),
      read_all_fields_(false),
      write_all_fields_(false),
      field_len_generator_(NULL),
      key_generator_(NULL),
      key_batch_(),
      batch_remaining_(0),
      key_generator_batch_(0),
      op_chooser_(generator_),
      key_chooser_(NULL),
      field_chooser_(NULL),
      scan_len_chooser_(NULL),
      insert_key_sequence_(3),
      ordered_inserts_(true),
      record_count_(0),
      uniform_letter_dist_('a', 'z')
  {}
  
  virtual ~CoreWorkload() {
    if (field_len_generator_) delete field_len_generator_;
    if (key_chooser_) delete key_chooser_;
    if (field_chooser_) delete field_chooser_;
    if (scan_len_chooser_) delete scan_len_chooser_;
  }
  
 protected:
  Generator<uint64_t> *GetFieldLenGenerator(const utils::Properties &p);
  std::string BuildKeyName(uint64_t key_num);
  void UpdateKeyName(uint64_t key_num, std::string &buffer);

  std::default_random_engine generator_;
  std::string table_name_;
  int field_count_;
  bool read_all_fields_;
  bool write_all_fields_;
  Generator<uint64_t> *field_len_generator_;
  BatchedCounterGenerator *key_generator_;
  batch key_batch_;
  uint64_t batch_remaining_;
  CounterGenerator key_generator_batch_;
  DiscreteGenerator<Operation> op_chooser_;
  Generator<uint64_t> *key_chooser_;
  Generator<uint64_t> *field_chooser_;
  Generator<uint64_t> *scan_len_chooser_;
  CounterGenerator insert_key_sequence_;
  bool ordered_inserts_;
  size_t record_count_;
  int zero_padding_;

  std::uniform_int_distribution<char> uniform_letter_dist_;
};

inline void CoreWorkload::InitKeyBuffer(std::string &buffer) {
  buffer = BuildKeyName(0);
}

  inline bool CoreWorkload::NextSequenceKey(std::string &buffer, uint64_t limit) {
  if (batch_remaining_ == 0) {
    key_generator_->MarkCompleted(key_batch_);
    key_batch_ = key_generator_->NextBatch();
    batch_remaining_ = key_batch_.second;
    key_generator_batch_.Set(key_batch_.first);
  }
  if (limit <= key_generator_batch_.Last() + 1) {
    return false;
  }
  uint64_t key_num = key_generator_batch_.Next();
  batch_remaining_--;
  UpdateKeyName(key_num, buffer);
  return true;
}

inline std::string CoreWorkload::NextTransactionKey() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > key_generator_->Last());
  return BuildKeyName(key_num);
}

inline std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
  if (!ordered_inserts_) {
    key_num = utils::Hash(key_num);
  }
  std::string key_num_str = std::to_string(key_num);
  int zeros = zero_padding_ - key_num_str.length();
  zeros = std::max(0, zeros);
  return std::string("user").append(zeros, '0').append(key_num_str);
}

inline void CoreWorkload::UpdateKeyName(uint64_t key_num, std::string &buffer) {
  if (!ordered_inserts_) {
    key_num = utils::Hash(key_num);
  }
  char internal_buffer[21];
  snprintf(internal_buffer, sizeof(internal_buffer), "%020lu", key_num);
  int len = buffer.size();
  assert(24 <= len);
  for (unsigned int i = 0; i < sizeof(internal_buffer) - 1; i++) {
    buffer[len - 20 + i] = internal_buffer[i];
  }
}

inline std::string CoreWorkload::NextFieldName() {
  return std::string("field").append(std::to_string(field_chooser_->Next()));
}
  
} // ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
