//
//  batched_counter_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BATCHED_COUNTER_GENERATOR_H_
#define YCSB_C_BATCHED_COUNTER_GENERATOR_H_

#include "generator.h"

#include <cstdint>
#include <atomic>
#include <mutex>
#include <set>

namespace ycsbc {

typedef std::pair<uint64_t, uint64_t> batch;

class BatchedCounterGenerator : public Generator<uint64_t> {
 public:
  BatchedCounterGenerator(uint64_t start, uint64_t batch_size) : batch_size_(batch_size), mutex_(), available_(), outstanding_(), next_batch_start_(start) {}

  uint64_t Next() { assert(false); }

  batch NextBatch() {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t batch_start;
    uint64_t batch_size;
    if (available_.size()) {
      auto next_batch = available_.lower_bound(0);
      batch_start = next_batch->first;
      batch_size = next_batch->second;
      available_.erase(batch_start);
    } else {
      batch_start = next_batch_start_;
      batch_size = batch_size_;
      next_batch_start_ += batch_size;
    }

    outstanding_.insert(batch_start);
    return batch(batch_start, batch_size);
  }

  uint64_t Last() {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t last = next_batch_start_;

    if (outstanding_.size()) {
      last = std::min(last, *outstanding_.lower_bound(0));
    }

    if (available_.size()) {
      last = std::min(last, available_.lower_bound(0)->first);
    }

    return last;
  }

  uint64_t Set(uint64_t start) { assert(false); }

  void MarkPartial(batch partial, uint64_t remaining) {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(outstanding_.count(partial.first) == 1);
    outstanding_.erase(partial.first);
    if (remaining) {
      available_[partial.first + partial.second - remaining] = remaining;
    }
  }

  void MarkCompleted(batch completed) {
    MarkPartial(completed, 0);
  }

 private:
  uint64_t batch_size_;
  std::mutex mutex_;
  std::map<uint64_t, uint64_t> available_;
  std::set<uint64_t> outstanding_;
  uint64_t next_batch_start_;
};

} // ycsbc

#endif // YCSB_C_COUNTER_GENERATOR_H_
