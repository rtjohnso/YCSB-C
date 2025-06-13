//
//  discrete_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/6/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DISCRETE_GENERATOR_H_
#define YCSB_C_DISCRETE_GENERATOR_H_

#include "generator.h"

#include <cassert>
#include <vector>
#include <random>
#include "utils.h"

// Scale factor for the discrete generator, used to convert weights to integers.
//   higher scale factor allows for more precision in weights
#define SCALE 1000000000  // 1'000'000'000

namespace ycsbc {

template <typename Value>
class DiscreteGenerator : public Generator<Value> {
 public:
  DiscreteGenerator(std::default_random_engine &gen) : generator_(gen), dist_(0, SCALE - 1), sum_(0) { }
  void AddValue(Value value, double weight);
  void Reset();
  void UpdateGenerator();

  Value Next();
  Value Last() { return last_; }

 private:
  std::default_random_engine &generator_;
  std::uniform_int_distribution<uint32_t> dist_;
  std::vector<std::pair<Value, uint32_t>> values_;
  uint32_t sum_;
  Value last_;
};

template <typename Value>
inline void DiscreteGenerator<Value>::AddValue(Value value, double weight) {
  if (values_.empty()) {
    last_ = value;
  }

  uint32_t iw = static_cast<uint32_t>(std::lround(weight * SCALE));
  assert(iw > 0);
  values_.push_back(std::make_pair(value, iw));
  sum_ += iw;
}

template <typename Value>
inline void DiscreteGenerator<Value>::Reset() {
  values_.clear();
  sum_ = 0;
  last_ = Value();
}

// Update the distribution range based on the sum of weights
//   should be called after adding all values
template <typename Value>
inline void DiscreteGenerator<Value>::UpdateGenerator() {
  assert(sum_ > 0);
  dist_ = std::uniform_int_distribution<uint32_t>(0, sum_ - 1);
}

template <typename Value>
inline Value DiscreteGenerator<Value>::Next() {
  uint32_t chooser = dist_(generator_);

  for (auto p = values_.cbegin(); p != values_.cend(); ++p) {
    if (chooser < p->second) {
      return last_ = p->first;
    }
    chooser -= p->second;
  }
  
  assert(false);
  return last_;
}

} // ycsbc

#endif // YCSB_C_DISCRETE_GENERATOR_H_
