//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/lock_stl_db.h"
#include "db/redis_db.h"
#include "db/tbb_rand_db.h"
#include "db/tbb_scan_db.h"
#include "db/splinter_db.h"
#include "db/transactional_splinter_db.h"
#include "db/rocks_db.h"
#include "db/transaction_rocks_db.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props, bool preloaded) {
  if (props["dbname"] == "basic") {
    return new BasicDB(props);
  } else if (props["dbname"] == "lock_stl") {
    assert(!preloaded);
    return new LockStlDB;
  } else if (props["dbname"] == "redis") {
    int port = stoi(props["port"]);
    int slaves = stoi(props["slaves"]);
    return new RedisDB(props["host"].c_str(), port, slaves);
  } else if (props["dbname"] == "rocksdb") {
    return new RocksDB(props, preloaded);
  } else if (props["dbname"] == "transaction_rocksdb") {
    return new TransactionRocksDB(props, preloaded);
  } else if (props["dbname"] == "optimistic_transaction_rocksdb") {
    return new OptimisticTransactionRocksDB(props, preloaded);
  } else if (props["dbname"] == "splinterdb") {
    return new SplinterDB(props, preloaded);
  } else if (props["dbname"] == "transactional_splinterdb") {
    return new TransactionalSplinterDB(props, preloaded);
  } else if (props["dbname"] == "tbb_rand") {
    assert(!preloaded);
    return new TbbRandDB;
  } else if (props["dbname"] == "tbb_scan") {
    assert(!preloaded);
    return new TbbScanDB;
  } else return NULL;
}

