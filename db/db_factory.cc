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
#include "db/classic_splinterdb.h"
#include "db/rocks_db.h"

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
  } else if (props["dbname"] == "classic_splinterdb") {
    return new ClassicSplinterDB(props, preloaded);
  } else if (props["dbname"] == "tbb_rand") {
    assert(!preloaded);
    return new TbbRandDB;
  } else if (props["dbname"] == "tbb_scan") {
    assert(!preloaded);
    return new TbbScanDB;
  } else return NULL;
}

