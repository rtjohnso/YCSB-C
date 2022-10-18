//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "core/client.h"
#include "core/core_workload.h"
#include "core/timer.h"
#include "core/utils.h"
#include "db/db_factory.h"
#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

typedef struct WorkloadProperties {
  string filename;
  bool preloaded;
  utils::Properties props;
} WorkloadProperties;

std::map<string, string> default_props = {
    {"threadcount", "1"},
    {"dbname", "basic"},
    {"progress", "none"},

    //
    // Basicdb config defaults
    //
    {"basicdb.verbose", "0"},

    //
    // splinterdb config defaults
    //
    {"splinterdb.filename", "splinterdb.db"},
    {"splinterdb.cache_size_mb", "4096"},
    {"splinterdb.disk_size_gb", "128"},

    {"splinterdb.max_key_size", "24"},
    {"splinterdb.use_log", "1"},

    // All these options use splinterdb's internal defaults
    {"splinterdb.page_size", "0"},
    {"splinterdb.extent_size", "0"},
    {"splinterdb.io_flags", "0"},
    {"splinterdb.io_perms", "0"},
    {"splinterdb.io_async_queue_depth", "0"},
    {"splinterdb.cache_use_stats", "0"},
    {"splinterdb.cache_logfile", "0"},
    {"splinterdb.btree_rough_count_height", "0"},
    {"splinterdb.filter_remainder_size", "0"},
    {"splinterdb.filter_index_size", "0"},
    {"splinterdb.memtable_capacity", "0"},
    {"splinterdb.fanout", "0"},
    {"splinterdb.max_branches_per_node", "0"},
    {"splinterdb.use_stats", "0"},
    {"splinterdb.reclaim_threshold", "0"},
    {"splinterdb.isolation_level", "1"},

    {"rocksdb.database_filename", "rocksdb.db"},
//    {"rocksdb.isolation_level", "3"},
};

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[], utils::Properties &props,
                      WorkloadProperties &load_workload,
                      vector<WorkloadProperties> &run_workloads);

typedef enum progress_mode {
  no_progress,
  hash_progress,
  percent_progress,
} progress_mode;

static inline void ReportProgress(progress_mode pmode, uint64_t total_ops,
                                  volatile uint64_t *global_op_counter,
                                  uint64_t stepsize,
                                  volatile uint64_t *last_printed) {
  uint64_t old_counter = __sync_fetch_and_add(global_op_counter, stepsize);
  uint64_t new_counter = old_counter + stepsize;
  if (100 * old_counter / total_ops != 100 * new_counter / total_ops) {
    if (pmode == hash_progress) {
      cout << "#" << flush;
    } else if (pmode == percent_progress) {
      uint64_t my_percent = 100 * new_counter / total_ops;
      while (*last_printed + 1 != my_percent) {
      }
      cout << 100 * new_counter / total_ops << "%\r" << flush;
      *last_printed = my_percent;
    }
  }
}

static inline void ProgressUpdate(progress_mode pmode, uint64_t total_ops,
                                  volatile uint64_t *global_op_counter,
                                  uint64_t i, volatile uint64_t *last_printed) {
  uint64_t sync_interval = 0 < total_ops / 1000 ? total_ops / 1000 : 1;
  if ((i % sync_interval) == 0) {
    ReportProgress(pmode, total_ops, global_op_counter, sync_interval,
                   last_printed);
  }
}

static inline void ProgressFinish(progress_mode pmode, uint64_t total_ops,
                                  volatile uint64_t *global_op_counter,
                                  uint64_t i, volatile uint64_t *last_printed) {
  uint64_t sync_interval = 0 < total_ops / 1000 ? total_ops / 1000 : 1;
  ReportProgress(pmode, total_ops, global_op_counter, i % sync_interval,
                 last_printed);
}

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl,
                   const uint64_t num_ops, bool is_loading, progress_mode pmode,
                   uint64_t total_ops, volatile uint64_t *global_op_counter,
                   volatile uint64_t *last_printed) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  uint64_t oks = 0;

  if (is_loading) {
    for (uint64_t i = 0; i < num_ops; ++i) {
      oks += client.DoInsert();
      ProgressUpdate(pmode, total_ops, global_op_counter, i, last_printed);
    }
  } else {
    for (uint64_t i = 0; i < num_ops; ++i) {
      oks += client.DoTransaction();
      ProgressUpdate(pmode, total_ops, global_op_counter, i, last_printed);
    }
  }
  ProgressFinish(pmode, total_ops, global_op_counter, num_ops, last_printed);
  db->Close();
  return oks;
}

std::atomic<unsigned long> ycsbc::Client::total_abort_cnt = 0;

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  WorkloadProperties load_workload;
  vector<WorkloadProperties> run_workloads;
  ParseCommandLine(argc, argv, props, load_workload, run_workloads);

  const unsigned int num_threads = stoi(props.GetProperty("threadcount", "1"));
  progress_mode pmode = no_progress;
  if (props.GetProperty("progress", "none") == "hash") {
    pmode = hash_progress;
  } else if (props.GetProperty("progress", "none") == "percent") {
    pmode = percent_progress;
  }
  vector<future<int>> actual_ops;
  uint64_t record_count;
  uint64_t total_ops;
  uint64_t sum;
  utils::Timer<double> timer;

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props, load_workload.preloaded);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  record_count =
      stoi(load_workload.props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  uint64_t batch_size = sqrt(record_count);
  if (record_count / batch_size < num_threads)
    batch_size = record_count / num_threads;
  if (batch_size < 1)
    batch_size = 1;

  ycsbc::BatchedCounterGenerator key_generator(
      load_workload.preloaded ? record_count : 0, batch_size);
  ycsbc::CoreWorkload wls[num_threads];
  for (unsigned int i = 0; i < num_threads; ++i) {
    wls[i].InitLoadWorkload(load_workload.props, num_threads, i,
                            &key_generator);
  }

  // Perform the Load phase
  if (!load_workload.preloaded) {
    timer.Start();
    {
      cerr << "# Loading records:\t" << record_count << endl;
      uint64_t load_progress = 0;
      uint64_t last_printed = 0;
      for (unsigned int i = 0; i < num_threads; ++i) {
        uint64_t start_op = (record_count * i) / num_threads;
        uint64_t end_op = (record_count * (i + 1)) / num_threads;
        actual_ops.emplace_back(
            async(launch::async, DelegateClient, db, &wls[i], end_op - start_op,
                  true, pmode, record_count, &load_progress, &last_printed));
      }
      assert(actual_ops.size() == num_threads);
      sum = 0;
      for (auto &n : actual_ops) {
        assert(n.valid());
        sum += n.get();
      }
      if (pmode != no_progress) {
        cout << "\n";
      }
    }
    double load_duration = timer.End();
    cerr << "# Load throughput (KTPS)" << endl;
    cerr << props["dbname"] << '\t' << load_workload.filename << '\t'
         << num_threads << '\t';
    cerr << sum / load_duration / 1000 << endl;
  }

  // Perform any Run phases
  for (unsigned int i = 0; i < run_workloads.size(); i++) {
    auto workload = run_workloads[i];
    for (unsigned int i = 0; i < num_threads; ++i) {
      wls[i].InitRunWorkload(workload.props, num_threads, i);
    }
    actual_ops.clear();
    total_ops =
        stoi(workload.props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    uint64_t ops_per_transactions =
      stoi(workload.props.GetProperty(ycsbc::CoreWorkload::OPS_PER_TRANSACTION_PROPERTY,
				      ycsbc::CoreWorkload::OPS_PER_TRANSACTION_DEFAULT));
    timer.Start();
    {
      cerr << "# Transaction count:\t" << total_ops << endl;
      uint64_t run_progress = 0;
      uint64_t last_printed = 0;
      for (unsigned int i = 0; i < num_threads; ++i) {
        uint64_t start_op = (total_ops * i) / num_threads;
        uint64_t end_op = (total_ops * (i + 1)) / num_threads;
        uint64_t num_transactions = (end_op - start_op) / ops_per_transactions;
        actual_ops.emplace_back(async(launch::async, DelegateClient, db,
                                      &wls[i], num_transactions, false, pmode,
                                      total_ops, &run_progress, &last_printed));
      }
      assert(actual_ops.size() == num_threads);
      sum = 0;
      for (auto &n : actual_ops) {
        assert(n.valid());
        sum += n.get() * ops_per_transactions;
      }
      if (pmode != no_progress) {
        cout << "\n";
      }
    }
    double run_duration = timer.End();

    cerr << "# Transaction throughput (KTPS)" << endl;
    cerr << props["dbname"] << '\t' << workload.filename << '\t' << num_threads
         << '\t';
    cerr << sum / run_duration / 1000 << endl;

    cerr << "# Abort count:\t" << ycsbc::Client::total_abort_cnt << '\n';
  }

  delete db;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props,
                      WorkloadProperties &load_workload,
                      vector<WorkloadProperties> &run_workloads) {
  bool saw_load_workload = false;
  WorkloadProperties *last_workload = NULL;
  int argindex = 1;

  for (auto const &[key, val] : default_props) {
    props.SetProperty(key, val);
  }

  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-progress") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("progress", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-W") == 0 ||
               strcmp(argv[argindex], "-P") == 0 ||
               strcmp(argv[argindex], "-L") == 0) {
      WorkloadProperties workload;
      workload.preloaded = strcmp(argv[argindex], "-P") == 0;
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      workload.filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        workload.props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
      if (strcmp(argv[argindex - 2], "-W") == 0) {
        run_workloads.push_back(workload);
        last_workload = &run_workloads[run_workloads.size() - 1];
      } else if (saw_load_workload) {
        UsageMessage(argv[0]);
        exit(0);
      } else {
        saw_load_workload = true;
        load_workload = workload;
        last_workload = &load_workload;
      }
    } else if (strcmp(argv[argindex], "-p") == 0 ||
               strcmp(argv[argindex], "-w") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      std::string propkey = argv[argindex];
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      std::string propval = argv[argindex];
      if (strcmp(argv[argindex - 2], "-w") == 0) {
        if (last_workload) {
          last_workload->props.SetProperty(propkey, propval);
        } else {
          UsageMessage(argv[0]);
          exit(0);
        }
      } else {
        props.SetProperty(propkey, propval);
      }
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc || !saw_load_workload) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]"
       << "-L <load-workload.spec> [-W run-workload.spec] ..." << endl;
  cout << "       Perform the given Load workload, then each Run workload"
       << endl;
  cout << "Usage: " << command << " [options]"
       << "-P <load-workload.spec> [-W run-workload.spec] ... " << endl;
  cout << "       Perform each given Run workload on a database that has been "
          "preloaded with the given Load workload"
       << endl;
  cout << "Options:" << endl;
  cout << "  -threads <n>: execute using <n> threads (default: "
       << default_props["threadcount"] << ")" << endl;
  cout << "  -db <dbname>: specify the name of the DB to use (default: "
       << default_props["dbname"] << ")" << endl;
  cout
      << "  -L <file>: Initialize the database with the specified Load workload"
      << endl;
  cout << "  -P <file>: Indicates that the database has been preloaded with "
          "the specified Load workload"
       << endl;
  cout << "  -W <file>: Perform the Run workload specified in <file>" << endl;
  cout << "  -p <prop> <val>: set property <prop> to value <val>" << endl;
  cout << "  -w <prop> <val>: set a property in the previously specified "
          "workload"
       << endl;
  cout << "Exactly one Load workload is allowed, but multiple Run workloads "
          "may be given.."
       << endl;
  cout << "Run workloads will be executed in the order given on the command "
          "line."
       << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}
