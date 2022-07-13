#include <stdint.h>
#include "latency_table.h"

typedef struct running_times {
   uint64_t earliest_thread_start_time;
   uint64_t last_thread_finish_time;
   uint64_t sum_of_wall_clock_times;
   uint64_t sum_of_cpu_times;
   uint64_t cleanup_time;
} running_times;

typedef struct latency_tables {
   latency_table pos_queries;
   latency_table neg_queries;
   latency_table all_queries;
   latency_table deletes;
   latency_table inserts;
   latency_table updates;
   latency_table scans;
} latency_tables;
