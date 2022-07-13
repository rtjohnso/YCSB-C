#include <assert.h>
#include "reporting.h"
#include "latency_table.h"

void
compute_derived_log_latency_tables(latency_tables *tables)
{
   sum_latency_tables(tables->all_queries,
                      tables->pos_queries,
                      tables->neg_queries);
}

void
compute_phase_latency_tables(ycsb_phase *phase)
{
   uint64_t i;
   for (i = 0; i < phase->nlogs; i++)
      compute_log_latency_tables(&phase->params[i]);

   for (i = 0; i < phase->nlogs; i++) {
      add_latency_table(phase->tables.pos_queries,
                        phase->params[i].tables.pos_queries);
      add_latency_table(phase->tables.neg_queries,
                        phase->params[i].tables.neg_queries);
      add_latency_table(phase->tables.all_queries,
                        phase->params[i].tables.all_queries);
      add_latency_table(phase->tables.deletes, phase->params[i].tables.deletes);
      add_latency_table(phase->tables.inserts, phase->params[i].tables.inserts);
      add_latency_table(phase->tables.updates, phase->params[i].tables.updates);
      add_latency_table(phase->tables.scans, phase->params[i].tables.scans);
   }
}

void
compute_all_phase_latency_tables(ycsb_phase *phases, int num_phases)
{
   uint64_t i;
   for (i = 0; i < num_phases; i++)
      compute_phase_latency_tables(&phases[i]);
}

void
compute_phase_statistics(ycsb_phase *phase)
{
   uint64_t i;

   for (i = 0; i < phase->nlogs; i++) {
      if (i == 0
          || phase->params[i].times.earliest_thread_start_time
                < phase->times.earliest_thread_start_time)
      {
         phase->times.earliest_thread_start_time =
            phase->params[i].times.earliest_thread_start_time;
      }
      if (i == 0
          || phase->params[i].times.last_thread_finish_time
                > phase->times.last_thread_finish_time)
      {
         phase->times.last_thread_finish_time =
            phase->params[i].times.last_thread_finish_time;
      }
      phase->times.sum_of_wall_clock_times +=
         phase->params[i].times.sum_of_wall_clock_times;
      phase->times.sum_of_cpu_times += phase->params[i].times.sum_of_cpu_times;
      phase->total_ops += phase->params[i].total_ops;
   }
}

void
compute_all_phase_statistics(ycsb_phase *phases, int num_phases)
{
   uint64_t i;
   for (i = 0; i < num_phases; i++)
      compute_phase_statistics(&phases[i]);
}

void
compute_all_report_data(ycsb_phase *phases, int num_phases)
{
   compute_all_phase_latency_tables(phases, num_phases);
   compute_all_phase_statistics(phases, num_phases);
}

void
write_log_latency_table(char         *phase_name,
                        uint64_t      lognum,
                        char         *operation_name,
                        latency_table table)
{
   char filename[1024];
   snprintf(filename,
            sizeof(filename),
            "%s.%02lu.%s.latency.df",
            phase_name,
            lognum,
            operation_name);
   write_latency_table(filename, table);
}

void
write_log_latency_cdf(char         *phase_name,
                      uint64_t      lognum,
                      char         *operation_name,
                      latency_table table)
{
   char filename[1024];
   snprintf(filename,
            sizeof(filename),
            "%s.%02lu.%s.latency.cdf",
            phase_name,
            lognum,
            operation_name);
   write_latency_cdf(filename, table);
}

void
write_phase_latency_table(char         *phase_name,
                          char         *operation_name,
                          latency_table table)
{
   char filename[1024];
   snprintf(filename,
            sizeof(filename),
            "%s.%s.latency.df",
            phase_name,
            operation_name);
   write_latency_table(filename, table);
}

void
write_phase_latency_cdf(char         *phase_name,
                        char         *operation_name,
                        latency_table table)
{
   char filename[1024];
   snprintf(filename,
            sizeof(filename),
            "%s.%s.latency.cdf",
            phase_name,
            operation_name);
   write_latency_cdf(filename, table);
}

void
write_phase_latency_tables(ycsb_phase *phase)
{
   uint64_t i;
   for (i = 0; i < phase->nlogs; i++) {
      write_log_latency_table(
         phase->name, i, "pos_query", phase->params[i].tables.pos_queries);
      write_log_latency_table(
         phase->name, i, "neg_query", phase->params[i].tables.neg_queries);
      write_log_latency_table(
         phase->name, i, "all_query", phase->params[i].tables.all_queries);
      write_log_latency_table(
         phase->name, i, "delete", phase->params[i].tables.deletes);
      write_log_latency_table(
         phase->name, i, "insert", phase->params[i].tables.inserts);
      write_log_latency_table(
         phase->name, i, "update", phase->params[i].tables.updates);
      write_log_latency_table(
         phase->name, i, "scan", phase->params[i].tables.scans);
   }

   for (i = 0; i < phase->nlogs; i++) {
      write_log_latency_cdf(
         phase->name, i, "pos_query", phase->params[i].tables.pos_queries);
      write_log_latency_cdf(
         phase->name, i, "neg_query", phase->params[i].tables.neg_queries);
      write_log_latency_cdf(
         phase->name, i, "all_query", phase->params[i].tables.all_queries);
      write_log_latency_cdf(
         phase->name, i, "delete", phase->params[i].tables.deletes);
      write_log_latency_cdf(
         phase->name, i, "insert", phase->params[i].tables.inserts);
      write_log_latency_cdf(
         phase->name, i, "update", phase->params[i].tables.updates);
      write_log_latency_cdf(
         phase->name, i, "scan", phase->params[i].tables.scans);
   }

   write_phase_latency_table(
      phase->name, "pos_query", phase->tables.pos_queries);
   write_phase_latency_table(
      phase->name, "neg_query", phase->tables.neg_queries);
   write_phase_latency_table(
      phase->name, "all_query", phase->tables.all_queries);
   write_phase_latency_table(phase->name, "delete", phase->tables.deletes);
   write_phase_latency_table(phase->name, "insert", phase->tables.inserts);
   write_phase_latency_table(phase->name, "update", phase->tables.updates);
   write_phase_latency_table(phase->name, "scan", phase->tables.scans);

   write_phase_latency_cdf(phase->name, "pos_query", phase->tables.pos_queries);
   write_phase_latency_cdf(phase->name, "neg_query", phase->tables.neg_queries);
   write_phase_latency_cdf(phase->name, "all_query", phase->tables.all_queries);
   write_phase_latency_cdf(phase->name, "delete", phase->tables.deletes);
   write_phase_latency_cdf(phase->name, "insert", phase->tables.inserts);
   write_phase_latency_cdf(phase->name, "update", phase->tables.updates);
   write_phase_latency_cdf(phase->name, "scan", phase->tables.scans);
}

void
print_operation_statistics(platform_log_handle output,
                           char               *operation_name,
                           latency_table       table)
{
   platform_handle_log(
      output, "%s_count: %lu\n", operation_name, num_latencies(table));
   platform_handle_log(
      output, "%s_total_latency: %lu\n", operation_name, total_latency(table));
   platform_handle_log(
      output, "%s_min_latency: %lu\n", operation_name, min_latency(table));
   platform_handle_log(
      output, "%s_mean_latency: %lf\n", operation_name, mean_latency(table));
   platform_handle_log(output,
                       "%s_median_latency: %lu\n",
                       operation_name,
                       latency_percentile(table, 50.0));
   platform_handle_log(output,
                       "%s_99.0_latency: %lu\n",
                       operation_name,
                       latency_percentile(table, 99));
   platform_handle_log(output,
                       "%s_99.5_latency: %lu\n",
                       operation_name,
                       latency_percentile(table, 99.5));
   platform_handle_log(output,
                       "%s_99.9_latency: %lu\n",
                       operation_name,
                       latency_percentile(table, 99.9));
   platform_handle_log(
      output, "%s_max_latency: %lu\n", operation_name, max_latency(table));

   platform_handle_log(
      output, "%s_000_latency: %lu\n", operation_name, min_latency(table));
   int i;
   for (i = 5; i < 100; i += 5) {
      platform_handle_log(output,
                          "%s_%03d_latency: %lu\n",
                          operation_name,
                          i,
                          latency_percentile(table, i));
   }
   platform_handle_log(
      output, "%s_100_latency: %lu\n", operation_name, max_latency(table));
}

void
print_statistics_file(platform_log_handle output,
                      uint64_t            total_ops,
                      running_times      *times,
                      latency_tables     *tables)
{
   uint64_t wall_clock_time =
      times->last_thread_finish_time - times->earliest_thread_start_time;
   platform_handle_log(output, "total_operations: %lu\n", total_ops);
   platform_handle_log(output, "wall_clock_time: %lu\n", wall_clock_time);
   platform_handle_log(
      output, "sum_of_wall_clock_times: %lu\n", times->sum_of_wall_clock_times);
   platform_handle_log(
      output, "sum_of_cpu_times: %ld\n", times->sum_of_cpu_times);
   platform_handle_log(
      output,
      "mean_overall_latency: %f\n",
      total_ops ? 1.0 * times->sum_of_wall_clock_times / total_ops : 0);
   platform_handle_log(
      output,
      "mean_overall_throughput: %f\n",
      wall_clock_time ? 1000000000.0 * total_ops / wall_clock_time : 0);

   print_operation_statistics(output, "pos_query", tables->pos_queries);
   print_operation_statistics(output, "neg_query", tables->neg_queries);
   print_operation_statistics(output, "all_query", tables->all_queries);
   print_operation_statistics(output, "delete", tables->deletes);
   print_operation_statistics(output, "insert", tables->inserts);
   print_operation_statistics(output, "update", tables->updates);
   print_operation_statistics(output, "scan", tables->scans);
}

void
write_log_statistics_file(char            *phase_name,
                          uint64_t         lognum,
                          ycsb_log_params *params)
{
   char filename[1024];
   snprintf(
      filename, sizeof(filename), "%s.%02lu.statistics", phase_name, lognum);
   FILE *output = fopen(filename, "w");
   assert(output != NULL);

   print_statistics_file(
      output, params->total_ops, &params->times, &params->tables);

   fclose(output);
}

void
write_phase_statistics_files(ycsb_phase *phase)
{
   uint64_t i;
   for (i = 0; i < phase->nlogs; i++)
      write_log_statistics_file(phase->name, i, &phase->params[i]);

   char filename[1024];
   snprintf(filename, sizeof(filename), "%s.statistics", phase->name);
   FILE *output = fopen(filename, "w");
   assert(output != NULL);

   print_statistics_file(
      output, phase->total_ops, &phase->times, &phase->tables);

   fclose(output);
}

void
write_all_reports(ycsb_phase *phases, int num_phases)
{
   uint64_t i;
   for (i = 0; i < num_phases; i++) {
      write_phase_latency_tables(&phases[i]);
      write_phase_statistics_files(&phases[i]);
   }
}
