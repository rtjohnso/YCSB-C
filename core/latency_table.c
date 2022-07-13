#include <stdio.h>
#include <assert.h>
#include "latency_table.h"


/* This is the fastest way I can come up
 * with to do a log-scale-esque transform. */
void
record_latency(latency_table table, uint64_t latency)
{
   uint64_t rough_log2 = 64 - __builtin_clzll(latency);
   uint64_t exponent;
   uint64_t mantissa;
   if (LATENCY_MANTISSA_BITS + 1 < rough_log2) {
      exponent = rough_log2 - LATENCY_MANTISSA_BITS;
      mantissa = latency >> (exponent);
   } else if (LATENCY_MANTISSA_BITS + 1 == rough_log2) {
      exponent = 1;
      mantissa = latency - LATENCY_MANTISSA_LIMIT;
   } else {
      exponent = 0;
      mantissa = latency;
   }
   assert(exponent < LATENCY_EXPONENT_LIMIT);
   assert(mantissa < LATENCY_MANTISSA_LIMIT);
   table[exponent][mantissa]++;
}

/*
 * dest = table1 + table2
 * dest may be physically equal to table1 or table2 or both.
 */
void
sum_latency_tables(latency_table dest,
                   latency_table table1,
                   latency_table table2)
{
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++) {
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++) {
         dest[exponent][mantissa] =
            table1[exponent][mantissa] + table2[exponent][mantissa];
      }
   }
}

/* Thread-safe code for folding a table into dest. */
void
add_latency_table(latency_table dest, latency_table table)
{
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++) {
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++) {
         __sync_fetch_and_add(&dest[exponent][mantissa],
                              table[exponent][mantissa]);
      }
   }
}

uint64_t
num_latencies(latency_table table)
{
   uint64_t count = 0;
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         count += table[exponent][mantissa];
   return count;
}

uint64_t
compute_latency(uint64_t exponent, uint64_t mantissa)
{
   if (exponent == 0)
      return mantissa;
   else if (exponent == 1)
      return mantissa + LATENCY_MANTISSA_LIMIT;
   else
      return (mantissa + LATENCY_MANTISSA_LIMIT) << (exponent - 1);
}

uint64_t
total_latency(latency_table table)
{
   uint64_t total = 0;
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         total +=
            table[exponent][mantissa] * compute_latency(exponent, mantissa);
   return total;
}

double
mean_latency(latency_table table)
{
   uint64_t nl = num_latencies(table);
   if (nl)
      return 1.0 * total_latency(table) / nl;
   else
      return 0.0;
}

uint64_t
min_latency(latency_table table)
{
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         if (table[exponent][mantissa])
            return compute_latency(exponent, mantissa);
   return 0;
}

uint64_t
max_latency(latency_table table)
{
   int64_t exponent;
   int64_t mantissa;

   for (exponent = LATENCY_EXPONENT_LIMIT - 1; exponent >= 0; exponent--)
      for (mantissa = LATENCY_MANTISSA_LIMIT - 1; mantissa >= 0; mantissa--)
         if (table[exponent][mantissa])
            return compute_latency(exponent, mantissa);
   return 0;
}

uint64_t
ith_latency(latency_table table, uint64_t rank)
{
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         if (rank < table[exponent][mantissa]) {
            return compute_latency(exponent, mantissa);
         } else {
            rank -= table[exponent][mantissa];
         }
   return max_latency(table);
}

uint64_t
latency_percentile(latency_table table, float percent)
{
   return ith_latency(table, (percent / 100) * num_latencies(table));
}

void
print_latency_table(FILE *stream, latency_table table)
{
   uint64_t exponent;
   uint64_t mantissa;
   bool     started = 0;
   uint64_t max     = max_latency(table);

   fprintf(stream, "latency count\n");
   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         if (started || table[exponent][mantissa]) {
           fprintf(stream, "%20lu %20lu\n",
                   compute_latency(exponent, mantissa),
                   table[exponent][mantissa]);
            started = 1;
            if (max == compute_latency(exponent, mantissa))
               return;
         }
}

void
write_latency_table(char *filename, latency_table table)
{
   FILE *stream = fopen(filename, "w");
   assert(stream != NULL);
   print_latency_table(stream, table);
   fclose(stream);
}

void
print_latency_cdf(FILE *stream, latency_table table)
{
   uint64_t count_so_far = 0;
   uint64_t exponent;
   uint64_t mantissa;
   uint64_t total = num_latencies(table);

   if (total == 0)
      total = 1;

   fprintf(stream, "latency count\n");
   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++) {
         count_so_far += table[exponent][mantissa];
         if (count_so_far)
           fprintf(stream, "%20lu %f\n",
                   compute_latency(exponent, mantissa),
                   1.0 * count_so_far / total);
         if (count_so_far == total)
            return;
      }
}

void
write_latency_cdf(char *filename, latency_table table)
{
   FILE *stream = fopen(filename, "w");
   assert(stream != NULL);
   print_latency_cdf(stream, table);
   fclose(stream);
}
