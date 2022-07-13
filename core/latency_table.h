#ifndef LATENCY_TABLE_H_
#define LATENCY_TABLE_H_

#include <stdint.h>
#include <stdio.h>

#define LATENCY_MANTISSA_BITS  (10ULL)
#define LATENCY_EXPONENT_LIMIT (64ULL - LATENCY_MANTISSA_BITS)
#define LATENCY_MANTISSA_LIMIT (1ULL << LATENCY_MANTISSA_BITS)
typedef uint64_t latency_table[LATENCY_EXPONENT_LIMIT][LATENCY_MANTISSA_LIMIT];

void
record_latency(latency_table table, uint64_t latency);

void
sum_latency_tables(latency_table dest,
                   latency_table table1,
                   latency_table table2);

void
add_latency_table(latency_table dest, latency_table table);

uint64_t
num_latencies(latency_table table);

uint64_t
compute_latency(uint64_t exponent, uint64_t mantissa);

uint64_t
total_latency(latency_table table);

double
mean_latency(latency_table table);

uint64_t
min_latency(latency_table table);

uint64_t
max_latency(latency_table table);

uint64_t
ith_latency(latency_table table, uint64_t rank);

uint64_t
latency_percentile(latency_table table, float percent);

void
print_latency_table(FILE *stream, latency_table table);

void
write_latency_table(char *filename, latency_table table);

void
print_latency_cdf(FILE *stream, latency_table table);

void
write_latency_cdf(char *filename, latency_table table);

#endif // ifdef LATENCY_TABLE_H_
