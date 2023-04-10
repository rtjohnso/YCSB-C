THREADS=8
MEMSIZE=10
BUFFERSIZE=3
WORKLOADDIR=workloads_1k
VALUESIZE=1k

CACHESIZE=$((MEMSIZE - BUFFERSIZE))
CACHESIZEBYTES=$(($CACHESIZE * 1024 * 1024 * 1024))

rm -rf /mnt/benchmark/rocksdb

set -x

cgexec -g memory:benchmark ./ycsbc -db rocksdb -L ${WORKLOADDIR}/load.spec -W ${WORKLOADDIR}/workloada.spec -W ${WORKLOADDIR}/workloadb.spec -W ${WORKLOADDIR}/workloadc.spec -W ${WORKLOADDIR}/workloadd.spec -W ${WORKLOADDIR}/workloadf.spec -W ${WORKLOADDIR}/workloade.spec -p rocksdb.database_filename /mnt/benchmark/rocksdb -threads $THREADS -p rocksdb.config_file rocks.ini -p rocksdb.options.max_background_jobs 20 -p rocksdb.cache_size $CACHESIZEBYTES > rocks_ycsb_${THREADS}thr_1K_${MEMSIZE}GiB 2>&1
