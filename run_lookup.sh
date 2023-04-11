MEMSIZE=10
BUFFERSIZE=5
WORKLOADDIR=workloads
WORKLOAD=c
WORKLOADFILE=workload${WORKLOAD}.spec

CACHESIZE=$((MEMSIZE - BUFFERSIZE))
CACHESIZEBYTES=$(($CACHESIZE * 1024 * 1024 * 1024))

rm -rf /mnt/benchmark/rocksdb

set -x

cgexec -g memory:benchmark ./ycsbc -db rocksdb -L ${WORKLOADDIR}/load.spec -p rocksdb.database_filename /mnt/benchmark/rocksdb -threads 14 -p rocksdb.config_file rocks.ini -p rocksdb.options.max_background_jobs 20 -p rocksdb.cache_size $CACHESIZEBYTES

sync
echo 3 > /proc/sys/vm/drop_caches
iostat -m | grep nvme

THREADS=8
cgexec -g memory:benchmark ./ycsbc -db rocksdb -P ${WORKLOADDIR}/load.spec -W ${WORKLOADDIR}/${WORKLOADFILE} -p rocksdb.database_filename /mnt/benchmark/rocksdb -threads $THREADS -p rocksdb.config_file rocks.ini -p rocksdb.options.max_background_jobs 20 -p rocksdb.cache_size $CACHESIZEBYTES > rocks_lookup_${WORKLOAD}_${MEMSIZE}GiB 2>&1

sync
echo 3 > /proc/sys/vm/drop_caches
iostat -m | grep nvme

THREADS=12
cgexec -g memory:benchmark ./ycsbc -db rocksdb -P ${WORKLOADDIR}/load.spec -W ${WORKLOADDIR}/${WORKLOADFILE} -p rocksdb.database_filename /mnt/benchmark/rocksdb -threads $THREADS -p rocksdb.config_file rocks.ini -p rocksdb.options.max_background_jobs 20 -p rocksdb.cache_size $CACHESIZEBYTES >> rocks_lookup_${WORKLOAD}_${MEMSIZE}GiB 2>&1

sync
echo 3 > /proc/sys/vm/drop_caches
iostat -m | grep nvme

THREADS=16
cgexec -g memory:benchmark ./ycsbc -db rocksdb -P ${WORKLOADDIR}/load.spec -W ${WORKLOADDIR}/${WORKLOADFILE} -p rocksdb.database_filename /mnt/benchmark/rocksdb -threads $THREADS -p rocksdb.config_file rocks.ini -p rocksdb.options.max_background_jobs 20 -p rocksdb.cache_size $CACHESIZEBYTES >> rocks_lookup_${WORKLOAD}_${MEMSIZE}GiB 2>&1

sync
echo 3 > /proc/sys/vm/drop_caches
iostat -m | grep nvme

THREADS=20
cgexec -g memory:benchmark ./ycsbc -db rocksdb -P ${WORKLOADDIR}/load.spec -W ${WORKLOADDIR}/${WORKLOADFILE} -p rocksdb.database_filename /mnt/benchmark/rocksdb -threads $THREADS -p rocksdb.config_file rocks.ini -p rocksdb.options.max_background_jobs 20 -p rocksdb.cache_size $CACHESIZEBYTES >> rocks_lookup_${WORKLOAD}_${MEMSIZE}GiB 2>&1

sync
echo 3 > /proc/sys/vm/drop_caches
iostat -m | grep nvme

THREADS=24
cgexec -g memory:benchmark ./ycsbc -db rocksdb -P ${WORKLOADDIR}/load.spec -W ${WORKLOADDIR}/${WORKLOADFILE} -p rocksdb.database_filename /mnt/benchmark/rocksdb -threads $THREADS -p rocksdb.config_file rocks.ini -p rocksdb.options.max_background_jobs 20 -p rocksdb.cache_size $CACHESIZEBYTES >> rocks_lookup_${WORKLOAD}_${MEMSIZE}GiB 2>&1

sync
echo 3 > /proc/sys/vm/drop_caches
iostat -m | grep nvme

THREADS=28
cgexec -g memory:benchmark ./ycsbc -db rocksdb -P ${WORKLOADDIR}/load.spec -W ${WORKLOADDIR}/${WORKLOADFILE} -p rocksdb.database_filename /mnt/benchmark/rocksdb -threads $THREADS -p rocksdb.config_file rocks.ini -p rocksdb.options.max_background_jobs 20 -p rocksdb.cache_size $CACHESIZEBYTES >> rocks_lookup_${WORKLOAD}_${MEMSIZE}GiB 2>&1
