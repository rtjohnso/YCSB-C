#!/bin/bash

NTHREADS=1
DB=optimistic_transaction_rocksdb
WL=workloads/myworkload.spec

PARAMS="-W $WL -L $WL"

if [ `uname` = FreeBSD ]; then
  GETOPT=/usr/local/bin/getopt
else
  GETOPT=getopt
fi

eval set -- "$(${GETOPT} -o i:t:ph -- $@)"

while true ; do
        case "$1" in
                -p) DB=transaction_rocksdb; shift 1 ;;
                -t) NTHREADS=$2; shift 2 ;;
                -i) PARAMS+=" -p rocksdb.isolation_level $2"; shift 2 ;;
                -h) printf "$0 options:\n\t-t [# threads]\n\t-i [isolation_level: 1=read_committed, 2=snapshot_isolation, 3=monotonic_atomic_views(default)]\n\t-p: pessimistic transaction db\nExample: $0 -t 4 -i 3 -p\n"; exit ;;
                --) shift ; break ;;
        esac
done

./ycsbc -db $DB -threads $NTHREADS $PARAMS

if [[ -d rocksdb.db ]]; then
    rm -rf rocksdb.db
fi
