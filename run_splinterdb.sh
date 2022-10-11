#!/bin/bash

NTHREADS=1
WL=workloads/myworkload.spec

PARAMS="-W $WL -L $WL"

if [ `uname` = FreeBSD ]; then
  GETOPT=/usr/local/bin/getopt
else
  GETOPT=getopt
fi

eval set -- "$(${GETOPT} -o i:t:h -- $@)"

while true ; do
        case "$1" in
                -t) NTHREADS=$2; shift 2 ;;
                -i) PARAMS+=" -p splinterdb.isolation_level $2"; shift 2 ;;
                -h) printf "$0 options:\n\t-t [# threads]\n\t-i [isolation_level: 1=serializable(default), 2=snapshot_isolation]\nExample: $0 -t 4 -i 1\n"; exit ;;
                --) shift ; break ;;
        esac
done

./ycsbc -db transactional_splinterdb -threads $NTHREADS $PARAMS

