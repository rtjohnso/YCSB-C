#!/bin/bash

DB=transactional_splinterdb
THREADS=$1
FIELDLENGTH=1024

RECORDCOUNT=84000000
TXNPERTREAD=1000000
OPSPERTRANSACTION=2
OPERATIONCOUNT=$(($OPSPERTRANSACTION * $THREADS * $TXNPERTREAD))

./ycsbc -db $DB -threads $THREADS \
-L workloads/load.spec \
-w fieldlength $FIELDLENGTH \
-w recordcount $RECORDCOUNT \
-W workloads/workloada.spec \
-w requestdistribution zipfian \
-w operationcount $OPERATIONCOUNT \
-w opspertransaction $OPSPERTRANSACTION