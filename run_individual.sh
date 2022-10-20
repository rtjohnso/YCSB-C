#!/bin/bash

DB=${1:-"transactional_splinterdb"}
THREADS=${2:-1}
DIST=${3:-"uniform"}
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
-w requestdistribution $DIST \
-w operationcount $OPERATIONCOUNT \
-w opspertransaction $OPSPERTRANSACTION