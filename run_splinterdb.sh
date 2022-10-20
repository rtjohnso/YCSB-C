#!/bin/bash -x

if [[ "x$1" == "xsplinterdb" || "x$1" == "xtransactional_splinterdb" ]]
then
  DB=$1
else
  echo "Usage: $0 [splinterdb|transactional_splinterdb]"
  exit 1
fi

THREADS=(1 2 4 8)

echo "Run for the uniform distribution"

OUT=uniform.out

rm -f $OUT

for t in ${THREADS[@]}
do
  bash run_individual.sh $DB $t uniform >> $OUT 2>&1
done

OUT=zipf.out

rm -f $OUT

echo "Run for the zipfian distribution"

for t in ${THREADS[@]}
do
  bash run_individual.sh $DB $t zipfian >> $OUT 2>&1
done