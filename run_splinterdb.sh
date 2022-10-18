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

for t in ${THREADS[@]}
do
  bash run_individual.sh $DB $t uniform
done

echo "Run for the zipfian distribution"

for t in ${THREADS[@]}
do
  bash run_individual.sh $DB $t zipfian
done