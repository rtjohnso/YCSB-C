#!/bin/bash -x

for t in 1 4 12
do
  bash run_individual.sh transactional_splinterdb $t
done