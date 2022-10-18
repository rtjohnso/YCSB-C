#!/bin/bash -x

for t in 1 2 4 8
do
  bash run_individual.sh transactional_splinterdb $t
done