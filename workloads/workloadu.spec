# Yahoo! Cloud System Benchmark
# Workload C: Read only
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#
#   Read/update ratio: 100/0
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

operationcount=692736661
recordcount=692736661

workload=com.yahoo.ycsb.workloads.CoreWorkload
fieldcount=1

keylength=20
zeropadding=20
fieldlength=100

readallfields=true
requestdistribution=uniform
readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0
