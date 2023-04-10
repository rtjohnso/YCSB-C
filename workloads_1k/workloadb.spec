# Yahoo! Cloud System Benchmark
# Workload B: Read mostly workload
#   Application example: photo tagging; add a tag is an update, but most operations are to read tags
#
#   Read/update ratio: 95/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

operationcount=81965024
recordcount=81965024
workload=com.yahoo.ycsb.workloads.CoreWorkload
fieldcount=1

keylength=20
zeropadding=20
fieldlength=1024

readallfields=true
requestdistribution=zipfian
readproportion=0.95
updateproportion=0.05
scanproportion=0
insertproportion=0
