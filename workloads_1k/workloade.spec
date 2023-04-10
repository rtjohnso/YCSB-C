# Yahoo! Cloud System Benchmark
# Workload E: Short ranges
#   Application example: threaded conversations, where each scan is for the posts in a given thread (assumed to be clustered by thread id)
#
#   Scan/insert ratio: 95/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

# The insert order is hashed, not ordered. Although the scans are ordered, it does not necessarily
# follow that the data is inserted in order. For example, posts for thread 342 may not be inserted contiguously, but
# instead interspersed with posts from lots of other threads. The way the YCSB client works is that it will pick a start
# key, and then request a number of records; this works fine even for hashed insertion.

operationcount=8196502
recordcount=81965024
workload=com.yahoo.ycsb.workloads.CoreWorkload
fieldcount=1

keylength=20
zeropadding=20
fieldlength=1024

readallfields=true
requestdistribution=zipfian
readproportion=0
updateproportion=0
scanproportion=0.95
insertproportion=0.05

maxscanlength=100
scanlengthdistribution=uniform
