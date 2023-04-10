# Yahoo! Cloud System Benchmark
# Workload F: Read-modify-write workload
#   Application example: user database, where user records are read and modified by the user or to record user activity.
#
#   Read/read-modify-write ratio: 50/50
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
readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0
readmodifywriteproportion=0.5
