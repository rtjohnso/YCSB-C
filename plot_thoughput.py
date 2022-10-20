import sys
import matplotlib.pyplot as plt

outputfile = sys.argv[1]

f = open(outputfile, "r")

lines = f.readlines()

load_threads = []
load_tputs = []
run_threads = []
run_tputs = []

abort_counts = []

load_data = False
run_data = False

for line in lines:
    if load_data:
        fields = line.split()
        load_threads.append(fields[-2])
        load_tputs.append(fields[-1])
        load_data = False

    if line.startswith("# Load throughput (KTPS)"):
        load_data = True

    if run_data:
        fields = line.split()
        run_threads.append(fields[-2])
        run_tputs.append(fields[-1])
        run_data = False

    if line.startswith("# Transaction throughput (KTPS)"):
        run_data = True

    if line.startswith("# Abort count"):
        fields = line.split()
        abort_counts.append(fields[-1])

# print csv
print("threads,load,workload,aborts")
for i in range(0, len(load_threads)):
    print(load_threads[i], load_tputs[i],
          run_tputs[i], abort_counts[i], sep=',')

plt.plot(load_threads, load_tputs, label='load')
plt.plot(run_threads, run_tputs, label='run')

plt.ylabel("Throughput(ops/sec)")
plt.xlabel("# of threads")

plt.legend()

plt.show()
