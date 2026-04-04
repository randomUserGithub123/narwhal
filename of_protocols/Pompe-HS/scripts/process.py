import sys
import re
from os import listdir
from os.path import isfile, join

if (len(sys.argv) != 3):
    print("Usage: process.py {client for HotStuff | order or exec for Pompe} {result dir}")
    exit()

files = [f for f in listdir(sys.argv[2]) if isfile(join(sys.argv[2], f))]
#print onlyfiles

latency_pattern = r'\[hotstuff info\] (\d+\.\d+)'
scc_pattern = r'fairness_scc size=(\d+)'

count = 0
latencies = []
average = 0.0

# SCC sizes: pick the replica with the most entries (handles log truncation)
best_scc_sizes = []

for file in files:
    if sys.argv[1] in file:
        with open(sys.argv[2] + "/" + file, "r") as f:
            for line in f:
                match = re.search(latency_pattern, line)
                if match:
                    try:
                        latency = float(match.group(1))
                        count += 1
                        latencies.append(latency)
                        average += latency
                    except Exception as e:
                        pass

    # Parse SCC sizes from replica logs
    if 'replica' in file:
        scc_sizes = []
        try:
            with open(sys.argv[2] + "/" + file, "r", errors="replace") as f:
                for line in f:
                    match = re.search(scc_pattern, line)
                    if match:
                        scc_sizes.append(int(match.group(1)))
        except Exception:
            pass
        if len(scc_sizes) > len(best_scc_sizes):
            best_scc_sizes = scc_sizes

latencies.sort()

latency50 = latencies[int(count * 0.5)]
latency90 = latencies[int(count * 0.9)]
latency99 = latencies[int(count * 0.99)]
average = average / count

# print(latencies)
print("Total tx count: ", count)
print("50%: ", latency50 * 1000)
print("90%: ", latency90 * 1000)
print("99%: ", latency99 * 1000)
print("Average: ", average * 1000)

if best_scc_sizes:
    avg_scc = sum(best_scc_sizes) / len(best_scc_sizes)
    max_scc = max(best_scc_sizes)
    print("Avg cycle size: ", round(avg_scc, 4))
    print("Max cycle size: ", max_scc)
    print("Num cycles: ", len(best_scc_sizes))
else:
    print("Avg cycle size: ", 0.0)
    print("Max cycle size: ", 0)
    print("Num cycles: ", 0)