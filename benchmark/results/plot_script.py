#!/usr/bin/env python3
import re
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#######################################
# CONFIGURATION - EDIT THIS SECTION
#######################################

LOG_FILE = 'run_tilikum_BOF_21_01_2026-Attack-NONE-1-4-17-5-0-X-a7d859.txt'

# Regex patterns - one per field (easier to edit)
PATTERN_INPUT_RATE = r'Input rate: ([\d,]+) tx/s'
PATTERN_EXEC_TIME  = r'Execution time: (\d+) s'
PATTERN_LATENCY    = r'End-to-end Execution Latency: ([\d,]+) ms'
PATTERN_TPS        = r'Effective execution throughput: ([\d,.]+) tx/s'

# Whether to normalize TPS to 60 seconds
NORMALIZE_TPS = False
BASELINE_SECONDS = 60

#######################################
# PARSING
#######################################

with open(LOG_FILE, 'r') as f:
    content = f.read()

# Split by SUMMARY blocks
blocks = re.split(r'-+\s*SUMMARY:\s*-+', content)[1:]  # skip before first SUMMARY

if not blocks:
    print("ERROR: No SUMMARY blocks found")
    exit(1)

print(f"Found {len(blocks)} runs\n")

data = []
for block in blocks:
    def extract(pattern):
        m = re.search(pattern, block)
        if m:
            return m.group(1).replace(',', '')
        return None
    
    input_rate = extract(PATTERN_INPUT_RATE)
    exec_time = extract(PATTERN_EXEC_TIME)
    latency = extract(PATTERN_LATENCY)
    tps = extract(PATTERN_TPS)
    
    if not all([input_rate, exec_time, latency, tps]):
        continue
    
    input_rate = int(input_rate)
    exec_time = int(exec_time)
    latency = int(latency)
    tps = float(tps)
    
    if NORMALIZE_TPS and exec_time > 0:
        tps = (tps * exec_time) / BASELINE_SECONDS
    elif exec_time == 0:
        tps = 0
    
    data.append({'input_rate': input_rate, 'exec_time': exec_time, 'latency': latency, 'tps': tps})

df = pd.DataFrame(data)

#######################################
# AUTO-DETECT NOMINAL RATES (round to nearest 500)
#######################################

df['nominal_rate'] = (df['input_rate'] / 500).round() * 500
df['nominal_rate'] = df['nominal_rate'].astype(int)

#######################################
# FILTER 0 TPS AND REPORT
#######################################

for idx, row in df.iterrows():
    if row['tps'] == 0:
        print(f"SKIPPED DUE TO 0 tx/s: Input rate={row['input_rate']}")

df_valid = df[df['tps'] > 0]

if df_valid.empty:
    print("ERROR: No valid data after filtering")
    exit(1)

#######################################
# CALCULATE STATS
#######################################

stats = df_valid.groupby('nominal_rate').agg(
    avg_tps=('tps', 'mean'),
    std_tps=('tps', 'std'),
    avg_latency=('latency', 'mean'),
    std_latency=('latency', 'std')
).fillna(0).reset_index()

print("\n=== AVERAGE VALUES ===")
print(stats.to_string(index=False))

#######################################
# PLOTTING
#######################################

plt.figure(figsize=(10, 6))
plt.plot(stats['nominal_rate'], stats['avg_tps'], 'o-', linewidth=2, markersize=6)
plt.fill_between(stats['nominal_rate'],
                 stats['avg_tps'] - stats['std_tps'],
                 stats['avg_tps'] + stats['std_tps'], alpha=0.3)
plt.xlabel('Input Rate (tx/s)')
plt.ylabel('End-to-end TPS (tx/s)')
plt.title('Input Rate vs End-to-end TPS')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('plot1_input_vs_tps.png', dpi=150)
plt.close()

plt.figure(figsize=(10, 6))
plt.plot(stats['nominal_rate'], stats['avg_latency'], 's-', linewidth=2, markersize=6, color='orange')
plt.fill_between(stats['nominal_rate'],
                 stats['avg_latency'] - stats['std_latency'],
                 stats['avg_latency'] + stats['std_latency'], alpha=0.3, color='orange')
plt.xlabel('Input Rate (tx/s)')
plt.ylabel('Latency (ms)')
plt.title('Input Rate vs Latency')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('plot2_input_vs_latency.png', dpi=150)
plt.close()

plt.figure(figsize=(10, 6))
plt.plot(stats['avg_tps'], stats['avg_latency'], 'd-', linewidth=2, markersize=6, color='green')
plt.fill_between(stats['avg_tps'],
                 stats['avg_latency'] - stats['std_latency'],
                 stats['avg_latency'] + stats['std_latency'], alpha=0.3, color='green')
plt.xlabel('End-to-end TPS (tx/s)')
plt.ylabel('Latency (ms)')
plt.title('End-to-end TPS vs Latency')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('plot3_tps_vs_latency.png', dpi=150)
plt.close()

print("\nPlots saved: plot1_input_vs_tps.png, plot2_input_vs_latency.png, plot3_tps_vs_latency.png")