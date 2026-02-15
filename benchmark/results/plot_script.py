#!/usr/bin/env python3
import re
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#######################################
# CONFIGURATION - EDIT THIS SECTION
#######################################

LOG_FILES = {
    # 'Tilikum': 'run_tilikum_BOF_21_01_2026-Attack-NONE-1-4-17-5-0-X-a7d859.txt',
    'DoD': 'dod_change_tps.txt',
    'Haring': 'haring_change_tps_12_02_2026.txt',
}

# Full benchmark window duration (seconds).
# TPS values are normalized to this duration:
#   adjusted_tps = reported_tps * (exec_time / MAX_EXEC_TIME)
# This corrects runs that finished early (small exec_time) whose raw TPS
# would otherwise appear artificially high relative to the full window.
MAX_EXEC_TIME = 57

# Regex patterns - one per field (easier to edit)
PATTERN_INPUT_RATE = r'Input rate: ([\d,]+) tx/s'
PATTERN_EXEC_TIME  = r'Execution time: (-?\d+) s'
PATTERN_CONSENSUS_TIME = r'Consensus time: (\d+) s'
PATTERN_LATENCY_HARING = r'End-to-end latency \(finalization\): ([\d,]+) ms'
PATTERN_LATENCY_DOD = r'End-to-end latency: ([\d,]+) ms'
PATTERN_LATENCY_TILIKUM = r'End-to-end Execution Latency: ([\d,]+) ms'
PATTERN_TPS_HARING = r'End-to-end TPS: ([\d,.]+) tx/s'
PATTERN_TPS_DOD = r'End-to-end TPS: ([\d,.]+) tx/s'
PATTERN_TPS_TILIKUM = r'Effective execution throughput: ([\d,.]+) tx/s'

#######################################
# PARSING
#######################################

all_data = []

for system_name, log_file in LOG_FILES.items():
    with open(log_file, 'r') as f:
        content = f.read()

    # Split by SUMMARY blocks
    blocks = re.split(r'-+\s*SUMMARY:\s*-+', content)[1:]  # skip before first SUMMARY

    if not blocks:
        print(f"ERROR: No SUMMARY blocks found in {system_name}")
        continue

    print(f"Found {len(blocks)} runs in {system_name}\n")

    data = []
    for block in blocks:
        def extract(pattern):
            m = re.search(pattern, block)
            if m:
                return m.group(1).replace(',', '')
            return None

        input_rate = extract(PATTERN_INPUT_RATE)
        exec_time = extract(PATTERN_EXEC_TIME)

        # Use system-specific patterns
        if system_name == 'Haring':
            latency = extract(PATTERN_LATENCY_HARING)
            tps = extract(PATTERN_TPS_HARING)
            consensus_time = extract(PATTERN_CONSENSUS_TIME)
        elif system_name == 'DoD':
            latency = extract(PATTERN_LATENCY_DOD)
            tps = extract(PATTERN_TPS_DOD)
            consensus_time = 1
        else:  # Tilikum
            latency = extract(PATTERN_LATENCY_TILIKUM)
            tps = extract(PATTERN_TPS_TILIKUM)
            consensus_time = extract(PATTERN_CONSENSUS_TIME)

        if not all([input_rate, exec_time, latency, tps]):
            continue

        input_rate = int(input_rate)
        exec_time = int(exec_time)
        consensus_time = int(consensus_time) if consensus_time else 0
        latency = int(latency)
        tps = float(tps)

        # Skip runs with exec_time == -1 or consensus_time == 0
        if exec_time == -1 or consensus_time == 0:
            continue

        # -------------------------------------------------------
        # TPS ADJUSTMENT FOR PARTIAL RUNS
        # A run that lasted exec_time seconds only processed
        # transactions for that fraction of the benchmark window.
        # Normalise to MAX_EXEC_TIME so all TPS values are
        # comparable on the same time base.
        #   adjusted_tps = reported_tps * (exec_time / MAX_EXEC_TIME)
        # Runs with exec_time == MAX_EXEC_TIME are unaffected (×1.0).
        # Runs with exec_time == 0 collapse to 0 and are filtered below.
        # -------------------------------------------------------
        if exec_time != MAX_EXEC_TIME and tps > 0:
            raw_tps = tps
            tps = tps * (exec_time / MAX_EXEC_TIME)
            print(f"  TPS adjusted: input={input_rate} tx/s, "
                  f"exec={exec_time}s/{MAX_EXEC_TIME}s, "
                  f"raw={raw_tps:.1f} -> adjusted={tps:.1f} tx/s  ({system_name})")

        if tps > 0:
            data.append({
                'input_rate': input_rate,
                'exec_time': exec_time,
                'latency': latency,
                'tps': tps,
                'system': system_name
            })

    all_data.extend(data)

df = pd.DataFrame(all_data)

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
        print(f"SKIPPED DUE TO 0 tx/s: Input rate={row['input_rate']} ({row['system']})")

df_valid = df[df['tps'] > 0]

if df_valid.empty:
    print("ERROR: No valid data after filtering")
    exit(1)

#######################################
# CALCULATE STATS PER SYSTEM
#######################################

stats = df_valid.groupby(['system', 'nominal_rate']).agg(
    avg_tps=('tps', 'mean'),
    std_tps=('tps', 'std'),
    avg_latency=('latency', 'mean'),
    std_latency=('latency', 'std')
).fillna(0).reset_index()

print("\n=== AVERAGE VALUES (TPS adjusted to MAX_EXEC_TIME={} s) ===".format(MAX_EXEC_TIME))
for system in stats['system'].unique():
    print(f"\n{system}:")
    print(stats[stats['system'] == system].to_string(index=False))

#######################################
# PLOTTING
#######################################

colors = {'DoD': '#1f77b4', 'Haring': '#ff7f0e'}
markers = {'DoD': 'o', 'Haring': 's'}

# Plot 1: Input Rate vs End-to-end TPS
plt.figure(figsize=(10, 6))
for system in stats['system'].unique():
    data = stats[stats['system'] == system]
    plt.plot(data['nominal_rate'], data['avg_tps'],
             marker=markers[system], linewidth=2, markersize=6,
             label=system, color=colors[system])
    plt.fill_between(data['nominal_rate'],
                     data['avg_tps'] - data['std_tps'],
                     data['avg_tps'] + data['std_tps'],
                     alpha=0.3, color=colors[system])
plt.xlabel('Input Rate (tx/s)')
plt.ylabel('End-to-end TPS (tx/s)')
plt.title('Input Rate vs End-to-end TPS')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('plot1_input_vs_tps.png', dpi=150)
plt.close()

# Plot 2: Input Rate vs Latency
plt.figure(figsize=(10, 6))
for system in stats['system'].unique():
    data = stats[stats['system'] == system]
    plt.plot(data['nominal_rate'], data['avg_latency'],
             marker=markers[system], linewidth=2, markersize=6,
             label=system, color=colors[system])
    plt.fill_between(data['nominal_rate'],
                     data['avg_latency'] - data['std_latency'],
                     data['avg_latency'] + data['std_latency'],
                     alpha=0.3, color=colors[system])
plt.xlabel('Input Rate (tx/s)')
plt.ylabel('Latency (ms)')
plt.title('Input Rate vs Latency')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('plot2_input_vs_latency.png', dpi=150)
plt.close()

# Plot 3: End-to-end TPS vs Latency
plt.figure(figsize=(10, 6))
for system in stats['system'].unique():
    data = stats[stats['system'] == system]
    plt.plot(data['avg_tps'], data['avg_latency'],
             marker=markers[system], linewidth=2, markersize=6,
             label=system, color=colors[system])
    plt.fill_between(data['avg_tps'],
                     data['avg_latency'] - data['std_latency'],
                     data['avg_latency'] + data['std_latency'],
                     alpha=0.3, color=colors[system])
plt.xlabel('End-to-end TPS (tx/s)')
plt.ylabel('Latency (ms)')
plt.title('End-to-end TPS vs Latency')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('plot3_tps_vs_latency.png', dpi=150)
plt.close()

print("\nPlots saved: plot1_input_vs_tps.png, plot2_input_vs_latency.png, plot3_tps_vs_latency.png")