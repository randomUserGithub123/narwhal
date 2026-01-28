#!/usr/bin/env python3
import re
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

#######################################
# CONFIGURATION
#######################################

LOG_FILE = 'haring_changing_nodes_28_01_2026.txt'

# Regex patterns
PATTERN_COMMITTEE_SIZE = r'Committee size: (\d+) node\(s\)'
PATTERN_EXEC_TIME = r'Execution time: (-?\d+) s'
PATTERN_TPS = r'End-to-end TPS: ([\d,.]+) tx/s'
PATTERN_LATENCY = r'End-to-end latency \(finalization\): ([\d,]+) ms'

#######################################
# PARSING
#######################################

with open(LOG_FILE, 'r') as f:
    content = f.read()

# Split by SUMMARY blocks
blocks = re.split(r'-+\s*SUMMARY:\s*-+', content)[1:]  # skip before first SUMMARY

if not blocks:
    print(f"ERROR: No SUMMARY blocks found")
    exit(1)

print(f"Found {len(blocks)} runs\n")

data = []
skipped_count = 0

for block in blocks:
    def extract(pattern):
        m = re.search(pattern, block)
        if m:
            return m.group(1).replace(',', '')
        return None
    
    committee_size = extract(PATTERN_COMMITTEE_SIZE)
    exec_time = extract(PATTERN_EXEC_TIME)
    tps = extract(PATTERN_TPS)
    latency = extract(PATTERN_LATENCY)
    
    if not all([committee_size, exec_time, tps, latency]):
        print("WARNING: Missing data in block, skipping")
        skipped_count += 1
        continue
    
    committee_size = int(committee_size)
    exec_time = int(exec_time)
    tps = float(tps)
    latency = int(latency)
    
    # Skip runs with exec_time == -1
    if exec_time == -1:
        print(f"SKIPPED (exec_time=-1): Node count={committee_size}")
        skipped_count += 1
        continue
    
    data.append({
        'node_count': committee_size, 
        'exec_time': exec_time, 
        'tps': tps,
        'latency': latency
    })

print(f"\nTotal runs parsed: {len(data)}")
print(f"Runs skipped: {skipped_count}")

if not data:
    print("ERROR: No valid data after filtering")
    exit(1)

df = pd.DataFrame(data)

#######################################
# CALCULATE STATS PER NODE COUNT
#######################################

stats = df.groupby('node_count').agg(
    avg_tps=('tps', 'mean'),
    std_tps=('tps', 'std'),
    avg_latency=('latency', 'mean'),
    std_latency=('latency', 'std'),
    count=('tps', 'count')
).fillna(0).reset_index()

print("\n=== AVERAGE VALUES PER NODE COUNT ===")
print(stats.to_string(index=False))

#######################################
# PLOTTING
#######################################

# Create figure with dual y-axes
fig, ax1 = plt.subplots(figsize=(12, 7))

# Left y-axis - TPS
color_tps = '#1f77b4'
ax1.set_xlabel('Number of Nodes', fontsize=12, fontweight='bold')
ax1.set_ylabel('End-to-end TPS (tx/s)', fontsize=12, fontweight='bold', color=color_tps)
ax1.plot(stats['node_count'], stats['avg_tps'], 
         marker='o', linewidth=2.5, markersize=8,
         label='End-to-end TPS', color=color_tps)
ax1.fill_between(stats['node_count'],
                 stats['avg_tps'] - stats['std_tps'],
                 stats['avg_tps'] + stats['std_tps'], 
                 alpha=0.2, color=color_tps)
ax1.tick_params(axis='y', labelcolor=color_tps)
ax1.grid(True, alpha=0.3, linestyle='--')

# Right y-axis - Latency
ax2 = ax1.twinx()
color_latency = '#ff7f0e'
ax2.set_ylabel('End-to-end Latency (ms)', fontsize=12, fontweight='bold', color=color_latency)
ax2.plot(stats['node_count'], stats['avg_latency'], 
         marker='s', linewidth=2.5, markersize=8,
         label='End-to-end Latency', color=color_latency)
ax2.fill_between(stats['node_count'],
                 stats['avg_latency'] - stats['std_latency'],
                 stats['avg_latency'] + stats['std_latency'], 
                 alpha=0.2, color=color_latency)
ax2.tick_params(axis='y', labelcolor=color_latency)

# Title
plt.title('HARING Performance vs Node Count (Input Rate: 5000 tx/s)', 
          fontsize=14, fontweight='bold', pad=20)

# Combine legends
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=10)

# Set x-axis ticks to show all node counts
ax1.set_xticks(stats['node_count'])

plt.tight_layout()

# Save as PDF
output_file = 'haring_changing_nodes_performance.pdf'
with PdfPages(output_file) as pdf:
    pdf.savefig(fig, dpi=150, bbox_inches='tight')
    plt.close()

print(f"\n✓ Plot saved: {output_file}")