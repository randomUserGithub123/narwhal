#!/usr/bin/env python3
"""
Print sum of FINALIZED unique txs per worker.
Usage: python verify_tps.py [log_directory]   (default: ../logs/)
"""
import sys, os
from glob import glob
from os.path import join
from re import findall

log_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), '..', 'logs')
log_dir = os.path.abspath(log_dir)

for filename in sorted(glob(join(log_dir, 'worker-*.log'))):
    with open(filename) as f:
        log = f.read()
    tmp = findall(r'sub_dag_id=(\d+): FINALIZED! (\d+) transactions', log)
    total = sum(int(count) for _, count in tmp)
    num_subdags = len(tmp)
    print(f"{os.path.basename(filename)}: {total:,} txs ({num_subdags} sub_dags)")