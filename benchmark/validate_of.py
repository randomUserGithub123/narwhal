#!/usr/bin/env python3
"""
Batch-Order-Fairness Validator for DAG BFT Systems (Themis Protocol)

Validates that execution order respects the batch-order-fairness property:
- If γ fraction of nodes receive tx before tx', then tx should be ordered no later than tx'
- Transactions in Condorcet cycles can be ordered arbitrarily

Strategy:
1. Parse logs to extract per-node receive orders and per-subdag executions
2. Validate each sub_dag independently (in parallel) - O(subdag_size²) instead of O(total²)
3. Check for suspicious missing transactions
4. Verify execution convergence across nodes

Usage:
    python validate_order_fairness.py --logs-dir /path/to/logs --gamma 1.0 --f 4
"""

import re
import sys
import argparse
import networkx as nx
import numpy as np
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional
from pathlib import Path
from re import findall, search
from multiprocessing import Pool, cpu_count
import time


# =============================================================================
# REGEX PATTERNS (matching Rust log::info! format)
# =============================================================================

# Rust: log::info!("Receival of tx_digest {:?} at position {}", tx_digest, counter);
# Output: "Receival of tx_digest XYZ123= at position 42"
RECEIVE_PATTERN = re.compile(r'Receival of tx_digest\s+([^\s]+)\s+at position\s+(\d+)')

# Rust: log::info!("sub_dag_id={}: Executed {:?}", sub_dag_id, digest);
# Output: "sub_dag_id=1: Executed XYZ123="
EXEC_PATTERN = re.compile(r'sub_dag_id=(\d+):\s*Executed\s+([^\s]+)')


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class SubDagResult:
    """Validation result for a single sub_dag."""
    sub_dag_id: int
    num_txs: int
    is_valid: bool
    violations: List[Tuple[str, str, str]]
    num_sccs: int
    largest_scc: int


@dataclass
class ValidationResult:
    """Full validation result."""
    is_valid: bool
    total_sub_dags: int
    valid_sub_dags: int
    invalid_sub_dags: int
    total_txs: int
    violations: List[Tuple[int, str, str, str]]  # (sub_dag_id, tx1, tx2, reason)
    execution_converged: bool
    execution_mismatch_details: Optional[str] = None
    suspicious_missing: List[Tuple[str, int, str]] = field(default_factory=list)
    
    def __str__(self):
        status = "✓ VALID" if self.is_valid else "✗ INVALID"
        conv = "✓ YES" if self.execution_converged else "✗ NO"
        susp = "✗ YES" if self.suspicious_missing else "✓ NONE"
        
        s = f"""
╔══════════════════════════════════════════════════════════════╗
║       Batch-Order-Fairness Validation Result (Themis)        ║
╠══════════════════════════════════════════════════════════════╣
║  Status: {status:50} ║
║  Execution Converged: {conv:37} ║
║  Suspicious Missing: {susp:38} ║
╠══════════════════════════════════════════════════════════════╣
║  Total sub_dags:     {self.total_sub_dags:<38} ║
║  Valid sub_dags:     {self.valid_sub_dags:<38} ║
║  Invalid sub_dags:   {self.invalid_sub_dags:<38} ║
║  Total transactions: {self.total_txs:<38} ║
║  Total violations:   {len(self.violations):<38} ║
╚══════════════════════════════════════════════════════════════╝
"""
        if not self.execution_converged and self.execution_mismatch_details:
            s += f"\nExecution Convergence Issues:\n{self.execution_mismatch_details}\n"
        
        if self.suspicious_missing:
            s += f"\n⚠️  SUSPICIOUS MISSING TRANSACTIONS ({len(self.suspicious_missing)}):\n"
            s += "    Received by many nodes BEFORE last executed, but never executed!\n\n"
            for i, (tx, nodes, reason) in enumerate(self.suspicious_missing[:10], 1):
                tx_short = tx[:32] + "..." if len(tx) > 32 else tx
                s += f"  {i}. {tx_short}\n     {reason}\n"
            if len(self.suspicious_missing) > 10:
                s += f"  ... and {len(self.suspicious_missing) - 10} more\n"
        
        if self.violations:
            s += f"\nOrder-Fairness Violations:\n"
            for i, (sd, tx1, tx2, reason) in enumerate(self.violations[:20], 1):
                t1 = tx1[:16] + "..." if len(tx1) > 16 else tx1
                t2 = tx2[:16] + "..." if len(tx2) > 16 else tx2
                s += f"  {i}. [sub_dag {sd}] {t1} should precede {t2}\n"
            if len(self.violations) > 20:
                s += f"  ... and {len(self.violations) - 20} more\n"
        
        return s


# =============================================================================
# LOG PARSING (inspired by logs.py - fast findall approach)
# =============================================================================

def parse_worker_log(log_path: Path) -> dict:
    """
    Parse a single worker log file.
    
    Returns:
        {
            'node_id': str,
            'receive_orders': {tx_digest: position},
            'subdag_executions': {sub_dag_id: [tx_digests in order]}
        }
    """
    # Extract node ID from filename: worker-X-0.log -> X
    match = search(r'worker-(\d+)-', log_path.name)
    node_id = match.group(1) if match else log_path.stem
    
    with open(log_path, 'r') as f:
        content = f.read()
    
    # Parse receive orders using findall (single regex pass - fast!)
    receive_orders = {}
    for tx, pos in findall(RECEIVE_PATTERN, content):
        tx = tx.strip()
        if tx not in receive_orders:  # Keep first occurrence
            receive_orders[tx] = int(pos)
    
    # Parse sub_dag executions using findall
    subdag_execs = defaultdict(list)
    seen = defaultdict(set)
    
    for sd_id, tx in findall(EXEC_PATTERN, content):
        sd_id = int(sd_id)
        tx = tx.strip()
        if tx not in seen[sd_id]:  # Keep order, no duplicates
            subdag_execs[sd_id].append(tx)
            seen[sd_id].add(tx)
    
    return {
        'node_id': node_id,
        'receive_orders': receive_orders,
        'subdag_executions': dict(subdag_execs),
    }


# =============================================================================
# SUB_DAG VALIDATOR
# =============================================================================

def validate_subdag(args) -> SubDagResult:
    """
    Validate a single sub_dag's order-fairness (worker function for Pool).
    
    Uses NUMPY VECTORIZATION for fast pair counting.
    """
    sub_dag_id, txs, node_orders, gamma, f, n = args
    
    num_txs = len(txs)
    num_pairs = num_txs * (num_txs - 1) // 2
    
    print(f"[WORKER] sub_dag {sub_dag_id}: START - {num_txs} txs, {num_pairs:,} pairs", flush=True)
    
    if not txs:
        print(f"[WORKER] sub_dag {sub_dag_id}: DONE (empty)", flush=True)
        return SubDagResult(sub_dag_id, 0, True, [], 0, 0)
    
    threshold = n * (1 - gamma) + f + 1
    start_time = time.time()
    
    # =========================================================================
    # NUMPY OPTIMIZATION: Precompute position matrix
    # orderings[tx_idx, node_idx] = position (or -1 if not received)
    # =========================================================================
    print(f"[WORKER] sub_dag {sub_dag_id}: Building position matrix...", flush=True)
    
    tx_to_idx = {tx: i for i, tx in enumerate(txs)}
    node_list = sorted(node_orders.keys())
    node_to_idx = {nid: i for i, nid in enumerate(node_list)}
    
    # Initialize with -1 (not received)
    pos_matrix = np.full((num_txs, n), -1, dtype=np.int32)
    
    for nid, orders in node_orders.items():
        node_idx = node_to_idx[nid]
        for tx in txs:
            if tx in orders:
                pos_matrix[tx_to_idx[tx], node_idx] = orders[tx]
    
    matrix_time = time.time() - start_time
    print(f"[WORKER] sub_dag {sub_dag_id}: Matrix built in {matrix_time:.2f}s", flush=True)
    
    # =========================================================================
    # VECTORIZED count_orderings using numpy
    # =========================================================================
    def count_orderings_fast(i: int, j: int) -> Tuple[int, int]:
        """Count orderings using precomputed matrix - FAST!"""
        pos_i = pos_matrix[i, :]  # shape: (n,)
        pos_j = pos_matrix[j, :]  # shape: (n,)
        
        # Both must be received (>= 0)
        both_received = (pos_i >= 0) & (pos_j >= 0)
        
        c_ij = np.sum((pos_i < pos_j) & both_received)
        c_ji = np.sum((pos_j < pos_i) & both_received)
        
        return int(c_ij), int(c_ji)
    
    # =========================================================================
    # Build Themis order graph
    # =========================================================================
    print(f"[WORKER] sub_dag {sub_dag_id}: Building order graph...", flush=True)
    G = nx.DiGraph()
    G.add_nodes_from(range(num_txs))  # Use indices for speed
    
    edges_added = 0
    pairs_done = 0
    last_print = time.time()
    
    for i in range(num_txs):
        for j in range(i + 1, num_txs):
            pairs_done += 1
            
            # Progress every 10 seconds
            now = time.time()
            if now - last_print > 10:
                pct = 100.0 * pairs_done / num_pairs if num_pairs > 0 else 100
                elapsed = now - start_time
                rate = pairs_done / elapsed if elapsed > 0 else 0
                eta = (num_pairs - pairs_done) / rate if rate > 0 else 0
                print(f"[WORKER] sub_dag {sub_dag_id}: graph {pairs_done:,}/{num_pairs:,} ({pct:.1f}%), "
                      f"{rate:.0f}/s, ETA {eta:.0f}s", flush=True)
                last_print = now
            
            c_ij, c_ji = count_orderings_fast(i, j)
            
            # Themis protocol edge logic
            if c_ij >= threshold and c_ji >= threshold:
                if c_ij >= c_ji:
                    G.add_edge(i, j)
                else:
                    G.add_edge(j, i)
                edges_added += 1
            elif c_ij >= threshold:
                G.add_edge(i, j)
                edges_added += 1
            elif c_ji >= threshold:
                G.add_edge(j, i)
                edges_added += 1
    
    graph_time = time.time() - start_time
    print(f"[WORKER] sub_dag {sub_dag_id}: Graph done - {edges_added} edges in {graph_time:.1f}s", flush=True)
    
    # =========================================================================
    # Find SCCs
    # =========================================================================
    print(f"[WORKER] sub_dag {sub_dag_id}: Finding SCCs...", flush=True)
    sccs = list(nx.strongly_connected_components(G))
    largest_scc = max(len(s) for s in sccs) if sccs else 0
    print(f"[WORKER] sub_dag {sub_dag_id}: {len(sccs)} SCCs, largest={largest_scc}", flush=True)
    
    idx_to_scc = {}
    for scc_idx, scc in enumerate(sccs):
        for node_idx in scc:
            idx_to_scc[node_idx] = scc_idx
    
    # =========================================================================
    # Build condensation graph
    # =========================================================================
    print(f"[WORKER] sub_dag {sub_dag_id}: Building condensation...", flush=True)
    H = nx.condensation(G, sccs)
    
    # =========================================================================
    # Check violations
    # =========================================================================
    print(f"[WORKER] sub_dag {sub_dag_id}: Checking violations...", flush=True)
    
    violations = []
    check_start = time.time()
    checks_done = 0
    last_print = check_start
    
    for i in range(num_txs):
        for j in range(i + 1, num_txs):
            checks_done += 1
            
            # Progress every 10 seconds
            now = time.time()
            if now - last_print > 10:
                pct = 100.0 * checks_done / num_pairs if num_pairs > 0 else 100
                print(f"[WORKER] sub_dag {sub_dag_id}: check {checks_done:,}/{num_pairs:,} ({pct:.1f}%), "
                      f"violations={len(violations)}", flush=True)
                last_print = now
            
            scc_i, scc_j = idx_to_scc.get(i), idx_to_scc.get(j)
            
            if scc_i is None or scc_j is None or scc_i == scc_j:
                continue
            
            # Execution order: i comes before j in txs list
            exec_i, exec_j = i, j  # indices ARE execution positions
            
            if nx.has_path(H, scc_i, scc_j):
                # i should come before j
                if exec_i > exec_j:
                    c_ij, c_ji = count_orderings_fast(i, j)
                    violations.append((txs[i], txs[j], f"{c_ij}/{n} nodes saw tx_i first"))
            elif nx.has_path(H, scc_j, scc_i):
                # j should come before i
                if exec_j > exec_i:
                    c_ij, c_ji = count_orderings_fast(i, j)
                    violations.append((txs[j], txs[i], f"{c_ji}/{n} nodes saw tx_j first"))
    
    total_time = time.time() - start_time
    status = "✓ VALID" if len(violations) == 0 else f"✗ {len(violations)} VIOLATIONS"
    print(f"[WORKER] sub_dag {sub_dag_id}: DONE in {total_time:.1f}s - {status}", flush=True)
    
    return SubDagResult(
        sub_dag_id=sub_dag_id,
        num_txs=len(txs),
        is_valid=len(violations) == 0,
        violations=violations,
        num_sccs=len(sccs),
        largest_scc=largest_scc
    )


# =============================================================================
# MAIN VALIDATOR
# =============================================================================

class OrderFairnessValidator:
    """
    Validates batch-order-fairness by chunking into sub_dags.
    
    Complexity: O(num_subdags × avg_subdag_size²) instead of O(total_txs²)
    """
    
    def __init__(self, gamma: float = 1.0, f: int = 0):
        if not (0.5 < gamma <= 1.0):
            raise ValueError("gamma must be in (0.5, 1.0]")
        
        self.gamma = gamma
        self.f = f
        
        # Per-node data
        self.node_orders: Dict[str, Dict[str, int]] = {}
        self.node_subdag_execs: Dict[str, Dict[int, List[str]]] = {}
        
        # Aggregated data
        self.all_txs: Set[str] = set()
        self.subdag_txs: Dict[int, List[str]] = {}  # Canonical execution per sub_dag
        self.full_exec_order: List[str] = []
        
        self.n: int = 0
        self.execution_converged: bool = True
        self.convergence_details: Optional[str] = None
    
    def parse_logs(self, logs_dir: str):
        """Parse all worker logs in parallel."""
        logs_path = Path(logs_dir)
        log_files = sorted(logs_path.glob("worker-*-0.log"))
        
        if not log_files:
            raise FileNotFoundError(f"No worker-*-0.log files found in {logs_dir}")
        
        print(f"[PROGRESS] Found {len(log_files)} worker log files", flush=True)
        print(f"[PROGRESS] Parsing in parallel ({cpu_count()} CPUs)...", flush=True)
        
        start = time.time()
        with Pool() as p:
            results = p.map(parse_worker_log, log_files)
        print(f"[PROGRESS] Parsing completed in {time.time() - start:.1f}s", flush=True)
        
        # Merge results
        for r in results:
            nid = r['node_id']
            self.node_orders[nid] = r['receive_orders']
            self.node_subdag_execs[nid] = r['subdag_executions']
            self.all_txs.update(r['receive_orders'].keys())
            
            n_recv = len(r['receive_orders'])
            n_sd = len(r['subdag_executions'])
            n_exec = sum(len(v) for v in r['subdag_executions'].values())
            print(f"[PROGRESS]   Node {nid}: {n_recv} received, {n_sd} sub_dags, {n_exec} executed", flush=True)
        
        self.n = len(self.node_orders)
        print(f"[PROGRESS] Total: {self.n} nodes, {len(self.all_txs)} unique txs", flush=True)
        
        self._check_convergence()
        self._build_canonical_order()
    
    def _check_convergence(self):
        """Verify all nodes converged to same execution order per sub_dag."""
        print(f"[PROGRESS] Checking execution convergence...", flush=True)
        
        all_subdags = set()
        for execs in self.node_subdag_execs.values():
            all_subdags.update(execs.keys())
        
        if not all_subdags:
            self.execution_converged = False
            self.convergence_details = "No sub_dag executions found in any log"
            return
        
        mismatches = []
        for sd_id in sorted(all_subdags):
            # Find reference (first non-empty)
            ref_order, ref_node = None, None
            for nid, execs in sorted(self.node_subdag_execs.items()):
                if sd_id in execs and execs[sd_id]:
                    ref_order, ref_node = execs[sd_id], nid
                    break
            
            if ref_order is None:
                continue
            
            # Compare others
            for nid, execs in sorted(self.node_subdag_execs.items()):
                if sd_id in execs and execs[sd_id] and execs[sd_id] != ref_order:
                    mismatches.append(f"sub_dag {sd_id}: node {nid} differs from node {ref_node}")
                    if len(mismatches) >= 10:
                        break
            if len(mismatches) >= 10:
                break
        
        if mismatches:
            self.execution_converged = False
            self.convergence_details = "\n".join(mismatches[:10])
            if len(mismatches) > 10:
                self.convergence_details += f"\n... and {len(mismatches) - 10} more"
            print(f"[PROGRESS] ✗ NOT converged: {len(mismatches)} mismatches", flush=True)
        else:
            self.execution_converged = True
            print(f"[PROGRESS] ✓ All nodes converged", flush=True)
    
    def _build_canonical_order(self):
        """Build canonical tx list per sub_dag from first node with data."""
        print(f"[PROGRESS] Building canonical execution order...", flush=True)
        
        all_subdags = set()
        for execs in self.node_subdag_execs.values():
            all_subdags.update(execs.keys())
        
        for sd_id in sorted(all_subdags):
            for nid, execs in sorted(self.node_subdag_execs.items()):
                if sd_id in execs and execs[sd_id]:
                    self.subdag_txs[sd_id] = execs[sd_id]
                    break
        
        # Full order = sub_dag 1 txs ++ sub_dag 2 txs ++ ...
        self.full_exec_order = []
        for sd_id in sorted(self.subdag_txs.keys()):
            self.full_exec_order.extend(self.subdag_txs[sd_id])
        
        sizes = [len(txs) for txs in self.subdag_txs.values()]
        print(f"[PROGRESS] {len(self.subdag_txs)} sub_dags, {len(self.full_exec_order)} total executed", flush=True)
        if sizes:
            print(f"[PROGRESS] Sub_dag sizes: min={min(sizes)}, max={max(sizes)}, avg={sum(sizes)/len(sizes):.0f}", flush=True)
    
    def _detect_suspicious(self) -> List[Tuple[str, int, str]]:
        """Find txs received early by many nodes but never executed."""
        print(f"[PROGRESS] Detecting suspicious missing transactions...", flush=True)
        
        executed = set(self.full_exec_order)
        non_executed = self.all_txs - executed
        
        if not non_executed:
            print(f"[PROGRESS] All received txs were executed", flush=True)
            return []
        
        print(f"[PROGRESS] Checking {len(non_executed)} non-executed txs...", flush=True)
        
        # Find cutoff per node (position of last executed tx)
        cutoffs = {}
        for nid, orders in self.node_orders.items():
            last_pos = max((orders.get(tx, -1) for tx in self.full_exec_order), default=-1)
            if last_pos >= 0:
                cutoffs[nid] = last_pos
        
        suspicious = []
        min_nodes = self.n - self.f
        
        for tx in non_executed:
            n_recv, n_before = 0, 0
            for nid, orders in self.node_orders.items():
                if tx in orders:
                    n_recv += 1
                    if nid in cutoffs and orders[tx] < cutoffs[nid]:
                        n_before += 1
            
            if n_recv >= min_nodes and n_before >= min_nodes:
                suspicious.append((tx, n_recv, f"Received by {n_recv}/{self.n}, {n_before} before cutoff"))
        
        print(f"[PROGRESS] Found {len(suspicious)} suspicious missing", flush=True)
        return suspicious
    
    def validate(self) -> ValidationResult:
        """Run full validation with parallel sub_dag processing."""
        print(f"\n{'='*60}", flush=True)
        print(f"[PROGRESS] STARTING VALIDATION", flush=True)
        print(f"[PROGRESS] gamma={self.gamma}, f={self.f}, n={self.n}", flush=True)
        print(f"[PROGRESS] threshold = {self.n * (1 - self.gamma) + self.f + 1:.2f}", flush=True)
        print(f"{'='*60}\n", flush=True)
        
        if not self.node_orders:
            raise ValueError("No data loaded. Call parse_logs first.")
        
        # Detect suspicious missing
        suspicious = self._detect_suspicious()
        
        # Prepare parallel tasks
        tasks = [
            (sd_id, txs, self.node_orders, self.gamma, self.f, self.n)
            for sd_id, txs in sorted(self.subdag_txs.items())
        ]
        
        # Show what we're about to process
        print(f"\n[PROGRESS] Sub_dag breakdown:", flush=True)
        total_pairs = 0
        for sd_id, txs in sorted(self.subdag_txs.items()):
            n_txs = len(txs)
            n_pairs = n_txs * (n_txs - 1) // 2
            total_pairs += n_pairs
            print(f"[PROGRESS]   sub_dag {sd_id}: {n_txs} txs, {n_pairs:,} pairs", flush=True)
        
        print(f"\n[PROGRESS] TOTAL: {len(tasks)} sub_dags, {total_pairs:,} pairs", flush=True)
        print(f"[PROGRESS] Starting parallel validation...\n", flush=True)
        
        start = time.time()
        
        # Use imap_unordered to see results as they complete
        results = []
        with Pool() as p:
            for result in p.imap_unordered(validate_subdag, tasks):
                results.append(result)
                elapsed = time.time() - start
                print(f"[PROGRESS] Completed {len(results)}/{len(tasks)} sub_dags in {elapsed:.1f}s", flush=True)
        
        elapsed = time.time() - start
        print(f"\n[PROGRESS] All validation completed in {elapsed:.1f}s", flush=True)
        
        # Aggregate
        valid, invalid = 0, 0
        all_violations = []
        
        for r in results:
            if r.is_valid:
                valid += 1
            else:
                invalid += 1
                for tx1, tx2, reason in r.violations:
                    all_violations.append((r.sub_dag_id, tx1, tx2, reason))
                print(f"[PROGRESS] ✗ sub_dag {r.sub_dag_id}: {len(r.violations)} violations", flush=True)
        
        is_valid = invalid == 0 and self.execution_converged and len(suspicious) == 0
        
        print(f"\n{'='*60}", flush=True)
        print(f"[RESULT] {'✓ VALID' if is_valid else '✗ INVALID'}", flush=True)
        print(f"[RESULT] Valid: {valid}/{len(tasks)} sub_dags", flush=True)
        print(f"[RESULT] Violations: {len(all_violations)}", flush=True)
        print(f"[RESULT] Suspicious missing: {len(suspicious)}", flush=True)
        print(f"{'='*60}\n", flush=True)
        
        return ValidationResult(
            is_valid=is_valid,
            total_sub_dags=len(tasks),
            valid_sub_dags=valid,
            invalid_sub_dags=invalid,
            total_txs=len(self.full_exec_order),
            violations=all_violations,
            execution_converged=self.execution_converged,
            execution_mismatch_details=self.convergence_details,
            suspicious_missing=suspicious
        )


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Validate Themis batch-order-fairness property"
    )
    parser.add_argument(
        "--logs-dir", "-d", type=str, default="/var/scratch/mputnik/narwhal/benchmark/logs/",
        help="Directory containing worker-*-0.log files"
    )
    parser.add_argument(
        "--gamma", "-g", type=float, default=1.0,
        help="Order-fairness parameter γ (default: 1.0)"
    )
    parser.add_argument(
        "--f", type=int, default=0,
        help="Byzantine fault tolerance bound (default: 0)"
    )
    parser.add_argument(
        "--output", "-o", type=str,
        help="Output file for results (default: stdout)"
    )
    
    args = parser.parse_args()
    
    validator = OrderFairnessValidator(gamma=args.gamma, f=args.f)
    validator.parse_logs(args.logs_dir)
    result = validator.validate()
    
    output = str(result)
    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f"Results written to {args.output}")
    else:
        print(output)
    
    sys.exit(0 if result.is_valid else 1)


if __name__ == "__main__":
    main()