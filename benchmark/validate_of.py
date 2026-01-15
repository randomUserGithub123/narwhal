import re
import os
import sys
import argparse
import networkx as nx
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional
from pathlib import Path
from re import findall, search


@dataclass
class ValidationResult:
    """Results of batch-order-fairness validation."""
    is_valid: bool
    total_tx_pairs: int
    constrained_pairs: int  # Pairs with a definite order (not in same SCC)
    cycle_pairs: int        # Pairs in Condorcet cycles (arbitrary order allowed)
    violations: List[Tuple[str, str, str]]  # (tx1, tx2, reason)
    num_sccs: int
    largest_scc_size: int
    execution_converged: bool  # Did all nodes converge to same execution order?
    execution_mismatch_details: Optional[str] = None
    # Suspicious transactions: received by many nodes before cutoff but not executed
    suspicious_missing_txs: List[Tuple[str, int, float, str]] = field(default_factory=list)
    # Format: (tx_digest, nodes_that_received, avg_position_ratio, reason)
    
    def __str__(self):
        status = "✓ VALID" if self.is_valid else "✗ INVALID"
        converge_status = "✓ YES" if self.execution_converged else "✗ NO"
        suspicious_status = "✗ YES" if self.suspicious_missing_txs else "✓ NONE"
        s = f"""
╔══════════════════════════════════════════════════════════════╗
║          Batch-Order-Fairness Validation Result              ║
║                    (Themis Protocol)                         ║
╠══════════════════════════════════════════════════════════════╣
║  Status: {status:50} ║
║  Execution Converged: {converge_status:37} ║
║  Suspicious Missing Txs: {suspicious_status:34} ║
╠══════════════════════════════════════════════════════════════╣
║  Total transaction pairs analyzed: {self.total_tx_pairs:<24} ║
║  Pairs with ordering constraints:  {self.constrained_pairs:<24} ║
║  Pairs in Condorcet cycles:        {self.cycle_pairs:<24} ║
║  Number of SCCs:                   {self.num_sccs:<24} ║
║  Largest SCC size:                 {self.largest_scc_size:<24} ║
║  Violations found:                 {len(self.violations):<24} ║
╚══════════════════════════════════════════════════════════════╝
"""
        if not self.execution_converged and self.execution_mismatch_details:
            s += f"\nExecution Convergence Issue:\n{self.execution_mismatch_details}\n"
        
        if self.suspicious_missing_txs:
            s += f"\n⚠️  SUSPICIOUS MISSING TRANSACTIONS ({len(self.suspicious_missing_txs)}):\n"
            s += "    These txs were received by many/all nodes BEFORE the last executed tx,\n"
            s += "    but are NOT in the execution list. Possible censorship or bug!\n\n"
            for i, (tx, num_nodes, avg_pos_ratio, reason) in enumerate(self.suspicious_missing_txs[:10], 1):
                tx_short = tx[:24] + "..." if len(tx) > 24 else tx
                s += f"  {i}. {tx_short}\n"
                s += f"     Received by: {num_nodes} nodes, Avg position: {avg_pos_ratio:.1%} through receive order\n"
                s += f"     {reason}\n"
            if len(self.suspicious_missing_txs) > 10:
                s += f"  ... and {len(self.suspicious_missing_txs) - 10} more suspicious txs\n"
        
        if self.violations:
            s += "\nBatch-Order-Fairness Violations:\n"
            for i, (tx1, tx2, reason) in enumerate(self.violations[:20], 1):
                tx1_short = tx1[:16] + "..." if len(tx1) > 16 else tx1
                tx2_short = tx2[:16] + "..." if len(tx2) > 16 else tx2
                s += f"  {i}. {tx1_short} should come before {tx2_short}\n"
                s += f"     Reason: {reason}\n"
            if len(self.violations) > 20:
                s += f"  ... and {len(self.violations) - 20} more violations\n"
        return s


class BatchOrderFairnessValidator:
    """
    Validates batch-order-fairness property for DAG BFT consensus.
    
    Implements the Themis protocol validation:
    - Threshold: n * (1 - γ) + f + 1
    - When both directions meet threshold, higher count wins
    - SCCs (Condorcet cycles) allow arbitrary ordering
    
    Based on themis_protocol() from adv_reorder.py
    """
    
    # Regex patterns for log parsing (matching your format)
    RECEIVE_PATTERN = re.compile(
        r"Receival of tx_digest\s+([^ \n]+)\s+at position\s+(\d+)"
    )
    
    # Execution pattern matching your parser: r'Executed ([^ \n]+)'
    EXECUTED_PATTERN = re.compile(r"Executed\s+([^ \n]+)")
    
    # Finalization pattern
    FINALIZED_PATTERN = re.compile(
        r"sub_dag_id=(\d+):\s*FINALIZED!\s*(\d+)\s*transactions"
    )
    
    def __init__(self, gamma: float = 0.51, f: int = 0, verbose: bool = False,
                 executed_only: bool = True):
        """
        Initialize the validator.
        
        Args:
            gamma: Order-fairness parameter (0.5 < γ ≤ 1)
            f: Byzantine fault tolerance bound
            verbose: Print detailed progress information
            executed_only: If True, only consider transactions that were actually
                          executed (important when benchmark ends with SIGKILL,
                          leaving some received txs unfinalized)
        """
        if not (0.5 < gamma <= 1.0):
            raise ValueError("gamma must be in range (0.5, 1.0]")
        
        self.gamma = gamma
        self.f = f
        self.verbose = verbose
        self.executed_only = executed_only
        
        # Node receive orders: node_id -> {tx_digest -> position}
        self.node_orders: Dict[str, Dict[str, int]] = defaultdict(dict)
        
        # Execution order per node: node_id -> [tx_digests in order]
        self.node_execution_orders: Dict[str, List[str]] = defaultdict(list)
        
        # Canonical execution order (from first node or consensus)
        self.execution_order: List[str] = []
        
        # All unique transaction digests (from receive orders)
        self.all_txs: Set[str] = set()
        
        # Transactions to actually validate (may be filtered to executed only)
        self.txs_to_validate: Set[str] = set()
        
        # Number of nodes
        self.n: int = 0
        
        # Execution convergence status
        self.execution_converged: bool = True
        self.execution_mismatch_details: Optional[str] = None
        
    def _log(self, msg: str):
        """Print message if verbose mode is enabled."""
        if self.verbose:
            print(f"[INFO] {msg}")
    
    def parse_logs_dir(self, logs_dir: str):
        """
        Parse all worker logs from directory.
        
        Expects files named: worker-X-0.log where X is node index
        
        Args:
            logs_dir: Directory containing worker log files
        """
        logs_path = Path(logs_dir)
        
        # Find worker log files: worker-X-0.log
        log_files = sorted(logs_path.glob("worker-*-0.log"))
        
        if not log_files:
            raise FileNotFoundError(
                f"No worker log files found in {logs_dir}. "
                f"Expected format: worker-X-0.log"
            )
        
        self._log(f"Found {len(log_files)} worker log files")
        
        for log_file in log_files:
            # Extract node ID from filename (e.g., "worker-0-0.log" -> "0")
            match = re.search(r'worker-(\d+)-', log_file.name)
            if match:
                node_id = match.group(1)
            else:
                node_id = log_file.stem
            
            self._parse_single_worker_log(node_id, log_file)
        
        self.n = len(self.node_orders)
        self._log(f"Parsed {self.n} nodes, {len(self.all_txs)} unique transactions")
        
        # Check execution convergence and set canonical execution order
        self._check_execution_convergence()
        
    def _parse_single_worker_log(self, node_id: str, log_path: Path):
        """
        Parse a single worker's log file for receive orders and execution.
        
        Based on your parsing code pattern.
        """
        self._log(f"Parsing {log_path} as node {node_id}")
        
        with open(log_path, 'r') as f:
            log_content = f.read()
        
        # Parse receive orders
        for match in self.RECEIVE_PATTERN.finditer(log_content):
            tx_digest = match.group(1).strip()
            position = int(match.group(2))
            
            if tx_digest not in self.node_orders[node_id]:
                self.node_orders[node_id][tx_digest] = position
                self.all_txs.add(tx_digest)
        
        # Parse execution order (rank by order of appearance)
        # Matching your parser: keeps earliest rank if tx appears multiple times
        exec_order = []
        seen_txs = set()
        for line in log_content.splitlines():
            match = self.EXECUTED_PATTERN.search(line)
            if match:
                tx = match.group(1).strip()
                if tx not in seen_txs:
                    exec_order.append(tx)
                    seen_txs.add(tx)
        
        self.node_execution_orders[node_id] = exec_order
        
        self._log(f"  Node {node_id}: {len(self.node_orders[node_id])} receive orders, "
                  f"{len(exec_order)} executed txs")
    
    def _check_execution_convergence(self):
        """
        Check that all nodes converged to the same execution order.
        Also sets txs_to_validate based on executed_only flag.
        """
        if not self.node_execution_orders:
            self.execution_converged = False
            self.execution_mismatch_details = "No execution orders found in any log"
            return
        
        # Get reference execution order (from first node with data)
        reference_node = None
        reference_order = None
        
        for node_id, exec_order in sorted(self.node_execution_orders.items()):
            if exec_order:
                reference_node = node_id
                reference_order = exec_order
                break
        
        if reference_order is None:
            self.execution_converged = False
            self.execution_mismatch_details = "All nodes have empty execution orders"
            return
        
        self.execution_order = reference_order
        
        # Compare all nodes to reference
        mismatches = []
        for node_id, exec_order in sorted(self.node_execution_orders.items()):
            if not exec_order:
                continue
                
            if exec_order != reference_order:
                # Find first difference
                min_len = min(len(exec_order), len(reference_order))
                for i in range(min_len):
                    if exec_order[i] != reference_order[i]:
                        mismatches.append(
                            f"Node {node_id} differs from node {reference_node} at position {i}: "
                            f"{exec_order[i][:20]}... vs {reference_order[i][:20]}..."
                        )
                        break
                else:
                    mismatches.append(
                        f"Node {node_id} has different length: {len(exec_order)} vs {len(reference_order)}"
                    )
        
        if mismatches:
            self.execution_converged = False
            self.execution_mismatch_details = "\n".join(mismatches[:5])
            if len(mismatches) > 5:
                self.execution_mismatch_details += f"\n... and {len(mismatches) - 5} more mismatches"
        else:
            self.execution_converged = True
            self._log(f"All {len(self.node_execution_orders)} nodes converged to same execution order")
        
        # Set transactions to validate based on executed_only flag
        if self.executed_only:
            self.txs_to_validate = set(self.execution_order)
            not_executed = len(self.all_txs) - len(self.txs_to_validate)
            self._log(f"executed_only=True: validating {len(self.txs_to_validate)} executed txs "
                      f"(ignoring {not_executed} received-but-not-executed txs)")
        else:
            self.txs_to_validate = self.all_txs.copy()
            self._log(f"executed_only=False: validating all {len(self.txs_to_validate)} received txs")
    
    def _normalize_digest(self, digest: str) -> str:
        """Normalize transaction digest string."""
        digest = digest.strip()
        digest = digest.strip('"\'[](){}<>')
        digest = re.sub(r'^Digest\(', '', digest)
        digest = digest.rstrip(')')
        return digest
    
    def _compute_threshold(self) -> float:
        """
        Compute the Themis threshold for edge creation.
        
        From adv_reorder.py themis_protocol():
            threshold = n * (1-gamma) + f + 1
        """
        threshold = self.n * (1 - self.gamma) + self.f + 1
        self._log(f"Themis threshold for n={self.n}, γ={self.gamma}, f={self.f}: {threshold}")
        return threshold
    
    def detect_suspicious_missing_txs(self) -> List[Tuple[str, int, float, str]]:
        """
        Detect transactions that were received by many nodes BEFORE the last
        executed transaction, but are NOT in the execution list.
        
        This could indicate:
        - Censorship attack
        - Liveness bug
        - Protocol violation
        
        Returns:
            List of (tx_digest, num_nodes_received, avg_position_ratio, reason)
        """
        if not self.execution_order:
            return []
        
        executed_set = set(self.execution_order)
        non_executed = self.all_txs - executed_set
        
        if not non_executed:
            self._log("No non-executed transactions to check for suspicious missing")
            return []
        
        self._log(f"Checking {len(non_executed)} non-executed txs for suspicious missing...")
        
        # For each node, find the position of the LAST executed transaction
        # This is our "cutoff" - anything received before this should have been executed
        node_cutoffs: Dict[str, int] = {}
        node_max_positions: Dict[str, int] = {}
        
        for node_id, orders in self.node_orders.items():
            max_pos = max(orders.values()) if orders else 0
            node_max_positions[node_id] = max_pos
            
            # Find position of last executed tx at this node
            last_exec_pos = -1
            for tx in self.execution_order:
                if tx in orders:
                    last_exec_pos = max(last_exec_pos, orders[tx])
            
            if last_exec_pos >= 0:
                node_cutoffs[node_id] = last_exec_pos
        
        suspicious = []
        
        for tx in non_executed:
            nodes_received = []
            positions_before_cutoff = []
            position_ratios = []
            
            for node_id, orders in self.node_orders.items():
                if tx in orders:
                    pos = orders[tx]
                    nodes_received.append(node_id)
                    
                    # Check if this tx was received BEFORE the last executed tx
                    if node_id in node_cutoffs:
                        cutoff = node_cutoffs[node_id]
                        if pos < cutoff:
                            positions_before_cutoff.append(node_id)
                        
                        # Calculate position ratio (how early in the receive order)
                        max_pos = node_max_positions.get(node_id, cutoff)
                        if max_pos > 0:
                            position_ratios.append(pos / max_pos)
            
            num_received = len(nodes_received)
            num_before_cutoff = len(positions_before_cutoff)
            
            # Suspicious if:
            # 1. Received by at least (n - f) nodes (or all nodes)
            # 2. AND received BEFORE the cutoff at most of those nodes
            min_nodes_for_suspicion = self.n - self.f
            
            if num_received >= min_nodes_for_suspicion and num_before_cutoff >= min_nodes_for_suspicion:
                avg_pos_ratio = sum(position_ratios) / len(position_ratios) if position_ratios else 1.0
                
                reason = (f"Received by {num_received}/{self.n} nodes, "
                         f"{num_before_cutoff} received it BEFORE last executed tx")
                
                suspicious.append((tx, num_received, avg_pos_ratio, reason))
                
                self._log(f"  SUSPICIOUS: {tx[:32]}... - {reason}")
        
        # Sort by average position ratio (earlier = more suspicious)
        suspicious.sort(key=lambda x: x[2])
        
        if suspicious:
            self._log(f"Found {len(suspicious)} suspicious missing transactions!")
        else:
            self._log("No suspicious missing transactions found")
        
        return suspicious
    
    def _count_orderings(self, tx1: str, tx2: str) -> Tuple[int, int]:
        """
        Count how many nodes received tx1 before tx2 and vice versa.
        
        Returns:
            (count_tx1_before_tx2, count_tx2_before_tx1)
        """
        count_1_before_2 = 0
        count_2_before_1 = 0
        
        for node_id, orders in self.node_orders.items():
            pos1 = orders.get(tx1)
            pos2 = orders.get(tx2)
            
            if pos1 is not None and pos2 is not None:
                if pos1 < pos2:
                    count_1_before_2 += 1
                elif pos2 < pos1:
                    count_2_before_1 += 1
        
        return count_1_before_2, count_2_before_1
    
    def build_order_graph(self) -> nx.DiGraph:
        """
        Build the order graph using Themis protocol rules.
        
        Only considers transactions in txs_to_validate (which respects
        the executed_only flag - important for SIGKILL scenarios).
        
        Direct port from adv_reorder.py themis_protocol():
        
        for first, second in pair_indices:
            if total_counts[(first,second)] >= threshold and total_counts[(second, first)] >= threshold:
                if total_counts[(first,second)] >= total_counts[(second,first)]:
                    graphs[index].add_edge(*(first,second))
                else:
                    graphs[index].add_edge(*(second,first))
            elif total_counts[(first,second)] >= threshold:
                graphs[index].add_edge(*(first,second))
            elif total_counts[(second,first)] >= threshold:
                graphs[index].add_edge(*(second,first))
        
        Returns:
            NetworkX DiGraph representing the order constraints
        """
        G = nx.DiGraph()
        G.add_nodes_from(self.txs_to_validate)
        
        threshold = self._compute_threshold()
        tx_list = list(self.txs_to_validate)
        num_txs = len(tx_list)
        
        self._log(f"Building Themis order graph for {num_txs} transactions...")
        
        edges_added = 0
        both_threshold = 0
        single_threshold = 0
        no_edge = 0
        
        for i, tx1 in enumerate(tx_list):
            for tx2 in tx_list[i+1:]:
                count_1_2, count_2_1 = self._count_orderings(tx1, tx2)
                
                # Themis protocol edge addition (exact logic from adv_reorder.py)
                if count_1_2 >= threshold and count_2_1 >= threshold:
                    # Both meet threshold - higher count wins
                    both_threshold += 1
                    if count_1_2 >= count_2_1:
                        G.add_edge(tx1, tx2)
                    else:
                        G.add_edge(tx2, tx1)
                    edges_added += 1
                elif count_1_2 >= threshold:
                    G.add_edge(tx1, tx2)
                    edges_added += 1
                    single_threshold += 1
                elif count_2_1 >= threshold:
                    G.add_edge(tx2, tx1)
                    edges_added += 1
                    single_threshold += 1
                else:
                    no_edge += 1
        
        self._log(f"Order graph: {G.number_of_nodes()} nodes, {edges_added} edges")
        self._log(f"  Both met threshold (tiebreak): {both_threshold}")
        self._log(f"  Single direction threshold: {single_threshold}")
        self._log(f"  No edge (neither met threshold): {no_edge}")
        
        return G
    
    def validate(self) -> ValidationResult:
        """
        Validate that execution order respects Themis batch-order-fairness.
        
        Also detects suspicious missing transactions (received by many nodes
        before the cutoff, but not executed).
        
        Only considers transactions in txs_to_validate (respects executed_only flag).
        
        Returns:
            ValidationResult with detailed validation information
        """
        if not self.node_orders:
            raise ValueError("No node orders loaded. Call parse_logs_dir first.")
        if not self.execution_order:
            raise ValueError("No execution order found in logs.")
        if not self.txs_to_validate:
            raise ValueError("No transactions to validate (txs_to_validate is empty).")
        
        # First, detect suspicious missing transactions
        suspicious_missing = self.detect_suspicious_missing_txs()
        
        # Build order graph using Themis protocol (only for txs_to_validate)
        G = self.build_order_graph()
        
        # Find strongly connected components (Condorcet cycles)
        # From adv_reorder.py:
        #   SCC = list(nx.strongly_connected_components(G))
        #   H = nx.algorithms.components.condensation(G, SCC)
        sccs = list(nx.strongly_connected_components(G))
        self._log(f"Found {len(sccs)} strongly connected components")
        
        scc_sizes = [len(scc) for scc in sccs]
        largest_scc = max(scc_sizes) if scc_sizes else 0
        
        if largest_scc > 1:
            large_sccs = [s for s in scc_sizes if s > 1]
            self._log(f"  {len(large_sccs)} SCCs with size > 1 (Condorcet cycles)")
            self._log(f"  Largest SCC: {largest_scc} transactions")
        
        # Map each tx to its SCC index
        tx_to_scc: Dict[str, int] = {}
        for scc_idx, scc in enumerate(sccs):
            for tx in scc:
                tx_to_scc[tx] = scc_idx
        
        # Build condensation graph (DAG of SCCs)
        H = nx.algorithms.components.condensation(G, sccs)
        
        # Build execution position map
        exec_pos: Dict[str, int] = {
            tx: pos for pos, tx in enumerate(self.execution_order)
        }
        
        # Validate: check if execution respects the SCC DAG ordering
        violations = []
        total_pairs = 0
        constrained_pairs = 0
        cycle_pairs = 0
        
        # Only check transactions in txs_to_validate that were also executed
        validated_txs = [tx for tx in self.txs_to_validate if tx in exec_pos]
        
        for i, tx1 in enumerate(validated_txs):
            for tx2 in validated_txs[i+1:]:
                total_pairs += 1
                
                scc1 = tx_to_scc.get(tx1)
                scc2 = tx_to_scc.get(tx2)
                
                if scc1 is None or scc2 is None:
                    continue
                
                if scc1 == scc2:
                    # Same SCC (Condorcet cycle) - any order is valid
                    cycle_pairs += 1
                    continue
                
                constrained_pairs += 1
                
                # Check if there's a path in condensation graph
                # If scc1 -> scc2 in H, then tx1 must come before tx2 in execution
                # If scc2 -> scc1 in H, then tx2 must come before tx1 in execution
                
                if nx.has_path(H, scc1, scc2):
                    # tx1 should come before tx2
                    if exec_pos[tx1] > exec_pos[tx2]:
                        count_1_2, count_2_1 = self._count_orderings(tx1, tx2)
                        violations.append((
                            tx1, tx2,
                            f"Constraint: {count_1_2}/{self.n} nodes saw tx1 first, "
                            f"threshold={self._compute_threshold():.1f}"
                        ))
                elif nx.has_path(H, scc2, scc1):
                    # tx2 should come before tx1
                    if exec_pos[tx2] > exec_pos[tx1]:
                        count_1_2, count_2_1 = self._count_orderings(tx1, tx2)
                        violations.append((
                            tx2, tx1,
                            f"Constraint: {count_2_1}/{self.n} nodes saw tx2 first, "
                            f"threshold={self._compute_threshold():.1f}"
                        ))
        
        # is_valid requires: no violations, execution converged, AND no suspicious missing
        is_valid = (len(violations) == 0 and 
                    self.execution_converged and 
                    len(suspicious_missing) == 0)
        
        return ValidationResult(
            is_valid=is_valid,
            total_tx_pairs=total_pairs,
            constrained_pairs=constrained_pairs,
            cycle_pairs=cycle_pairs,
            violations=violations,
            num_sccs=len(sccs),
            largest_scc_size=largest_scc,
            execution_converged=self.execution_converged,
            execution_mismatch_details=self.execution_mismatch_details,
            suspicious_missing_txs=suspicious_missing
        )
    
    def get_statistics(self) -> Dict:
        """Get detailed statistics about the ordering data."""
        stats = {
            'num_nodes': self.n,
            'num_transactions_received': len(self.all_txs),
            'num_transactions_executed': len(self.execution_order),
            'num_transactions_validated': len(self.txs_to_validate),
            'executed_only': self.executed_only,
            'gamma': self.gamma,
            'f': self.f,
            'threshold': self._compute_threshold() if self.n > 0 else None,
            'execution_converged': self.execution_converged,
        }
        
        # Transactions received but not executed (due to SIGKILL etc)
        if self.executed_only:
            not_executed = len(self.all_txs) - len(self.txs_to_validate)
            stats['txs_ignored_not_executed'] = not_executed
        
        # Transaction coverage per node
        if self.node_orders:
            coverage = [len(orders) for orders in self.node_orders.values()]
            stats['min_txs_per_node'] = min(coverage)
            stats['max_txs_per_node'] = max(coverage)
            stats['avg_txs_per_node'] = sum(coverage) / len(coverage)
        
        return stats
    
    def print_order_analysis(self, tx1: str, tx2: str):
        """Print detailed analysis of ordering between two transactions."""
        count_1_2, count_2_1 = self._count_orderings(tx1, tx2)
        threshold = self._compute_threshold()
        
        print(f"\nOrder Analysis: {tx1[:20]}... vs {tx2[:20]}...")
        print(f"  Nodes seeing tx1 first: {count_1_2}/{self.n}")
        print(f"  Nodes seeing tx2 first: {count_2_1}/{self.n}")
        print(f"  Threshold: {threshold:.2f}")
        
        if count_1_2 >= threshold and count_2_1 >= threshold:
            winner = "tx1 -> tx2" if count_1_2 >= count_2_1 else "tx2 -> tx1"
            print(f"  Both meet threshold, tiebreak: {winner}")
        elif count_1_2 >= threshold:
            print(f"  Edge: tx1 -> tx2")
        elif count_2_1 >= threshold:
            print(f"  Edge: tx2 -> tx1")
        else:
            print(f"  No edge (neither meets threshold)")


def main():
    parser = argparse.ArgumentParser(
        description="Validate Themis batch-order-fairness property in DAG BFT logs"
    )
    parser.add_argument(
        "--logs-dir", "-d",
        type=str,
        required=True,
        help="Directory containing worker-X-0.log files"
    )
    parser.add_argument(
        "--gamma", "-g",
        type=float,
        default=1.0,
        help="Order-fairness parameter γ (default: 0.51)"
    )
    parser.add_argument(
        "--f",
        type=int,
        default=1,
        help="Byzantine fault tolerance bound (default: 0)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed progress information"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Output file for results (default: stdout)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print detailed statistics"
    )
    parser.add_argument(
        "--all-received",
        action="store_true",
        help="Validate ALL received transactions, not just executed ones. "
             "By default (without this flag), only executed transactions are "
             "validated. This is important when benchmark ends with SIGKILL, "
             "leaving some received txs unfinalized."
    )
    
    args = parser.parse_args()
    
    # executed_only is True by default (safe for SIGKILL scenarios)
    # --all-received flag disables this
    executed_only = not args.all_received
    
    # Create validator
    validator = BatchOrderFairnessValidator(
        gamma=args.gamma,
        f=args.f,
        verbose=args.verbose,
        executed_only=executed_only
    )
    
    # Parse logs
    validator.parse_logs_dir(args.logs_dir)
    
    # Print statistics if requested
    if args.stats or args.verbose:
        stats = validator.get_statistics()
        print("\n=== Themis Order Statistics ===")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        print()
    
    # Validate
    try:
        result = validator.validate()
        
        # Output results
        output = str(result)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            print(f"Results written to {args.output}")
        else:
            print(output)
        
        # Exit with appropriate code
        sys.exit(0 if result.is_valid else 1)
        
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()