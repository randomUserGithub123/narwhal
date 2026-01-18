import re
import sys
import glob
from collections import defaultdict
from pathlib import Path


def parse_primary_keys(log_dir: str) -> dict:
    """Parse primary-X.log files to extract public key -> node ID mapping."""
    
    # Pattern: "Primary QtKsQzq8zr87APxY listening to primary messages"
    primary_pattern = re.compile(
        r'Primary (\S+) listening to primary messages'
    )
    
    pubkey_to_node = {}
    
    log_files = sorted(glob.glob(f"{log_dir}/primary-*.log"))
    
    for log_file in log_files:
        # Extract node ID from filename (e.g., "primary-0.log" -> 0)
        match = re.search(r'primary-(\d+)\.log', log_file)
        if not match:
            continue
        node_id = int(match.group(1))
        
        with open(log_file, 'r') as f:
            for line in f:
                match = primary_pattern.search(line)
                if match:
                    pubkey = match.group(1)
                    pubkey_to_node[pubkey] = node_id
                    print(f"  Node {node_id}: {pubkey}")
                    break
    
    return pubkey_to_node


def find_node_for_pubkey(author_key: str, pubkey_to_node: dict) -> int:
    """Find node ID for a public key (handles truncated keys)."""
    for full_key, node_id in pubkey_to_node.items():
        if full_key.startswith(author_key) or author_key.startswith(full_key):
            return node_id
    return None


def parse_all_receive_orders(log_dir: str) -> dict:
    """Parse receive orders from ALL worker-X-0.log files.
    
    Returns: dict mapping node_id -> {tx_digest -> receive_position}
    """
    
    receive_pattern = re.compile(
        r'Receival of tx_digest (\S+) at position (\d+)'
    )
    
    all_receive_orders = {}
    
    log_files = sorted(glob.glob(f"{log_dir}/worker-*-0.log"))
    
    for log_file in log_files:
        match = re.search(r'worker-(\d+)-0\.log', log_file)
        if not match:
            continue
        node_id = int(match.group(1))
        
        receive_order = {}
        
        with open(log_file, 'r') as f:
            for line in f:
                match = receive_pattern.search(line)
                if match:
                    tx_digest = match.group(1)
                    position = int(match.group(2))
                    receive_order[tx_digest] = position
        
        all_receive_orders[node_id] = receive_order
        print(f"  Node {node_id}: {len(receive_order)} tx_digests received")
    
    return all_receive_orders


def parse_local_orders_from_subdags(log_dir: str, pubkey_to_node: dict) -> dict:
    """Parse LocalOrders logged after sub-dag receive from all worker logs.
    
    Returns: dict mapping verifier_node_id -> list of (sub_dag_id, author_node_id, author_key, seq, tx_digests)
    """
    
    local_order_pattern = re.compile(
        r'sub_dag_id=(\d+): Author (\S+), LocalOrder seq=(\d+), lo_digest=(\S+), tx_digests=\[([^\]]*)\]'
    )
    
    all_local_orders = {}
    
    log_files = sorted(glob.glob(f"{log_dir}/worker-*-0.log"))
    
    for log_file in log_files:
        match = re.search(r'worker-(\d+)-0\.log', log_file)
        if not match:
            continue
        verifier_node_id = int(match.group(1))
        
        local_orders = []
        
        with open(log_file, 'r') as f:
            for line in f:
                match = local_order_pattern.search(line)
                if match:
                    sub_dag_id = int(match.group(1))
                    author_key = match.group(2)
                    seq = int(match.group(3))
                    lo_digest = match.group(4)
                    tx_digests_str = match.group(5)
                    
                    # Find author's node ID
                    author_node_id = find_node_for_pubkey(author_key, pubkey_to_node)
                    
                    # Parse tx_digests list
                    tx_digests = []
                    if tx_digests_str.strip():
                        for td in tx_digests_str.split(', '):
                            td = td.strip()
                            if td:
                                tx_digests.append(td)
                    
                    local_orders.append({
                        'sub_dag_id': sub_dag_id,
                        'author_node_id': author_node_id,
                        'author_key': author_key,
                        'seq': seq,
                        'lo_digest': lo_digest,
                        'tx_digests': tx_digests,
                    })
        
        all_local_orders[verifier_node_id] = local_orders
        print(f"  Node {verifier_node_id}: {len(local_orders)} LocalOrders logged")
    
    return all_local_orders


def verify_local_orders(all_receive_orders: dict, all_local_orders: dict, pubkey_to_node: dict):
    """Verify that each LocalOrder matches the AUTHOR's receive order."""
    
    all_passed = True
    total_checks = 0
    total_failures = 0
    
    for verifier_node_id, local_orders in sorted(all_local_orders.items()):
        print(f"\n{'='*70}")
        print(f"Verifying LocalOrders logged by Node {verifier_node_id}")
        print(f"{'='*70}")
        
        # Group by sub_dag_id
        by_subdag = defaultdict(list)
        for lo in local_orders:
            by_subdag[lo['sub_dag_id']].append(lo)
        
        node_failures = 0
        node_checks = 0
        
        for sub_dag_id in sorted(by_subdag.keys()):
            print(f"\n  Sub-dag {sub_dag_id}:")
            
            for lo in sorted(by_subdag[sub_dag_id], key=lambda x: (x['author_node_id'] or 999, x['seq'])):
                author_node_id = lo['author_node_id']
                author_key = lo['author_key']
                seq = lo['seq']
                tx_digests = lo['tx_digests']
                
                author_node_str = f"Node {author_node_id}" if author_node_id is not None else "Unknown"
                
                if not tx_digests:
                    print(f"    [{author_node_str}] Author {author_key[:20]}... seq={seq}: (empty)")
                    continue
                
                # Get the AUTHOR's receive order (NOT the verifier's)
                if author_node_id is None:
                    print(f"    [{author_node_str}] Author {author_key[:20]}... seq={seq}: SKIP (unknown author node)")
                    continue
                
                if author_node_id not in all_receive_orders:
                    print(f"    [{author_node_str}] Author {author_key[:20]}... seq={seq}: SKIP (no receive log for author)")
                    continue
                
                author_receive_order = all_receive_orders[author_node_id]
                
                # Get positions from AUTHOR's receive order
                positions = []
                unknown_digests = []
                
                for idx, td in enumerate(tx_digests):
                    if td in author_receive_order:
                        positions.append((td, idx, author_receive_order[td]))
                    else:
                        unknown_digests.append((td, idx))
                
                # Check if positions are monotonically increasing
                is_ordered = True
                violations = []
                
                for i in range(1, len(positions)):
                    prev_td, prev_lo_idx, prev_author_pos = positions[i-1]
                    curr_td, curr_lo_idx, curr_author_pos = positions[i]
                    
                    if curr_author_pos < prev_author_pos:
                        is_ordered = False
                        violations.append({
                            'prev_td': prev_td,
                            'prev_lo_idx': prev_lo_idx,
                            'prev_author_pos': prev_author_pos,
                            'curr_td': curr_td,
                            'curr_lo_idx': curr_lo_idx,
                            'curr_author_pos': curr_author_pos,
                        })
                
                node_checks += 1
                total_checks += 1
                
                status = "✓ PASS" if is_ordered else "✗ FAIL"
                known_count = len(positions)
                unknown_count = len(unknown_digests)
                
                print(f"    [{author_node_str}] Author {author_key[:20]}... seq={seq}: {status} "
                      f"({known_count} known, {unknown_count} unknown)")
                
                if not is_ordered:
                    node_failures += 1
                    total_failures += 1
                    all_passed = False
                    
                    print(f"\n      VIOLATIONS FOUND ({len(violations)} total):")
                    print(f"      Checking against Node {author_node_id}'s receive order")
                    print(f"      Legend: lo_idx = index in LocalOrder, author_recv_pos = position in Node {author_node_id}'s receive log")
                    print()
                    
                    for v_idx, v in enumerate(violations[:3]):
                        print(f"      Violation #{v_idx + 1}:")
                        print(f"        BEFORE: {v['prev_td'][:50]}...")
                        print(f"                lo_idx={v['prev_lo_idx']}, author_recv_pos={v['prev_author_pos']}")
                        print(f"        AFTER:  {v['curr_td'][:50]}...")
                        print(f"                lo_idx={v['curr_lo_idx']}, author_recv_pos={v['curr_author_pos']} <- OUT OF ORDER!")
                        print(f"        Issue: In Node {author_node_id}'s receive log, tx at lo_idx={v['curr_lo_idx']} was received")
                        print(f"               at position {v['curr_author_pos']}, but tx at lo_idx={v['prev_lo_idx']} was received")
                        print(f"               at position {v['prev_author_pos']} (should be earlier)")
                        print()
                    
                    if len(violations) > 3:
                        print(f"      ... and {len(violations) - 3} more violations")
                    print()
                
                if unknown_digests:
                    print(f"      Unknown digests (not in Node {author_node_id}'s receive log):")
                    for ud, ud_idx in unknown_digests[:3]:
                        print(f"        lo_idx={ud_idx}: {ud[:50]}...")
                    if len(unknown_digests) > 3:
                        print(f"        ... and {len(unknown_digests) - 3} more")
        
        print(f"\n  Node {verifier_node_id} summary: {node_checks - node_failures}/{node_checks} passed")
    
    return all_passed, total_checks, total_failures


def main():
    if len(sys.argv) < 2:
        log_dir = "."
    else:
        log_dir = sys.argv[1]
    
    print(f"Log directory: {log_dir}")
    
    # Step 1: Extract public key -> node ID mapping
    print(f"\n{'='*70}")
    print("STEP 1: EXTRACTING NODE PUBLIC KEYS FROM PRIMARY LOGS")
    print(f"{'='*70}")
    pubkey_to_node = parse_primary_keys(log_dir)
    
    if not pubkey_to_node:
        print("ERROR: No primary public keys found!")
        sys.exit(1)
    
    # Step 2: Parse receive orders from ALL nodes
    print(f"\n{'='*70}")
    print("STEP 2: PARSING RECEIVE ORDERS FROM ALL WORKER LOGS")
    print(f"{'='*70}")
    all_receive_orders = parse_all_receive_orders(log_dir)
    
    # Step 3: Parse LocalOrders logged after sub-dag receive
    print(f"\n{'='*70}")
    print("STEP 3: PARSING LOCAL ORDERS FROM SUB-DAG PROCESSING")
    print(f"{'='*70}")
    all_local_orders = parse_local_orders_from_subdags(log_dir, pubkey_to_node)
    
    # Step 4: Verify each LocalOrder against the AUTHOR's receive order
    print(f"\n{'='*70}")
    print("STEP 4: VERIFYING LOCAL ORDERS AGAINST AUTHOR'S RECEIVE ORDER")
    print(f"{'='*70}")
    all_passed, total_checks, total_failures = verify_local_orders(
        all_receive_orders, all_local_orders, pubkey_to_node
    )
    
    # Final summary
    print(f"\n{'='*70}")
    print("FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"Total LocalOrders checked: {total_checks}")
    print(f"Total failures: {total_failures}")
    print(f"Total passed: {total_checks - total_failures}")
    
    print(f"\nNode mapping:")
    for pubkey, node_id in sorted(pubkey_to_node.items(), key=lambda x: x[1]):
        print(f"  Node {node_id}: {pubkey}")
    
    if all_passed:
        print("\n✓ ALL CHECKS PASSED - LocalOrders match author's receive order!")
        sys.exit(0)
    else:
        print(f"\n✗ {total_failures} FAILURES - LocalOrders don't match author's receive order!")
        sys.exit(1)


if __name__ == "__main__":
    main()