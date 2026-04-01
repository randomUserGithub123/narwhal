# Copyright(C) Facebook, Inc. and its affiliates.
# Modified for FairDAG-RL v4: FairDAG metrics parsed from worker logs.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
from collections import defaultdict
import random

from benchmark.utils import Print


def _to_posix(string):
    x = datetime.fromisoformat(string.replace('Z', '+00:00'))
    return datetime.timestamp(x)


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, attack_type=10, arbitragers=1, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.attack_type = attack_type
        self.arbitragers = arbitragers
        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries)
            self.workers = len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        self.all_sent_samples = {}
        for d in self.sent_samples:
            self.all_sent_samples.update(d)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        (proposals, commits, self.configs, primary_ips, blocks_to_heights,
         monitor_attacks, fissure_attacks, sluggish_attacks, speculative_attacks,
        ) = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.blocks_to_heights = self._merge_blocks_to_heights([x.items() for x in blocks_to_heights])
        self.monitor_attacks = self._merge_monitor([x.items() for x in monitor_attacks])
        self.fissure_attacks = self._merge_frontrun_attack([x.items() for x in fissure_attacks])
        self.sluggish_attacks = self._merge_frontrun_attack([x.items() for x in sluggish_attacks])
        self.speculative_attacks = self._merge_frontrun_attack([x.items() for x in speculative_attacks])

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips, fair_ordered_txs_list, fair_ordered_seqs_list, fair_graph_stats_list, arrival_times_list = zip(*results)
        self.sizes = {k: v for x in sizes for k, v in x.items() if k in self.commits}
        self.all_received_samples = self._merge_dicts(self.received_samples)

        self.fair_ordered_txs = self._merge_results([x.items() for x in fair_ordered_txs_list])

        # Protocol's final ordering (position in log, not timestamp).
        # Take from first worker that has it (all workers finalize same order).
        self.fair_ordered_seqs = {}
        for seqs in fair_ordered_seqs_list:
            if seqs and not self.fair_ordered_seqs:
                self.fair_ordered_seqs = seqs

        self.fair_graph_stats = []
        for stats_list in fair_graph_stats_list:
            self.fair_graph_stats.extend(stats_list)

        # Adversarial reordering: build per-NODE arrival times.
        workers_per_node = self.workers if isinstance(self.workers, int) else 1
        n_nodes = self.committee_size if isinstance(self.committee_size, int) else len(primaries)
        self.per_node_arrivals = []
        for node_idx in range(n_nodes):
            node_arrivals = {}
            for w_offset in range(workers_per_node):
                w_idx = node_idx * workers_per_node + w_offset
                if w_idx < len(arrival_times_list):
                    for tx_uid, t in arrival_times_list[w_idx].items():
                        if tx_uid not in node_arrivals or t < node_arrivals[tx_uid]:
                            node_arrivals[tx_uid] = t
            self.per_node_arrivals.append(node_arrivals)

        self.collocate = set(primary_ips) == set(workers_ips)
        if self.misses != 0:
            Print.warn(f'Clients missed their target rate {self.misses:,} time(s)')

    # ---- Merge helpers ----

    @staticmethod
    def _merge_dicts(input):
        merged_dict = defaultdict(list)
        for d in input:
            for k, v in d.items():
                merged_dict[k].append(v)
        return dict(merged_dict)

    @staticmethod
    def _merge_results(input):
        merged = {}
        for x in input:
            for k, v in x:
                if k not in merged or merged[k] > v:
                    merged[k] = v
        return merged

    @staticmethod
    def _merge_blocks_to_heights(input):
        merged = {}
        for x in input:
            for block, height in x:
                if block not in merged:
                    merged[block] = int(height)
        return merged

    @staticmethod
    def _merge_monitor(input):
        merged = {}
        for x in input:
            for victim_header, attack_header in x:
                if victim_header not in merged:
                    merged[victim_header] = attack_header
        return merged

    @staticmethod
    def _merge_frontrun_attack(input):
        merged = {}
        for x in input:
            for victim_header, attack_header in x:
                if victim_header not in merged:
                    merged[victim_header] = attack_header
        return merged

    # ---- Log parsers ----

    def _parse_clients(self, log):
        size = int(search(r"Transactions size: (\d+)", log).group(1))
        rate = int(search(r"Transactions rate: (\d+)", log).group(1))
        tmp = search(r"\[(.*Z) .* Start ", log).group(1)
        start = _to_posix(tmp)
        misses = len(findall(r"rate too high", log))
        tmp = findall(r"\[(.*Z) .* Sending transaction (\d+)", log)
        samples = {int(s): _to_posix(t) for t, s in tmp}
        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        tmp = findall(r"\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)", log)
        tmp = [(d, _to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])
        tmp = findall(r"\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)", log)
        tmp = [(d, _to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])
        tmp = findall(r"\[(.*Z) .* FairDag Committed ([^ ]+) in height (\d+)", log)
        tmp = [(header, height) for t, header, height in tmp]
        blocks_to_heights = self._merge_blocks_to_heights([tmp])
        tmp = findall(r"\[(.*Z) .* FairDag Monitor attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)", log)
        tmp = [(victim_header, attack_header) for t, attack_header, victim_header, round in tmp]
        monitor_attacks = self._merge_monitor([tmp])
        tmp = findall(r"\[(.*Z) .* FairDag Created a fissure attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)", log)
        tmp = [(victim_header, attack_header) for t, attack_header, victim_header, round in tmp]
        fissure_attacks = self._merge_frontrun_attack([tmp])
        tmp = findall(r"\[(.*Z) .* FairDag Created a sluggish attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)", log)
        tmp = [(victim_header, attack_header) for t, attack_header, victim_header, round in tmp]
        sluggish_attacks = self._merge_frontrun_attack([tmp])
        tmp = findall(r"\[(.*Z) .* FairDag Created a speculative attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)", log)
        tmp = [(victim_header, attack_header) for t, attack_header, victim_header, round in tmp]
        speculative_attacks = self._merge_frontrun_attack([tmp])
        configs = {
            "header_size": int(search(r"Header size .* (\d+)", log).group(1)),
            "max_header_delay": int(search(r"Max header delay .* (\d+)", log).group(1)),
            "gc_depth": int(search(r"Garbage collection depth .* (\d+)", log).group(1)),
            "sync_retry_delay": int(search(r"Sync retry delay .* (\d+)", log).group(1)),
            "sync_retry_nodes": int(search(r"Sync retry nodes .* (\d+)", log).group(1)),
            "batch_size": int(search(r"Batch size .* (\d+)", log).group(1)),
            "max_batch_delay": int(search(r"Max batch delay .* (\d+)", log).group(1)),
        }
        ip = search(r"booted on (\d+.\d+.\d+.\d+)", log).group(1)
        return (proposals, commits, configs, ip, blocks_to_heights,
                monitor_attacks, fissure_attacks, sluggish_attacks, speculative_attacks)

    def _parse_workers(self, log):
        tmp = findall(r"Batch ([^ ]+) contains (\d+) B", log)
        sizes = {d: int(s) for d, s in tmp}
        tmp = findall(r"Batch ([^ ]+) contains tx (\d+)", log)
        samples = {int(s): d for d, s in tmp}
        ip = search(r"booted on (\d+.\d+.\d+.\d+)", log).group(1)

        tmp = findall(r"\[(.*Z) .* FairDAG-RL ordered transaction: (\d+)", log)
        fair_ordered_txs = {int(tx_id): _to_posix(t) for t, tx_id in tmp}
        # Position in log = protocol's final order (no timestamp needed)
        fair_ordered_seqs = {}
        for pos, (t, tx_id) in enumerate(tmp):
            tx = int(tx_id)
            if tx not in fair_ordered_seqs:
                fair_ordered_seqs[tx] = pos

        fair_graph_stats = []
        tmp_old = findall(r"\[(.*Z) .* FairnessLayer: finalized (\d+) transactions from graph (\d+) \(round (\d+)\)\. Total ordered: (\d+)", log)
        for t, num_txs, graph_idx, round_num, total in tmp_old:
            fair_graph_stats.append({'timestamp': _to_posix(t), 'num_txs': int(num_txs), 'graph_idx': int(graph_idx), 'round': int(round_num), 'total_ordered': int(total)})
        tmp_new = findall(r"\[(.*Z) .* FairDAG: FINALIZED sub_dag_id=(\d+), (\d+) transactions", log)
        for t, sub_dag_id, num_txs in tmp_new:
            fair_graph_stats.append({'timestamp': _to_posix(t), 'num_txs': int(num_txs), 'graph_idx': int(sub_dag_id), 'round': 0, 'total_ordered': 0})

        tmp = findall(r'\[(.*Z) .* fairness_arrival tx_uid=(\d+) oi=(\d+)', log)
        arrival_times = {int(tx_uid): _to_posix(t) for t, tx_uid, oi in tmp}
        return sizes, samples, ip, fair_ordered_txs, fair_ordered_seqs, fair_graph_stats, arrival_times

    # ---- Attack results ----

    def _attack_results(self, attack_dict):
        attack_num = 0; succ_num = 0
        for victim_header, attacking_header in attack_dict.items():
            if victim_header in self.blocks_to_heights and attacking_header in self.blocks_to_heights:
                attack_num += 1
                if self.blocks_to_heights[victim_header] > self.blocks_to_heights[attacking_header]:
                    succ_num += 1
        return succ_num, attack_num
    def _attack_monitor_results(self): return self._attack_results(self.monitor_attacks)
    def _fissure_attack_results(self): return self._attack_results(self.fissure_attacks)
    def _sluggish_attack_results(self): return self._attack_results(self.sluggish_attacks)
    def _speculative_attack_results(self): return self._attack_results(self.speculative_attacks)

    # ---- Throughput & latency ----

    def _committed_tx_stats(self):
        committed_count = 0; latencies = []
        for tx_id, batch_ids in self.all_received_samples.items():
            if tx_id not in self.all_sent_samples: continue
            send_time = self.all_sent_samples[tx_id]
            commit_times = [self.commits[b] for b in batch_ids if b in self.commits]
            if commit_times:
                committed_count += 1
                latencies.append(min(commit_times) - send_time)
        return committed_count, latencies

    def _consensus_throughput(self):
        if not self.commits: return 0, 0, 0
        start = min(self.proposals.values()); end = max(self.commits.values())
        duration = end - start
        if duration <= 0: return 0, 0, 0
        committed_count, _ = self._committed_tx_stats()
        tps = committed_count / duration; bps = tps * self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [commit_time - self.proposals[bd] for bd, commit_time in self.commits.items() if bd in self.proposals]
        return mean(latency) if latency else 0

    # ---- FairDAG-RL metrics ----

    def _fairdag_tx_stats(self):
        fair_count = 0; latencies = []
        for tx_id, fair_time in self.fair_ordered_txs.items():
            if tx_id not in self.all_sent_samples: continue
            fair_count += 1; latencies.append(fair_time - self.all_sent_samples[tx_id])
        return fair_count, latencies

    def _fairdag_throughput(self):
        if not self.fair_ordered_txs: return 0, 0, 0
        start = min(self.start); end = max(self.fair_ordered_txs.values()); duration = end - start
        if duration <= 0: return 0, 0, 0
        fair_count, _ = self._fairdag_tx_stats()
        return fair_count / duration, (fair_count / duration) * self.size[0], duration

    def _fairdag_latency(self):
        _, latencies = self._fairdag_tx_stats()
        return mean(latencies) if latencies else 0

    def _fairdag_finalization_overhead(self):
        overheads = []
        for tx_id, fair_time in self.fair_ordered_txs.items():
            if tx_id not in self.all_received_samples: continue
            commit_times = [self.commits[b] for b in self.all_received_samples[tx_id] if b in self.commits]
            if commit_times: overheads.append(fair_time - min(commit_times))
        return mean(overheads) if overheads else 0

    def _fairdag_graph_summary(self):
        if not self.fair_graph_stats: return 0, 0, 0
        txs_per_graph = [s['num_txs'] for s in self.fair_graph_stats]
        max_total = max((s['total_ordered'] for s in self.fair_graph_stats), default=0)
        total_ordered = max_total if max_total > 0 else sum(txs_per_graph)
        return len(self.fair_graph_stats), mean(txs_per_graph) if txs_per_graph else 0, total_ordered

    def _end_to_end_throughput(self):
        if not self.fair_ordered_txs: return 0, 0, 0
        return self._fairdag_throughput()
    def _end_to_end_latency(self):
        return self._fairdag_latency()

    # ------------------------------------------------------------------
    # Adversarial reordering: Dist(tx,tx') vs fraction reversed
    # ------------------------------------------------------------------

    def _adversarial_reordering(self, n_sample_pairs=100_000, time_window_ms=500):
        """
        Cross-client tx pairs grouped by Dist(tx,tx').
        Ground truth = honest majority ordering.
        Same-client pairs always have Dist=N in TCP systems (useless).
        """
        N = self.committee_size
        if not isinstance(N, int) or N < 2: return None
        if not hasattr(self, 'per_node_arrivals') or not self.per_node_arrivals: return None
        # Use protocol's ordering position (not timestamps — timestamps
        # lose intra-subdag order due to millisecond quantization).
        finalized = self.fair_ordered_seqs
        if not finalized: return None

        f = self.faults if isinstance(self.faults, int) else 0
        min_coverage = N - f
        start_dist = N % 2
        dist_values = list(range(start_dist, N + 1, 2))

        tx_median_arrival = {}
        for tx in finalized:
            arrivals = [na[tx] for na in self.per_node_arrivals if tx in na]
            if len(arrivals) >= min_coverage:
                tx_median_arrival[tx] = sorted(arrivals)[len(arrivals) // 2]
        if len(tx_median_arrival) < 2: return None

        tx_to_client = {}
        for cidx, csent in enumerate(self.sent_samples):
            for tx in csent:
                if tx in tx_median_arrival: tx_to_client[tx] = cidx
        if len(set(tx_to_client.values())) < 2: return None

        all_txs_sorted = sorted(tx_median_arrival.keys(), key=lambda tx: tx_median_arrival[tx])
        n_total = len(all_txs_sorted)
        random.seed(42)
        time_window_s = time_window_ms / 1000.0
        all_pairs = []; attempts = 0

        while len(all_pairs) < n_sample_pairs and attempts < n_sample_pairs * 20:
            i = random.randint(0, n_total - 2)
            tx1 = all_txs_sorted[i]; client1 = tx_to_client.get(tx1)
            if client1 is None: attempts += 1; continue
            sr = min(200, n_total - i - 1)
            if sr < 1: attempts += 1; continue
            j = i + random.randint(1, sr)
            if j >= n_total: attempts += 1; continue
            tx2 = all_txs_sorted[j]; client2 = tx_to_client.get(tx2)
            if client2 is None or client1 == client2: attempts += 1; continue
            if abs(tx_median_arrival[tx2] - tx_median_arrival[tx1]) > time_window_s: attempts += 1; continue
            all_pairs.append((tx1, tx2)); attempts += 1
        if not all_pairs: return None

        dist_counts = {d: 0 for d in dist_values}
        dist_reversed = {d: 0 for d in dist_values}
        for tx1, tx2 in all_pairs:
            c1 = 0; c2 = 0; nb = 0
            for na in self.per_node_arrivals:
                if tx1 in na and tx2 in na:
                    nb += 1
                    if na[tx1] < na[tx2]: c1 += 1
                    elif na[tx2] < na[tx1]: c2 += 1
            if nb < min_coverage: continue
            dist = abs(c1 - c2)
            if dist not in dist_counts:
                dist = min(dist_values, key=lambda d: abs(d - dist))
            dist_counts[dist] += 1
            if c1 > c2 and finalized[tx2] < finalized[tx1]: dist_reversed[dist] += 1
            elif c2 > c1 and finalized[tx1] < finalized[tx2]: dist_reversed[dist] += 1

        return {d: (dist_reversed[d] / dist_counts[d], dist_counts[d]) if dist_counts[d] > 0 else (0.0, 0) for d in dist_values}

    # ---- Output ----

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']
        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000
        fairdag_overhead = self._fairdag_finalization_overhead() * 1_000
        fairdag_count, _ = self._fairdag_tx_stats()
        num_graphs, avg_txs_per_graph, total_fair_ordered = self._fairdag_graph_summary()
        attack_print = ""
        ms, mt = self._attack_monitor_results(); fs, ft = self._fissure_attack_results()
        ss, st = self._sluggish_attack_results(); sps, spt = self._speculative_attack_results()
        if mt != 0: attack_print = f"Baseline front-running rate: {round(ms/mt*100, 2):,}% ({ms:,}/{mt:,})"
        elif ft != 0: attack_print = f"Fissure front-running rate: {round(fs/ft*100, 2):,}% ({fs:,}/{ft:,})"
        elif st != 0: attack_print = f"Sluggish front-running rate: {round(ss/st*100, 2):,}% ({ss:,}/{st:,})"
        elif spt != 0: attack_print = f"Speculative front-running rate: {round(sps/spt*100, 2):,}% ({sps:,}/{spt:,})"
        reorder_results = self._adversarial_reordering()
        reorder_print = ""
        if reorder_results:
            reorder_print = '\n + ADVERSARIAL REORDERING (Dist -> fraction reversed):\n'
            for d in sorted(reorder_results.keys()):
                frac, count = reorder_results[d]
                reorder_print += f'   Dist={d:3d}: {frac:.4f}  ({count:,} pairs)\n'
        return (
            '\n-----------------------------------------\n SUMMARY:\n-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n\n'
            ' + FAIRDAG-RL:\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            f' Fair ordering overhead: {round(fairdag_overhead):,} ms\n'
            f' Fair-ordered txs: {fairdag_count:,}\n'
            f' Graphs finalized: {num_graphs:,}\n'
            f' Avg txs/graph: {round(avg_txs_per_graph, 1)}\n'
            f' Total fair-ordered (from logs): {total_fair_ordered:,}\n\n'
            f' + ATTACK:\n {attack_print} \n'
            f'{reorder_print}'
            '-----------------------------------------\n')

    def print(self, filename):
        with open(filename, 'a') as f: f.write(self.result())

    @classmethod
    def process(cls, directory, attack_type=10, arbitragers=1, faults=0):
        clients = [open(fn).read() for fn in sorted(glob(join(directory, "client-*.log")))]
        primaries = [open(fn).read() for fn in sorted(glob(join(directory, "primary-*.log")))]
        workers = [open(fn).read() for fn in sorted(glob(join(directory, "worker-*.log")))]
        return cls(clients, primaries, workers, attack_type=attack_type, arbitragers=arbitragers, faults=faults)