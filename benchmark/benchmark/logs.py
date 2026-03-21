# Copyright(C) Facebook, Inc. and its affiliates.
# Modified for FairDAG-RL v4: FairDAG metrics parsed from worker logs.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
from collections import defaultdict

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
            self.committee_size = len(primaries) + int(faults)
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
        (
            proposals,
            commits,
            self.configs,
            primary_ips,
            blocks_to_heights,
            monitor_attacks,
            fissure_attacks,
            sluggish_attacks,
            speculative_attacks,
        ) = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.blocks_to_heights = self._merge_blocks_to_heights(
            [x.items() for x in blocks_to_heights]
        )
        self.monitor_attacks = self._merge_monitor([x.items() for x in monitor_attacks])
        self.fissure_attacks = self._merge_frontrun_attack(
            [x.items() for x in fissure_attacks]
        )
        self.sluggish_attacks = self._merge_frontrun_attack(
            [x.items() for x in sluggish_attacks]
        )
        self.speculative_attacks = self._merge_frontrun_attack(
            [x.items() for x in speculative_attacks]
        )

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips, fair_ordered_txs_list, fair_graph_stats_list = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        self.all_received_samples = self._merge_dicts(self.received_samples)

        # FairDAG-RL: merge fair ordering results across workers.
        # Keep the EARLIEST timestamp per tx (first worker to finalize).
        self.fair_ordered_txs = self._merge_results(
            [x.items() for x in fair_ordered_txs_list]
        )

        # FairDAG-RL: merge graph finalization stats.
        self.fair_graph_stats = []
        for stats_list in fair_graph_stats_list:
            self.fair_graph_stats.extend(stats_list)

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    # ------------------------------------------------------------------
    # Merge helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_dicts(input):
        merged_dict = defaultdict(list)
        for d in input:
            for k, v in d.items():
                merged_dict[k].append(v)
        return dict(merged_dict)

    @staticmethod
    def _merge_results(input):
        # Keep the earliest timestamp.
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

    # ------------------------------------------------------------------
    # Log parsers
    # ------------------------------------------------------------------

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

        tmp = findall(
            r"\[(.*Z) .* FairDag Monitor attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)",
            log,
        )
        tmp = [
            (victim_header, attack_header)
            for t, attack_header, victim_header, round in tmp
        ]
        monitor_attacks = self._merge_monitor([tmp])

        tmp = findall(
            r"\[(.*Z) .* FairDag Created a fissure attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)",
            log,
        )
        tmp = [
            (victim_header, attack_header)
            for t, attack_header, victim_header, round in tmp
        ]
        fissure_attacks = self._merge_frontrun_attack([tmp])

        tmp = findall(
            r"\[(.*Z) .* FairDag Created a sluggish attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)",
            log,
        )
        tmp = [
            (victim_header, attack_header)
            for t, attack_header, victim_header, round in tmp
        ]
        sluggish_attacks = self._merge_frontrun_attack([tmp])

        tmp = findall(
            r"\[(.*Z) .* FairDag Created a speculative attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)",
            log,
        )
        tmp = [
            (victim_header, attack_header)
            for t, attack_header, victim_header, round in tmp
        ]
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

        return (
            proposals,
            commits,
            configs,
            ip,
            blocks_to_heights,
            monitor_attacks,
            fissure_attacks,
            sluggish_attacks,
            speculative_attacks,
        )

    def _parse_workers(self, log):
        tmp = findall(r"Batch ([^ ]+) contains (\d+) B", log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r"Batch ([^ ]+) contains tx (\d+)", log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r"booted on (\d+.\d+.\d+.\d+)", log).group(1)

        # FairDAG-RL: parse fair ordering output from worker logs.
        tmp = findall(
            r"\[(.*Z) .* FairDAG-RL ordered transaction: (\d+)",
            log,
        )
        fair_ordered_txs = {int(tx_id): _to_posix(t) for t, tx_id in tmp}

        # FairDAG-RL: graph finalization stats.
        tmp = findall(
            r"\[(.*Z) .* FairnessLayer: finalized (\d+) transactions from graph (\d+) \(round (\d+)\)\. Total ordered: (\d+)",
            log,
        )
        fair_graph_stats = [
            {
                'timestamp': _to_posix(t),
                'num_txs': int(num_txs),
                'graph_idx': int(graph_idx),
                'round': int(round_num),
                'total_ordered': int(total),
            }
            for t, num_txs, graph_idx, round_num, total in tmp
        ]

        return sizes, samples, ip, fair_ordered_txs, fair_graph_stats

    # ------------------------------------------------------------------
    # Attack results
    # ------------------------------------------------------------------

    def _attack_results(self, attack_dict):
        """Generic helper: count committed attacks and successes."""
        attack_num = 0
        succ_num = 0
        for victim_header, attacking_header in attack_dict.items():
            if (
                victim_header in self.blocks_to_heights
                and attacking_header in self.blocks_to_heights
            ):
                attack_num += 1
                if (
                    self.blocks_to_heights[victim_header]
                    > self.blocks_to_heights[attacking_header]
                ):
                    succ_num += 1
        return succ_num, attack_num

    def _attack_monitor_results(self):
        return self._attack_results(self.monitor_attacks)

    def _fissure_attack_results(self):
        return self._attack_results(self.fissure_attacks)

    def _sluggish_attack_results(self):
        return self._attack_results(self.sluggish_attacks)

    def _speculative_attack_results(self):
        return self._attack_results(self.speculative_attacks)

    # ------------------------------------------------------------------
    # Throughput & latency — direct counting of all transactions
    # ------------------------------------------------------------------

    def _committed_tx_stats(self):
        """
        Walk every transaction and return:
          - committed_count: unique txs that were committed (Tusk level)
          - latencies: list of (earliest_commit - send_time) per committed tx
        """
        committed_count = 0
        latencies = []

        for tx_id, batch_ids in self.all_received_samples.items():
            if tx_id not in self.all_sent_samples:
                continue
            send_time = self.all_sent_samples[tx_id]

            commit_times = [
                self.commits[b] for b in batch_ids if b in self.commits
            ]
            if commit_times:
                committed_count += 1
                latencies.append(min(commit_times) - send_time)

        return committed_count, latencies

    # ---- Consensus (proposal → commit) ----

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0

        start = min(self.proposals.values())
        end = max(self.commits.values())
        duration = end - start
        if duration <= 0:
            return 0, 0, 0

        committed_count, _ = self._committed_tx_stats()
        tps = committed_count / duration
        bps = tps * self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = []
        for batch_digest, commit_time in self.commits.items():
            if batch_digest in self.proposals:
                latency.append(commit_time - self.proposals[batch_digest])
        return mean(latency) if latency else 0

    # ------------------------------------------------------------------
    # FairDAG-RL: End-to-end fair ordering metrics
    # ------------------------------------------------------------------

    def _fairdag_tx_stats(self):
        """
        For each tx that was both client-sent AND fair-ordered,
        compute the end-to-end latency: fair_order_time - client_send_time.

        Returns:
            fair_count:  number of txs that were fair-ordered
            latencies:   list of latencies (seconds)
        """
        fair_count = 0
        latencies = []

        for tx_id, fair_time in self.fair_ordered_txs.items():
            if tx_id not in self.all_sent_samples:
                continue
            send_time = self.all_sent_samples[tx_id]
            fair_count += 1
            latencies.append(fair_time - send_time)

        return fair_count, latencies

    def _fairdag_throughput(self):
        """
        FairDAG-RL end-to-end throughput:
            fair-ordered txs / (last_fair_order - first_client_start)
        """
        if not self.fair_ordered_txs:
            return 0, 0, 0

        start = min(self.start)
        end = max(self.fair_ordered_txs.values())
        duration = end - start
        if duration <= 0:
            return 0, 0, 0

        fair_count, _ = self._fairdag_tx_stats()
        tps = fair_count / duration
        bps = tps * self.size[0]
        return tps, bps, duration

    def _fairdag_latency(self):
        """Mean end-to-end latency: fair_order_time - client_send_time."""
        _, latencies = self._fairdag_tx_stats()
        return mean(latencies) if latencies else 0

    def _fairdag_finalization_overhead(self):
        """
        Overhead of fair ordering on top of Tusk commit.
        For each tx that was both Tusk-committed and fair-ordered:
            overhead = fair_order_time - tusk_commit_time
        """
        overheads = []

        for tx_id, fair_time in self.fair_ordered_txs.items():
            if tx_id not in self.all_received_samples:
                continue

            batch_ids = self.all_received_samples[tx_id]
            commit_times = [
                self.commits[b] for b in batch_ids if b in self.commits
            ]
            if commit_times:
                tusk_time = min(commit_times)
                overheads.append(fair_time - tusk_time)

        return mean(overheads) if overheads else 0

    def _fairdag_graph_summary(self):
        """
        Summarize graph finalization events:
            - Number of graphs finalized
            - Mean txs per graph
            - Total txs fair-ordered
        """
        if not self.fair_graph_stats:
            return 0, 0, 0

        num_graphs = len(self.fair_graph_stats)
        txs_per_graph = [s['num_txs'] for s in self.fair_graph_stats]
        total_ordered = max(
            (s['total_ordered'] for s in self.fair_graph_stats), default=0
        )
        avg_txs = mean(txs_per_graph) if txs_per_graph else 0

        return num_graphs, avg_txs, total_ordered

    # ---- End-to-end = client send → fair ordering output ----

    def _end_to_end_throughput(self):
        if not self.fair_ordered_txs:
            return 0, 0, 0
        return self._fairdag_throughput()

    def _end_to_end_latency(self):
        return self._fairdag_latency()

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

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

        # FairDAG-RL metrics
        fairdag_overhead = self._fairdag_finalization_overhead() * 1_000
        fairdag_count, _ = self._fairdag_tx_stats()
        num_graphs, avg_txs_per_graph, total_fair_ordered = self._fairdag_graph_summary()

        # Attack calculation
        attack_print = ""
        monitor_succ_num, monitor_total = self._attack_monitor_results()
        fissure_succ_num, fissure_total = self._fissure_attack_results()
        sluggish_succ_num, sluggish_total = self._sluggish_attack_results()
        speculative_succ_num, speculative_total = self._speculative_attack_results()
        if monitor_total != 0:
            attack_print = f"Baseline front-running rate: {round(monitor_succ_num/monitor_total*100, 2):,}% ({monitor_succ_num:,}/{monitor_total:,})"
        elif fissure_total != 0:
            attack_print = f"Fissure front-running rate: {round(fissure_succ_num/fissure_total*100, 2):,}% ({fissure_succ_num:,}/{fissure_total:,})"
        elif sluggish_total != 0:
            attack_print = f"Sluggish front-running rate: {round(sluggish_succ_num/sluggish_total*100, 2):,}% ({sluggish_succ_num:,}/{sluggish_total:,})"
        elif speculative_total != 0:
            attack_print = f"Speculative front-running rate: {round(speculative_succ_num/speculative_total*100, 2):,}% ({speculative_succ_num:,}/{speculative_total:,})"

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            ' + FAIRDAG-RL:\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            f' Fair ordering overhead: {round(fairdag_overhead):,} ms\n'
            f' Fair-ordered txs: {fairdag_count:,}\n'
            f' Graphs finalized: {num_graphs:,}\n'
            f' Avg txs/graph: {round(avg_txs_per_graph, 1)}\n'
            f' Total fair-ordered (from logs): {total_fair_ordered:,}\n'
            '\n'
            ' + ATTACK:\n'
            f' {attack_print} \n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, attack_type=10, arbitragers=1, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, "client-*.log"))):
            with open(filename, "r") as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, "primary-*.log"))):
            with open(filename, "r") as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, "worker-*.log"))):
            with open(filename, "r") as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, attack_type=attack_type, arbitragers=arbitragers, faults=faults)