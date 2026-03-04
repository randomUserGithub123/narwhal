# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
from collections import defaultdict

from benchmark.utils import Print


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

        # self.sent_samples is a tuple of dictionaries. each element is for one client
        # each dictionary has sample_tx id -> timestamp, used for throughput/latencies
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
        sizes, self.received_samples, workers_ips = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # self.received_samples: for each worker, maps sample_tx_id -> batch_digest
        # transactions may be duplicated across workers
        self.all_received_samples = self._merge_dicts(self.received_samples)

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

    def _merge_dicts(self, input):
        merged_dict = defaultdict(list)
        for d in input:
            for k, v in d.items():
                merged_dict[k].append(v)
        return dict(merged_dict)

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if k not in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _merge_blocks_to_heights(self, input):
        merged = {}
        for x in input:
            for block, height in x:
                if block not in merged:
                    merged[block] = int(height)
        return merged

    def _merge_monitor(self, input):
        merged = {}
        for x in input:
            for victim_header, attack_header in x:
                if victim_header not in merged:
                    merged[victim_header] = attack_header
        return merged

    def _merge_frontrun_attack(self, input):
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
        start = self._to_posix(tmp)

        misses = len(findall(r"rate too high", log))

        # Changed: match ALL transactions, not just samples
        tmp = findall(r"\[(.*Z) .* Sending transaction (\d+)", log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_workers(self, log):
        tmp = findall(r"Batch ([^ ]+) contains (\d+) B", log)
        sizes = {d: int(s) for d, s in tmp}

        # Changed: match "contains tx" instead of "contains sample tx"
        tmp = findall(r"Batch ([^ ]+) contains tx (\d+)", log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r"booted on (\d+.\d+.\d+.\d+)", log).group(1)

        return sizes, samples, ip

    def _parse_primaries(self, log):
        tmp = findall(r"\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)", log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r"\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)", log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
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
    # Throughput & latency — based on unique committed transactions
    # ------------------------------------------------------------------
    #
    # Strategy:
    #   We use the *sample transactions* as ground truth.  For every
    #   sample tx we know:
    #     • when the client sent it           (all_sent_samples)
    #     • which batch(es) it landed in      (all_received_samples)
    #     • when each batch was committed      (commits)
    #
    #   A sample tx counts as "uniquely committed" when at least one of
    #   its batches appears in self.commits.
    #
    #   Throughput  =  unique_committed_samples / duration
    #                  (scaled up to the full tx population)
    #
    #   Latency     =  mean over committed samples of
    #                     (earliest_commit_of_any_batch − send_time)
    #
    # ------------------------------------------------------------------

    def _committed_sample_stats(self):
        """
        Walk every sample transaction and compute:
          • committed_count   – number of unique sample txs that were committed
          • total_count       – total sample txs tracked
          • per-tx latencies  – list of (earliest_commit − send_time) values
        """
        committed_count = 0
        total_count = 0
        latencies = []

        for tx_id, batch_ids in self.all_received_samples.items():
            if tx_id not in self.all_sent_samples:
                continue
            total_count += 1
            send_time = self.all_sent_samples[tx_id]

            # Collect commit times of every batch that holds this tx
            commit_times = [
                self.commits[b] for b in batch_ids if b in self.commits
            ]
            if commit_times:
                committed_count += 1
                earliest = min(commit_times)
                latencies.append(earliest - send_time)

        return committed_count, total_count, latencies

    # ---- Consensus (proposal → commit) ----

    def _consensus_throughput(self):
        """
        Unique committed transactions per second measured over the
        consensus window (first proposal → last commit).

        We derive unique tx count from committed sample ratio:
            total_committed_txs ≈ (committed_samples / total_samples)
                                   × total_txs_in_committed_batches
        where total_txs_in_committed_batches = sum(sizes) / tx_size.
        """
        if not self.commits or not self.sizes:
            return 0, 0, 0

        start = min(self.proposals.values())
        end = max(self.commits.values())
        duration = end - start
        if duration <= 0:
            return 0, 0, 0

        committed_samples, total_samples, _ = self._committed_sample_stats()
        if total_samples == 0:
            return 0, 0, 0

        commit_ratio = committed_samples / total_samples
        total_txs_in_batches = sum(self.sizes.values()) / self.size[0]
        unique_txs = total_txs_in_batches * commit_ratio

        tps = unique_txs / duration
        bps = tps * self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        """
        Per-batch latency: proposal time → earliest commit time.
        Average over all committed batches.
        """
        latency = []
        for batch_digest, commit_time in self.commits.items():
            if batch_digest in self.proposals:
                latency.append(commit_time - self.proposals[batch_digest])
        return mean(latency) if latency else 0

    # ---- End-to-end (client send → commit) ----

    def _end_to_end_throughput(self):
        """
        Unique committed transactions per second measured over the
        end-to-end window (earliest client start → last commit).

        Same unique-tx derivation as consensus throughput.
        """
        if not self.commits or not self.sizes:
            return 0, 0, 0

        start = min(self.start)
        end = max(self.commits.values())
        duration = end - start
        if duration <= 0:
            return 0, 0, 0

        committed_samples, total_samples, _ = self._committed_sample_stats()
        if total_samples == 0:
            return 0, 0, 0

        commit_ratio = committed_samples / total_samples
        total_txs_in_batches = sum(self.sizes.values()) / self.size[0]
        unique_txs = total_txs_in_batches * commit_ratio

        tps = unique_txs / duration
        bps = tps * self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        """
        For every sample transaction:
          • start   = timestamp the client sent it
          • end     = EARLIEST commit time among all batches containing it
          • latency = end − start

        Return the average over all committed sample transactions.
        """
        _, _, latencies = self._committed_sample_stats()
        return mean(latencies) if latencies else 0

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
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
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