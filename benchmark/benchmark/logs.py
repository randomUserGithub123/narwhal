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
        self.committee_size = len(primaries)
        self.workers = len(workers) // len(primaries)

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_txs = zip(*results)
        self.misses = sum(misses)

        # Merge all sent txs: tx_id -> earliest send_time across clients
        self.all_sent_txs = {}
        for d in self.sent_txs:
            for tx_id, t in d.items():
                if tx_id not in self.all_sent_txs or t < self.all_sent_txs[tx_id]:
                    self.all_sent_txs[tx_id] = t

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
        (
            sizes,
            self.received_samples,
            workers_ips,
            finalization_events,
            tx_id_to_digest_list,
            digest_to_finalized_time_list,
            finalized_digest_sets,
        ) = zip(*results)

        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Merge tx_id -> digest from all workers.
        # "Received tx" is logged in both batch_maker and processor,
        # so duplicates are expected.  The digest is deterministic so
        # we just keep the first occurrence per tx_id.
        self.tx_id_to_digest = {}
        for d in tx_id_to_digest_list:
            for tx_id, digest in d.items():
                if tx_id not in self.tx_id_to_digest:
                    self.tx_id_to_digest[tx_id] = digest

        # Merge digest -> min finalized time across all workers.
        self.digest_to_finalized_time = {}
        for d in digest_to_finalized_time_list:
            for digest, t in d.items():
                if digest not in self.digest_to_finalized_time or t < self.digest_to_finalized_time[digest]:
                    self.digest_to_finalized_time[digest] = t

        # Take the largest unique finalized set across workers.
        largest_set = set()
        for s in finalized_digest_sets:
            if len(s) > len(largest_set):
                largest_set = s
        self.finalized_digests = largest_set

        self.finalization_events = self._merge_results([x.items() for x in finalization_events])

        self.all_received_samples = self._merge_dicts(self.received_samples)

        self._avg_redundancy()

        self.collocate = set(primary_ips) == set(workers_ips)

        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_dicts(self, input):
        merged_dict = defaultdict(list)
        for d in input:
            for k, v in d.items():
                merged_dict[k].append(v)
        return dict(merged_dict)

    def _merge_results(self, input):
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

    def _parse_clients(self, log):
        if search(r"(?:panicked|Error)", log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        # Parse ALL sent txs: "Sending tx: {id}" with timestamps.
        # Every tx (sample and standard) is logged by the client.
        tmp = findall(r'\[(.*Z) .* Sending tx: (\d+)', log)
        sent_txs = {int(tx_id): self._to_posix(t) for t, tx_id in tmp}

        return size, rate, start, misses, sent_txs

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        # Record committing heights of certificates (blocks)
        tmp = findall(r"\[(.*Z) .* FairDag Committed ([^ ]+) in height (\d+)", log)
        tmp = [(header, height) for t, header, height in tmp]
        blocks_to_heights = self._merge_blocks_to_heights([tmp])

        # Parse attack data
        tmp = findall(
            r"\[(.*Z) .* FairDag Monitor attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)",
            log,
        )
        tmp = [(victim_header, attack_header) for t, attack_header, victim_header, round in tmp]
        monitor_attacks = self._merge_monitor([tmp])

        tmp = findall(
            r"\[(.*Z) .* FairDag Created a fissure attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)",
            log,
        )
        tmp = [(victim_header, attack_header) for t, attack_header, victim_header, round in tmp]
        fissure_attacks = self._merge_frontrun_attack([tmp])

        tmp = findall(
            r"\[(.*Z) .* FairDag Created a sluggish attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)",
            log,
        )
        tmp = [(victim_header, attack_header) for t, attack_header, victim_header, round in tmp]
        sluggish_attacks = self._merge_frontrun_attack([tmp])

        tmp = findall(
            r"\[(.*Z) .* FairDag Created a speculative attacking header ([^ ]+), victim header ([^ ]+), in round (\d+)",
            log,
        )
        tmp = [(victim_header, attack_header) for t, attack_header, victim_header, round in tmp]
        speculative_attacks = self._merge_frontrun_attack([tmp])

        configs = {
            'header_size': int(search(r'Header size .* (\d+)', log).group(1)),
            'max_header_delay': int(search(r'Max header delay .* (\d+)', log).group(1)),
            'gc_depth': int(search(r'Garbage collection depth .* (\d+)', log).group(1)),
            'sync_retry_delay': int(search(r'Sync retry delay .* (\d+)', log).group(1)),
            'sync_retry_nodes': int(search(r'Sync retry nodes .* (\d+)', log).group(1)),
            'batch_size': int(search(r'Batch size .* (\d+)', log).group(1)),
            'max_batch_delay': int(search(r'Max batch delay .* (\d+)', log).group(1)),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

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
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        # Parse finalization summary events (for throughput calculation).
        # Format: "sub_dag_id=5: FINALIZED! 42 transactions (3 duplicates skipped)"
        tmp = findall(r'\[(.*Z) .* sub_dag_id=(\d+): FINALIZED! (\d+) transactions', log)
        finalization_events = {
            int(sub_dag_id): (self._to_posix(t), int(count))
            for t, sub_dag_id, count in tmp
        }

        # === Parse "Received tx {id} with digest {digest}" ===
        # Logged in both batch_maker and processor, so duplicates expected.
        # Digest is base64: [A-Za-z0-9+/=]+
        # We keep first occurrence per tx_id (digest is deterministic).
        tx_id_to_digest = {}
        tmp = findall(r'Received tx (\d+) with digest ([A-Za-z0-9+/=]+)', log)
        for tx_id_str, digest in tmp:
            tx_id = int(tx_id_str)
            if tx_id not in tx_id_to_digest:
                tx_id_to_digest[tx_id] = digest

        # === Parse "FINALIZED! digest {digest}" with timestamps ===
        # Format: "sub_dag_id=5: FINALIZED! digest 5TDEZx4GgR/re1Ml6Z+o...="
        # Take the MIN timestamp per digest across all sub_dag_ids.
        digest_to_finalized_time = {}
        tmp = findall(r'\[(.*Z) .* FINALIZED! digest ([A-Za-z0-9+/=]+)', log)
        for t, digest in tmp:
            ts = self._to_posix(t)
            if digest not in digest_to_finalized_time or ts < digest_to_finalized_time[digest]:
                digest_to_finalized_time[digest] = ts

        # The set of all unique finalized digests on this worker.
        finalized_digest_set = set(digest_to_finalized_time.keys())

        return (
            sizes,
            samples,
            ip,
            finalization_events,
            tx_id_to_digest,
            digest_to_finalized_time,
            finalized_digest_set,
        )

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _attack_monitor_results(self):
        attack_num = 0
        succ_num = 0
        for victim_header, attacking_header in self.monitor_attacks.items():
            if (
                victim_header in self.blocks_to_heights
                and attacking_header in self.blocks_to_heights
            ):
                attack_num += 1
                if self.blocks_to_heights[victim_header] > self.blocks_to_heights[attacking_header]:
                    succ_num += 1
        return succ_num, attack_num

    def _fissure_attack_results(self):
        attack_num = 0
        succ_num = 0
        for victim_header, attacking_header in self.fissure_attacks.items():
            if (
                victim_header in self.blocks_to_heights
                and attacking_header in self.blocks_to_heights
            ):
                attack_num += 1
                if self.blocks_to_heights[victim_header] > self.blocks_to_heights[attacking_header]:
                    succ_num += 1
        return succ_num, attack_num

    def _sluggish_attack_results(self):
        attack_num = 0
        succ_num = 0
        for victim_header, attacking_header in self.sluggish_attacks.items():
            if (
                victim_header in self.blocks_to_heights
                and attacking_header in self.blocks_to_heights
            ):
                attack_num += 1
                if self.blocks_to_heights[victim_header] > self.blocks_to_heights[attacking_header]:
                    succ_num += 1
        return succ_num, attack_num

    def _speculative_attack_results(self):
        attack_num = 0
        succ_num = 0
        for victim_header, attacking_header in self.speculative_attacks.items():
            if (
                victim_header in self.blocks_to_heights
                and attacking_header in self.blocks_to_heights
            ):
                attack_num += 1
                if self.blocks_to_heights[victim_header] > self.blocks_to_heights[attacking_header]:
                    succ_num += 1
        return succ_num, attack_num

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values()) / self.avg_redundancy

        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def get_execution_time(self):
        start = min(self.start)
        end = max(self.digest_to_finalized_time.values())
        duration = end - start
        return duration

    def _end_to_end_throughput(self):
        """
        Use the largest unique finalized digest set across workers.
        If only one node is ahead (finalized more), we take its set.
        Duration runs from earliest client send to latest finalization.
        """
        if not self.finalized_digests or not self.digest_to_finalized_time:
            return 0, 0, -1

        start = min(self.start)
        end = max(self.digest_to_finalized_time.values())
        duration = end - start

        total_finalized_txs = len(self.finalized_digests)

        tps = total_finalized_txs / duration
        bps = tps * self.size[0]

        return tps, bps, duration

    def _end_to_end_latency(self):
        """
        Simplified end-to-end latency via direct chain:

          1. Client logs  "Sending tx: {tx_id}"                       -> tx_id : send_time
          2. Worker logs  "Received tx {tx_id} with digest {digest}"  -> tx_id : digest
          3. Worker logs  "FINALIZED! digest {digest}"                -> digest : finalize_time

          latency = min(finalize_time) - send_time

        Duplicates in step 2 (batch_maker + processor both log it)
        are harmless because the digest is deterministic -- we just
        keep the first occurrence per tx_id.

        For step 3 we take the MIN finalized timestamp per digest
        across all workers and sub_dag_ids.
        """
        latency = []
        matched = 0
        no_digest = 0
        not_finalized = 0

        for tx_id, send_time in self.all_sent_txs.items():
            # Step 1: tx_id -> digest
            digest = self.tx_id_to_digest.get(tx_id)
            if digest is None:
                no_digest += 1
                continue

            # Step 2: digest -> min finalization time
            finalize_time = self.digest_to_finalized_time.get(digest)
            if finalize_time is None:
                not_finalized += 1
                continue

            # Step 3: compute latency
            lat = finalize_time - send_time
            if lat > 0:
                latency.append(lat)
                matched += 1

        return (
            mean(latency) if latency else 0,
            matched,
            no_digest,
            not_finalized,
        )

    def _avg_redundancy(self):
        lengths = [len(batches) for batches in self.all_received_samples.values()]
        self.avg_redundancy = mean(lengths) if lengths else 1

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, consensus_duration = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency, num_matched, num_no_digest, num_not_finalized = self._end_to_end_latency()
        end_to_end_latency *= 1000

        # Attack calculation (header-level)
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

        # Debug stats
        debug_stats = (
            f" Debug: sent_txs={len(self.all_sent_txs)}, "
            f"tx_id_to_digest={len(self.tx_id_to_digest)}, "
            f"finalized_digests={len(self.finalized_digests)} (largest worker set), "
            f"digest_timestamps={len(self.digest_to_finalized_time)}"
        )

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
            f' Consensus time: {round(consensus_duration):,} s\n'
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
            f' Average Redundancy: Transactions are in {round(self.avg_redundancy, 2)} batches\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end finalized txs: {len(self.finalized_digests):,} (largest unique set)\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms '
            f'(matched={num_matched}, no_digest={num_no_digest}, not_finalized={num_not_finalized})\n'
            '\n'
            ' + ATTACK:\n'
            f' {attack_print}\n'
            '\n'
            f'{debug_stats}\n'
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
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)