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
        self.size, self.rate, self.start, misses, self.sent_samples = zip(*results)
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
            batch_to_header,
            blocks_to_heights,
            monitor_attacks,
            fissure_attacks,
            sluggish_attacks,
            speculative_attacks,
            header_to_batches_list,
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

        # Merge header_to_batches from all primaries
        self.header_to_batches = self._merge_header_to_batches(header_to_batches_list)

        # batch_digest -> (round, short_author) from Committed logs
        self.batch_to_header = {}
        for d in batch_to_header:
            self.batch_to_header.update(d)

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
            subdag_to_headers,
        ) = zip(*results)

        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        self.finalization_events = self._merge_results([x.items() for x in finalization_events])

        # sub_dag_id -> set of (round, full_author)
        self.subdag_to_headers = {}
        for d in subdag_to_headers:
            for sub_dag_id, headers_set in d.items():
                if sub_dag_id not in self.subdag_to_headers:
                    self.subdag_to_headers[sub_dag_id] = set()
                self.subdag_to_headers[sub_dag_id].update(headers_set)

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

    def _merge_header_to_batches(self, dicts):
        """Merge header_to_batches from multiple primaries."""
        merged = defaultdict(set)
        for d in dicts:
            for h, batches in d.items():
                merged[h].update(batches)
        return {h: list(bs) for h, bs in merged.items()}

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

        # Only sample txs are logged: "Sending sample transaction {counter}"
        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        # Parse batch_digest -> (round, short_author) from Committed logs.
        # Log line: "Committed B{round}({short_author}) -> {batch_digest}"
        # These are per-header logs, NOT per-tx. Lightweight.
        batch_to_header = {}
        tmp_full = findall(r'Committed B(\d+)\(([^\)]+)\) -> ([^ \n]+)', log)
        for round_str, short_author, batch_digest in tmp_full:
            round_num = int(round_str)
            batch_to_header[batch_digest] = (round_num, short_author.strip())

        # Record committing heights of certificates (blocks)
        tmp = findall(r"\[(.*Z) .* FairDag Committed ([^ ]+) in height (\d+)", log)
        tmp = [(header, height) for t, header, height in tmp]
        blocks_to_heights = self._merge_blocks_to_heights([tmp])

        # Build header_to_batches by correlating "Committed B..." with "FairDag Committed..."
        # These are per-header logs, NOT per-tx. Lightweight.
        header_to_batches = defaultdict(list)
        lines = log.splitlines()

        for i, line in enumerate(lines):
            m = search(r'Committed B\d+\([^\)]+\) -> ([^ \n]+)', line)
            if not m:
                continue
            batch_digest = m.group(1).strip()

            # Look ahead 1-3 lines for the FairDag Committed line
            for j in range(i + 1, min(i + 4, len(lines))):
                m2 = search(r'FairDag Committed ([^ ]+) in height (\d+)', lines[j])
                if m2:
                    header_id = m2.group(1).strip()
                    header_to_batches[header_id].append(batch_digest)
                    break

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
            batch_to_header,
            blocks_to_heights,
            monitor_attacks,
            fissure_attacks,
            sluggish_attacks,
            speculative_attacks,
            dict(header_to_batches),
        )

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        # Parse finalization summary events (for TPS).
        # Log format: "sub_dag_id=5: FINALIZED! 42 transactions (3 duplicates skipped)"
        # One line per sub-dag. Lightweight.
        tmp = findall(r'\[(.*Z) .* sub_dag_id=(\d+): FINALIZED! (\d+) transactions', log)
        finalization_events = {
            int(sub_dag_id): (self._to_posix(t), int(count))
            for t, sub_dag_id, count in tmp
        }

        # Parse sub_dag_id -> (round, full_author) from "Received sub-dag" logs.
        # These are per-subdag logs, NOT per-tx. Lightweight.
        subdag_to_headers = {}
        all_lines = log.split('\n')
        current_subdag_content = None

        for i, line in enumerate(all_lines):
            if 'Received sub-dag : [' in line:
                start_idx = line.find('Received sub-dag : [') + len('Received sub-dag : [')
                content = line[start_idx:]
                if content.endswith(']'):
                    content = content[:-1]
                current_subdag_content = content

            elif 'SUBDAG STRUCTURE: sub_dag_id=' in line and current_subdag_content:
                match = search(r'sub_dag_id=(\d+)', line)
                if match:
                    sub_dag_id = int(match.group(1))
                    headers_set = set()

                    round_matches = findall(r'\((\d+), \[([^\]]*)\]\)', current_subdag_content)
                    for round_str, authors_str in round_matches:
                        round_num = int(round_str)
                        if authors_str.strip():
                            authors = [a.strip() for a in authors_str.split(', ') if a.strip()]
                            for full_author in authors:
                                headers_set.add((round_num, full_author))

                    subdag_to_headers[sub_dag_id] = headers_set
                    current_subdag_content = None

        return (
            sizes,
            samples,
            ip,
            finalization_events,
            subdag_to_headers,
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
        """Duration from first client send to last finalization.
        Returns 0 if no finalization events were recorded."""
        if not self.finalization_events:
            return 0
        start = min(self.start)
        end = max(t for t, _ in self.finalization_events.values())
        duration = end - start
        return duration if duration > 0 else 0

    def _end_to_end_throughput(self):
        """TPS from finalization summary counts.

        Uses the lightweight 'FINALIZED! N transactions' summary line
        (one per sub-dag, no per-tx logging needed).
        Duration: first client send -> last finalization event.
        """
        if not self.finalization_events:
            return 0, 0, -1

        start = min(self.start)
        end = max(t for t, _ in self.finalization_events.values())
        duration = end - start

        if duration <= 0:
            return 0, 0, -1

        total_finalized_txs = sum(count for _, count in self.finalization_events.values())

        tps = total_finalized_txs / duration
        bps = tps * self.size[0]

        return tps, bps, duration

    def _end_to_end_latency(self):
        """End-to-end FINALIZATION latency using sample transactions.

        Chain (all per-header or per-subdag logs, NOT per-tx):
          1. Client:   "Sending sample transaction {id}"              -> sample_id : send_time
          2. Worker:   "Batch X contains sample tx {id}"              -> sample_id : batch_digest
          3. Primary:  "Committed B{round}({author}) -> X"            -> batch_digest : (round, author)
          4. Worker:   "SUBDAG STRUCTURE: sub_dag_id={id}"            -> (round, author) : sub_dag_id
          5. Worker:   "sub_dag_id={id}: FINALIZED! N transactions"   -> sub_dag_id : finalization_time

        Latency = finalization_time - send_time

        Zero per-tx logging required. All log lines are either:
        - benchmark-gated sample logs (~1/sec/client)
        - per-header/per-subdag structural logs (already emitted)
        """
        latency = []

        # Build reverse index: (round, full_author) -> sub_dag_id
        header_to_subdag = {}
        for sub_dag_id in sorted(self.subdag_to_headers.keys()):
            for (round_num, full_author) in self.subdag_to_headers[sub_dag_id]:
                header_to_subdag[(round_num, full_author)] = sub_dag_id

        def find_subdag_for_header(round_num, short_author):
            for (r, full_author), sid in header_to_subdag.items():
                if r == round_num and full_author.startswith(short_author):
                    return sid
            return None

        for sample_tx_id, send_time in self.all_sent_samples.items():
            batch_digests = self.all_received_samples.get(sample_tx_id)
            if not batch_digests:
                continue

            min_latency = None

            for batch_digest in batch_digests:
                # Step 3: batch -> (round, short_author)
                header = self.batch_to_header.get(batch_digest)
                if header is None:
                    continue

                round_num, short_author = header

                # Step 4: (round, author) -> sub_dag_id
                sub_dag_id = find_subdag_for_header(round_num, short_author)
                if sub_dag_id is None:
                    continue

                # Step 5: sub_dag_id -> finalization_time
                finalization_data = self.finalization_events.get(sub_dag_id)
                if finalization_data is None:
                    continue

                finalization_time, _ = finalization_data

                lat = finalization_time - send_time
                if lat > 0:
                    min_latency = min(min_latency, lat) if min_latency else lat

            if min_latency is not None:
                latency.append(min_latency)

        return (
            mean(latency) if latency else 0,
            len(latency),
        )

    def _end_to_end_latency_commit_based(self):
        """Commit-based latency (for comparison / sanity check).

        Simpler chain: sample_tx -> batch -> commit_time.
        Understates true e2e latency since it stops at commit, not finalization.
        """
        latency = []
        for tx_id, batches_ids in self.all_received_samples.items():
            if tx_id not in self.all_sent_samples:
                continue
            start = self.all_sent_samples[tx_id]

            min_time = None
            for batch_id in batches_ids:
                if batch_id in self.commits:
                    end = self.commits[batch_id]
                    lat = end - start
                    if lat > 0:
                        min_time = min(min_time, lat) if min_time else lat

            if min_time:
                latency.append(min_time)

        return mean(latency) if latency else 0

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
        end_to_end_latency, num_samples_latency = self._end_to_end_latency()
        end_to_end_latency *= 1000
        end_to_end_latency_commit = self._end_to_end_latency_commit_based() * 1_000

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
        total_finalized = sum(count for _, count in self.finalization_events.values()) if self.finalization_events else 0
        debug_stats = (
            f" Debug: samples_sent={len(self.all_sent_samples)}, "
            f"samples_received={len(self.all_received_samples)}, "
            f"finalization_subdags={len(self.finalization_events)}, "
            f"total_finalized_txs={total_finalized}, "
            f"batch_to_header={len(self.batch_to_header)}, "
            f"subdag_to_headers={len(self.subdag_to_headers)}"
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
            f' End-to-end latency (finalization): {round(end_to_end_latency):,} ms '
            f'(from {num_samples_latency} sample txs)\n'
            f' End-to-end latency (commit-based): {round(end_to_end_latency_commit):,} ms\n'
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