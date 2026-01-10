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
    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        self.committee_size = len(primaries)
        self.workers = len(workers) // len(primaries)

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
        proposals, commits, self.configs, primary_ips, batch_to_header = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])

        # batch_digest -> (round, short_author) from Committed logs
        # short_author is like "8g4Oeq1VFCmKe4M3"
        self.batch_to_header = {}
        for d in batch_to_header:
            self.batch_to_header.update(d)

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips, finalization_events, subdag_to_headers = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        self.finalization_events = self._merge_results([x.items() for x in finalization_events])

        # sub_dag_id -> set of (round, full_author) from "Received sub-dag" logs
        # full_author is like "8g4Oeq1VFCmKe4M36+1OjATqofUl9iksjrJ4IQmgr5g="
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
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r"(?:panicked|Error)", log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

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

        # Parse batch_digest -> (round, short_author) from Committed logs
        # Format: "Committed B{round}({short_author}) -> {batch_digest}"
        # Example: "Committed B1(7jv81x0Age2f+E6q) -> CKWkjWdfoadusvEfcNlgDG+FwG8hT3d2GWozD8tUrUw="
        batch_to_header = {}
        tmp_full = findall(r'Committed B(\d+)\(([^\)]+)\) -> ([^ \n]+)', log)
        for round_str, short_author, batch_digest in tmp_full:
            round_num = int(round_str)
            batch_to_header[batch_digest] = (round_num, short_author.strip())

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return proposals, commits, configs, ip, batch_to_header

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        tmp = findall(r'\[(.*Z) .* sub_dag_id=(\d+): FINALIZED! (\d+) transactions', log)
        finalization_events = {
            int(sub_dag_id): (self._to_posix(t), int(count))
            for t, sub_dag_id, count in tmp
        }

        # Parse sub_dag_id -> (round, full_author) from "Received sub-dag" logs
        # Format: "Received sub-dag : [(1, [author1, author2, ...]), (2, [...]), ...]"
        # Example: "Received sub-dag : [(1, [7jv81x0Age2f+E6qtC2NV5PvHPK8H0EByZWkSXAU+8I=, ...]), ...]"
        subdag_to_headers = {}
        
        # Find all sub-dag logs and extract the sub_dag_id from the context
        # We need to correlate "Received sub-dag" with the sub_dag_id from subsequent logs
        # Strategy: Parse sub_dag_id from "SUBDAG STRUCTURE: sub_dag_id=X" which follows "Received sub-dag"
        
        # First, find all "Received sub-dag" entries
        subdag_matches = findall(
            r'Received sub-dag : \[(.*?)\](?=\s*\n)',
            log,
            flags=0
        )
        
        # Find sub_dag_id assignments from "SUBDAG STRUCTURE" logs
        structure_matches = findall(
            r'SUBDAG STRUCTURE: sub_dag_id=(\d+)',
            log
        )
        
        # Parse the full sub-dag content with sub_dag_id
        # Better approach: find lines with both "Received sub-dag" content and correlate with structure
        all_lines = log.split('\n')
        current_subdag_content = None
        
        for i, line in enumerate(all_lines):
            if 'Received sub-dag : [' in line:
                # Extract the content between [ and ]
                start_idx = line.find('Received sub-dag : [') + len('Received sub-dag : [')
                # Handle multi-line or find the closing bracket
                content = line[start_idx:]
                if content.endswith(']'):
                    content = content[:-1]
                current_subdag_content = content
                
            elif 'SUBDAG STRUCTURE: sub_dag_id=' in line and current_subdag_content:
                # Extract sub_dag_id
                match = search(r'sub_dag_id=(\d+)', line)
                if match:
                    sub_dag_id = int(match.group(1))
                    headers_set = set()
                    
                    # Parse the content: (round, [author1, author2, ...]), ...
                    # Use regex to find each (round, [authors]) pair
                    round_matches = findall(r'\((\d+), \[([^\]]*)\]\)', current_subdag_content)
                    
                    for round_str, authors_str in round_matches:
                        round_num = int(round_str)
                        if authors_str.strip():
                            # Split authors by comma, handling base64 which contains + and /
                            # Authors are comma-space separated
                            authors = [a.strip() for a in authors_str.split(', ') if a.strip()]
                            for full_author in authors:
                                headers_set.add((round_num, full_author))
                    
                    subdag_to_headers[sub_dag_id] = headers_set
                    current_subdag_content = None

        return sizes, samples, ip, finalization_events, subdag_to_headers

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

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

    def _end_to_end_throughput(self):
        if not self.finalization_events:
            return 0, 0, 0

        start = min(self.start)
        end = max(ts for ts, _ in self.finalization_events.values())
        duration = end - start

        total_finalized_txs = sum(count for _, count in self.finalization_events.values()) / self.avg_redundancy

        tps = total_finalized_txs / duration
        bps = tps * self.size[0]

        return tps, bps, duration

    def _end_to_end_latency(self):
        """
        Compute end-to-end latency based on FINALIZATION time.
        
        Chain: sample_tx_id → batch_digest → (round, short_author) → sub_dag_id → finalization_time
        
        Key insight: short_author is a PREFIX of full_author
        - Committed logs: "B1(7jv81x0Age2f+E6q)" -> short_author
        - Sub-dag logs: "7jv81x0Age2f+E6qtC2NV5PvHPK8H0EByZWkSXAU+8I=" -> full_author
        - Match: full_author.startswith(short_author)
        """
        latency = []
        
        # Build index: (round, short_author) -> sub_dag_id
        # We need to match short_author (prefix) with full_author
        header_to_subdag = {}
        
        for sub_dag_id in sorted(self.subdag_to_headers.keys()):
            for (round_num, full_author) in self.subdag_to_headers[sub_dag_id]:
                # Create entry for prefix matching
                # The short_author in Committed logs is a prefix of full_author
                header_to_subdag[(round_num, full_author)] = sub_dag_id

        # Helper function to find sub_dag_id by matching short_author prefix
        def find_subdag_for_header(round_num, short_author):
            """Find sub_dag_id where a full_author starts with short_author"""
            for (r, full_author), sub_dag_id in header_to_subdag.items():
                if r == round_num and full_author.startswith(short_author):
                    return sub_dag_id
            return None

        for sample_tx_id, send_time in self.all_sent_samples.items():
            batch_digests = self.all_received_samples.get(sample_tx_id)
            if not batch_digests:
                continue

            min_latency = None

            for batch_digest in batch_digests:
                # Step 1: batch_digest -> (round, short_author)
                header = self.batch_to_header.get(batch_digest)
                if header is None:
                    continue
                
                round_num, short_author = header

                # Step 2: (round, short_author) -> sub_dag_id (using prefix match)
                sub_dag_id = find_subdag_for_header(round_num, short_author)
                if sub_dag_id is None:
                    continue

                # Step 3: sub_dag_id -> finalization_time
                finalization_data = self.finalization_events.get(sub_dag_id)
                if finalization_data is None:
                    continue

                finalization_time, _ = finalization_data
                lat = finalization_time - send_time

                if lat > 0:
                    min_latency = min(min_latency, lat) if min_latency else lat

            if min_latency is not None:
                latency.append(min_latency)

        return mean(latency) if latency else 0

    def _end_to_end_latency_commit_based(self):
        """
        Original commit-based latency calculation (kept for comparison).
        """
        latency = []
        for tx_id, batches_ids in self.all_received_samples.items():
            min_time = None
            assert tx_id in self.all_sent_samples
            start = self.all_sent_samples[tx_id]

            for batch_id in batches_ids:
                if batch_id in self.commits:
                    end = self.commits[batch_id]
                    min_time = min(min_time, end - start) if min_time else end - start

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
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000
        end_to_end_latency_commit = self._end_to_end_latency_commit_based() * 1_000

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
            f' Average Redundancy: Transactions are in {round(self.avg_redundancy, 2)} batches\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency (finalization): {round(end_to_end_latency):,} ms\n'
            f' End-to-end latency (commit-based): {round(end_to_end_latency_commit):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
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