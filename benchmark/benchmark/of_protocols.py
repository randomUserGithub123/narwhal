# Copyright(C) Facebook, Inc. and its affiliates.
import os
import re
import ast
import subprocess
from math import ceil
import datetime
from time import sleep
from random import shuffle

from benchmark.commands import CommandMaker
from benchmark.config import BenchParameters, NodeParameters, ConfigError
from benchmark.utils import Print, BenchError, PathMaker
from benchmark.preserve import *

from types import SimpleNamespace

BANNED_NODES = ["node009", "node010", "node018", "node021", "node034", "node056"]

class OFBench:
    BASE_PORT = 4000

    def __init__(self, bench_parameters_dict, node_parameters_dict, local, username):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid bench parameters", e)
        self.local = local
        if(
            not self.local
        ):
            self.preserve_manager = PreserveManager(username)
        else:
            self.preserve_manager = None
        self.username = username
        self._wd = os.getcwd()
        self._hostnames = None

        self.clients_hostnames = [None]

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file, hostname=None):
        abs_log_path = os.path.abspath(log_file)

        if self.local:
            full_cmd = f"ulimit -s unlimited && {command}"
            subprocess.Popen(
                ["bash", "-lc", full_cmd],
                stdout=open(abs_log_path, "w"),
                stderr=subprocess.STDOUT,
                cwd=PathMaker.hotstuff_code_path(
                    flavor=self.flavor
                ),
            )
        else:
            assert hostname is not None, "Hostname must be provided in remote mode"
            full_cmd = f"source /etc/profile; cd {PathMaker.hotstuff_code_path(flavor=self.flavor)}; ulimit -s unlimited; {command} > {abs_log_path} 2>&1 &"
            ssh_cmd = f"ssh {hostname} '{full_cmd}'"
            print(f"[remote-run] {ssh_cmd}")
            subprocess.Popen(ssh_cmd, shell=True)

    def _kill_nodes(self):

        is_pompe_flavor = self.flavor == "pompe"
        app_name = "hotstuff"
        if is_pompe_flavor:
            app_name = "pompe"
        
        Print.info("Killing all running HotStuff processes...")

        try:
            if not self.local:
                
                hosts = self._get_hostnames()

                for client_host in self.clients_hostnames:
                    if client_host is not None:
                        subprocess.run(
                            f"ssh {client_host} \"pkill -TERM -f 'examples/{app_name}-client' || true\"",
                            shell=True,
                            stderr=subprocess.DEVNULL,
                        )

                sleep(5)

                for host in hosts:
                    Print.info(f"[{host}] Checking for remaining processes (SIGKILL only if needed)")
                    subprocess.run(
                        f"ssh {host} \"pgrep -f 'examples/{app_name}' >/dev/null && pkill -9 -f 'examples/{app_name}' || true\"",
                        shell=True,
                        stderr=subprocess.DEVNULL,
                    )

                self.preserve_manager.kill_reservation("LAST")

            else:
                
                subprocess.run(
                    f"pkill -f 'examples/{app_name}-app' || true",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )
                subprocess.run(
                    f"pkill -f 'examples/{app_name}-client' || true",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )

                sleep(5)

                subprocess.run(
                    f"pkill -9 -f 'examples/{app_name}-app' || true",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )
                subprocess.run(
                    f"pkill -9 -f 'examples/{app_name}-client' || true",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )

        except Exception as e:
            Print.warn(f"Error during kill: {e}")

    def _preserve_machines(self, nodes):
        # we need one machine per node + 1 for client
        self._amount_for_nodes = self.nodes[0]
        self._num_machines = self._amount_for_nodes + 1

        time_string = str(datetime.timedelta(seconds=self.duration + 72 + nodes * 2)) # extra time to set up things
        self.reservation_id = self.preserve_manager.create_reservation(self._num_machines + len(BANNED_NODES), time_string)

    def _get_hostnames(self):
        if self._hostnames:
            return self._hostnames

        reservations = self.preserve_manager.get_own_reservations()
        for v in reservations.values():
            # print(v)
            # should be exactly one
            self._hostnames = v.assigned_machines
            return self._hostnames
        return []

    def _parse_hotstuff_logs(self, client_logs):

        # if self.flavor != "pompe": 
        #     abs_paths = []
        #     for lf in client_logs:
        #         abs_log_path = os.path.abspath(
        #             os.path.join(os.path.dirname(os.path.dirname(__file__)), lf)
        #         )
        #         if not os.path.exists(abs_log_path):
        #             raise BenchError("Themis client log not found", FileNotFoundError(abs_log_path))
        #         abs_paths.append(abs_log_path)

        #     cmd = ["python", "./scripts/thr_hist.py", "--interval", "1"]

        #     proc = subprocess.run(
        #         cmd,
        #         cwd=PathMaker.hotstuff_code_path(
        #             flavor=self.flavor
        #         ),
        #         input="".join(open(p, "r").read() for p in abs_paths),
        #         stdout=subprocess.PIPE,
        #         stderr=subprocess.PIPE,
        #         text=True,
        #     )

        #     if proc.returncode != 0:
        #         print(proc.stderr)
        #         raise BenchError(
        #             "Failed to parse Themis logs with thr_hist.py",
        #             proc.stderr,
        #         )

        #     throughput = None
        #     lat_raw = None
        #     lat_wo = None

        #     for line in proc.stdout.splitlines():
        #         line = line.strip()
        #         if line.startswith("[") and line.endswith("]") and throughput is None:
        #             try:
        #                 throughput = ast.literal_eval(line)
        #             except Exception:
        #                 pass
        #         m = re.match(r"lat = ([0-9.]+)ms", line)
        #         if m:
        #             val = float(m.group(1))
        #             if lat_raw is None:
        #                 lat_raw = val
        #             elif lat_wo is None:
        #                 lat_wo = val

        #     print(
        #         "\nthroughput: ", throughput,
        #         "\nlatency_avg_ms: ", lat_raw,
        #         "\nlatency_avg_wo_outliers_ms: ", lat_wo,
        #     )
        # else:

        mode = "client" if self.flavor != "pompe" else "exec"
        
        results_dir = os.path.abspath(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), PathMaker.logs_path())
        )

        cmd = ["python", "./scripts/process.py", mode, results_dir]

        proc = subprocess.run(
            cmd,
            cwd=PathMaker.hotstuff_code_path(flavor="pompe"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        print(proc.stdout)

        if proc.returncode != 0:
            raise BenchError("process.py failed", proc.stderr)
        
        lines = proc.stdout.strip().split('\n')
        for line in lines:
            if "Total tx count: " in line:
                count = int(line.split(": ")[1].strip())
            elif "Average: " in line:
                average_latency = float(line.split(": ")[1].strip())

        print("\n============ Summary ============")
        print(f"TPS: {count / self.duration}")
        print(f"Average latency: {average_latency} ms")
        print("=================================\n")

        # Compute adversarial reordering metric
        reorder_str = self._compute_adversarial_reordering()

        tps = count / self.duration
        rate = self.bench_parameters.rate[0]
        return SimpleNamespace(
            result=lambda: f"{rate} {tps} {average_latency}\n{reorder_str}"
        )

    def _compute_adversarial_reordering(self):
        """
        Parse replica logs for fairness_arrival / fairness_ordered lines
        and compute the Dist(tx,tx') vs fraction-reversed metric.
        """
        import re as _re
        from collections import defaultdict

        n_replicas = self.nodes[0]
        num_faults = int(self.node_parameters.json.get('faults', 0))
        min_coverage = n_replicas - num_faults

        # 1. Parse per-replica arrival times and protocol ordering
        per_node_arrivals = []   # list of dicts: tx_uid -> timestamp
        protocol_order = {}      # tx_uid -> position (from first replica that has it)

        for i in range(n_replicas):
            log_file = os.path.abspath(
                os.path.join(os.path.dirname(os.path.dirname(__file__)),
                             PathMaker.hotstuff_log_file(f"replica-{i}"))
            )
            arrivals_i = {}
            if not os.path.exists(log_file):
                Print.warn(f"Replica log not found: {log_file}")
                per_node_arrivals.append(arrivals_i)
                continue

            try:
                with open(log_file, 'r') as f:
                    log_content = f.read()
            except Exception as e:
                Print.warn(f"Failed to read {log_file}: {e}")
                per_node_arrivals.append(arrivals_i)
                continue

            # Parse arrivals: fairness_arrival tx_uid=<uid> ts=<sec>.<usec>
            for m in _re.finditer(r'fairness_arrival tx_uid=(\d+) ts=(\d+\.\d+)', log_content):
                tx_uid = int(m.group(1))
                ts = float(m.group(2))
                if tx_uid not in arrivals_i or ts < arrivals_i[tx_uid]:
                    arrivals_i[tx_uid] = ts

            per_node_arrivals.append(arrivals_i)

            # Parse protocol ordering (take from first replica that has entries)
            if not protocol_order:
                pos = 0
                for m in _re.finditer(r'fairness_ordered tx_uid=(\d+)', log_content):
                    tx_uid = int(m.group(1))
                    if tx_uid not in protocol_order:
                        protocol_order[tx_uid] = pos
                        pos += 1

        if not protocol_order:
            Print.warn("No fairness_ordered entries found in replica logs")
            return "ADVERSARIAL REORDERING: no data"

        # 2. Build eligible tx list (in protocol order, seen by >= min_coverage nodes)
        eligible = []
        for tx_uid in sorted(protocol_order.keys(), key=lambda t: protocol_order[t]):
            n_seen = sum(1 for na in per_node_arrivals if tx_uid in na)
            if n_seen >= min_coverage:
                eligible.append(tx_uid)

        n_txs = len(eligible)
        if n_txs < 2:
            Print.warn(f"Only {n_txs} eligible txs, need >= 2")
            return "ADVERSARIAL REORDERING: not enough eligible txs"

        Print.info(f"Adversarial reordering: {n_txs} eligible txs, {n_txs*(n_txs-1)//2} pairs, {n_replicas} nodes, min_coverage={min_coverage}")

        # 3. For each pair, compute Dist and check reversal
        start_dist = n_replicas % 2
        max_dist = n_replicas
        # Buckets: dist -> (count, reversed)
        buckets = defaultdict(lambda: [0, 0])

        fin_pos = {tx: protocol_order[tx] for tx in eligible}

        for i_idx in range(n_txs):
            tx_i = eligible[i_idx]
            fi = fin_pos[tx_i]
            for j_idx in range(i_idx + 1, n_txs):
                tx_j = eligible[j_idx]
                fj = fin_pos[tx_j]

                c1 = 0  # nodes where tx_i arrived before tx_j
                c2 = 0  # nodes where tx_j arrived before tx_i
                both = 0

                for na in per_node_arrivals:
                    if tx_i not in na or tx_j not in na:
                        continue
                    both += 1
                    if na[tx_i] < na[tx_j]:
                        c1 += 1
                    elif na[tx_j] < na[tx_i]:
                        c2 += 1

                if both < min_coverage:
                    continue

                dist = abs(c1 - c2)

                # Snap to valid dist value (same parity as n)
                if dist % 2 != start_dist % 2:
                    dist = max(dist - 1, start_dist)
                dist = min(dist, max_dist)

                buckets[dist][0] += 1

                # Reversed = majority says one order, protocol says opposite
                reversed_pair = (c1 > c2 and fj < fi) or (c2 > c1 and fi < fj)
                if reversed_pair:
                    buckets[dist][1] += 1

        # 4. Format output
        dist_values = list(range(start_dist, max_dist + 1, 2))
        total_pairs = 0
        total_reversed = 0
        lines = []
        for d in dist_values:
            count, rev = buckets.get(d, [0, 0])
            total_pairs += count
            total_reversed += rev
            frac = rev / count if count > 0 else 0.0
            lines.append(f"   Dist={d:3d}: {frac:.4f}  ({count:,} pairs, {rev:,} reversed)")

        overall_frac = total_reversed / total_pairs if total_pairs > 0 else 0.0
        header = f"ADVERSARIAL REORDERING (n={n_replicas}, f={num_faults}, eligible={n_txs}, total_pairs={total_pairs:,}, total_reversed={total_reversed:,}, overall={overall_frac:.4f}):"
        result = header + "\n" + "\n".join(lines)
        print(f"\n{result}\n")
        return result

    def run(self, debug=False, local=True, flavor="themis"):
        
        assert isinstance(debug, bool)

        self.flavor = flavor

        Print.heading(
            f"Starting {flavor} local benchmark"
        )

        self._kill_nodes()

        try:

            Print.info("Setting up testbed...")
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            subprocess.run(
                "rm -rf log* hotstuff.conf hotstuff-sec*.conf ./conf-gen/*.conf  nodes.txt ips.txt", 
                shell=True, 
                cwd=PathMaker.hotstuff_code_path(
                    flavor=self.flavor
                )
            )

            sleep(0.5)

            Print.info(f"Compiling {flavor} ...")
            cmd = CommandMaker.compile_hotstuff()
            subprocess.run(cmd, shell=True, check=True, cwd=PathMaker.hotstuff_code_path(
                flavor=self.flavor
            ))

            if(
                local
            ):
                replica_IPs = ['127.0.0.1'] * self.nodes[0]
                self.clients_hostnames = [None]
            else:
                self._preserve_machines(self.nodes[0])
                sleep(5)
                all_hostnames = self._get_hostnames()
                for banned_node in BANNED_NODES:
                    if banned_node in all_hostnames:
                        all_hostnames.remove(banned_node)
                shuffle(all_hostnames)
                all_hostnames = all_hostnames[:self._num_machines]
                replica_IPs = all_hostnames[:self._amount_for_nodes]
                self.clients_hostnames = all_hostnames[self._amount_for_nodes:]

            Print.info(f"Generating {flavor} configuration files...")
            if(
                flavor == "rashnu"
            ):
                sb_users = int(self.node_parameters.json['lo_size'])
                sb_prob = 0.95
                sb_skew_factor = 0.99
            else:
                # The params do not apply, so it does not matter
                sb_users = 1000000
                sb_prob = 0.9 # Keep like this as otherwise default tx_size increases
                sb_skew_factor = 0.1
            

            cmd = CommandMaker.generate_hotstuff_config(
                n_replica_ips=replica_IPs,
                block_size=self.node_parameters.json['lo_size'],
                fairness=self.node_parameters.json['gamma'],
                sb_users=sb_users,
                sb_prob=sb_prob,
                sb_skew_factor=sb_skew_factor,
                flavor=self.flavor
            )
            subprocess.run(
                cmd,
                shell=True,
                check=True,
                cwd=PathMaker.hotstuff_code_path(
                    flavor=flavor
                ),
            )

            logs_dir = os.path.abspath(PathMaker.logs_path())
            os.makedirs(logs_dir, exist_ok=True)
            Print.info(f"Logs directory ensured at {logs_dir}")

            Print.info(f"Starting {flavor} Replicas ...")
            num_faults = int(self.node_parameters.json.get('faults', 0))
            replica_cmds = CommandMaker.run_hotstuff_replicas(
                n_replicas=self.nodes[0],
                is_pompe_variant=(
                    flavor == "pompe"
                ),
                num_faults=num_faults
            )
            for i, cmd in enumerate(replica_cmds):
                log_file = PathMaker.hotstuff_log_file(f"replica-{i}")
                self._background_run(cmd, log_file, replica_IPs[i])

            sleep(5) # Wait for replicas to be spawned, otherwise client will silently exit

            Print.info(f"Starting {flavor} Client(s) ...")
            client_logs = []
            for i, hostname in enumerate(self.clients_hostnames):
                client_cmd = CommandMaker.run_hotstuff_client(
                    idx=0,
                    max_async=int(self.bench_parameters.rate[0]),
                    tx_size=self.tx_size,
                    fairness=self.node_parameters.json['gamma'],
                    sb_users=sb_users,
                    sb_prob=sb_prob,
                    sb_skew_factor=sb_skew_factor,
                    flavor=self.flavor
                )
                client_log = PathMaker.hotstuff_log_file(f"client-{i}")
                if self.flavor != "pompe":
                    client_logs.append(client_log)
                else:
                    client_logs.append(
                        os.path.join("logs", f"client{i}.exec.log")
                    )
                self._background_run(client_cmd, client_log, hostname=hostname)

            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)

            self._kill_nodes()

            sleep(2 * self.nodes[0])

            Print.info(f"Parsing {flavor} logs...")
            return self._parse_hotstuff_logs(client_logs)

        except Exception as e:
            try:
                self._kill_nodes()
            except BenchError:
                pass
            raise BenchError(f"Failed to run {flavor} benchmark", e)