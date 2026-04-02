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
        self.num_clients = bench_parameters_dict.get('num_clients', 1)
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
        # One machine per replica + 1 machine per 4 clients (like DAS)
        self._amount_for_nodes = self.nodes[0]
        self._num_client_machines = ceil(self.num_clients / 4)
        self._num_machines = self._amount_for_nodes + self._num_client_machines

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
        ALL-PAIRS adversarial reordering via Rust binary (adv_reorder).
        The binary uses rayon for parallel computation — handles 400M+ pairs in seconds.
        """
        import subprocess, json, shutil

        N = self.nodes[0]
        num_faults = int(self.node_parameters.json.get('faults', 0))

        # Find the Rust binary
        binary = shutil.which('adv_reorder')
        if binary is None:
            for candidate in [
                './adv_reorder',
                '../adv_reorder/target/release/adv_reorder',
                os.path.join(self._wd, 'adv_reorder'),
                os.path.join(self._wd, 'adv_reorder/target/release/adv_reorder'),
            ]:
                if os.path.isfile(candidate):
                    binary = candidate
                    break
        if binary is None:
            Print.warn("adv_reorder binary not found, skipping reordering metric")
            return "ADVERSARIAL REORDERING: binary not found"

        # Logs directory
        logs_dir = os.path.abspath(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), PathMaker.logs_path())
        )

        cmd = [
            binary,
            '-d', logs_dir,
            '-n', str(N),
            '-f', str(num_faults),
            '-p', 'hotstuff-replica-*.log',   # Themis log pattern
            '--verbose',
        ]
        Print.info(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                Print.warn(f"adv_reorder failed: {result.stderr[:500]}")
                return "ADVERSARIAL REORDERING: binary failed"

            # Print stderr (progress info) live
            if result.stderr:
                print(result.stderr)

            data = json.loads(result.stdout)

            # Format output table (same style as logs.py)
            start_dist = N % 2
            dist_values = list(range(start_dist, N + 1, 2))
            results_map = {}
            for entry in data.get('results', []):
                results_map[entry['dist']] = entry

            lines = []
            total_pairs = data.get('total_pairs', 0)
            total_reversed = data.get('total_reversed', 0)
            for d in dist_values:
                e = results_map.get(d, {'fraction_reversed': 0.0, 'count': 0, 'reversed': 0})
                lines.append(f"   Dist={d:3d}: {e['fraction_reversed']:.4f}  ({e['count']:,} pairs, {e['reversed']:,} reversed)")

            overall = total_reversed / total_pairs if total_pairs > 0 else 0.0
            n_txs = data.get('eligible_txs', 0)
            header = f"ADVERSARIAL REORDERING (n={N}, f={num_faults}, eligible={n_txs}, total_pairs={total_pairs:,}, total_reversed={total_reversed:,}, overall={overall:.4f}):"
            result_str = header + "\n" + "\n".join(lines)
            print(f"\n{result_str}\n")
            return result_str

        except subprocess.TimeoutExpired:
            Print.warn("adv_reorder timed out (600s)")
            return "ADVERSARIAL REORDERING: timed out"
        except (json.JSONDecodeError, KeyError) as e:
            Print.warn(f"Failed to parse adv_reorder output: {e}")
            return f"ADVERSARIAL REORDERING: parse error: {e}"

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

            num_clients = self.num_clients

            if(
                local
            ):
                replica_IPs = ['127.0.0.1'] * self.nodes[0]
                self.clients_hostnames = [None] * num_clients
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
                client_machines = all_hostnames[self._amount_for_nodes:]
                # Pack up to 4 clients per machine (like DAS)
                self.clients_hostnames = [
                    client_machines[i // 4] for i in range(num_clients)
                ]

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

            Print.info(f"Starting {flavor} Client(s) ({num_clients} clients)...")
            client_logs = []
            rate_share = ceil(int(self.bench_parameters.rate[0]) / num_clients)
            for i, hostname in enumerate(self.clients_hostnames):
                client_cmd = CommandMaker.run_hotstuff_client(
                    idx=i,
                    max_async=rate_share,
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