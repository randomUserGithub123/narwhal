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

BANNED_NODES = ["node018", "node056"]

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

    def _kill_nodes(self, is_pompe_flavor: bool = False):

        app_name = "hotstuff"
        if is_pompe_flavor:
            app_name = "pompe"
        
        Print.info("Killing all running HotStuff processes...")

        try:
            if not self.local:
                
                hosts = self._get_hostnames()

                client_host = hosts[-1]
                Print.info(f"[{client_host}] Sending SIGTERM to {app_name}-client (graceful shutdown)")
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

    def _preserve_machines(self):
        # we need one machine per node + 1 for client
        self._amount_for_nodes = self.nodes[0]
        self._num_machines = self._amount_for_nodes + 1

        time_string = str(datetime.timedelta(seconds=self.duration + 60)) # extra time to set up things
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
        
        abs_paths = []
        for lf in client_logs:
            abs_log_path = os.path.abspath(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), lf)
            )
            if not os.path.exists(abs_log_path):
                raise BenchError("Themis client log not found", FileNotFoundError(abs_log_path))
            abs_paths.append(abs_log_path)

        cmd = ["python", "./scripts/thr_hist.py", "--interval", "1"]

        proc = subprocess.run(
            cmd,
            cwd=PathMaker.hotstuff_code_path(
                flavor=self.flavor
            ),
            input="".join(open(p, "r").read() for p in abs_paths),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if proc.returncode != 0:
            print(proc.stderr)
            raise BenchError(
                "Failed to parse Themis logs with thr_hist.py",
                proc.stderr,
            )

        throughput = None
        lat_raw = None
        lat_wo = None

        for line in proc.stdout.splitlines():
            line = line.strip()
            if line.startswith("[") and line.endswith("]") and throughput is None:
                try:
                    throughput = ast.literal_eval(line)
                except Exception:
                    pass
            m = re.match(r"lat = ([0-9.]+)ms", line)
            if m:
                val = float(m.group(1))
                if lat_raw is None:
                    lat_raw = val
                elif lat_wo is None:
                    lat_wo = val

        print(
            "\nthroughput: ", throughput,
            "\nlatency_avg_ms: ", lat_raw,
            "\nlatency_avg_wo_outliers_ms: ", lat_wo,
        )

    def run(self, debug=False, local=True, flavor="themis"):
        
        assert isinstance(debug, bool)

        self.flavor = flavor

        Print.heading(
            f"Starting {flavor} local benchmark"
        )

        self._kill_nodes(
            is_pompe_flavor=(
                self.flavor == "pompe"
            )
        )

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
                clients_hostnames = [None]
            else:
                self._preserve_machines()
                sleep(5)
                all_hostnames = self._get_hostnames()
                for banned_node in BANNED_NODES:
                    if banned_node in all_hostnames:
                        all_hostnames.remove(banned_node)
                shuffle(all_hostnames)
                all_hostnames = all_hostnames[:self._num_machines]
                replica_IPs = all_hostnames[:self._amount_for_nodes]
                clients_hostnames = all_hostnames[self._amount_for_nodes:]

            Print.info(f"Generating {flavor} configuration files...")
            if(
                flavor == "themis"
            ):
                # The params do not apply, so it does not matter
                sb_users = 1000000
                sb_prob = 0.9
                sb_skew_factor = 0.1
            elif(
                flavor == "rashnu"
            ):
                sb_users = 100
                sb_prob = 0.95
                sb_skew_factor = 0.99
            elif(
                flavor == "dikaios"
            ):
                # The params do not apply, so it does not matter
                sb_users = 1000000
                sb_prob = 0.9
                sb_skew_factor = 0.1
            elif(
                flavor == "pompe"
            ):
                # The params do not apply, so it does not matter
                sb_users = 1000000
                sb_prob = 0.9
                sb_skew_factor = 0.1

            cmd = CommandMaker.generate_hotstuff_config(
                n_replica_ips=replica_IPs,
                block_size=self.node_parameters.json['lo_size'],
                fairness=self.node_parameters.json['gamma'],
                sb_users=sb_users,
                sb_prob=sb_prob,
                sb_skew_factor=sb_skew_factor,
                is_pompe_variant=(
                    flavor == "pompe"
                ),
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
            replica_cmds = CommandMaker.run_hotstuff_replicas(
                n_replicas=self.nodes[0],
                is_pompe_variant=(
                    flavor == "pompe"
                )
            )
            for i, cmd in enumerate(replica_cmds):
                log_file = PathMaker.hotstuff_log_file(f"replica-{i}")
                self._background_run(cmd, log_file, replica_IPs[i])

            sleep(5) # Wait for replicas to be spawned, otherwise client will silently exit

            Print.info(f"Starting {flavor} Client(s) ...")
            client_logs = []
            for i, hostname in enumerate(clients_hostnames):
                client_cmd = CommandMaker.run_hotstuff_client(
                    idx=0,
                    max_async=int(self.bench_parameters.rate[0] / self.nodes[0]),
                    tx_size=self.tx_size,
                    fairness=self.node_parameters.json['gamma'],
                    sb_users=sb_users,
                    sb_prob=sb_prob,
                    sb_skew_factor=sb_skew_factor,
                    is_pompe_variant=(
                        flavor == "pompe"
                    )
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

            self._kill_nodes(
                is_pompe_flavor=(
                    self.flavor == "pompe"
                )
            )

            sleep(5)

            Print.info(f"Parsing {flavor} logs...")
            return self._parse_hotstuff_logs(client_logs)

        except Exception as e:
            try:
                self._kill_nodes(
                    is_pompe_flavor=(
                        self.flavor == "pompe"
                    )
                )
            except BenchError:
                pass
            raise BenchError(f"Failed to run {flavor} benchmark", e)
