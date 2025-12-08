# Copyright(C) Facebook, Inc. and its affiliates.
import os
import re
import ast
import subprocess
import datetime
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import BenchParameters, NodeParameters, ConfigError
from benchmark.utils import Print, BenchError, PathMaker
from benchmark.preserve import *

BANNED_NODES = []

class ThemisBench:
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
                cwd=PathMaker.themis_code_path(),
            )
        else:
            assert hostname is not None, "Hostname must be provided in remote mode"
            full_cmd = f"source /etc/profile; cd {PathMaker.themis_code_path()}; ulimit -s unlimited; {command} > {abs_log_path} 2>&1 &"
            ssh_cmd = f"ssh {hostname} '{full_cmd}'"
            print(f"[remote-run] {ssh_cmd}")
            subprocess.Popen(ssh_cmd, shell=True)

    def _kill_nodes(self):
        
        Print.info("Killing all running HotStuff processes...")

        try:
            if not self.local:
                
                hosts = self._get_hostnames()

                client_host = hosts[-1]
                Print.info(f"[{host}] Sending SIGTERM to hotstuff-client (graceful shutdown)")
                subprocess.run(
                    f"ssh {client_host} \"pkill -TERM -f 'examples/hotstuff-client' || true\"",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )

                sleep(5)

                for host in hosts:
                    Print.info(f"[{host}] Checking for remaining processes (SIGKILL only if needed)")
                    subprocess.run(
                        f"ssh {host} \"pgrep -f 'examples/hotstuff' >/dev/null && pkill -9 -f 'examples/hotstuff' || true\"",
                        shell=True,
                        stderr=subprocess.DEVNULL,
                    )

                self.preserve_manager.kill_reservation("LAST")

            else:
                
                subprocess.run(
                    "pkill -f 'examples/hotstuff-app' || true",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )
                subprocess.run(
                    "pkill -f 'examples/hotstuff-client' || true",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )

                sleep(5)

                subprocess.run(
                    "pkill -9 -f 'examples/hotstuff-app' || true",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )
                subprocess.run(
                    "pkill -9 -f 'examples/hotstuff-client' || true",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )

        except Exception as e:
            Print.warn(f"Error during kill: {e}")

    def _preserve_machines(self):
        # we need one machine per node + one machine for client
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

    def _parse_themis_logs(self):
        log_file = PathMaker.themis_log_file("client")
        abs_log_path = os.path.abspath(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), log_file)
        )

        if not os.path.exists(abs_log_path):
            raise BenchError(
                "Themis client log not found",
                FileNotFoundError(abs_log_path),
            )

        cmd = ["python", "./scripts/thr_hist.py", "--interval", "1"]

        proc = subprocess.run(
            cmd,
            cwd=PathMaker.themis_code_path(),
            stdin=open(abs_log_path, "r"),
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

    def run(self, debug=False, local=True):
        assert isinstance(debug, bool)
        Print.heading("Starting Themis local benchmark")

        self._kill_nodes()

        try:

            Print.info("Setting up testbed...")
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            subprocess.run(
                "rm -f log* hotstuff.conf hotstuff-sec*.conf nodes.txt ips.txt", 
                shell=True, 
                cwd=PathMaker.themis_code_path()
            )

            sleep(0.5)

            Print.info("Compiling Themis ...")
            cmd = CommandMaker.compile_themis()
            subprocess.run(cmd, shell=True, check=True, cwd=PathMaker.themis_code_path())

            if(
                local
            ):
                replica_IPs = ['127.0.0.1'] * self.nodes[0]
                clients_hostnames = [None]
            else:
                self._preserve_machines()
                sleep(1.5)
                all_hostnames = self._get_hostnames()
                all_hostnames = all_hostnames[:self._num_machines]
                replica_IPs = all_hostnames[:self._amount_for_nodes]
                clients_hostnames = all_hostnames[self._amount_for_nodes:]

            Print.info("Generating Themis configuration files...")
            cmd = CommandMaker.generate_themis_config(
                n_replica_ips=replica_IPs,
                # block_size=int(self.bench_parameters.rate[0] / self.nodes[0]),
                block_size=100, # Hardcode for now
                fairness=self.node_parameters.json['gamma'],
            )
            subprocess.run(
                cmd,
                shell=True,
                check=True,
                cwd=PathMaker.themis_code_path(),
            )

            logs_dir = os.path.abspath(PathMaker.logs_path())
            os.makedirs(logs_dir, exist_ok=True)
            Print.info(f"Logs directory ensured at {logs_dir}")

            Print.info("Starting Themis Replicas ...")
            replica_cmds = CommandMaker.run_themis_replicas(self.nodes[0])
            for i, cmd in enumerate(replica_cmds):
                log_file = PathMaker.themis_log_file(f"replica-{i}")
                self._background_run(cmd, log_file, replica_IPs[i])

            sleep(5) # Wait for replicas to be spawned, otherwise client will silently exit

            Print.info("Starting Themis Client ...")
            client_cmd = CommandMaker.run_themis_client(
                idx=0,
                max_async=int(self.bench_parameters.rate[0] / self.nodes[0]),
                fairness=self.node_parameters.json['gamma'],
            )
            client_log = PathMaker.themis_log_file("client")
            self._background_run(client_cmd, client_log, clients_hostnames[0])

            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)

            self._kill_nodes()

            Print.info("Parsing Themis logs...")
            return self._parse_themis_logs()

        except Exception as e:
            try:
                self._kill_nodes()
            except BenchError:
                pass
            raise BenchError("Failed to run Themis benchmark", e)
