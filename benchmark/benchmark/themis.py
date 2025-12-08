# Copyright(C) Facebook, Inc. and its affiliates.
import os
import re
import ast
import subprocess
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import BenchParameters, NodeParameters, ConfigError
from benchmark.utils import Print, BenchError, PathMaker


class ThemisBench:
    BASE_PORT = 4000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid bench parameters", e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        abs_log_path = os.path.abspath(log_file)
        log_fd = open(abs_log_path, "w")

        full_cmd = f"ulimit -s unlimited && {command}"

        proc = subprocess.Popen(
            ["bash", "-lc", full_cmd],
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            cwd=PathMaker.themis_code_path(),
        )
        return proc

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            pass

        # SIGKILL to hotstuff-appp; SIGTERM to hotstuff-client
        try:
            subprocess.run(
                "pkill -9 -f 'examples/hotstuff-app' || true",
                shell=True,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                "pkill -f 'examples/hotstuff-client' || true",
                shell=True,
                stderr=subprocess.DEVNULL,
            )
        except subprocess.SubprocessError:
            pass

        sleep(5)

        # SIGKILL to hotstuff-client
        try:
            subprocess.run(
                "pkill -9 -f 'examples/hotstuff-client' || true",
                shell=True,
                stderr=subprocess.DEVNULL,
            )
        except subprocess.SubprocessError:
            pass

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
            else:
                # TODO: Generate from 'preserve' in DAS
                pass

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

            Print.info("Starting Themis Replicas ...")
            replica_cmds = CommandMaker.run_themis_replicas(self.nodes[0])
            for i, cmd in enumerate(replica_cmds):
                log_file = PathMaker.themis_log_file(f"replica-{i}")
                self._background_run(cmd, log_file)

            sleep(5) # Wait for replicas to be spawned, otherwise client will silently exit

            Print.info("Starting Themis Client ...")
            client_cmd = CommandMaker.run_themis_client(
                idx=0,
                max_async=int(self.bench_parameters.rate[0] / self.nodes[0]),
                fairness=self.node_parameters.json['gamma'],
            )
            client_log = PathMaker.themis_log_file("client")
            self._background_run(client_cmd, client_log)

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
