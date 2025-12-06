# Copyright(C) Facebook, Inc. and its affiliates.
import os
import re
import ast
import subprocess
from os.path import basename, splitext
from time import sleep
from datetime import datetime, timedelta
from statistics import mean

from benchmark.commands import CommandMaker
from benchmark.config import BenchParameters, NodeParameters, ConfigError
from benchmark.utils import Print, BenchError, PathMaker


class ThemisResult:
    def __init__(self, throughput_series, avg_throughput, avg_latency_ms, avg_latency_wo_ms):
        self.throughput_series = throughput_series
        self.avg_throughput = avg_throughput
        self.avg_latency_ms = avg_latency_ms
        self.avg_latency_wo_ms = avg_latency_wo_ms

    def result(self):
        return {
            "throughput_series": self.throughput_series,
            "avg_throughput_txs_per_sec": self.avg_throughput,
            "avg_latency_ms": self.avg_latency_ms,
            "avg_latency_wo_outliers_ms": self.avg_latency_wo_ms,
        }


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
        name = splitext(basename(log_file))[0]
        abs_log_path = os.path.abspath(log_file)
        cmd = f"bash -c '{command} 2> {abs_log_path}'"
        subprocess.run(["tmux", "new", "-d", "-s", name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError("Failed to kill testbed", e)

    def _parse_themis_logs(self, interval: float = 1.0, log_indices=None) -> ThemisResult:

        themis_dir = PathMaker.themis_code_path()
        if log_indices is None:
            log_indices = [0]

        buffer_parts = []
        for idx in log_indices:
            path = os.path.join(themis_dir, f"log{idx}")
            if os.path.exists(path):
                with open(path, "r") as f:
                    buffer_parts.append(f.read())

        if not buffer_parts:
            raise BenchError("No Themis logs found (no logN files present).")

        combined_logs = "".join(buffer_parts)

        cmd = ["python3", "scripts/thr_hist.py", "--interval", str(interval)]
        try:
            proc = subprocess.run(
                cmd,
                input=combined_logs,
                text=True,
                capture_output=True,
                cwd=themis_dir,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise BenchError(
                f"thr_hist.py failed with error:\n{e.stderr}"
            ) from e

        lines = [l.strip() for l in proc.stdout.splitlines() if l.strip()]
        if len(lines) < 2:
            raise BenchError(f"Unexpected thr_hist.py output:\n{proc.stdout}")

        try:
            values = ast.literal_eval(lines[0])
        except Exception as e:
            raise BenchError(f"Failed to parse throughput series: {lines[0]}") from e

        lat_re = re.compile(r"lat = ([0-9.]+)ms")
        m1 = lat_re.search(lines[1])
        if not m1:
            raise BenchError(f"Failed to parse latency from: {lines[1]}")
        avg_lat = float(m1.group(1))

        if len(lines) >= 3:
            m2 = lat_re.search(lines[2])
            avg_lat_wo = float(m2.group(1)) if m2 else avg_lat
        else:
            avg_lat_wo = avg_lat

        avg_thr = (sum(values) / len(values) / interval) if values else 0.0

        return ThemisResult(values, avg_thr, avg_lat, avg_lat_wo)

    def run(self, debug=False, local=True):
        assert isinstance(debug, bool)
        Print.heading("Starting Themis local benchmark")

        self._kill_nodes()

        try:
            Print.info("Setting up testbed...")

            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            subprocess.run("rm -f log*", shell=True, cwd=PathMaker.themis_code_path())
            subprocess.run("rm -f hotstuff.conf hotstuff-sec*.conf nodes.txt", shell=True, cwd=PathMaker.themis_code_path())

            sleep(0.5)

            Print.info("Compiling Themis ...")
            cmd = CommandMaker.compile_themis()
            subprocess.run(cmd, shell=True, check=True, cwd=PathMaker.themis_code_path())

            Print.info("Generating Themis configuration files...")
            ips_path = os.path.join(PathMaker.themis_code_path(), "ips.txt")
            with open(ips_path, "w") as f:
                for _ in range(self.nodes[0]):
                    f.write("127.0.0.1\n")
            cmd = CommandMaker.generate_themis_config(
                n_replicas=self.nodes[0],
                block_size=500, # Hardcode for now
                fairness=self.node_parameters.json['gamma'],
                sb_users=(self.rate[0] * self.bench_parameters.duration),
                ips_file=os.path.basename(ips_path),
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

            Print.info("Starting Themis Client ...")
            client_cmd = CommandMaker.run_themis_client(
                idx=0,
                max_async=self.rate[0],
                fairness=self.node_parameters.json['gamma'],
                sb_users=(self.rate[0] * self.bench_parameters.duration),
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
