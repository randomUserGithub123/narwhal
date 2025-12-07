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

        try:
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
        except subprocess.SubprocessError:
            pass

    def _parse_themis_logs(self):
        # TODO: Parse logs
        pass

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
                max_async=400, # Hardcode for now
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
