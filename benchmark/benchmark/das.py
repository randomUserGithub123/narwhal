# pyright: basic
# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess, os
import datetime
from math import ceil
from os.path import basename, splitext
from time import sleep, time as time_func
from random import choice, randrange, shuffle
import random

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    LocalCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
    DASCommittee
)
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker
from benchmark.preserve import *

BANNED_NODES = [] # ["node008", "node020", "node021", "node055"]

class DASBench:
    BASE_PORT = 4000

    def __init__(self, bench_parameters_dict, node_parameters_dict, username):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)
        self.preserve_manager = PreserveManager(username)
        self.username = username
        self._wd = os.getcwd()
        self._hostnames = None

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file, hostname):
        # name = splitext(basename(log_file))[0]
        cmd = f"{command} 2> {log_file}"
        #subprocess.run(["tmux", "new", "-d", "-s", name, cmd], check=True)
        # Print.info(f"Running {cmd} on {hostname}")
        process = f"ssh {hostname} 'source /etc/profile; cd {self._wd}; {cmd}'"
        
        subprocess.Popen(process, shell=True)

    def _kill_nodes(self):

        hosts = self._get_hostnames()
        cmd = CommandMaker.cleanup(self.username)

        for host in hosts:
            print(f"Cleaning up {host}")
            self._background_run(cmd, "/dev/null", host)
        sleep(5)
        self.preserve_manager.kill_reservation("LAST")
    
    def _preserve_machines(self):
        # we need one machine per node (primary and workers are colocated) and 1 per 4 clients (which is nodes*workers)
        if self.collocate:
            self._amount_for_nodes = self.nodes[0]
            self._num_machines = self._amount_for_nodes + ceil(self.nodes[0] * self.workers / 4)
        else:
            # one machine per primary, 1 machine per worker and 1 machine per 4 clients (same amount)
            self._amount_for_nodes = self.nodes[0] + self.nodes[0] * self.workers
            self._num_machines = self._amount_for_nodes + ceil(self.nodes[0] * self.workers / 4)
        time_string = str(datetime.timedelta(seconds=self.duration + 30)) # extra time to set up things
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

    def run(self, debug=False, build=True):
        assert isinstance(debug, bool)
        Print.heading("Starting DAS benchmark")

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info("Setting up testbed...")
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup(self.username)}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            if build:
                # Recompile the latest code.
                cmd = CommandMaker.compile().split()
                subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

                # Create alias for the client and nodes binary.
                cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
                subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]

            self._preserve_machines()
            sleep(5)
            all_hostnames = self._get_hostnames()
            for n in BANNED_NODES:
                if n in all_hostnames:
                    all_hostnames.remove(n)
            all_hostnames = all_hostnames[:self._num_machines]
            shuffle(all_hostnames)
            nodes_amount = self._amount_for_nodes
            nodes_hostnames = all_hostnames[:nodes_amount]
            clients_hostnames = all_hostnames[nodes_amount:]
            
            committee = DASCommittee(
                names,
                self.BASE_PORT,
                self.workers,
                self.faults,
                nodes_hostnames
            )
            committee.print(PathMaker.committee_file())
            # print(committee.json)

            self.node_parameters.print(PathMaker.parameters_file())

            # Run the clients (they will wait for the nodes to be ready).
            workers_addresses = committee.workers_addresses()
            rate_share = ceil(rate / committee.workers())
            for i, addresses in enumerate(workers_addresses):
                for id, address in addresses:
                    cmd = CommandMaker.run_client(
                        address,
                        self.tx_size,
                        rate_share,
                        [x for y in workers_addresses for _, x in y],
                    )
                    log_file = PathMaker.client_log_file(i, id)
                    self._background_run(cmd, log_file, clients_hostnames[i % len(clients_hostnames)])

            # FEATURE: 'is_byzantine' primary
            random.seed(int(time_func()))
            byzantine_replica_IDs = random.sample(list(range(committee.size())), int(self.faults))

            # Run the primaries (except the faulty ones).
            for i, address in enumerate(committee.primary_addresses(self.faults)):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, username=self.username),
                    PathMaker.parameters_file(),
                    is_byzantine=(
                        i in byzantine_replica_IDs
                    ),
                    debug=debug
                )
                log_file = PathMaker.primary_log_file(i)
                print(f"Launching primary on {address}")
                self._background_run(cmd, log_file, address.split(":")[0])

            # Run the workers (except the faulty ones).
            for i, addresses in enumerate(workers_addresses):
                for id, address in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i,username=self.username, j=id),
                        PathMaker.parameters_file(),
                        id
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    print(f"Launching worker on {address}")
                    self._background_run(cmd, log_file, address.split(":")[0])

            # Wait for all transactions to be processed.
            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)
            self._kill_nodes()

            sleep(5)

            # Parse logs and return the parser.
            Print.info("Parsing logs...")
            log_values =  LogParser.process(
                PathMaker.logs_path(),
                faults=self.faults,
            )

            cmd = f"{CommandMaker.cleanup(username=self.username)}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

            return log_values
        except (subprocess.SubprocessError, ParseError) as e:
            cmd	= f"{CommandMaker.cleanup(username=self.username)}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            self._kill_nodes()
            raise BenchError("Failed to run benchmark", e)
