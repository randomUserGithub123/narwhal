# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess, os
import datetime
from math import ceil
from os.path import basename, splitext
from time import sleep
from random import choice, randrange, sample
import traceback

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

BANNED_NODES = []

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
        try:
            hosts = self._get_hostnames()
            cmd = CommandMaker.cleanup()

            for host in hosts:
                self._background_run(cmd, "/dev/null", host)

            self.preserve_manager.kill_reservation("LAST")
        except Exception as e:
            print(
                f"""Exception : {str(e)}\nTraceback: {traceback.format_exc()}"""
            )
    
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

    def run(self, debug=False, console=False, build=True):
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
                Print.info("Rebuilding binaries")
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
            sleep(1.5)
            all_hostnames = self._get_hostnames()
            all_hostnames = all_hostnames[:self._num_machines]
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
            
            # ### Default Narwhal Approach : 

            # # Exclude OF_worker as clients do not send txs to it
            # rate_share = ceil(rate / (committee.workers() - committee.size()))

            # counter = 0
            # for i, addresses in enumerate(workers_addresses):
            #     for id, address in addresses:
            #         if(
            #             int(id) == 0 # Clients do not send txs to OF_worker
            #         ):
            #             continue
            #         cmd = CommandMaker.run_client(
            #             address,
            #             self.tx_size,
            #             rate_share,
            #             [x for y in workers_addresses for _, x in y],
            #         )
            #         log_file = PathMaker.client_log_file(i, id)
            #         print(f"Launching client on {clients_hostnames[counter // 4]}")
            #         self._background_run(cmd, log_file, clients_hostnames[counter // 4])
            #         counter += 1

            ### Giulio Approach :
            # current method has 1 client per worker, which means multiple clients per primary, but one primary per client
            # we want 2f+1 primaries per client, so we can send to one worker of each primary
            # assuming each node has the same amount of workers, we will spawn W*N clients and each of them communicates with N workers

            # Exclude OF_worker as clients do not send txs to it
            rate_share = ceil(rate / ((committee.workers() - committee.size()) * committee.size()))

            clients_workers_addresses = (
                []
            )  # list of lists, contains addressess of each worker each client should connect to

            # For each client, choose one worker id. communicate with all workers with that id
            for c_id in range(committee.workers() - committee.size()):
                worker_id = (c_id % (self.workers - 1)) + 1
                workers = []
                for addresses in workers_addresses:
                    for w_id, w_address in addresses:
                        if w_id == worker_id:
                            workers.append(w_address)
                            break
                clients_workers_addresses.append((f"{worker_id}", workers))
            
            for i, (id, worker_list) in enumerate(clients_workers_addresses):
                addresses = ",".join(worker_list)
                cmd = CommandMaker.run_client(
                    addresses,
                    self.tx_size,
                    rate_share,
                    worker_list,
                )
                log_file = PathMaker.client_log_file(i, id)
                print(f"Launching client on {clients_hostnames[i // 4]}")
                self._background_run(cmd, log_file, clients_hostnames[i // 4])

            # Run the primaries.
            faulty_node_ids = sample(
                list(range(0, nodes)),
                self.faults
            )
            for i, address in enumerate(committee.primary_addresses()):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, username=self.username),
                    PathMaker.parameters_file(),
                    is_byzantine=int(
                        i in faulty_node_ids
                    ),
                    debug=debug,
                )
                log_file = PathMaker.primary_log_file(i)
                print(f"Launching primary on {address}")
                self._background_run(cmd, log_file, address.split(":")[0])

            # Run the workers.
            for i, addresses in enumerate(workers_addresses):
                for id, address in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id, username=self.username),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        is_byzantine=int(
                            i in faulty_node_ids
                        ),
                        debug=debug,
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
            self._kill_nodes()
            raise BenchError("Failed to run benchmark", e)
