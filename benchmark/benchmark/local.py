# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep
from random import sample

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    LocalCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 4000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f"{command} 2> {log_file}"
        subprocess.run(["tmux", "new", "-d", "-s", name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError("Failed to kill testbed", e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading("Starting local benchmark")

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info("Setting up testbed...")
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

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
            committee = LocalCommittee(names, self.BASE_PORT, self.workers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Run the clients (they will wait for the nodes to be ready).
            workers_addresses = committee.workers_addresses()

            # ### Default Narwhal Approach : 

            # # Exclude OF_worker as clients do not send txs to it
            # rate_share = ceil(rate / (committee.workers() - committee.size()))

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
            #         self._background_run(cmd, log_file)

            ### Giulio Approach : 
            # current method has 1 client per worker, which means multiple clients per primary, but one primary per client
            # we want 2f+1 primaries per client, so we can send to one worker of each primary
            # assuming each node has the same amount of workers, we will spawn W*N clients and each of them communicates with N workers
            clients_workers_addresses = (
                []
            )  # list of lists, contains addressess of each worker each client should connect to

            # Exclude OF_worker as clients do not send txs to it
            rate_share = ceil(rate / ((committee.workers() - committee.size()) * committee.size()))

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
                    [x for y in workers_addresses for _, x in y],
                )
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(cmd, log_file)

            # Run the primaries.
            faulty_node_ids = sample(
                list(range(0, nodes)),
                self.faults
            )
            for i, address in enumerate(committee.primary_addresses()):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    is_byzantine=int(
                        i in faulty_node_ids
                    ),
                    debug=debug,
                )
                log_file = PathMaker.primary_log_file(i)
                self._background_run(cmd, log_file)

            # Run the workers.
            for i, addresses in enumerate(workers_addresses):
                for id, address in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        is_byzantine=int(
                            i in faulty_node_ids
                        ),
                        debug=debug,
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info("Parsing logs...")
            return LogParser.process(PathMaker.logs_path(), faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError("Failed to run benchmark", e)
