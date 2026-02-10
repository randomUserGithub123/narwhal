import subprocess
import os
import datetime
import traceback
from math import ceil
from time import sleep
from random import shuffle

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    NodeParameters,
    BenchParameters,
    ConfigError,
    DASCommittee,
)
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker
from benchmark.grid5000_preserve import PreserveManager


BANNED_NODES = []


class Grid5000Bench:

    BASE_PORT = 4000

    def __init__(
        self,
        bench_parameters_dict: dict,
        node_parameters_dict: dict,
        username: str,
        site: str,
    ):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

        self.username = username
        self.site = site
        self.preserve_manager = PreserveManager(username, site=site)
        self._wd = os.getcwd()
        self._hostnames = None
        self._job_id = None

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    # ── SSH helpers (direct from frontend) ──────────────────────────────
    def _background_run(self, command: str, log_file: str, hostname: str):
        """
        Run *command* on *hostname* via SSH (non-blocking).
        *hostname* is a short name like ``parasilo-3`` — reachable
        directly from the site frontend.
        """
        remote = (f"source /etc/profile; cd {self._wd}; "
                  f"{command} 2> {log_file}")
        ssh = f"ssh -o StrictHostKeyChecking=no {hostname} '{remote}'"
        subprocess.Popen(ssh, shell=True)

    def _run_on_host(self, command: str, hostname: str):
        """Run *command* synchronously on *hostname*, return stdout."""
        remote = f"source /etc/profile; cd {self._wd}; {command}"
        ssh = f"ssh -o StrictHostKeyChecking=no {hostname} '{remote}'"
        result = subprocess.run(ssh, shell=True, capture_output=True, text=True)
        return result.stdout

    # ── Kill ────────────────────────────────────────────────────────────
    def _kill_nodes(self):
        try:
            hosts = self._get_hostnames()
            cmd = CommandMaker.cleanup(username=self.username)
            for host in hosts:
                self._background_run(cmd, "/dev/null", host)
        except Exception:
            pass

        try:
            self.preserve_manager.kill_reservation("LAST")
        except Exception as e:
            print(f"Exception killing reservation: {e}\n"
                  f"{traceback.format_exc()}")

    # ── Machine allocation ──────────────────────────────────────────────
    def _compute_machine_counts(self):
        """Compute how many machines are needed (mirrors DASBench)."""
        nodes = self.nodes[0]
        if self.collocate:
            self._amount_for_nodes = nodes
            self._num_machines = nodes + ceil(nodes * (self.workers - 1) / 4)
        else:
            self._amount_for_nodes = nodes + nodes * self.workers
            self._num_machines = (
                self._amount_for_nodes + ceil(nodes * (self.workers - 1) / 4)
            )

    def _distribute_across_clusters(self, nodes):
        """
        Distribute *nodes* primaries across the largest viable clusters.

        - Only considers clusters large enough to hold >= 1 primary+workers
        - Picks at most 4 of the largest clusters
        - Never requests more nodes than a cluster actually has
        """
        MAX_CLUSTERS = 4

        available = self.preserve_manager.clusters
        if not available:
            raise BenchError("No clusters found on this site!", None)

        if self.collocate:
            machines_per_primary = 1
        else:
            machines_per_primary = 1 + self.workers

        client_machines = ceil(nodes * (self.workers - 1) / 4)

        # Only clusters big enough for at least 1 primary, sorted largest first
        viable = [c for c in available
                  if c["nodes_count"] >= machines_per_primary]
        viable.sort(key=lambda c: c["nodes_count"], reverse=True)

        if not viable:
            # Fallback: just use the single biggest cluster
            viable = sorted(available,
                            key=lambda c: c["nodes_count"], reverse=True)[:1]

        use_clusters = viable[:min(MAX_CLUSTERS, nodes, len(viable))]
        cluster_names = [c["uid"] for c in use_clusters]
        cluster_capacity = {c["uid"]: c["nodes_count"] for c in use_clusters}

        Print.info(
            "Using %d cluster(s): %s" % (
                len(cluster_names),
                ", ".join("%s(cap=%d)" % (n, cluster_capacity[n])
                          for n in cluster_names),
            )
        )

        assignment = {name: 0 for name in cluster_names}
        primary_to_cluster = {}

        for i in range(nodes):
            cluster = cluster_names[i % len(cluster_names)]
            assignment[cluster] += machines_per_primary
            primary_to_cluster[i] = cluster

        for i in range(client_machines):
            cluster = cluster_names[i % len(cluster_names)]
            assignment[cluster] += 1

        # Never exceed cluster capacity
        for name in list(assignment.keys()):
            cap = cluster_capacity[name]
            if assignment[name] > cap:
                Print.info(
                    "Clamping %s from %d to %d (capacity limit)"
                    % (name, assignment[name], cap)
                )
                assignment[name] = cap

        self._primary_to_cluster = primary_to_cluster
        return {k: v for k, v in assignment.items() if v > 0}

    def _preserve_machines(self, nodes: int):
        """Reserve machines, possibly across multiple clusters."""
        self._compute_machine_counts()
        total_seconds = self.duration + 75 + 2 * nodes
        walltime = str(datetime.timedelta(seconds=total_seconds))

        cluster_requests = self._distribute_across_clusters(nodes)

        # Check if we actually need multiple clusters
        if len(cluster_requests) == 1:
            cluster_name = list(cluster_requests.keys())[0]
            count = list(cluster_requests.values())[0] + len(BANNED_NODES)
            self._job_id = self.preserve_manager.create_reservation(
                count, walltime, cluster=cluster_name
            )
        else:
            # Add buffer for banned nodes to the largest cluster
            largest = max(cluster_requests, key=cluster_requests.get)
            cluster_requests[largest] += len(BANNED_NODES)
            self._job_id = self.preserve_manager.create_multi_cluster_reservation(
                cluster_requests, walltime
            )

        Print.info(f"Waiting for Grid'5000 job {self._job_id} to start ...")
        self.preserve_manager.wait_for_reservation(self._job_id, timeout=600)

    def _get_hostnames(self):
        if self._hostnames:
            return self._hostnames
        reservations = self.preserve_manager.get_own_reservations()
        for v in reservations.values():
            self._hostnames = v.assigned_machines
            return self._hostnames
        return []

    # ── Main benchmark loop ─────────────────────────────────────────────
    def run(self, debug=False, console=False, build=True):
        assert isinstance(debug, bool)
        Print.heading("Starting Grid'5000 benchmark")

        self._kill_nodes()

        try:
            Print.info("Setting up testbed ...")
            nodes, rate = self.nodes[0], self.rate[0]
            arbitragers = self.arbitragers
            attack_type = self.attack_type

            # Clean local logs/state
            cmd = (f"{CommandMaker.clean_logs()} ; "
                   f"{CommandMaker.cleanup(self.username)}")
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)

            # ── 1. Build ────────────────────────────────────────────────
            if build:
                Print.info("Rebuilding binaries")
                cmd = CommandMaker.compile().split()
                subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())
                cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
                subprocess.run([cmd], shell=True)

            # ── 2. Generate keys ────────────────────────────────────────
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]
            names = [x.name for x in keys]

            # ── 3. Reserve machines (possibly multi-cluster) ────────────
            self._preserve_machines(nodes=nodes)
            sleep(5)

            all_hostnames = self._get_hostnames()
            for banned in BANNED_NODES:
                if banned in all_hostnames:
                    all_hostnames.remove(banned)
            all_hostnames = all_hostnames[:self._num_machines]

            # Group by cluster for smart assignment
            by_cluster = PreserveManager.group_nodes_by_cluster(all_hostnames)
            Print.info(
                f"Allocated {len(all_hostnames)} machines across "
                f"{len(by_cluster)} cluster(s): "
                + ", ".join(f"{k}({len(v)})" for k, v in by_cluster.items())
            )

            # Shuffle within each cluster for randomness
            for hosts in by_cluster.values():
                shuffle(hosts)

            # ── 4. Assign machines to roles ─────────────────────────────
            # Flatten back, but cluster-grouped: primary i prefers
            # cluster assigned in _distribute_across_clusters
            nodes_hostnames = []
            clients_hostnames = []

            # If we have cluster mapping, assign per cluster
            if hasattr(self, '_primary_to_cluster') and len(by_cluster) > 1:
                # Build per-primary hostname pools from their cluster
                cluster_iterators = {
                    c: iter(hosts) for c, hosts in by_cluster.items()
                }
                for i in range(nodes):
                    cluster = self._primary_to_cluster.get(
                        i, list(by_cluster.keys())[0]
                    )
                    it = cluster_iterators.get(cluster)
                    if it is None:
                        it = cluster_iterators[list(by_cluster.keys())[0]]
                    if self.collocate:
                        # 1 machine for primary + all workers
                        nodes_hostnames.append(next(it))
                    else:
                        # 1 for primary + W for workers
                        nodes_hostnames.append(next(it))  # primary
                        for _ in range(self.workers):
                            nodes_hostnames.append(next(it))  # workers

                # Remaining machines → clients
                for c, it in cluster_iterators.items():
                    clients_hostnames.extend(list(it))
            else:
                # Single cluster or fallback: simple split like DAS
                shuffle(all_hostnames)
                nodes_amount = self._amount_for_nodes
                nodes_hostnames = all_hostnames[:nodes_amount]
                clients_hostnames = all_hostnames[nodes_amount:]

            Print.info(
                f"Role assignment: {len(nodes_hostnames)} node machines, "
                f"{len(clients_hostnames)} client machines"
            )

            # ── 5. Build committee ──────────────────────────────────────
            committee = DASCommittee(
                names,
                self.BASE_PORT,
                self.workers,
                self.faults,
                arbitragers,
                attack_type,
                nodes_hostnames,
            )
            committee.print(PathMaker.committee_file())
            print(committee.json)
            self.node_parameters.print(PathMaker.parameters_file())

            # ── 6. Launch clients ───────────────────────────────────────
            workers_addresses = committee.workers_addresses()
            rate_share = ceil(rate / (committee.workers() - committee.size()))

            clients_workers_addresses = []
            for c_id in range(committee.workers() - committee.size()):
                worker_id = (c_id % (self.workers - 1)) + 1
                workers = []
                for addresses in workers_addresses:
                    for w_id, w_address in addresses:
                        if w_id == worker_id:
                            workers.append(w_address)
                            break
                clients_workers_addresses.append((f"{worker_id}", workers))

            for i, (wid, worker_list) in enumerate(clients_workers_addresses):
                addresses = ",".join(worker_list)
                cmd = CommandMaker.run_client(
                    addresses, self.tx_size, rate_share, worker_list,
                )
                log_file = PathMaker.client_log_file(i, wid)
                host = clients_hostnames[i // 4]
                print(f"Launching client on {host}")
                self._background_run(cmd, log_file, host)

            # ── 7. Launch primaries ─────────────────────────────────────
            faulty_node_ids = committee.get_byzantine_nodes(f=self.faults)
            for i, address in enumerate(committee.primary_addresses()):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, username=self.username),
                    PathMaker.parameters_file(),
                    is_byzantine=int(i in faulty_node_ids),
                    debug=debug,
                )
                log_file = PathMaker.primary_log_file(i)
                host = address.split(":")[0]
                print(f"Launching primary on {host}")
                self._background_run(cmd, log_file, host)

            # ── 8. Launch workers ───────────────────────────────────────
            for i, addresses in enumerate(workers_addresses):
                for wid, address in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, wid, username=self.username),
                        PathMaker.parameters_file(),
                        wid,
                        is_byzantine=int(i in faulty_node_ids),
                        debug=debug,
                    )
                    log_file = PathMaker.worker_log_file(i, wid)
                    host = address.split(":")[0]
                    print(f"Launching worker {i}-{wid} on {host}")
                    self._background_run(cmd, log_file, host)

            # ── 9. Wait ─────────────────────────────────────────────────
            Print.info(f"Running benchmark ({self.duration} sec) ...")
            sleep(self.duration)
            self._kill_nodes()
            sleep(nodes * 2)

            # ── 10. Parse logs (NFS = already local) ────────────────────
            Print.info("Parsing logs ...")
            log_values = LogParser.process(
                PathMaker.logs_path(),
                attack_type=attack_type,
                arbitragers=arbitragers,
                faults=self.faults,
            )

            cmd = f"{CommandMaker.cleanup(username=self.username)}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            return log_values

        except subprocess.SubprocessError as e:
            cmd = f"{CommandMaker.cleanup(username=self.username)}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            self._kill_nodes()
            raise BenchError("Failed to run benchmark", e)
        except ParseError as e:
            raise BenchError("Error parsing logs — possible panic", e)