#!/usr/bin/env python3

from __future__ import print_function

import os
import time
import requests

# ── Defaults ────────────────────────────────────────────────────────────────
DEFAULT_TIME = "0:15:00"
DEFAULT_TIMEOUT = 1800  # seconds

# Grid'5000 REST API base
API_BASE = "https://api.grid5000.fr/stable"


# ── Exceptions ──────────────────────────────────────────────────────────────
class InvalidNumMachinesException(Exception):
    pass

class ReservationFailedException(Exception):
    pass

class ReservationNotFoundException(Exception):
    pass


# ── Data class (same interface as DAS PreserveReservation) ──────────────────
class Grid5000Reservation:
    def __init__(self, reservation_id, username, start_time, end_time,
                 state, num_machines, assigned_machines):
        self.__reservation_id = reservation_id
        self.__username = username
        self.__start_time = start_time
        self.__end_time = end_time
        self.__state = state
        self.__num_machines = num_machines
        self.__assigned_machines = assigned_machines

    @property
    def reservation_id(self):
        return self.__reservation_id

    @property
    def username(self):
        return self.__username

    @property
    def start_time(self):
        return self.__start_time

    @property
    def end_time(self):
        return self.__end_time

    @property
    def state(self):
        return self.__state

    @property
    def num_machines(self):
        return self.__num_machines

    @property
    def assigned_machines(self):
        return self.__assigned_machines


# ── Helpers ─────────────────────────────────────────────────────────────────
_STATE_MAP = {
    "waiting": "W", "launching": "L", "running": "R",
    "terminated": "T", "error": "E", "hold": "W",
}

def _normalise_state(oar_state: str):
    return _STATE_MAP.get(oar_state.lower(), oar_state[0].upper())

def _short_hostname(fqdn: str):
    """``parasilo-1.rennes.grid5000.fr`` → ``parasilo-1``"""
    return fqdn.split(".")[0]

def _seconds_to_walltime(seconds: int):
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h}:{m:02d}:{s:02d}"

def _api_get(path: str) -> dict:
    """GET from the Grid'5000 API.  No auth needed from a frontend."""
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def _api_post(path: str, payload: dict) -> dict:
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.post(url, json=payload)
    if not r.ok:
        print(f"[Grid'5000] API error {r.status_code}: {r.text}")
        r.raise_for_status()
    return r.json()

def _api_delete(path: str):
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.delete(url)
    if r.status_code not in (200, 202, 204):
        r.raise_for_status()

def _reservation_from_job(job: dict) -> Grid5000Reservation:
    assigned = []
    nodes = job.get("assigned_nodes", [])
    if nodes:
        assigned = sorted([_short_hostname(n) for n in nodes])
    return Grid5000Reservation(
        reservation_id=int(job["uid"]),
        username=job.get("user", "unknown"),
        start_time=job.get("started_at", ""),
        end_time=job.get("stopped_at", ""),
        state=_normalise_state(job.get("state", "unknown")),
        num_machines=len(assigned) if assigned else job.get("nodes_count", 0),
        assigned_machines=assigned,
    )


# ── Cluster & site discovery ───────────────────────────────────────────────
def discover_sites():
    """Return all Grid'5000 site names."""
    data = _api_get("sites")
    return sorted([s["uid"] for s in data.get("items", [])])


def discover_clusters(site: str):
    """
    Return all clusters on *site* with node counts and exotic flag.

    Returns
    -------
    list of dict
        ``[{"uid": "parasilo", "nodes_count": 24, "queues": [...],
            "exotic": False}, ...]``

    Notes
    -----
    Grid'5000 *exotic* clusters (motion-capture studios, specialised GPU
    racks, etc.) require the OAR ``exotic`` job type AND cannot be mixed
    with standard clusters inside a single multi-resource (``+``) OAR job.
    The ``exotic`` flag is detected from three complementary API signals so
    that the caller can separate the two populations before scheduling.
    """
    data = _api_get(f"sites/{site}/clusters")
    clusters = []
    for item in data.get("items", []):
        uid = item["uid"]
        nodes_data = _api_get(f"sites/{site}/clusters/{uid}/nodes")
        nodes_items = nodes_data.get("items", [])
        node_count = len(nodes_items)

        # ── Exotic detection (three complementary signals) ───────────────
        # Signal 1: cluster-level "type" or "supported_job_types" field
        cluster_types = (
            item.get("supported_job_types", [])
            + ([item["type"]] if "type" in item else [])
        )
        # Signal 2: first node's supported_job_types (same hardware class)
        node_types = (
            nodes_items[0].get("supported_job_types", [])
            if nodes_items else []
        )
        is_exotic = (
            "exotic" in cluster_types
            or "exotic" in node_types
            or item.get("type", "").lower() == "exotic"
        )

        clusters.append({
            "uid": uid,
            "nodes_count": node_count,
            "queues": item.get("queues", ["default"]),
            "exotic": is_exotic,
        })
    return clusters


# ── Manager ─────────────────────────────────────────────────────────────────
class PreserveManager:
    def __init__(self, username: str, site: str):
        self.__username = username
        self.__site = site
        self._jobs_path = f"sites/{self.__site}/jobs"

        # Auto-discover clusters
        print(f"[Grid'5000] Discovering clusters on {site} ...")
        self._clusters = discover_clusters(site)
        summary = ", ".join(
            f"{c['uid']}({c['nodes_count']})" for c in self._clusters
        )
        print(f"[Grid'5000] Available clusters: {summary}")

    @property
    def username(self):
        return self.__username

    @property
    def site(self):
        return self.__site

    @property
    def clusters(self):
        return self._clusters

    @property
    def cluster_names(self):
        return [c["uid"] for c in self._clusters]

    def cluster_node_count(self, cluster_name: str):
        for c in self._clusters:
            if c["uid"] == cluster_name:
                return c["nodes_count"]
        return 0

    # ── Listing ─────────────────────────────────────────────────────────
    def get_reservations(self):
        data = _api_get(f"{self._jobs_path}?state=waiting,launching,running")
        result = {}
        for j in data.get("items", []):
            r = _reservation_from_job(j)
            result[r.reservation_id] = r
        return result

    def get_own_reservations(self):
        data = _api_get(
            f"{self._jobs_path}?state=waiting,launching,running"
            f"&user={self.__username}"
        )
        result = {}
        for j in data.get("items", []):
            r = _reservation_from_job(j)
            result[r.reservation_id] = r
        return result

    # ── Single-cluster reservation ──────────────────────────────────────
    def create_reservation(self, num_machines: int, walltime: str,
                           cluster: None = None):
        """
        Reserve *num_machines* whole nodes for *walltime*.

        Parameters
        ----------
        num_machines : int
            Number of physical nodes.
        walltime : str
            ``HH:MM:SS`` or bare seconds.
        cluster : None
            Restrict to one cluster.  ``None`` → any on the site.
        """
        if num_machines < 1:
            raise InvalidNumMachinesException("Need at least 1 machine.")

        if walltime.replace(":", "").isdigit() and ":" not in walltime:
            walltime = _seconds_to_walltime(int(walltime))

        if cluster:
            resources = (f"{{cluster='{cluster}'}}/nodes={num_machines},"
                         f"walltime={walltime}")
        else:
            resources = f"nodes={num_machines},walltime={walltime}"

        print(f"[Grid'5000] Reserving {num_machines} nodes on {self.__site}"
              f" (cluster={cluster or 'any'}) for {walltime}")

        resp = _api_post(self._jobs_path, {
            "name": "narwhal-bench",
            "command": "sleep infinity",
            "resources": resources,
            "types": ["allow_classic_ssh", "exotic"],
        })
        job_id = int(resp["uid"])
        print(f"[Grid'5000] Job submitted — ID {job_id}")
        return job_id

    # ── Multi-cluster reservation (single OAR job) ──────────────────────
    def create_multi_cluster_reservation(
        self,
        cluster_requests,
        walltime: str,
    ):
        """
        Reserve nodes across multiple clusters in ONE OAR job.

        Uses OAR's ``+`` syntax:
        ``{cluster='A'}/nodes=3+{cluster='B'}/nodes=4,walltime=...``

        Parameters
        ----------
        cluster_requests 
            ``{"parasilo": 3, "paravance": 4}``
        walltime : str
            Duration as ``HH:MM:SS``.

        Returns
        -------
        int
            OAR job ID.
        """
        if not cluster_requests:
            raise InvalidNumMachinesException("Empty cluster request.")

        if walltime.replace(":", "").isdigit() and ":" not in walltime:
            walltime = _seconds_to_walltime(int(walltime))

        parts = []
        total = 0
        for cname, count in cluster_requests.items():
            if count < 1:
                continue
            parts.append(f"{{cluster='{cname}'}}/nodes={count}")
            total += count

        resource_str = "+".join(parts) + f",walltime={walltime}"

        desc = ", ".join(f"{c}={n}" for c, n in cluster_requests.items())
        print(f"[Grid'5000] Multi-cluster: {desc} ({total} total) {walltime}")
        print(f"[Grid'5000] OAR resource string: {resource_str}")

        resp = _api_post(self._jobs_path, {
            "name": "narwhal-bench",
            "command": "sleep infinity",
            "resources": resource_str,
            "types": ["allow_classic_ssh", "exotic"],
        })
        job_id = int(resp["uid"])
        print(f"[Grid'5000] Job submitted — ID {job_id}")
        return job_id

    # ── Fetch ───────────────────────────────────────────────────────────
    def fetch_reservation(self, reservation_id) -> Grid5000Reservation:
        if str(reservation_id).upper() == "LAST":
            own = self.get_own_reservations()
            if not own:
                raise ReservationNotFoundException("No reservations found.")
            return own[sorted(own.keys())[-1]]
        job = _api_get(f"{self._jobs_path}/{int(reservation_id)}")
        return _reservation_from_job(job)

    # ── Wait ────────────────────────────────────────────────────────────
    def wait_for_reservation(
        self, reservation_id,
        timeout: int = DEFAULT_TIMEOUT, quiet: bool = False,
    ) -> Grid5000Reservation:
        if str(reservation_id).upper() == "LAST":
            own = self.get_own_reservations()
            reservation_id = sorted(own.keys())[-1]

        start = time.time()
        poll = 5
        while True:
            job = _api_get(f"{self._jobs_path}/{int(reservation_id)}")
            state = job.get("state", "unknown")
            if state in ("running", "terminated", "error"):
                break
            elapsed = time.time() - start
            if elapsed >= timeout:
                raise TimeoutError(
                    f"Job {reservation_id} not started in {timeout}s "
                    f"(state: {state})"
                )
            if not quiet:
                print(f"[Grid'5000] Job {reservation_id} state={state} "
                      f"({elapsed:.0f}s elapsed, next check in {poll}s)")
            time.sleep(poll)
            if elapsed > 60: poll = 10
            if elapsed > 300: poll = 15
            if elapsed > 900: poll = 30

        if state == "error":
            raise ReservationFailedException(f"Job {reservation_id} errored.")
        return _reservation_from_job(job)

    # ── Kill ────────────────────────────────────────────────────────────
    def kill_reservation(self, reservation_id):
        if str(reservation_id).upper() == "LAST":
            own = self.get_own_reservations()
            if not own:
                raise ReservationNotFoundException("No reservations found.")
            reservation_id = sorted(own.keys())[-1]

        job = _api_get(f"{self._jobs_path}/{int(reservation_id)}")
        if job.get("user") != self.__username:
            raise ReservationNotFoundException("Job doesn't belong to you.")
        _api_delete(f"{self._jobs_path}/{int(reservation_id)}")
        print(f"[Grid'5000] Job {reservation_id} deleted.")

    # ── Utility: group hostnames by cluster ─────────────────────────────
    @staticmethod
    def group_nodes_by_cluster(hostnames):
        """
        ``["parasilo-1", "parasilo-2", "paravance-5"]``
        → ``{"parasilo": ["parasilo-1", "parasilo-2"],
              "paravance": ["paravance-5"]}``
        """
        groups = {}
        for h in hostnames:
            parts = h.rsplit("-", 1)
            cluster = parts[0] if len(parts) == 2 and parts[1].isdigit() else h
            groups.setdefault(cluster, []).append(h)
        return groups