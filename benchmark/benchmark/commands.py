# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup(username=None):
        if username:
            return f'rm -r /var/scratch/{username}/narwhal/benchmark/.db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        else:
            return f"rm .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}"

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'
    
    @staticmethod
    def compile_themis():
        return (
            'rm -f CMakeCache.txt '
            '&& rm -rf CMakeFiles cmake_install.cmake Makefile build '
            '&& cmake -DCMAKE_BUILD_TYPE=Release '
            '-DBUILD_SHARED=ON '
            '-DHOTSTUFF_PROTO_LOG=ON '
            '-DCMAKE_CXX_FLAGS="-include cstdint" && '
            'make -j'
        )

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, committee, store, parameters, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} primary')

    @staticmethod
    def run_worker(keys, committee, store, parameters, id, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} worker --id {id}')

    @staticmethod
    def run_client(address, size, rate, nodes):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return f'./benchmark_client {address} --size {size} --rate {rate} {nodes}'
    
    # Themis
    @staticmethod
    def run_themis_replicas(n_replicas):

        assert isinstance(n_replicas, int) and n_replicas > 0

        cmds = []
        for i in range(n_replicas):
            cmd = (
                f"./examples/hotstuff-app --conf ./hotstuff-sec{i}.conf"
            )
            cmds.append(cmd)
        return cmds

    # Themis
    @staticmethod
    def run_themis_client(
        idx=0,
        max_async=400,
        fairness=1.0,
        sb_users=1000000,
        sb_prob=0.9,
        sb_skew_factor=0.1,
        iter_count=-1,
    ):
    
        cmd = (
            f"./examples/hotstuff-client "
            f"--idx {idx} "
            f"--iter {iter_count} "
            f"--max-async {max_async} "
            f"--fairness-parameter {fairness} "
            f"--sb-users {sb_users} "
            f"--sb-prob-choose_mtx {sb_prob} "
            f"--sb-skew-factor {sb_skew_factor} "
        )
        return cmd

    # Themis
    @staticmethod
    def generate_themis_config(
        n_replica_ips: list,
        base_port=10000,
        block_size=100,
        fairness=1.0,
        sb_users=1000000,
        sb_prob=0.9,
        sb_skew_factor=0.1,
    ):
        
        assert isinstance(base_port, int)

        ips_path = join(PathMaker.themis_code_path(), "ips.txt")
        with open(ips_path, "w") as f:
            for replica_ip in n_replica_ips:
                f.write(f"{replica_ip}\n")

        cmd = (
            f"python3 scripts/gen_conf.py " # There are other default params in 'gen_conf.py'
            f"--prefix hotstuff "
            f"--pport {base_port} "
            f"--cport {base_port + 10000} "
            f"--block-size {block_size} "
            f"--sb-users {sb_users} "
            f"--sb-prob-choose_mtx {sb_prob} "
            f"--sb-skew-factor {sb_skew_factor} "
            f"--fairness-parameter {fairness} "
            f" --ips {ips_path} "
        )

        return cmd

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
