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

        themis_dir = PathMaker.themis_code_path()
        cmds = []
        for i in range(n_replicas):
            cmd = (
                f"cd {themis_dir} && "
                f"./examples/hotstuff-app --conf ./hotstuff-sec{i}.conf > log{i} 2>&1"
            )
            cmds.append(cmd)
        return cmds

    # Themis
    @staticmethod
    def run_themis_client(
        idx=0,
        max_async=400,
        fairness=1.0,
        sb_users=1_000_000,
        sb_prob=0.9,
        iter_count=-1,
    ):
    
        themis_dir = PathMaker.themis_code_path()
        cmd = (
            f"cd {themis_dir} && "
            f"./examples/hotstuff-client "
            f"--idx {idx} "
            f"--iter {iter_count} "
            f"--max-async {max_async} "
            f"--fairness-parameter {fairness} "
            f"--sb-users {sb_users} "
            f"--sb-prob-choose_mtx {sb_prob}"
        )
        return cmd

    # Themis
    @staticmethod
    def generate_themis_config(
        n_replicas,
        base_port=10000,
        block_size=100,
        fairness=1.0,
        sb_users=1_000_000,
        sb_prob=0.9,
        pace_maker="dummy",
        themis_dir=None,
        ips_file=None,
    ):
        
        assert isinstance(n_replicas, int) and n_replicas > 0
        assert isinstance(base_port, int)

        if themis_dir is None:
            themis_dir = PathMaker.themis_code_path()

        cmd = (
            f"python3 scripts/gen_conf.py "
            f"--prefix hotstuff "
            f"--iter 10 "
            f"--pport {base_port} "
            f"--cport {base_port + 10000} "
            f"--block-size {block_size} "
            f"--pace-maker {pace_maker} "
            f"--sb-users {sb_users} "
            f"--sb-prob-choose_mtx {sb_prob} "
            f"--fairness-parameter {fairness} "
            f"--keygen ./hotstuff-keygen "
            f"--tls-keygen ./hotstuff-tls-keygen "
        )

        if ips_file is not None:
            cmd += f" --ips {ips_file}"

        return cmd

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
