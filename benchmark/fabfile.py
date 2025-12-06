# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

import os

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print, PathMaker
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError
from benchmark.das import DASBench
from benchmark.themis import ThemisBench

@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': 50_000,
        'tx_size': 512,
        'duration': 20,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)

@task
def das(ctx, debug=True, console=False, build=True, username="mputnik"):
    for faults, workers_per_node, nodes, runs in [
        (0, 1, 4, 1),
        (0, 1, 10, 1),
    ]:
        """Run benchmarks on DAS5"""
        bench_params = {
            'faults': faults,
            'nodes': nodes,
            'workers': workers_per_node,
            'rate': 50_000,
            'tx_size': 512,
            'duration': 60,
            "collocate": False,
        }
        node_params = {
            "header_size": 1_000,  # bytes
            "max_header_delay": 200,  # ms
            "gc_depth": 50,  # rounds
            "sync_retry_delay": 10_000,  # ms
            "sync_retry_nodes": 3,  # number of nodes
            "batch_size": 500_000,  # bytes
            "max_batch_delay": 200,  # ms
        }
        if console:
            os.system('export RUSTFLAGS="--cfg tokio_unstable"')
        try:
            filename = PathMaker.local_result_file(
                faults,
                workers_per_node,
                nodes,
            )
            for i in range(runs):
                print(f"DAS run {i}\n")
                ret = DASBench(bench_params, node_params, username).run(debug, console, build)
                print(ret.result())
                ret.print(filename)
                
        except BenchError as e:
            Print.error(e)

@task
def themis(ctx, debug=True, local=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 1,
        'nodes': 5,
        'workers': 2, # Not used in Themis
        'rate': 10_000,
        'tx_size': 512,
        'duration': 30,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 50_000,  # bytes
        'max_batch_delay': 200,  # ms
        "lo_size": 400, # number of entries in LocalOrder queue
        "lo_max_delay": 200, # ms
        "gamma": 1.0, # batch-OF parameter
    }

    node_params.update(
        {
            "faults": bench_params["faults"]
        }
    )

    assert node_params['gamma'] > 0.5 and node_params['gamma'] <= 1.0
    assert bench_params['nodes'] > (
        (4 * node_params['faults']) /
        (2 * node_params['gamma'] - 1)
    )

    try:
        ret = ThemisBench(bench_params, node_params).run(debug, local=local)
        print(ret.result())
    except BenchError as e:
        Print.error(e)

@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=False):
    ''' Run benchmarks on AWS '''
    bench_params = {
        'faults': 3,
        'nodes': [10],
        'workers': 1,
        'collocate': True,
        'rate': [10_000, 110_000],
        'tx_size': 512,
        'duration': 300,
        'runs': 2,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
