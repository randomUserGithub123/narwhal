# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

import os
from re import findall, search
from multiprocessing import Pool
from time import sleep
import time
import re
from pathlib import Path
import math

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.commands import CommandMaker
import subprocess
from benchmark.utils import Print, PathMaker
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError
from benchmark.das import DASBench
from benchmark.grid5000 import Grid5000Bench

@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    # log format: local-{attack_type}-{arbitragers}-{faults}-{workers}-{nodes}.txt
    benchmark_configurations = [
        (0, 1, 1, 2, 6, 1),
        # (2, 3, 0, 2, 10, 1),
        # (3, 3, 0, 2, 10, 1),
        # (4, 3, 0, 2, 10, 1)
    ]  # benchmark format: (attack_type, frontrunners, crashes, workers, nodes, runs)
    start_time = time.time()
    for at, arbis, ff, wks, nds, rs in benchmark_configurations:
        """Run benchmarks on localhost"""
        runs = rs
        attack_type = at
        arbitragers = arbis
        faults = ff
        workers = wks
        nodes = nds
        bench_params = {
            "faults": faults,  # faults: the number of crashed nodes f_l
            "arbitragers": arbitragers,  # arbitragers: the number of frontrunning attackers f_a
            "attack_type": attack_type,  # frontrunning strategies: 0: no attack; 1: fissure; 2: sluggish; 3: speculative; 10: baseline
            "nodes": nodes,  # nodes: the number of nodes n
            "workers": workers,  # workers: the number of workers f_w
            "rate": 300,  # rate: the transaction sending rate
            "tx_size": 128,
            "duration": 20,
        }
        node_params = {
            "header_size": 1_000,  # bytes
            "max_header_delay": 200,  # ms
            "gc_depth": 50,  # garbage collection depth
            "sync_retry_delay": 10_000,  # ms
            "sync_retry_nodes": 3,  # number of nodes
            "batch_size": 500_000,  # bytes
            "max_batch_delay": 200,  # ms
            "lo_size": 200, # number of entries in LocalOrder queue
            "lo_max_delay": 100, # ms
            "gamma": 1.0, # batch-OF parameter
        }

        node_params.update(
            {
                "fault_threshold": int(math.floor(
                    (bench_params["nodes"] - 1) / 4
                ))
            }
        )

        assert bench_params['faults'] <= node_params['fault_threshold']

        assert bench_params['workers'] > 1 # One worker has only the task of batch-OF
        assert node_params['gamma'] > 0.5 and node_params['gamma'] <= 1.0
        assert bench_params['nodes'] > (
            (4 * node_params['fault_threshold']) /
            (2 * node_params['gamma'] - 1)
        )

        try:
            filename = PathMaker.local_result_file(
                attack_type,
                arbitragers,
                faults,
                workers,
                nodes,
            )
            for i in range(runs):
                print(f"Local repeat {i}th\n")
                print(
                    f"type {attack_type}, arbs {arbitragers}, fault {faults}, workers {workers}, nodes {nodes}\n"
                )
                ret = LocalBench(bench_params, node_params).run(debug)
                print(ret.result())
                ret.print(filename)
                cmd = CommandMaker.kill().split()
                subprocess.run(cmd, stderr=subprocess.DEVNULL)
                sleep(1)
            if attack_type == 1:
                succ_num, total_num = _get_fissure_total(filename)
                fissure_total_result = ""
                if total_num != 0:
                    fissure_total_result = f"\nCumulative fissure front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(fissure_total_result)
                    print(fissure_total_result)
            elif attack_type == 2:
                succ_num, total_num = _get_sluggish_attack_total(filename)
                sluggish_total_result = ""
                if total_num != 0:
                    sluggish_total_result = f"\nCumulative sluggish front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(sluggish_total_result)
                    print(sluggish_total_result)
            elif attack_type == 3:
                succ_num, total_num = _get_speculative_attack_total(filename)
                speculative_total_result = ""
                if total_num != 0:
                    speculative_total_result = f"\nCumulative speculative front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(speculative_total_result)
                    print(speculative_total_result)
            elif attack_type == 10:
                succ_num, total_num = _get_monitor_total(filename)
                monitor_total_result = ""
                if total_num != 0:
                    monitor_total_result = f"\nCumulative baseline front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(monitor_total_result)
                    print(monitor_total_result)

        except BenchError as e:
            Print.error(e)
    end_time = time.time()
    runtime_min = (end_time - start_time) / 60
    print("Runtime: ", runtime_min, "minutes")

@task
def das(ctx, debug=True, console=False, build=True, username="mputnik"):
    for attack_type, arbitragers, faults, workers_per_node, nodes, runs, input_rate in [
        # ###### N = 5 ######
        # ###### N = 5 ######
        # (0, 1, 1, 2, 5, 6, 500),
        # (0, 1, 1, 2, 5, 6, 1000),
        # (0, 1, 1, 2, 5, 6, 1500),
        # (0, 1, 1, 2, 5, 6, 2000),
        # (0, 1, 1, 2, 5, 6, 2500),
        # (0, 1, 1, 2, 5, 6, 3000),
        # (0, 1, 1, 2, 5, 6, 3500),
        # (0, 1, 1, 2, 5, 6, 4000),
        # (0, 1, 1, 2, 5, 6, 4500),
        (0, 1, 1, 2, 5, 1, 5000),
        # (0, 1, 1, 2, 5, 6, 5500),
        # (0, 1, 1, 2, 5, 6, 6000),
        # (0, 1, 1, 2, 5, 6, 6500),
        # (0, 1, 1, 2, 5, 6, 7000),
        # ###### N = 9 ######
        # (1, 3, 0, 2, 9, 1, 5000),
        # (2, 3, 0, 2, 9, 1, 5000),
        # (3, 3, 0, 2, 9, 1, 5000),
        # (10, 3, 0, 2, 9, 1, 5000),
        # ###### N = 14 ######
        # (1, 3, 0, 2, 14, 1, 5000),
        # (2, 3, 0, 2, 14, 1, 5000),
        # (3, 3, 0, 2, 14, 1, 5000),
        # (10, 3, 0, 2, 14, 1, 5000),
        # ###### N = 17 ######
        # (1, 3, 0, 2, 17, 1, 5000),
        # (2, 3, 0, 2, 17, 1, 5000),
        # (0, 1, 4, 2, 17, 5, 500),
        # (0, 1, 4, 2, 17, 5, 1000),
        # (0, 1, 4, 2, 17, 5, 1500),
        # (0, 1, 4, 2, 17, 5, 2000),
        # (0, 1, 4, 2, 17, 5, 2500),
        # (0, 1, 4, 2, 17, 5, 3000),
        # (0, 1, 4, 2, 17, 5, 3500),
        # (0, 1, 4, 2, 17, 5, 4000),
        # (0, 1, 4, 2, 17, 5, 4500),
        # (0, 1, 4, 2, 17, 3, 5000),
        # (3, 1, 4, 2, 17, 3, 6000),
        # (3, 1, 4, 2, 17, 3, 7000),
        # (3, 1, 4, 2, 17, 3, 8000),
        # (3, 1, 4, 2, 17, 3, 9000),
        # (3, 1, 4, 2, 17, 3, 10_000),
        # (3, 1, 4, 2, 17, 3, 15_000),
        # (3, 1, 4, 2, 17, 3, 20_000),
        # (3, 1, 4, 2, 17, 3, 25_000),
        # (3, 1, 4, 2, 17, 3, 30_000),
        # (3, 1, 4, 2, 17, 3, 35_000),
        # (10, 3, 0, 2, 17, 1, 5000),
    ]:
        
        assert attack_type in [
            0, # no attack
            1, # fissure
            2, # sluggish
            3, # speculative
            10 # baseline
        ]

        """Run benchmarks on DAS5"""
        bench_params = {
            'faults': faults,
            "arbitragers": arbitragers,  # arbitragers: the number of frontrunning attackers f_a
            "attack_type": attack_type,  # frontrunning strategies: 0: no attack; 1: fissure; 2: sluggish; 3: speculative; 10: baseline
            'nodes': nodes,
            'workers': workers_per_node,
            'rate': input_rate,
            'tx_size': 128,
            'duration': 60,
            "collocate": True,
        }
        node_params = {
            "header_size": 512,  # bytes
            "max_header_delay": 2000,  # ms
            "gc_depth": 50,  # rounds
            "sync_retry_delay": 5_000,  # ms
            "sync_retry_nodes": 3,  # number of nodes
            "batch_size": 4_000,  # bytes
            "max_batch_delay": 1000,  # ms
            "lo_size": 200, # number of entries in LocalOrder queue
            "lo_max_delay": 1000, # ms
            "gamma": 1.0, # batch-OF parameter
            "scc_ordering": "alphabetical", # batch-OF parameter
        }

        node_params.update(
            {
                "fault_threshold": int(math.floor(
                    (bench_params["nodes"] - 1) / 4
                ))
            }
        )

        assert bench_params['faults'] <= node_params['fault_threshold']

        assert bench_params['workers'] > 1 # One worker has only the task of batch-OF
        assert node_params['gamma'] > 0.5 and node_params['gamma'] <= 1.0
        assert bench_params['nodes'] > (
            (4 * node_params['fault_threshold']) /
            (2 * node_params['gamma'] - 1)
        )

        assert node_params["scc_ordering"] in [
            "alphabetical",
            "hamiltonian_cycle",
            "ranked_pairs"
        ]

        if console:
            os.system('export RUSTFLAGS="--cfg tokio_unstable"')
        try:
            filename = PathMaker.local_result_file(
                attack_type,
                arbitragers,
                faults,
                workers_per_node,
                nodes,
            )
            run = 0
            while run < runs:
                print(f"\n=============================\nDAS experiment[{attack_type, arbitragers, faults, workers_per_node, nodes, input_rate}] run[{run}]\n=============================\n")
                ret = DASBench(
                    bench_params, 
                    node_params, 
                    username
                ).run(debug, console, build)

                consensus_tps_raw, _, _ = ret._consensus_throughput()
                if int(consensus_tps_raw) <= 1e-9:
                    continue # Skip until DAS nodes work

                print(ret.result())
                ret.print(filename)
                run += 1

            if attack_type == 1:
                succ_num, total_num = _get_fissure_total(filename)
                fissure_total_result = ""
                if total_num != 0:
                    fissure_total_result = f"\nCumulative fissure front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(fissure_total_result)
                    print(fissure_total_result)
            elif attack_type == 2:
                succ_num, total_num = _get_sluggish_attack_total(filename)
                sluggish_total_result = ""
                if total_num != 0:
                    sluggish_total_result = f"\nCumulative sluggish front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(sluggish_total_result)
                    print(sluggish_total_result)
            elif attack_type == 3:
                succ_num, total_num = _get_speculative_attack_total(filename)
                speculative_total_result = ""
                if total_num != 0:
                    speculative_total_result = f"\nCumulative speculative front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(speculative_total_result)
                    print(speculative_total_result)
            elif attack_type == 10:
                succ_num, total_num = _get_monitor_total(filename)
                monitor_total_result = ""
                if total_num != 0:
                    monitor_total_result = f"\nCumulative baseline front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(monitor_total_result)
                    print(monitor_total_result)
                
        except BenchError as e:
            Print.error(e)

@task
def grid5000(ctx, debug=True, console=False, build=True, username="jdecouch", site="luxembourg"):
    for attack_type, arbitragers, faults, workers_per_node, nodes, runs, input_rate in [
        # ###### N = 5 ######
        # (0, 1, 1, 2, 5, 6, 500),
        # (0, 1, 1, 2, 5, 6, 1000),
        # (0, 1, 1, 2, 5, 6, 1500),
        # (0, 1, 1, 2, 5, 6, 2000),
        # (0, 1, 1, 2, 5, 6, 2500),
        # (0, 1, 1, 2, 5, 6, 3000),
        # (0, 1, 1, 2, 5, 6, 3500),
        # (0, 1, 1, 2, 5, 6, 4000),
        # (0, 1, 1, 2, 5, 6, 4500),
        (0, 1, 1, 2, 5, 1, 5000),
        # (0, 1, 1, 2, 5, 6, 5500),
        # (0, 1, 1, 2, 5, 6, 6000),
        # (0, 1, 1, 2, 5, 6, 6500),
        # (0, 1, 1, 2, 5, 6, 7000),
        # ###### N = 9 ######
        # (1, 3, 0, 2, 9, 1, 5000),
        # (2, 3, 0, 2, 9, 1, 5000),
        # (3, 3, 0, 2, 9, 1, 5000),
        # (10, 3, 0, 2, 9, 1, 5000),
        # ###### N = 14 ######
        # (1, 3, 0, 2, 14, 1, 5000),
        # (2, 3, 0, 2, 14, 1, 5000),
        # (3, 3, 0, 2, 14, 1, 5000),
        # (10, 3, 0, 2, 14, 1, 5000),
        # ###### N = 17 ######
        # (1, 3, 0, 2, 17, 1, 5000),
        # (2, 3, 0, 2, 17, 1, 5000),
        # (0, 1, 4, 2, 17, 5, 500),
        # (0, 1, 4, 2, 17, 5, 1000),
        # (0, 1, 4, 2, 17, 5, 1500),
        # (0, 1, 4, 2, 17, 5, 2000),
        # (0, 1, 4, 2, 17, 5, 2500),
        #(0, 1, 4, 2, 17, 1, 3000),
        # (0, 1, 4, 2, 17, 5, 3500),
        # (0, 1, 4, 2, 17, 5, 4000),
        # (0, 1, 4, 2, 17, 5, 4500),
        # (0, 1, 4, 2, 17, 3, 5000),
        # (3, 1, 4, 2, 17, 3, 6000),
        # (3, 1, 4, 2, 17, 3, 7000),
        # (3, 1, 4, 2, 17, 3, 8000),
        # (3, 1, 4, 2, 17, 3, 9000),
        # (3, 1, 4, 2, 17, 3, 10_000),
        # (3, 1, 4, 2, 17, 3, 15_000),
        # (3, 1, 4, 2, 17, 3, 20_000),
        # (3, 1, 4, 2, 17, 3, 25_000),
        # (3, 1, 4, 2, 17, 3, 30_000),
        # (3, 1, 4, 2, 17, 3, 35_000),
        # (10, 3, 0, 2, 17, 1, 5000),
    ]:
        
        assert attack_type in [
            0, # no attack
            1, # fissure
            2, # sluggish
            3, # speculative
            10 # baseline
        ]

        """Run benchmarks on DAS5"""
        bench_params = {
            'faults': faults,
            "arbitragers": arbitragers,  # arbitragers: the number of frontrunning attackers f_a
            "attack_type": attack_type,  # frontrunning strategies: 0: no attack; 1: fissure; 2: sluggish; 3: speculative; 10: baseline
            'nodes': nodes,
            'workers': workers_per_node,
            'rate': input_rate,
            'tx_size': 128,
            'duration': 60,
            "collocate": True,
        }
        node_params = {
            "header_size": 512,  # bytes
            "max_header_delay": 2000,  # ms
            "gc_depth": 50,  # rounds
            "sync_retry_delay": 5_000,  # ms
            "sync_retry_nodes": 3,  # number of nodes
            "batch_size": 4_000,  # bytes
            "max_batch_delay": 1000,  # ms
            "lo_size": 200, # number of entries in LocalOrder queue
            "lo_max_delay": 1000, # ms
            "gamma": 1.0, # batch-OF parameter
            "scc_ordering": "alphabetical", # batch-OF parameter
        }

        node_params.update(
            {
                "fault_threshold": int(math.floor(
                    (bench_params["nodes"] - 1) / 4
                ))
            }
        )

        assert bench_params['faults'] <= node_params['fault_threshold']

        assert bench_params['workers'] > 1 # One worker has only the task of batch-OF
        assert node_params['gamma'] > 0.5 and node_params['gamma'] <= 1.0
        assert bench_params['nodes'] > (
            (4 * node_params['fault_threshold']) /
            (2 * node_params['gamma'] - 1)
        )

        assert node_params["scc_ordering"] in [
            "alphabetical",
            "hamiltonian_cycle",
            "ranked_pairs"
        ]

        if console:
            os.system('export RUSTFLAGS="--cfg tokio_unstable"')
        try:
            filename = PathMaker.local_result_file(
                attack_type,
                arbitragers,
                faults,
                workers_per_node,
                nodes,
            )
            run = 0
            while run < runs:
                print(f"\n=============================\Grid5000 experiment[{attack_type, arbitragers, faults, workers_per_node, nodes, input_rate}] run[{run}]\n=============================\n")
                ret = Grid5000Bench(
                    bench_params, 
                    node_params, 
                    username,
                    site
                ).run(debug, console, build)

                consensus_tps_raw, _, _ = ret._consensus_throughput()
                if int(consensus_tps_raw) <= 1e-9:
                    continue # Skip until DAS nodes work

                print(ret.result())
                ret.print(filename)
                run += 1

            if attack_type == 1:
                succ_num, total_num = _get_fissure_total(filename)
                fissure_total_result = ""
                if total_num != 0:
                    fissure_total_result = f"\nCumulative fissure front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(fissure_total_result)
                    print(fissure_total_result)
            elif attack_type == 2:
                succ_num, total_num = _get_sluggish_attack_total(filename)
                sluggish_total_result = ""
                if total_num != 0:
                    sluggish_total_result = f"\nCumulative sluggish front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(sluggish_total_result)
                    print(sluggish_total_result)
            elif attack_type == 3:
                succ_num, total_num = _get_speculative_attack_total(filename)
                speculative_total_result = ""
                if total_num != 0:
                    speculative_total_result = f"\nCumulative speculative front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(speculative_total_result)
                    print(speculative_total_result)
            elif attack_type == 10:
                succ_num, total_num = _get_monitor_total(filename)
                monitor_total_result = ""
                if total_num != 0:
                    monitor_total_result = f"\nCumulative baseline front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(monitor_total_result)
                    print(monitor_total_result)
                
        except BenchError as e:
            Print.error(e)

def _get_monitor_total(path):
    log = ""
    with open(path, "r") as f:
        log += f.read()
    tmp = findall(r" Baseline front-running rate: \d+.\d+% \((\d+)\/(\d+)\) ", log)
    tmp = [(s, t) for s, t in tmp]
    succ_num = 0
    total_num = 0
    for s, t in tmp:
        succ_num += int(s)
        total_num += int(t)
    return succ_num, total_num


def _get_fissure_total(path):
    log = ""
    with open(path, "r") as f:
        log += f.read()
    tmp = findall(r" Fissure front-running rate: \d+.\d+% \((\d+)\/(\d+)\) ", log)
    tmp = [(s, t) for s, t in tmp]
    succ_num = 0
    total_num = 0
    for s, t in tmp:
        succ_num += int(s)
        total_num += int(t)
    return succ_num, total_num


def _get_sluggish_attack_total(path):
    log = ""
    with open(path, "r") as f:
        log += f.read()
    tmp = findall(r" Sluggish front-running rate: \d+.\d+% \((\d+)\/(\d+)\) ", log)
    tmp = [(s, t) for s, t in tmp]
    succ_num = 0
    total_num = 0
    for s, t in tmp:
        succ_num += int(s)
        total_num += int(t)
    return succ_num, total_num


def _get_speculative_attack_total(path):
    log = ""
    with open(path, "r") as f:
        log += f.read()
    tmp = findall(r" Speculative front-running rate: \d+.\d+% \((\d+)\/(\d+)\) ", log)
    tmp = [(s, t) for s, t in tmp]
    succ_num = 0
    total_num = 0
    for s, t in tmp:
        succ_num += int(s)
        total_num += int(t)
    return succ_num, total_num

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

# fab cumulative --attack 10 --arbitragers 1 --faults 0 --workers 2 --nodes 4
@task
def cumulative(ctx, attack, arbitragers, faults, workers, nodes):
    filename = PathMaker.result_file(
        attack,
        arbitragers,
        faults,
        workers,
        nodes,
    )
    if int(attack) == 1:
        succ_num, total_num = _get_fissure_total(filename)
        fissure_total_result = ""
        if total_num != 0:
            fissure_total_result = f"\nCumulative fissure front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
        with open(filename, "a") as f:
            f.write(fissure_total_result)
    elif int(attack) == 2:
        succ_num, total_num = _get_sluggish_attack_total(filename)
        sluggish_total_result = ""
        if total_num != 0:
            sluggish_total_result = f"\nCumulative sluggish front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
        with open(filename, "a") as f:
            f.write(sluggish_total_result)
    elif int(attack) == 3:
        succ_num, total_num = _get_speculative_attack_total(filename)
        speculative_total_result = ""
        if total_num != 0:
            speculative_total_result = f"\nCumulative speculative front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
        with open(filename, "a") as f:
            f.write(speculative_total_result)
    elif int(attack) == 10:
        succ_num, total_num = _get_monitor_total(filename)
        monitor_total_result = ""
        if total_num != 0:
            monitor_total_result = f"\nCumulative baseline front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
        with open(filename, "a") as f:
            f.write(monitor_total_result)

##---- The following functions are used for artifact creation ----##


@task
def articrash(ctx, debug=True):
    # benchmark format: (attack_type, frontrunners, crashes, workers, nodes, runs)
    benchmark_configurations = [
        (10, 5, 0, 2, 10, 4),
        (10, 5, 2, 2, 10, 4),
        (1, 5, 0, 2, 10, 4),
        (1, 5, 2, 2, 10, 4),
        (2, 5, 0, 2, 10, 4),
        (2, 5, 2, 2, 10, 4),
        (3, 5, 0, 2, 10, 4),
        (3, 5, 2, 2, 10, 4),
    ]
    run_with_configurations(benchmark_configurations, debug)

    # Print only the last cumulative result for each attack type, with crash ratio
    result_files = [
        PathMaker.local_result_file(at, arbis, ff, wks, nds)
        for at, arbis, ff, wks, nds, _ in benchmark_configurations
    ]
    for file in result_files:
        if not Path(file).exists():
            continue
        with open(file, "r") as f:
            lines = f.readlines()
        # Find the last cumulative result line
        last_cumulative = None
        for line in lines:
            m = re.match(r"Cumulative (.+?) front-running results: ([\d.]+)%", line)
            if m:
                last_cumulative = (m.group(1).strip(), m.group(2))
        if last_cumulative:
            # Find Faults and Committee size from the config section
            faults = None
            committee = None
            for l in lines:
                m_faults = re.match(r"\s*Faults:\s*(\d+)", l)
                m_committee = re.match(r"\s*Committee size:\s*(\d+)", l)
                if m_faults:
                    faults = int(m_faults.group(1))
                if m_committee:
                    committee = int(m_committee.group(1))
                if faults is not None and committee is not None:
                    break
            crash_ratio = (
                f"{faults}/{committee}"
                if faults is not None and committee is not None
                else "N/A"
            )
            attack_name, success_rate = last_cumulative
            print(
                f"{attack_name.capitalize()} (crash node ratio: {crash_ratio}): {success_rate}%"
            )


@task
def artiworker(ctx, debug=True):
    # benchmark format: (attack_type, frontrunners, crashes, workers, nodes, runs)
    benchmark_configurations = [
        (10, 3, 0, 2, 10, 4),
        (10, 3, 0, 8, 10, 4),
        (1, 3, 0, 2, 10, 4),
        (1, 3, 0, 8, 10, 4),
        (2, 3, 0, 2, 10, 4),
        (2, 3, 0, 8, 10, 4),
        (3, 3, 0, 2, 10, 4),
        (3, 3, 0, 8, 10, 4),
    ]
    run_with_configurations(benchmark_configurations, debug)

    # Print only the last cumulative result for each attack type, with crash ratio
    result_files = [
        PathMaker.local_result_file(at, arbis, ff, wks, nds)
        for at, arbis, ff, wks, nds, _ in benchmark_configurations
    ]
    for file in result_files:
        if not Path(file).exists():
            continue
        with open(file, "r") as f:
            lines = f.readlines()
        # Find the last cumulative result line
        last_cumulative = None
        for line in lines:
            m = re.match(r"Cumulative (.+?) front-running results: ([\d.]+)%", line)
            if m:
                last_cumulative = (m.group(1).strip(), m.group(2))
        if last_cumulative:
            # Find Workers from the config section
            workers = None
            for l in lines:
                m_workers = re.match(r"\s*Worker\(s\) per node:\s*(\d+)", l)
                if m_workers:
                    workers = int(m_workers.group(1))
                if workers is not None:
                    break
            attack_name, success_rate = last_cumulative
            print(f"{attack_name.capitalize()} (workers: {workers}): {success_rate}%")


@task
def artifrontrunner(ctx, debug=True):
    # benchmark format: (attack_type, frontrunners, crashes, workers, nodes, runs)
    benchmark_configurations = [
        (10, 3, 0, 2, 10, 4),
        (10, 5, 0, 2, 10, 4),
        (1, 3, 0, 2, 10, 4),
        (1, 5, 0, 2, 10, 4),
        (2, 3, 0, 2, 10, 4),
        (2, 5, 0, 2, 10, 4),
        (3, 3, 0, 2, 10, 4),
        (3, 5, 0, 2, 10, 4),
    ]
    run_with_configurations(benchmark_configurations, debug)

    # Print only the last cumulative result for each attack type, with crash ratio
    result_files = [
        PathMaker.local_result_file(at, arbis, ff, wks, nds)
        for at, arbis, ff, wks, nds, _ in benchmark_configurations
    ]
    for file in result_files:
        if not Path(file).exists():
            continue
        with open(file, "r") as f:
            lines = f.readlines()
        # Find the last cumulative result line
        last_cumulative = None
        for line in lines:
            m = re.match(r"Cumulative (.+?) front-running results: ([\d.]+)%", line)
            if m:
                last_cumulative = (m.group(1).strip(), m.group(2))
        if last_cumulative:
            # Find Workers from the config section
            attackers = None
            for l in lines:
                m_workers = re.match(r"\s*Arbitragers:\s*(\d+)", l)
                if m_workers:
                    attackers = int(m_workers.group(1))
                if attackers is not None:
                    break
            attack_name, success_rate = last_cumulative
            print(
                f"{attack_name.capitalize()} (frontrunners: {attackers}): {success_rate}%"
            )


def run_with_configurations(benchmark_configurations, debug=True):
    start_time = time.time()
    for at, arbis, ff, wks, nds, rs in benchmark_configurations:
        """Run benchmarks on localhost"""
        runs = rs
        attack_type = at
        arbitragers = arbis
        faults = ff
        workers = wks
        nodes = nds
        bench_params = {
            "faults": faults,  # faults: the number of crashed nodes f_l
            "arbitragers": arbitragers,  # arbitragers: the number of frontrunning attackers f_a
            "attack_type": attack_type,  # frontrunning strategies: 0: no attack; 1: fissure; 2: sluggish; 3: speculative; 10: baseline
            "nodes": nodes,  # nodes: the number of nodes n
            "workers": workers,  # workers: the number of workers f_w
            "rate": 30_000,  # rate: the transaction sending rate
            "tx_size": 512,
            "duration": 60,  # duration: the duration of each run, seconds
        }
        node_params = {
            "header_size": 1_000,  # bytes
            "max_header_delay": 200,  # ms
            "gc_depth": 50,  # garbage collection depth
            "sync_retry_delay": 10_000,  # ms
            "sync_retry_nodes": 3,  # number of nodes
            "batch_size": 500_000,  # bytes
            "max_batch_delay": 200,  # ms
        }
        try:
            filename = PathMaker.local_result_file(
                attack_type,
                arbitragers,
                faults,
                workers,
                nodes,
            )
            for i in range(runs):
                print(f"Local repeat {i}th\n")
                print(
                    f"type {attack_type}, arbs {arbitragers}, fault {faults}, workers {workers}, nodes {nodes}\n"
                )
                ret = LocalBench(bench_params, node_params).run(debug)
                # local_result_file(attack_type, arbitragers, faults, nodes)
                ret.print(filename)
                cmd = CommandMaker.kill().split()
                subprocess.run(cmd, stderr=subprocess.DEVNULL)
                sleep(3)
            if attack_type == 1:  # fissure attack
                succ_num, total_num = _get_fissure_total(filename)
                fissure_total_result = ""
                if total_num != 0:
                    fissure_total_result = f"\nCumulative fissure front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(fissure_total_result)
            elif attack_type == 2:  # sluggish attack
                succ_num, total_num = _get_sluggish_attack_total(filename)
                sluggish_total_result = ""
                if total_num != 0:
                    sluggish_total_result = f"\nCumulative sluggish front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(sluggish_total_result)
            elif attack_type == 3:  # speculative attack
                succ_num, total_num = _get_speculative_attack_total(filename)
                speculative_total_result = ""
                if total_num != 0:
                    speculative_total_result = f"\nCumulative speculative front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(speculative_total_result)
            elif attack_type == 10:  # baseline (non-strategy attack)
                succ_num, total_num = _get_monitor_total(filename)
                monitor_total_result = ""
                if total_num != 0:
                    monitor_total_result = f"\nCumulative baseline front-running results: {round(succ_num/total_num*100, 2):,}% ({succ_num:,}/{total_num:,}) \n"
                with open(filename, "a") as f:
                    f.write(monitor_total_result)
        except BenchError as e:
            Print.error(e)
    end_time = time.time()
    runtime_min = (end_time - start_time) / 60
    print("Runtime: ", runtime_min, "minutes")