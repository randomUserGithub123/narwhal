> **Note to readers:** MystenLabs is making this codebase production-ready [here](https://github.com/MystenLabs/sui/tree/main/narwhal).

# Requirements
## Libraries
1. `Python`
2. `Rust`
    - `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
3. `clang`
    - `sudo apt install clang-20`
4. `tmux`
    - `sudo apt install tmux`

## Themis Setup
### Ensure openssl and libevent are installed on your machine, more specifically, you need:
* CMake >= 3.9 (cmake)
* C++14 (g++)
* libuv >= 1.10.0 (libuv1-dev)
* openssl >= 1.1.0 (libssl-dev)
* on Ubuntu: `sudo apt-get install libssl-dev libuv1-dev cmake make autoconf automake libtool pkg-config`
* `apt-get` might not get the newest `cmake`, ensure the `CMake >= 3.9`

### Build project
1. `cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED=ON -DHOTSTUFF_PROTO_LOG=ON -DCMAKE_CXX_FLAGS="-include cstdint"`
2. `make`

### Run Themis locally
* Make sure in the `scripts/run_demo.sh` file number of replicas are 0 to 3.
* Make sure the hotstuff.conf file has exactly 4 replica signatures.
* start 4 demo replicas with `scripts/run_demo.sh`
* start the demo client with `scripts/run_demo_client.sh` in another terminal
* Use Ctrl-C to terminate the client and replicas


## DAS Setup
1. Contact `das-account@cs.vu.nl` for access (you will get credentials via email)
2. Nodes need to be accessed from university ip address (e.g. [TU Delft VPN](https://docs.eduvpn.org/client/linux/installation.html))
    - [eduVPN](https://docs.eduvpn.org/client/linux/installation.html)
3. DAS-5 head node (fileserver) is where the code lives (is edited and compiled)
4. Program runs on compute nodes, orchestratred from the head node
5. `/home/<userid>` has limited size, for large data files use the shared storage `/var/scratch/<userid>/`
6. Configure SSH: `ssh-copy-id` and then `ssh mputnik@fs0.das5.cs.vu.nl`
7. Useful commands on the head node:
    - loading prun: `module load prun`
    - asking for reservation: `preserve -np 3 -t 900` (e.g. asking for 3 nodes for 15 minutes)
    - checking status: `preserve -llist` or `preserve -long-list`
    - cancel: `scancel -u $USER`
    - show all available modules (or software versions) available: `module avail`
    - show all “loaded” or “active” modules in your current session: `module list`
    - load a particular software version (or module): `module load <name/version>`
    - remove a particular software version (or module): `module rm <name/version>`

## DAS Editing of Code & Compiling
1. Code changes
    - Push to Gitlab repository
    - `git pull` in `/home/mputnik/giulio-msc-thesis-code` in DAS
    - `rm -rf /var/scratch/mputnik/giulio-msc-thesis-code` in DAS
    - `cp -r ~/giulio-msc-thesis-code /var/scratch/mputnik/` in DAS
    - `cd /var/scratch/mputnik/giulio-msc-thesis-code` in DAS

# Narwhal and Tusk

[![build status](https://img.shields.io/github/actions/workflow/status/asonnino/narwhal/rust.yml?branch=master&logo=github&style=flat-square)](https://github.com/asonnino/narwhal/actions)
[![rustc](https://img.shields.io/badge/rustc-1.51+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![python](https://img.shields.io/badge/python-3.9-blue?style=flat-square&logo=python&logoColor=white)](https://www.python.org/downloads/release/python-390/)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repo provides an implementation of [Narwhal and Tusk](https://arxiv.org/pdf/2105.11827.pdf). The codebase has been designed to be small, efficient, and easy to benchmark and modify. It has not been designed to run in production but uses real cryptography ([dalek](https://doc.dalek.rs/ed25519_dalek)), networking ([tokio](https://docs.rs/tokio)), and storage ([rocksdb](https://docs.rs/rocksdb)).

## Quick Start

The core protocols are written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:

```
$ git clone https://github.com/asonnino/narwhal.git
$ cd narwhal/benchmark
$ pip install -r requirements.txt
```

You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:

```
$ fab local
```

This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.

```
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Faults: 0 node(s)
 Committee size: 4 node(s)
 Worker(s) per node: 1 worker(s)
 Collocate primary and workers: True
 Input rate: 50,000 tx/s
 Transaction size: 512 B
 Execution time: 19 s

 Header size: 1,000 B
 Max header delay: 100 ms
 GC depth: 50 round(s)
 Sync retry delay: 10,000 ms
 Sync retry nodes: 3 node(s)
 batch size: 500,000 B
 Max batch delay: 100 ms

 + RESULTS:
 Consensus TPS: 46,478 tx/s
 Consensus BPS: 23,796,531 B/s
 Consensus latency: 464 ms

 End-to-end TPS: 46,149 tx/s
 End-to-end BPS: 23,628,541 B/s
 End-to-end latency: 557 ms
-----------------------------------------
```

## Next Steps

The next step is to read the paper [Narwhal and Tusk: A DAG-based Mempool and Efficient BFT Consensus](https://arxiv.org/pdf/2105.11827.pdf). It is then recommended to have a look at the README files of the [worker](https://github.com/asonnino/narwhal/tree/master/worker) and [primary](https://github.com/asonnino/narwhal/tree/master/primary) crates. An additional resource to better understand the Tusk consensus protocol is the paper [All You Need is DAG](https://arxiv.org/abs/2102.08325) as it describes a similar protocol.

The README file of the [benchmark folder](https://github.com/asonnino/narwhal/tree/master/benchmark) explains how to benchmark the codebase and read benchmarks' results. It also provides a step-by-step tutorial to run benchmarks on [Amazon Web Services (AWS)](https://aws.amazon.com) accross multiple data centers (WAN).

## License

This software is licensed as [Apache 2.0](LICENSE).
