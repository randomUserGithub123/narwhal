// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL with explicit FairUpdate (Themis-style).
//
// Indirect tx inclusion rules:
//   - Direct txs: ALWAYS included (full bytes + OI). Digest added to already_batched.
//   - Indirect txs: included ONLY if tx_digest is NOT in already_batched.
//     Once included, digest added to already_batched.
//   - Result: each tx_digest appears in at most one outgoing batch from this worker.
//   - Sealing threshold: bytes of direct txs only. Indirect entries ride along.
use crate::local_order_tracker::LocalOrderTracker;
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::{FairProposeMessage, FairUpdateVote, IndirectTxEntry, WorkerMessage};
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use log::debug;
use lz4_flex::compress_prepend_size;
use network::ReliableSender;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type BatchEntry = (Transaction, u64);
pub type Batch = Vec<BatchEntry>;

type TxDigest = u64;

// =========================================================================
// Edge compression (varint + delta + lz4)
// =========================================================================

fn pack_and_compress_edges(directed_edges: &[u32]) -> Vec<u8> {
    if directed_edges.is_empty() {
        return vec![];
    }

    let mut sorted: Vec<u32> = directed_edges.to_vec();
    sorted.sort_unstable();

    let mut deltas: Vec<u8> = Vec::with_capacity(sorted.len() * 3);
    let mut prev = 0u32;
    for &e in &sorted {
        let delta = e.wrapping_sub(prev);
        prev = e;
        let mut v = delta;
        while v >= 0x80 {
            deltas.push((v as u8) | 0x80);
            v >>= 7;
        }
        deltas.push(v as u8);
    }

    compress_prepend_size(&deltas)
}

// =========================================================================
// Pending FairUpdate proposal state
// =========================================================================

struct PendingFairProposal {
    pending_edges: Vec<u32>,
    vertex_to_digest: HashMap<u16, TxDigest>,
    missing_digests: HashSet<TxDigest>,
}

// =========================================================================
// BatchMaker
// =========================================================================

pub struct BatchMaker {
    /// The preferred batch size (in entry count: direct txs + indirect digests).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network (direct from clients).
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch of direct entries.
    current_batch: Batch,
    /// Holds indirect entries (digest, oi) for the current batch.
    current_indirect: Vec<IndirectTxEntry>,
    /// Current entry count (direct + indirect) toward sealing threshold.
    current_entry_count: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,

    // FairDAG-RL: shared local order tracker
    tracker: LocalOrderTracker,

    /// Persistent dedup set: tx_digests that have already appeared in any
    /// previous or current batch from this worker (direct or indirect).
    /// Ensures each tx_digest appears in at most one outgoing batch.
    already_batched: HashSet<TxDigest>,

    // FairDAG-RL: FairUpdate vote production
    rx_fair_propose: Receiver<FairProposeMessage>,
    pending_fair_proposals: HashMap<u64, PendingFairProposal>,
    ready_fair_proposals: Vec<(u64, Vec<u32>)>,

    // FairDAG-RL: indirect tx arrivals from WorkerReceiverHandler
    rx_indirect: Receiver<IndirectTxEntry>,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        tracker: LocalOrderTracker,
        rx_fair_propose: Receiver<FairProposeMessage>,
        rx_indirect: Receiver<IndirectTxEntry>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_indirect: Vec::with_capacity(1024),
                current_entry_count: 0,
                network: ReliableSender::new(),
                tracker,
                already_batched: HashSet::new(),
                rx_fair_propose,
                pending_fair_proposals: HashMap::new(),
                ready_fair_proposals: Vec::new(),
                rx_indirect,
            }
            .run()
            .await;
        });
    }

    fn vote_edge_direction(
        &self,
        u_vertex: u16,
        v_vertex: u16,
        u_digest: TxDigest,
        v_digest: TxDigest,
    ) -> (u16, u16) {
        let u_oi = self.tracker.get_oi(u_digest);
        let v_oi = self.tracker.get_oi(v_digest);

        match (u_oi, v_oi) {
            (Some(u_ord), Some(v_ord)) => {
                if u_ord < v_ord {
                    (u_vertex, v_vertex)
                } else if v_ord < u_ord {
                    (v_vertex, u_vertex)
                } else {
                    if u_vertex < v_vertex {
                        (u_vertex, v_vertex)
                    } else {
                        (v_vertex, u_vertex)
                    }
                }
            }
            (Some(_), None) => (u_vertex, v_vertex),
            (None, Some(_)) => (v_vertex, u_vertex),
            (None, None) => {
                if u_vertex < v_vertex {
                    (u_vertex, v_vertex)
                } else {
                    (v_vertex, u_vertex)
                }
            }
        }
    }

    #[inline]
    fn pack_directed_edge(from: u16, to: u16) -> u32 {
        ((from as u32) << 16) | (to as u32)
    }

    fn check_pending_proposals(&mut self, tx_digest: TxDigest) {
        let mut new_ready: Vec<(u64, Vec<u32>)> = Vec::new();

        for (sub_dag_id, proposal) in self.pending_fair_proposals.iter_mut() {
            if !proposal.missing_digests.remove(&tx_digest) {
                continue;
            }

            let tracker = &self.tracker;
            let vertex_to_digest = &proposal.vertex_to_digest;
            let missing_digests = &proposal.missing_digests;
            let mut newly_voted: Vec<u32> = Vec::new();

            proposal.pending_edges.retain(|&edge| {
                let u = (edge >> 16) as u16;
                let v = (edge & 0xFFFF) as u16;

                let u_dig = match vertex_to_digest.get(&u) {
                    Some(&d) => d,
                    None => return true,
                };
                let v_dig = match vertex_to_digest.get(&v) {
                    Some(&d) => d,
                    None => return true,
                };

                let u_resolved =
                    !missing_digests.contains(&u_dig) && tracker.get_oi(u_dig).is_some();
                let v_resolved =
                    !missing_digests.contains(&v_dig) && tracker.get_oi(v_dig).is_some();

                if u_resolved && v_resolved {
                    let u_oi = tracker.get_oi(u_dig);
                    let v_oi = tracker.get_oi(v_dig);
                    let (from, to) = match (u_oi, v_oi) {
                        (Some(u_ord), Some(v_ord)) => {
                            if u_ord < v_ord {
                                (u, v)
                            } else if v_ord < u_ord {
                                (v, u)
                            } else {
                                if u < v { (u, v) } else { (v, u) }
                            }
                        }
                        (Some(_), None) => (u, v),
                        (None, Some(_)) => (v, u),
                        (None, None) => {
                            if u < v { (u, v) } else { (v, u) }
                        }
                    };
                    newly_voted.push(Self::pack_directed_edge(from, to));
                    false
                } else {
                    true
                }
            });

            if !newly_voted.is_empty() {
                new_ready.push((*sub_dag_id, newly_voted));
            }
        }

        self.ready_fair_proposals.extend(new_ready);
        self.pending_fair_proposals
            .retain(|_, p| !p.pending_edges.is_empty());
    }

    fn handle_fair_propose(
        &mut self,
        sub_dag_id: u64,
        vertices: Vec<(u16, u64)>,
        missing_edges: Vec<u32>,
    ) {
        if vertices.is_empty() && missing_edges.is_empty() {
            debug!("FairUpdate: CLEANUP signal for sub_dag_id={}", sub_dag_id);
            self.pending_fair_proposals.remove(&sub_dag_id);
            return;
        }

        let mut vertex_to_digest: HashMap<u16, TxDigest> = HashMap::new();
        let mut missing_digests: HashSet<TxDigest> = HashSet::new();

        for &(vertex_idx, tx_digest) in &vertices {
            vertex_to_digest.insert(vertex_idx, tx_digest);
            if self.tracker.get_oi(tx_digest).is_none() {
                missing_digests.insert(tx_digest);
            }
        }

        let mut directed_votes: Vec<u32> = Vec::new();
        let mut pending_edges: Vec<u32> = Vec::new();

        for &edge in &missing_edges {
            let u = (edge >> 16) as u16;
            let v = (edge & 0xFFFF) as u16;

            let u_dig = vertex_to_digest.get(&u);
            let v_dig = vertex_to_digest.get(&v);

            match (u_dig, v_dig) {
                (Some(&u_d), Some(&v_d)) => {
                    let u_known = self.tracker.get_oi(u_d).is_some();
                    let v_known = self.tracker.get_oi(v_d).is_some();

                    if u_known && v_known {
                        let (from, to) = self.vote_edge_direction(u, v, u_d, v_d);
                        directed_votes.push(Self::pack_directed_edge(from, to));
                    } else {
                        pending_edges.push(edge);
                    }
                }
                _ => {
                    pending_edges.push(edge);
                }
            }
        }

        if !directed_votes.is_empty() {
            self.ready_fair_proposals
                .push((sub_dag_id, directed_votes));
        }

        if !pending_edges.is_empty() {
            self.pending_fair_proposals.insert(
                sub_dag_id,
                PendingFairProposal {
                    pending_edges,
                    vertex_to_digest,
                    missing_digests,
                },
            );
        }
    }

    /// Main loop.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Direct client transactions — always included, drive sealing.
                Some(transaction) = self.rx_transaction.recv() => {
                    let tx_digest = crate::local_order_tracker::extract_tx_digest(&transaction);
                    let oi = self.tracker.record(tx_digest);

                    self.current_batch.push((transaction, oi));
                    self.current_entry_count += 1;
                    // Mark as batched so indirect path won't duplicate it.
                    self.already_batched.insert(tx_digest);

                    self.check_pending_proposals(tx_digest);

                    if self.current_entry_count >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // Indirect tx arrivals — included if not already batched, count toward sealing.
                Some((tx_digest, local_oi)) = self.rx_indirect.recv() => {
                    if !self.already_batched.contains(&tx_digest) {
                        self.already_batched.insert(tx_digest);
                        self.current_indirect.push((tx_digest, local_oi));
                        self.current_entry_count += 1;

                        if self.current_entry_count >= self.batch_size {
                            self.seal().await;
                            timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                        }
                    }
                    // Always check pending proposals — even if we skip inclusion,
                    // the tx is in the tracker and may resolve a pending edge.
                    self.check_pending_proposals(tx_digest);
                },

                // Missing-edge notifications from FairDagProcessor.
                Some((sub_dag_id, vertices, missing_edges)) = self.rx_fair_propose.recv() => {
                    self.handle_fair_propose(sub_dag_id, vertices, missing_edges);
                },

                // Timer: seal even if batch is small.
                () = &mut timer => {
                    if self.current_entry_count > 0
                        || !self.ready_fair_proposals.is_empty()
                    {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch.iter().map(|(tx, _)| tx.len()).sum::<usize>();

        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|(tx, _)| tx.len() > 8)
            .filter_map(|(tx, _)| tx[1..9].try_into().ok())
            .collect();

        debug!(
            "FairDAG BatchMaker: sealing {} direct + {} indirect, {} vote batches",
            self.current_batch.len(),
            self.current_indirect.len(),
            self.ready_fair_proposals.len(),
        );

        self.current_entry_count = 0;
        let batch: Batch = self.current_batch.drain(..).collect();
        let indirect: Vec<IndirectTxEntry> = self.current_indirect.drain(..).collect();

        // Collect ready FairUpdate votes.
        let votes: Vec<FairUpdateVote> = self
            .ready_fair_proposals
            .drain(..)
            .map(|(sub_dag_id, directed_edges)| {
                let edge_count = directed_edges.len();
                let directed_edges_compressed = pack_and_compress_edges(&directed_edges);
                FairUpdateVote {
                    sub_dag_id,
                    edge_count,
                    directed_edges_compressed,
                }
            })
            .collect();

        let message = WorkerMessage::Batch(batch, indirect, votes);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            let digest = Digest(
                Sha512::digest(&serialized)[..32]
                    .try_into()
                    .unwrap(),
            );
            for id in tx_ids {
                info!(
                    "Batch {:?} contains tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }
            info!("Batch {:?} contains {} B", digest, size);
        }

        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}