// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL v6: explicit missing-edge updates with lz4 compression.
//
// When the FairDagProcessor identifies missing edges in a graph, it sends a
// MissingEdgeRequest to this BatchMaker. Once all requested tx digests have
// local OIs (via the shared LocalOrderTracker), a MissingEdgeUpdate is built
// and piggybacked on the next sealed batch as an lz4-compressed trailer.
use crate::local_order_tracker::{extract_tx_digest, LocalOrderTracker};
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::{MissingEdgeUpdate, WorkerMessage};
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
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;

/// FairDAG-RL: A batch entry is a transaction paired with its ordering indicator.
pub type BatchEntry = (Transaction, u64);

/// A batch is a sequence of (transaction, ordering_indicator) pairs.
pub type Batch = Vec<BatchEntry>;

/// A request from FairDagProcessor: which tx digests need local OIs
/// for missing edges in a graph at the given leader round.
#[derive(Debug, Clone)]
pub struct MissingEdgeRequest {
    pub leader_round: u64,
    pub tx_digests: Vec<u64>,
}

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    batch_size: usize,
    max_batch_delay: u64,
    rx_transaction: Receiver<Transaction>,
    tx_message: Sender<QuorumWaiterMessage>,
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    current_batch: Batch,
    current_batch_size: usize,
    network: ReliableSender,

    /// Shared tracker for local arrival order.
    tracker: LocalOrderTracker,

    /// Channel to receive missing-edge requests from FairDagProcessor.
    rx_missing_edges: Receiver<MissingEdgeRequest>,

    /// Pending requests waiting for all tx OIs.
    pending_edge_requests: Vec<MissingEdgeRequest>,

    /// Ready-to-send MissingEdgeUpdate payloads.
    ready_edge_updates: Vec<MissingEdgeUpdate>,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        tracker: LocalOrderTracker,
        rx_missing_edges: Receiver<MissingEdgeRequest>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
                tracker,
                rx_missing_edges,
                pending_edge_requests: Vec::new(),
                ready_edge_updates: Vec::new(),
            }
            .run()
            .await;
        });
    }

    /// Check pending missing-edge requests; promote to ready when all OIs available.
    fn check_pending_edge_requests(&mut self) {
        let mut still_pending = Vec::new();

        for req in self.pending_edge_requests.drain(..) {
            match self.tracker.batch_lookup_all(&req.tx_digests) {
                Some(orderings) => {
                    debug!(
                        "FairDAG BatchMaker: edge update ready for round {} ({} txs)",
                        req.leader_round, orderings.len()
                    );
                    self.ready_edge_updates.push(MissingEdgeUpdate {
                        leader_round: req.leader_round,
                        orderings,
                    });
                }
                None => {
                    still_pending.push(req);
                }
            }
        }

        self.pending_edge_requests = still_pending;
    }

    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(transaction) = self.rx_transaction.recv() => {
                    let tx_digest = extract_tx_digest(&transaction);
                    let oi = self.tracker.record(tx_digest);

                    debug!(
                        "FairDAG BatchMaker: tx {} → OI {} (counter at {})",
                        tx_digest, oi, self.tracker.current_counter()
                    );

                    self.current_batch_size += transaction.len();
                    self.current_batch.push((transaction, oi));

                    // Each new tx might unblock a pending request.
                    if !self.pending_edge_requests.is_empty() {
                        self.check_pending_edge_requests();
                    }

                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                Some(req) = self.rx_missing_edges.recv() => {
                    debug!(
                        "FairDAG BatchMaker: received MissingEdgeRequest for round {} ({} txs)",
                        req.leader_round, req.tx_digests.len()
                    );

                    // Try immediate resolution.
                    match self.tracker.batch_lookup_all(&req.tx_digests) {
                        Some(orderings) => {
                            debug!(
                                "FairDAG BatchMaker: immediately resolved for round {}",
                                req.leader_round
                            );
                            self.ready_edge_updates.push(MissingEdgeUpdate {
                                leader_round: req.leader_round,
                                orderings,
                            });
                        }
                        None => {
                            self.pending_edge_requests.push(req);
                        }
                    }
                },

                () = &mut timer => {
                    if !self.pending_edge_requests.is_empty() {
                        self.check_pending_edge_requests();
                    }

                    if !self.current_batch.is_empty() || !self.ready_edge_updates.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch with lz4-compressed edge updates.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|(tx, _)| tx.len() > 8)
            .filter_map(|(tx, _)| tx[1..9].try_into().ok())
            .collect();

        let update_count = self.ready_edge_updates.len();

        debug!(
            "FairDAG BatchMaker: sealing batch with {} entries, {} edge updates, OI range [{}, {}]",
            self.current_batch.len(),
            update_count,
            self.current_batch.first().map(|(_, oi)| *oi).unwrap_or(0),
            self.current_batch.last().map(|(_, oi)| *oi).unwrap_or(0),
        );

        self.current_batch_size = 0;
        let entries: Batch = self.current_batch.drain(..).collect();
        let updates: Vec<MissingEdgeUpdate> = self.ready_edge_updates.drain(..).collect();

        // Serialize and lz4-compress the edge updates.
        let compressed_updates: Vec<u8> = if updates.is_empty() {
            Vec::new()
        } else {
            let serialized_updates = bincode::serialize(&updates)
                .expect("Failed to serialize MissingEdgeUpdate list");
            compress_prepend_size(&serialized_updates)
        };

        debug!(
            "FairDAG BatchMaker: compressed {} updates: {} → {} bytes",
            update_count,
            if updates.is_empty() { 0 } else {
                bincode::serialize(&updates).map(|v| v.len()).unwrap_or(0)
            },
            compressed_updates.len(),
        );

        let message = WorkerMessage::Batch(entries, compressed_updates);
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