// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL: uses shared LocalOrderTracker to assign ordering
// indicators based on first-arrival time (regardless of source).
//
// FairDAG-RL v5: Explicit missing-edge updates.
//   - Receives MissingEdgeRequests from FairDagProcessor
//   - Waits until all requested tx digests have local OIs
//   - Appends EdgeUpdatePayload to the next sealed batch
use crate::local_order_tracker::{extract_tx_digest, LocalOrderTracker};
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::{EdgeUpdatePayload, SealedBatch, WorkerMessage};
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use log::{debug, warn};
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
pub type BatchEntry = (Transaction, u64); // (raw_tx, ordering_indicator)

/// A batch is a sequence of (transaction, ordering_indicator) pairs.
pub type Batch = Vec<BatchEntry>;

/// A request from FairDagProcessor telling this worker which transactions
/// are involved in missing edges for a graph at a given leader round.
/// The BatchMaker waits until ALL tx_digests have local OIs, then includes
/// an EdgeUpdatePayload in the next sealed batch.
#[derive(Debug, Clone)]
pub struct MissingEdgeRequest {
    pub leader_round: u64,
    pub tx_digests: Vec<u64>,
}

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch entries.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,

    // =========================================================================
    // FairDAG-RL: shared local order tracker
    // =========================================================================
    /// Shared tracker that records the local arrival order of transactions.
    tracker: LocalOrderTracker,

    // =========================================================================
    // FairDAG-RL v5: explicit missing-edge update handling
    // =========================================================================
    /// Channel to receive missing-edge requests from FairDagProcessor.
    rx_missing_edges: Receiver<MissingEdgeRequest>,
    /// Pending missing-edge requests that are waiting for all tx OIs to be available.
    pending_edge_requests: Vec<MissingEdgeRequest>,
    /// Edge update payloads ready to be included in the next sealed batch.
    ready_edge_updates: Vec<EdgeUpdatePayload>,
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

    /// Check all pending missing-edge requests. If all tx digests in a request
    /// now have local OIs, move the request to ready_edge_updates.
    fn check_pending_edge_requests(&mut self) {
        let mut still_pending = Vec::new();

        for req in self.pending_edge_requests.drain(..) {
            match self.tracker.batch_lookup_all(&req.tx_digests) {
                Some(orderings) => {
                    debug!(
                        "FairDAG BatchMaker: edge update ready for round {} ({} txs)",
                        req.leader_round,
                        orderings.len()
                    );
                    self.ready_edge_updates.push(EdgeUpdatePayload {
                        leader_round: req.leader_round,
                        orderings,
                    });
                }
                None => {
                    // Not all txs seen yet — keep waiting.
                    still_pending.push(req);
                }
            }
        }

        self.pending_edge_requests = still_pending;
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    // FairDAG-RL: record this tx in the shared tracker.
                    let tx_digest = extract_tx_digest(&transaction);
                    let oi = self.tracker.record(tx_digest);

                    debug!(
                        "FairDAG BatchMaker: tx {} → OI {} (counter at {})",
                        tx_digest, oi, self.tracker.current_counter()
                    );

                    self.current_batch_size += transaction.len();
                    self.current_batch.push((transaction, oi));

                    // Each new tx might unblock a pending edge request.
                    if !self.pending_edge_requests.is_empty() {
                        self.check_pending_edge_requests();
                    }

                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // Receive missing-edge requests from FairDagProcessor.
                Some(req) = self.rx_missing_edges.recv() => {
                    debug!(
                        "FairDAG BatchMaker: received MissingEdgeRequest for round {} with {} txs",
                        req.leader_round, req.tx_digests.len()
                    );

                    // Check if we can resolve it immediately.
                    match self.tracker.batch_lookup_all(&req.tx_digests) {
                        Some(orderings) => {
                            debug!(
                                "FairDAG BatchMaker: immediately resolved edge update for round {}",
                                req.leader_round
                            );
                            self.ready_edge_updates.push(EdgeUpdatePayload {
                                leader_round: req.leader_round,
                                orderings,
                            });
                        }
                        None => {
                            // Some txs not yet seen — park it.
                            self.pending_edge_requests.push(req);
                        }
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    // Also check pending edge requests on timer tick.
                    if !self.pending_edge_requests.is_empty() {
                        self.check_pending_edge_requests();
                    }

                    if !self.current_batch.is_empty() || !self.ready_edge_updates.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the chance to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
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

        let edge_update_count = self.ready_edge_updates.len();

        debug!(
            "FairDAG BatchMaker: sealing batch with {} entries, {} edge updates, OI range [{}, {}]",
            self.current_batch.len(),
            edge_update_count,
            self.current_batch.first().map(|(_, oi)| *oi).unwrap_or(0),
            self.current_batch.last().map(|(_, oi)| *oi).unwrap_or(0),
        );

        self.current_batch_size = 0;
        let entries: Batch = self.current_batch.drain(..).collect();
        let edge_updates: Vec<EdgeUpdatePayload> = self.ready_edge_updates.drain(..).collect();

        let sealed = SealedBatch {
            entries,
            edge_updates,
        };
        let message = WorkerMessage::Batch(sealed);
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

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}