// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL v5: Explicit Missing Edge Updates.
//
// The BatchMaker now:
//   1. Receives MissingEdgeRequest from the FairnessLayer (via channel).
//   2. For each request, waits until all referenced txs have local OIs.
//   3. Constructs a MissingEdgeUpdate with pairwise directional votes.
//   4. Serializes + lz4-compresses the update and attaches it to the next batch.
//   5. Receives GraphResolved notifications to stop producing stale updates.
use crate::local_order_tracker::{extract_tx_digest, LocalOrderTracker};
use crate::missing_edge_types::{
    EdgeDirection, FairnessToWorkerMessage, GraphId, GraphResolved,
    MissingEdgeRequest, MissingEdgeUpdate, PairwiseVote,
};
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use log::{debug, info as log_info, warn};
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

/// FairDAG-RL: A batch entry is a transaction paired with its ordering indicator.
pub type BatchEntry = (Transaction, u64); // (raw_tx, ordering_indicator)

/// A batch is a sequence of (transaction, ordering_indicator) pairs.
pub type Batch = Vec<BatchEntry>;

/// Tracks a pending missing edge request: we need local OIs for all txs
/// before we can produce the update.
struct PendingEdgeRequest {
    request: MissingEdgeRequest,
    /// Set of tx digests we still need to observe locally.
    waiting_for: HashSet<u64>,
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
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,

    // =========================================================================
    // FairDAG-RL: shared local order tracker
    // =========================================================================
    tracker: LocalOrderTracker,

    // =========================================================================
    // FairDAG-RL v5: Explicit missing edge updates
    // =========================================================================
    /// Channel to receive MissingEdgeRequest / GraphResolved from FairnessLayer.
    rx_fairness: Receiver<FairnessToWorkerMessage>,
    /// Our replica index (used when constructing MissingEdgeUpdate).
    our_replica_index: usize,
    /// Pending requests: graph_id → PendingEdgeRequest.
    /// Once all txs are available, we produce the update.
    pending_requests: HashMap<GraphId, PendingEdgeRequest>,
    /// Ready updates: graph_id → compressed MissingEdgeUpdate bytes.
    /// These are attached to the next sealed batch.
    ready_updates: Vec<Vec<u8>>,
    /// Resolved graph IDs — we stop producing updates for these.
    resolved_graphs: HashSet<GraphId>,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        tracker: LocalOrderTracker,
        rx_fairness: Receiver<FairnessToWorkerMessage>,
        our_replica_index: usize,
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
                rx_fairness,
                our_replica_index,
                pending_requests: HashMap::new(),
                ready_updates: Vec::new(),
                resolved_graphs: HashSet::new(),
            }
            .run()
            .await;
        });
    }

    /// Check if any pending requests are now ready (all txs have local OIs).
    fn check_pending_requests(&mut self) {
        let mut newly_ready: Vec<GraphId> = Vec::new();

        for (graph_id, pending) in &mut self.pending_requests {
            if self.resolved_graphs.contains(graph_id) {
                newly_ready.push(*graph_id); // will be discarded
                continue;
            }

            // Check if all waiting txs now have OIs.
            pending.waiting_for.retain(|tx_digest| {
                self.tracker.get_oi(*tx_digest).is_none()
            });

            if pending.waiting_for.is_empty() {
                newly_ready.push(*graph_id);
            }
        }

        for graph_id in newly_ready {
            if self.resolved_graphs.contains(&graph_id) {
                self.pending_requests.remove(&graph_id);
                continue;
            }

            if let Some(pending) = self.pending_requests.remove(&graph_id) {
                let update = self.produce_update(&pending.request);
                let serialized = bincode::serialize(&update)
                    .expect("Failed to serialize MissingEdgeUpdate");
                let compressed = lz4_flex::compress_prepend_size(&serialized);

                debug!(
                    "FairDAG BatchMaker: produced MissingEdgeUpdate for graph {} \
                     with {} votes, compressed {}→{} bytes",
                    graph_id,
                    update.votes.len(),
                    serialized.len(),
                    compressed.len(),
                );

                self.ready_updates.push(compressed);
            }
        }
    }

    /// Produce a MissingEdgeUpdate from a fully-ready request.
    fn produce_update(&self, request: &MissingEdgeRequest) -> MissingEdgeUpdate {
        let mut votes: Vec<PairwiseVote> = Vec::with_capacity(request.missing_pairs.len());

        for &(d1, d2) in &request.missing_pairs {
            let oi1 = self.tracker.get_oi(d1);
            let oi2 = self.tracker.get_oi(d2);

            let direction = match (oi1, oi2) {
                (Some(o1), Some(o2)) => {
                    if o1 < o2 {
                        EdgeDirection::Forward // d1 before d2
                    } else {
                        EdgeDirection::Reverse // d2 before d1
                    }
                }
                _ => EdgeDirection::Unknown,
            };

            votes.push(PairwiseVote {
                d1,
                d2,
                direction,
            });
        }

        MissingEdgeUpdate {
            graph_id: request.graph_id,
            replica_index: self.our_replica_index,
            votes,
        }
    }

    /// Handle a message from the FairnessLayer.
    fn handle_fairness_message(&mut self, msg: FairnessToWorkerMessage) {
        match msg {
            FairnessToWorkerMessage::MissingEdgeRequest(request) => {
                let graph_id = request.graph_id;

                if self.resolved_graphs.contains(&graph_id) {
                    debug!(
                        "FairDAG BatchMaker: ignoring MissingEdgeRequest for \
                         already-resolved graph {}",
                        graph_id
                    );
                    return;
                }

                // Determine which txs we still need to observe.
                let mut waiting_for: HashSet<u64> = HashSet::new();
                for &tx_digest in &request.missing_tx_digests {
                    if self.tracker.get_oi(tx_digest).is_none() {
                        waiting_for.insert(tx_digest);
                    }
                }

                debug!(
                    "FairDAG BatchMaker: received MissingEdgeRequest for graph {} \
                     with {} txs, {} pairs, waiting for {} txs",
                    graph_id,
                    request.missing_tx_digests.len(),
                    request.missing_pairs.len(),
                    waiting_for.len(),
                );

                self.pending_requests.insert(graph_id, PendingEdgeRequest {
                    request,
                    waiting_for,
                });
            }
            FairnessToWorkerMessage::GraphResolved(GraphResolved { graph_id }) => {
                debug!(
                    "FairDAG BatchMaker: graph {} resolved, removing pending requests",
                    graph_id
                );
                self.resolved_graphs.insert(graph_id);
                self.pending_requests.remove(&graph_id);
                // Also remove any ready updates for this graph.
                // (They may still be in ready_updates if not yet sealed.)
                // We keep them — they'll just be ignored by the fairness layer.
                // This is safe: the fairness layer ignores updates for resolved graphs.
            }
        }
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    let tx_digest = extract_tx_digest(&transaction);
                    let oi = self.tracker.record(tx_digest);

                    debug!(
                        "FairDAG BatchMaker: tx {} → OI {} (counter at {})",
                        tx_digest, oi, self.tracker.current_counter()
                    );

                    self.current_batch_size += transaction.len();
                    self.current_batch.push((transaction, oi));

                    // Check if any pending missing edge requests are now ready.
                    self.check_pending_requests();

                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // Handle fairness layer messages (missing edge requests / graph resolved).
                Some(msg) = self.rx_fairness.recv() => {
                    self.handle_fairness_message(msg);
                    // Immediately check if any pending requests are ready.
                    self.check_pending_requests();
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
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

        // FairDAG-RL v5: Collect all ready missing edge updates into a single
        // compressed blob. Multiple updates (for different graphs) are batched
        // together: serialize the Vec<MissingEdgeUpdate>, then lz4-compress.
        let compressed_updates: Option<Vec<u8>> = if !self.ready_updates.is_empty() {
            // Each ready_update is individually compressed. We concatenate by
            // wrapping them in a Vec and compressing the whole thing.
            // Actually, each is already individually compressed. For simplicity,
            // we'll just take one per seal. For multiple, we serialize the list.
            //
            // Design decision: We embed all ready updates as a single compressed
            // blob. We re-serialize the full list.
            let mut all_updates: Vec<MissingEdgeUpdate> = Vec::new();
            for compressed_single in self.ready_updates.drain(..) {
                let bytes = lz4_flex::decompress_size_prepended(&compressed_single)
                    .expect("Failed to decompress MissingEdgeUpdate");
                let update: MissingEdgeUpdate = bincode::deserialize(&bytes)
                    .expect("Failed to deserialize MissingEdgeUpdate");
                // Skip updates for already-resolved graphs.
                if !self.resolved_graphs.contains(&update.graph_id) {
                    all_updates.push(update);
                }
            }
            if all_updates.is_empty() {
                None
            } else {
                let serialized = bincode::serialize(&all_updates)
                    .expect("Failed to serialize MissingEdgeUpdate list");
                let compressed = lz4_flex::compress_prepend_size(&serialized);
                debug!(
                    "FairDAG BatchMaker: sealing {} missing edge updates, \
                     compressed {}→{} bytes",
                    all_updates.len(), serialized.len(), compressed.len(),
                );
                Some(compressed)
            }
        } else {
            None
        };

        debug!(
            "FairDAG BatchMaker: sealing batch with {} entries, OI range [{}, {}], \
             has_updates={}",
            self.current_batch.len(),
            self.current_batch.first().map(|(_, oi)| *oi).unwrap_or(0),
            self.current_batch.last().map(|(_, oi)| *oi).unwrap_or(0),
            compressed_updates.is_some(),
        );

        self.current_batch_size = 0;
        let batch: Batch = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch, compressed_updates);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            let digest = Digest(
                Sha512::digest(&serialized)[..32]
                    .try_into()
                    .unwrap(),
            );
            for id in tx_ids {
                log_info!(
                    "Batch {:?} contains tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }
            log_info!("Batch {:?} contains {} B", digest, size);
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