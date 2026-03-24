// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL v5: Explicit missing-edge contributions.
//
// When the FairDagProcessor discovers missing edges in a graph, it sends a
// MissingEdgeRequest to this BatchMaker via a channel. The BatchMaker tracks
// which tx digests are needed for each graph_round. Once ALL needed txs have
// been seen locally (via the shared LocalOrderTracker), it generates a
// MissingEdgeContribution containing this replica's OIs for those txs, and
// includes it (lz4-compressed) in the next sealed batch.
//
// A graph_round is contributed-to at most once — after generating the
// contribution, the entry is removed from pending state.

use crate::local_order_tracker::{extract_tx_digest, LocalOrderTracker, TxDigest};
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
use log::{debug, warn};
use network::ReliableSender;
use primary::Round;
use serde::{Deserialize, Serialize};
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

// =============================================================================
// Explicit missing-edge types
// =============================================================================

/// Sent by FairDagProcessor → BatchMaker when a graph has missing edges.
/// Contains the actual missing edge PAIRS (not just tx digests).
/// Each worker will vote a direction for each pair based on its local ordering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingEdgeRequest {
    /// The graph's leader round — unique identifier for the graph.
    pub graph_round: Round,
    /// The missing edge pairs. Each (d1, d2) is a pair of tx digests
    /// between which no edge could be added during initial graph construction.
    pub missing_pairs: Vec<(TxDigest, TxDigest)>,
    /// All unique tx digests involved in missing pairs.
    /// The batch_maker waits until ALL of these are in the local tracker.
    pub needed_tx_digests: Vec<TxDigest>,
}

/// This replica's contribution: directed edge votes for missing pairs.
/// For each missing pair (d1, d2), the worker votes a direction based on
/// which tx it saw first locally. Encoded as (from, to) = "from before to".
/// Serialized with bincode, then lz4-compressed before inclusion in batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingEdgeContribution {
    /// Which graph this contribution is for.
    pub graph_round: Round,
    /// Directed edge votes: each (from, to) means "I saw `from` before `to`".
    pub directed_votes: Vec<(TxDigest, TxDigest)>,
}

/// Tracks the state of a pending missing-edge contribution for one graph.
struct PendingEdgeGraph {
    graph_round: Round,
    /// The missing edge pairs we need to vote on.
    missing_pairs: Vec<(TxDigest, TxDigest)>,
    /// All unique tx digests we need to have seen before voting.
    needed_digests: HashSet<TxDigest>,
    /// Once we generate the contribution, this is set to true.
    contributed: bool,
}

/// Assemble client transactions into batches.
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
    // Explicit missing-edge state
    // =========================================================================
    /// Channel to receive missing edge requests from FairDagProcessor.
    rx_missing_edge: Receiver<MissingEdgeRequest>,
    /// Graphs awaiting contribution: graph_round → pending state.
    pending_edge_graphs: HashMap<Round, PendingEdgeGraph>,
    /// Graphs we already contributed to — never contribute again.
    completed_graph_rounds: HashSet<Round>,
    /// Contributions ready to be included in the next sealed batch.
    ready_contributions: Vec<MissingEdgeContribution>,
    /// Channel to send self-generated contributions directly to FairDagProcessor.
    /// Without this, our own contribution only goes into the sealed batch (which
    /// is processed by Processor for storage, NOT by WorkerReceiverHandler), so
    /// the FairDagProcessor would never see it.
    tx_self_contributions: Sender<Vec<MissingEdgeContribution>>,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        tracker: LocalOrderTracker,
        rx_missing_edge: Receiver<MissingEdgeRequest>,
        tx_self_contributions: Sender<Vec<MissingEdgeContribution>>,
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
                rx_missing_edge,
                pending_edge_graphs: HashMap::new(),
                completed_graph_rounds: HashSet::new(),
                ready_contributions: Vec::new(),
                tx_self_contributions,
            }
            .run()
            .await;
        });
    }

    /// Check all pending edge graphs to see if any are now ready
    /// (all needed tx digests have been seen in the local tracker).
    /// For each ready graph, votes a direction for each missing edge pair
    /// based on local arrival order, following Themis FairUpdate.
    fn check_pending_edge_graphs(&mut self) -> Vec<MissingEdgeContribution> {
        let mut newly_ready: Vec<Round> = Vec::new();

        for (round, pending) in &self.pending_edge_graphs {
            if pending.contributed {
                continue;
            }
            let needed: Vec<TxDigest> = pending.needed_digests.iter().copied().collect();
            if self.tracker.has_all(&needed) {
                newly_ready.push(*round);
            }
        }

        let mut self_contributions: Vec<MissingEdgeContribution> = Vec::new();

        for round in newly_ready {
            if let Some(pending) = self.pending_edge_graphs.get_mut(&round) {
                // Vote a direction for each missing pair using local OIs.
                let mut directed_votes: Vec<(TxDigest, TxDigest)> =
                    Vec::with_capacity(pending.missing_pairs.len());

                for &(d1, d2) in &pending.missing_pairs {
                    let oi1 = self.tracker.get_oi(d1);
                    let oi2 = self.tracker.get_oi(d2);

                    match (oi1, oi2) {
                        (Some(o1), Some(o2)) => {
                            if o1 <= o2 {
                                directed_votes.push((d1, d2));
                            } else {
                                directed_votes.push((d2, d1));
                            }
                        }
                        (Some(_), None) => directed_votes.push((d1, d2)),
                        (None, Some(_)) => directed_votes.push((d2, d1)),
                        (None, None) => {
                            if d1 < d2 { directed_votes.push((d1, d2)); }
                            else { directed_votes.push((d2, d1)); }
                        }
                    }
                }

                let contribution = MissingEdgeContribution {
                    graph_round: round,
                    directed_votes,
                };

                debug!(
                    "FairDAG BatchMaker: generated {} edge votes for graph round {}",
                    contribution.directed_votes.len(), round,
                );

                self_contributions.push(contribution.clone());
                self.ready_contributions.push(contribution);
                pending.contributed = true;
                self.completed_graph_rounds.insert(round);
            }
        }

        self.pending_edge_graphs.retain(|_, p| !p.contributed);
        self_contributions
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

                    // After recording a new tx, check if any pending edge graphs
                    // are now complete.
                    let self_contribs = self.check_pending_edge_graphs();
                    Self::send_self_contributions(&self.tx_self_contributions, self_contribs).await;

                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // Receive missing edge requests from FairDagProcessor.
                Some(request) = self.rx_missing_edge.recv() => {
                    if self.completed_graph_rounds.contains(&request.graph_round) {
                        debug!(
                            "FairDAG BatchMaker: ignoring duplicate missing-edge request for round {} (already contributed)",
                            request.graph_round
                        );
                        continue;
                    }

                    if self.pending_edge_graphs.contains_key(&request.graph_round) {
                        debug!(
                            "FairDAG BatchMaker: ignoring duplicate missing-edge request for round {} (already pending)",
                            request.graph_round
                        );
                        continue;
                    }

                    debug!(
                        "FairDAG BatchMaker: received missing-edge request for graph round {} ({} pairs, {} unique txs)",
                        request.graph_round,
                        request.missing_pairs.len(),
                        request.needed_tx_digests.len()
                    );

                    let needed_set: HashSet<TxDigest> =
                        request.needed_tx_digests.into_iter().collect();

                    self.pending_edge_graphs.insert(
                        request.graph_round,
                        PendingEdgeGraph {
                            graph_round: request.graph_round,
                            missing_pairs: request.missing_pairs,
                            needed_digests: needed_set,
                            contributed: false,
                        },
                    );

                    // Immediately check if we already have all needed txs.
                    let self_contribs = self.check_pending_edge_graphs();
                    Self::send_self_contributions(&self.tx_self_contributions, self_contribs).await;
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    // Also check pending edge graphs on timer ticks — a tx might
                    // have arrived via another worker's batch (recorded in tracker
                    // by WorkerReceiverHandler) without going through our tx channel.
                    let self_contribs = self.check_pending_edge_graphs();
                    Self::send_self_contributions(&self.tx_self_contributions, self_contribs).await;

                    if !self.current_batch.is_empty() || !self.ready_contributions.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the chance to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Forward self-generated contributions directly to FairDagProcessor.
    /// This is necessary because our own sealed batches go through
    /// QuorumWaiter → Processor (which only stores them), NOT through
    /// WorkerReceiverHandler (which extracts contributions).
    async fn send_self_contributions(
        tx: &Sender<Vec<MissingEdgeContribution>>,
        contributions: Vec<MissingEdgeContribution>,
    ) {
        if !contributions.is_empty() {
            debug!(
                "FairDAG BatchMaker: self-injecting {} contributions to FairDagProcessor",
                contributions.len()
            );
            if let Err(e) = tx.send(contributions).await {
                warn!("Failed to self-inject contributions to FairDagProcessor: {}", e);
            }
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

        // =====================================================================
        // Compress any ready missing-edge contributions with lz4_flex.
        // =====================================================================
        let compressed_contributions: Vec<u8> = if self.ready_contributions.is_empty() {
            Vec::new()
        } else {
            let contributions: Vec<MissingEdgeContribution> =
                self.ready_contributions.drain(..).collect();

            debug!(
                "FairDAG BatchMaker: sealing batch with {} missing-edge contributions",
                contributions.len()
            );

            let serialized = bincode::serialize(&contributions)
                .expect("Failed to serialize missing-edge contributions");
            lz4_flex::compress_prepend_size(&serialized)
        };

        debug!(
            "FairDAG BatchMaker: sealing batch with {} entries, {} bytes compressed contributions, OI range [{}, {}]",
            self.current_batch.len(),
            compressed_contributions.len(),
            self.current_batch.first().map(|(_, oi)| *oi).unwrap_or(0),
            self.current_batch.last().map(|(_, oi)| *oi).unwrap_or(0),
        );

        self.current_batch_size = 0;
        let batch: Batch = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch, compressed_contributions);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            use crypto::Digest;
            use ed25519_dalek::{Digest as _, Sha512};
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