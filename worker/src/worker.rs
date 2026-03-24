// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL (v6).
//
// Changes from plain Narwhal:
//   1. SharedLocalOrderTracker for OI assignment
//   2. WorkerMessage::Batch carries (Batch, compressed_updates)
//      - compressed_updates = lz4-compressed bincode of Vec<MissingEdgeUpdate>
//   3. PrimaryReceiverHandler dispatches ExecuteSubdag to FairDagProcessor
//   4. MissingEdgeRequest channel from FairDagProcessor → BatchMaker
//   5. No readd — nodes after last solid are discarded
use crate::batch_maker::{Batch, BatchMaker, MissingEdgeRequest, Transaction};
use crate::fairdag_processor::FairDagProcessor;
use crate::helper::Helper;
use crate::local_order_tracker::{extract_tx_digest, LocalOrderTracker};
use crate::primary_connector::PrimaryConnector;
use crate::processor::{Processor, SerializedBatchMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::{debug, error, info, warn};
use lz4_flex::decompress_size_prepended;
use network::{MessageHandler, Receiver, Writer};
use primary::{Certificate, PrimaryWorkerMessage, Round};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 10_000;

/// Indicates a serialized `WorkerPrimaryMessage` message.
pub type SerializedBatchDigestMessage = Vec<u8>;

// =============================================================================
// FairDAG-RL v6 wire types
// =============================================================================

/// A missing-edge update: this replica's local orderings for transactions
/// involved in missing edges of a graph at a given leader round.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingEdgeUpdate {
    /// The leader round of the graph that has missing edges.
    pub leader_round: u64,
    /// Local ordering indicators: (tx_digest, oi).
    pub orderings: Vec<(u64, u64)>,
}

/// The message exchanged between workers.
///
/// Batch carries:
///   - entries: Vec<(Transaction, u64)>  — regular tx + OI pairs
///   - compressed_updates: Vec<u8>       — lz4-compressed bincode of Vec<MissingEdgeUpdate>
///                                         (empty Vec<u8> if no updates)
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch, Vec<u8>),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

pub struct Worker {
    name: PublicKey,
    id: WorkerId,
    committee: Committee,
    parameters: Parameters,
    store: Store,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        parameters: Parameters,
        store: Store,
    ) {
        let tracker = LocalOrderTracker::new();

        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
        };

        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        let (tx_fairdag, rx_fairdag) = channel(CHANNEL_CAPACITY);
        let (tx_edge_updates, rx_edge_updates) = channel(CHANNEL_CAPACITY);
        let (tx_missing_edges, rx_missing_edges) = channel(CHANNEL_CAPACITY);

        worker.handle_primary_messages(tx_fairdag);
        worker.handle_clients_transactions(
            tx_primary.clone(),
            tracker.clone(),
            rx_missing_edges,
        );
        worker.handle_workers_messages(tx_primary, tracker, tx_edge_updates);

        FairDagProcessor::spawn(
            worker.committee.clone(),
            worker.store.clone(),
            rx_fairdag,
            rx_edge_updates,
            tx_missing_edges,
            worker.parameters.fault_threshold,
        );

        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary,
        );

        info!(
            "Worker {} successfully booted on {}",
            id,
            worker
                .committee
                .worker(&worker.name, &worker.id)
                .expect("Our public key or worker id is not in the committee")
                .transactions
                .ip()
        );
    }

    fn handle_primary_messages(
        &self,
        tx_fairdag: Sender<(Round, Vec<Certificate>)>,
    ) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            PrimaryReceiverHandler {
                tx_synchronizer,
                tx_fairdag,
            },
        );

        Synchronizer::spawn(
            self.name,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            rx_synchronizer,
        );

        info!("Worker {} listening to primary messages on {}", self.id, address);
    }

    fn handle_clients_transactions(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
        tracker: LocalOrderTracker,
        rx_missing_edges: tokio::sync::mpsc::Receiver<MissingEdgeRequest>,
    ) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            TxReceiverHandler { tx_batch_maker },
        );

        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            rx_batch_maker,
            tx_quorum_waiter,
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
            tracker,
            rx_missing_edges,
        );

        QuorumWaiter::spawn(
            self.committee.clone(),
            self.committee.stake(&self.name),
            rx_quorum_waiter,
            tx_processor,
        );

        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            tx_primary,
            true,
        );

        info!("Worker {} listening to client transactions on {}", self.id, address);
    }

    fn handle_workers_messages(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
        tracker: LocalOrderTracker,
        tx_edge_updates: Sender<(PublicKey, MissingEdgeUpdate)>,
    ) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            WorkerReceiverHandler {
                tx_helper,
                tx_processor,
                tracker,
                tx_edge_updates,
            },
        );

        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            rx_helper,
        );

        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            tx_primary,
            false,
        );

        info!("Worker {} listening to worker messages on {}", self.id, address);
    }
}

// =============================================================================
// Network message handlers
// =============================================================================

#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Handles incoming messages from other workers.
/// Decompresses lz4 edge updates from the batch trailer.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
    tracker: LocalOrderTracker,
    tx_edge_updates: Sender<(PublicKey, MissingEdgeUpdate)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(ref batch_entries, ref compressed_updates)) => {
                // Record indirect tx arrivals in tracker.
                for (tx_bytes, _sender_oi) in batch_entries {
                    let tx_digest = extract_tx_digest(tx_bytes);
                    self.tracker.record(tx_digest);
                }

                // Decompress and forward edge updates.
                // NOTE: we don't know the sender's PublicKey from the wire
                // message here. The authoritative attribution happens when
                // FairDagProcessor extracts committed batches from the store
                // (certificates carry the author). For real-time forwarding
                // we skip — the committed path handles it.

                self.tx_processor
                    .send(serialized.to_vec())
                    .await
                    .expect("Failed to send batch");
            }
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => {
                self.tx_helper
                    .send((missing, requestor))
                    .await
                    .expect("Failed to send batch request");
            }
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

/// Decompress an lz4-compressed edge update trailer into Vec<MissingEdgeUpdate>.
pub fn decompress_edge_updates(compressed: &[u8]) -> Vec<MissingEdgeUpdate> {
    if compressed.is_empty() {
        return Vec::new();
    }
    match decompress_size_prepended(compressed) {
        Ok(bytes) => bincode::deserialize(&bytes).unwrap_or_default(),
        Err(e) => {
            warn!("Failed to decompress edge updates: {}", e);
            Vec::new()
        }
    }
}

/// Handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
    tx_fairdag: Sender<(Round, Vec<Certificate>)>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(PrimaryWorkerMessage::ExecuteSubdag(leader_round, certificates)) => {
                info!(
                    "Worker received ExecuteSubdag for leader round {} with {} certs",
                    leader_round, certificates.len()
                );
                self.tx_fairdag
                    .send((leader_round, certificates))
                    .await
                    .expect("Failed to send subdag to FairDagProcessor");
            }
            Ok(message) => {
                self.tx_synchronizer
                    .send(message)
                    .await
                    .expect("Failed to send primary message to synchronizer");
            }
        }
        Ok(())
    }
}