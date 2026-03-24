// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL (v5).
//
// Changes from v4:
//   1. WorkerMessage::Batch now carries lz4-compressed MissingEdgeContributions
//   2. WorkerReceiverHandler extracts contributions and forwards to FairDagProcessor
//   3. New channel: FairDagProcessor → BatchMaker for MissingEdgeRequests
//   4. New channel: WorkerReceiverHandler → FairDagProcessor for received contributions

use crate::batch_maker::{
    Batch, BatchMaker, MissingEdgeContribution, MissingEdgeRequest, Transaction,
};
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

/// The message exchanged between workers.
/// FairDAG-RL v5: Batch carries Vec<(Transaction, u64)> + lz4-compressed
/// MissingEdgeContributions (empty Vec<u8> if none).
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    /// (batch_entries, lz4_compressed_contributions)
    /// If compressed_contributions is empty, there are no edge contributions.
    Batch(Batch, Vec<u8>),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

pub struct Worker {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
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
        // FairDAG-RL: Create the shared local order tracker.
        let tracker = LocalOrderTracker::new();

        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);

        // FairDAG-RL: Channel for committed subdags → FairDagProcessor.
        let (tx_fairdag, rx_fairdag) = channel(CHANNEL_CAPACITY);

        // FairDAG-RL v5: Channel for FairDagProcessor → BatchMaker (missing edge requests).
        let (tx_missing_edge, rx_missing_edge) = channel(CHANNEL_CAPACITY);

        // FairDAG-RL v5: Channel for received MissingEdgeContributions → FairDagProcessor.
        let (tx_edge_contributions, rx_edge_contributions) = channel(CHANNEL_CAPACITY);

        worker.handle_primary_messages(tx_fairdag);
        worker.handle_clients_transactions(
            tx_primary.clone(),
            tracker.clone(),
            rx_missing_edge,
        );
        worker.handle_workers_messages(tx_primary, tracker, tx_edge_contributions);

        // FairDAG-RL: Spawn the FairDagProcessor with edge update channels.
        FairDagProcessor::spawn(
            worker.committee.clone(),
            worker.store.clone(),
            rx_fairdag,
            worker.parameters.fault_threshold,
            tx_missing_edge,
            rx_edge_contributions,
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

    /// Spawn all tasks responsible to handle messages from our primary.
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

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
        tracker: LocalOrderTracker,
        rx_missing_edge: tokio::sync::mpsc::Receiver<MissingEdgeRequest>,
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
        Receiver::spawn(address, TxReceiverHandler { tx_batch_maker });

        // FairDAG-RL v5: BatchMaker gets the missing edge request channel.
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
            rx_missing_edge,
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

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    fn handle_workers_messages(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
        tracker: LocalOrderTracker,
        tx_edge_contributions: Sender<Vec<MissingEdgeContribution>>,
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
                tx_edge_contributions,
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

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

// =============================================================================
// Network message handlers
// =============================================================================

/// Handles incoming client transactions.
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
/// FairDAG-RL v5: extracts lz4-compressed MissingEdgeContributions from batches
/// and forwards them to FairDagProcessor.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
    tracker: LocalOrderTracker,
    /// Channel to forward received edge contributions to FairDagProcessor.
    tx_edge_contributions: Sender<Vec<MissingEdgeContribution>>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(ref batch_entries, ref compressed_contributions)) => {
                // FairDAG-RL: Record indirect arrivals.
                for (tx_bytes, _sender_oi) in batch_entries {
                    let tx_digest = extract_tx_digest(tx_bytes);
                    self.tracker.record(tx_digest);
                }

                // FairDAG-RL v5: Extract and forward edge contributions.
                if !compressed_contributions.is_empty() {
                    match lz4_flex::decompress_size_prepended(compressed_contributions) {
                        Ok(decompressed) => {
                            match bincode::deserialize::<Vec<MissingEdgeContribution>>(&decompressed)
                            {
                                Ok(contributions) if !contributions.is_empty() => {
                                    debug!(
                                        "FairDAG WorkerReceiver: extracted {} edge contributions from batch",
                                        contributions.len()
                                    );
                                    if let Err(e) =
                                        self.tx_edge_contributions.send(contributions).await
                                    {
                                        warn!(
                                            "Failed to forward edge contributions: {}",
                                            e
                                        );
                                    }
                                }
                                Ok(_) => {} // empty list, ignore
                                Err(e) => {
                                    warn!(
                                        "Failed to deserialize edge contributions: {}",
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to decompress edge contributions: {}", e);
                        }
                    }
                }

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

/// Handles incoming primary messages.
/// FairDAG-RL: dispatches ExecuteSubdag to the FairDagProcessor channel.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
    /// FairDAG-RL: channel to FairDagProcessor for committed subdags.
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
                    leader_round,
                    certificates.len()
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