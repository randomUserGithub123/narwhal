// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL with explicit FairUpdate (Themis-style).
//
// Changes from previous FairDAG-RL:
//   1. WorkerMessage::Batch now carries Vec<FairUpdateVote> alongside batch entries
//   2. FairProposeMessage type for FairDagProcessor → BatchMaker communication
//   3. Worker::spawn wires tx_fair_propose channel between FairDagProcessor and BatchMaker
//   4. WorkerReceiverHandler handles the new Batch format (ignores votes for tracking)
use crate::batch_maker::{Batch, BatchMaker, Transaction};
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

// =========================================================================
// FairUpdate types
// =========================================================================

/// A FairUpdate vote: a replica's directed-edge evidence for a parked graph.
/// Embedded in batches and extracted by FairDagProcessor on commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairUpdateVote {
    /// The sub-dag whose parked graph these votes resolve.
    pub sub_dag_id: u64,
    /// Number of directed edges (for decompression).
    pub edge_count: usize,
    /// lz4-compressed, varint+delta encoded directed edges.
    /// Each edge is packed as (from_vertex << 16 | to_vertex).
    pub directed_edges_compressed: Vec<u8>,
}

/// Message from FairDagProcessor to BatchMaker requesting edge votes.
///   - sub_dag_id: which parked graph needs votes
///   - Vec<(vertex_index, tx_digest)>: vertices involved in missing edges
///   - Vec<u32>: missing edge pairs, packed as (min(u,v) << 16 | max(u,v))
///
/// When vertices and edges are both empty, this is a cleanup signal.
pub type FairProposeMessage = (u64, Vec<(u16, u64)>, Vec<u32>);

// =========================================================================
// Worker messages
// =========================================================================

/// The message exchanged between workers.
/// FairDAG-RL: Batch carries Vec<(Transaction, u64)> (each tx with its OI)
/// plus Vec<FairUpdateVote> for explicit edge resolution.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch, Vec<FairUpdateVote>),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

// =========================================================================
// Worker
// =========================================================================

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

        // FairDAG-RL (Themis-style): Channel for missing-edge notifications
        // FairDagProcessor → BatchMaker.
        let (tx_fair_propose, rx_fair_propose) = channel(CHANNEL_CAPACITY);

        worker.handle_primary_messages(tx_fairdag);
        worker.handle_clients_transactions(
            tx_primary.clone(),
            tracker.clone(),
            rx_fair_propose,
        );
        worker.handle_workers_messages(tx_primary, tracker);

        // FairDAG-RL: Spawn the FairDagProcessor with the fair_propose sender.
        FairDagProcessor::spawn(
            worker.committee.clone(),
            worker.store.clone(),
            rx_fairdag,
            worker.parameters.fault_threshold,
            tx_fair_propose,
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
        rx_fair_propose: tokio::sync::mpsc::Receiver<FairProposeMessage>,
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

        // FairDAG-RL: BatchMaker gets the shared tracker + fair_propose channel.
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
            rx_fair_propose,
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
/// FairDAG-RL: records indirect tx arrivals in the shared tracker.
/// Ignores FairUpdateVote payloads (those are extracted by FairDagProcessor).
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
    tracker: LocalOrderTracker,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(ref batch_entries, ref _votes)) => {
                // FairDAG-RL: Record indirect arrivals (ignore votes — they
                // will be extracted by FairDagProcessor when reading from store).
                for (tx_bytes, _sender_oi) in batch_entries {
                    let tx_digest = extract_tx_digest(tx_bytes);
                    self.tracker.record(tx_digest);
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
                // FairDAG-RL: route to FairDagProcessor, NOT to synchronizer.
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
                // Synchronize and Cleanup go to the synchronizer as before.
                self.tx_synchronizer
                    .send(message)
                    .await
                    .expect("Failed to send primary message to synchronizer");
            }
        }
        Ok(())
    }
}