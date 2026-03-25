// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL with explicit FairUpdate (Themis-style).
//
// Key design for indirect tx propagation:
//   - WorkerReceiverHandler forwards ONLY direct entries from incoming batches
//     to BatchMaker's rx_indirect channel (one-hop propagation).
//   - Indirect entries from incoming batches are recorded in the tracker (for OI)
//     but NOT re-forwarded (prevents multi-hop amplification).
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairUpdateVote {
    pub sub_dag_id: u64,
    pub edge_count: usize,
    pub directed_edges_compressed: Vec<u8>,
}

/// Message from FairDagProcessor to BatchMaker requesting edge votes.
/// When vertices and edges are both empty, this is a cleanup signal.
pub type FairProposeMessage = (u64, Vec<(u16, u64)>, Vec<u32>);

/// Indirect tx arrival: (tx_digest, local_oi) from WorkerReceiverHandler → BatchMaker.
pub type IndirectTxEntry = (u64, u64);

// =========================================================================
// Worker messages
// =========================================================================

/// The message exchanged between workers.
///   - direct_entries: Vec<(Transaction, u64)> — full tx bytes + LOI (from clients)
///   - indirect_entries: Vec<(u64, u64)> — tx_digest + LOI (seen via other workers)
///   - votes: Vec<FairUpdateVote> — explicit edge resolution votes
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch, Vec<IndirectTxEntry>, Vec<FairUpdateVote>),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

// =========================================================================
// Worker
// =========================================================================

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
        let (tx_fair_propose, rx_fair_propose) = channel(CHANNEL_CAPACITY);
        let (tx_indirect, rx_indirect) = channel(CHANNEL_CAPACITY);

        worker.handle_primary_messages(tx_fairdag);
        worker.handle_clients_transactions(
            tx_primary.clone(),
            tracker.clone(),
            rx_fair_propose,
            rx_indirect,
        );
        worker.handle_workers_messages(tx_primary, tracker, tx_indirect);

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

    fn handle_clients_transactions(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
        tracker: LocalOrderTracker,
        rx_fair_propose: tokio::sync::mpsc::Receiver<FairProposeMessage>,
        rx_indirect: tokio::sync::mpsc::Receiver<IndirectTxEntry>,
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
            rx_fair_propose,
            rx_indirect,
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

    fn handle_workers_messages(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
        tracker: LocalOrderTracker,
        tx_indirect: Sender<IndirectTxEntry>,
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
                tx_indirect,
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
///
/// CRITICAL ONE-HOP RULE:
///   - DIRECT entries from the incoming batch: record in tracker AND forward
///     to BatchMaker as indirect candidates.
///   - INDIRECT entries from the incoming batch: record in tracker for OI only,
///     do NOT forward to BatchMaker. These are already someone else's one-hop
///     forward — re-forwarding would cause multi-hop amplification.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
    tracker: LocalOrderTracker,
    tx_indirect: Sender<IndirectTxEntry>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(ref direct_entries, ref _indirect_entries, ref _votes)) => {
                
                // Direct entries: record in tracker + forward to BatchMaker.
                for (tx_bytes, _sender_oi) in direct_entries {
                    let tx_digest = extract_tx_digest(tx_bytes);
                    let local_oi = self.tracker.record(tx_digest);
                    let _ = self.tx_indirect.send((tx_digest, local_oi)).await;
                }

                // // Indirect entries: record in tracker for OI only.
                // // Do NOT forward — prevents multi-hop amplification.
                // for &(tx_digest, _sender_oi) in indirect_entries {
                //     self.tracker.record(tx_digest);
                // }

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