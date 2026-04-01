// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL (v4).
//
// Changes from plain Narwhal:
//   1. SharedLocalOrderTracker for OI assignment (v3)
//   2. WorkerMessage::Batch carries (tx, oi) pairs (v3)
//   3. PrimaryReceiverHandler dispatches ExecuteSubdag to FairDagProcessor (v4)
//   4. Indirect tx propagation: direct entries from incoming batches are
//      forwarded to BatchMaker so they piggyback on outgoing batches (one-hop).
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

/// Indirect tx arrival: (tx_digest, local_oi) from WorkerReceiverHandler → BatchMaker.
pub type IndirectTxEntry = (u64, u64);

/// The message exchanged between workers.
/// FairDAG-RL: Batch carries Vec<(Transaction, u64)> as direct entries,
/// plus Vec<IndirectTxEntry> as indirect entries (tx_digest + OI only).
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch, Vec<IndirectTxEntry>),
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
    is_byzantine: bool,
    byzantine_active: bool,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        is_byzantine: bool,
        byzantine_active: bool,
    ) {

        if is_byzantine && byzantine_active {
            info!("BYZANTINE_ACTIVE: Worker {} is actively Byzantine (reversing orderings)", id);
        } else if is_byzantine {
            info!("BYZANTINE_DORMANT: Worker {} is Byzantine but not yet active", id);
        }

        // FairDAG-RL: Create the shared local order tracker.
        let tracker = LocalOrderTracker::new();

        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
            is_byzantine,
            byzantine_active,
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);

        // FairDAG-RL: Channel for committed subdags → FairDagProcessor.
        let (tx_fairdag, rx_fairdag) = channel(CHANNEL_CAPACITY);

        // FairDAG-RL: Channel for indirect tx entries from WorkerReceiverHandler → BatchMaker.
        let (tx_indirect, rx_indirect) = channel(CHANNEL_CAPACITY);

        worker.handle_primary_messages(tx_fairdag);
        worker.handle_clients_transactions(tx_primary.clone(), tracker.clone(), rx_indirect);
        worker.handle_workers_messages(tx_primary, tracker, tx_indirect);

        // FairDAG-RL: Spawn the FairDagProcessor. It reads batches from the
        // local store and runs the fairness layer.
        FairDagProcessor::spawn(
            worker.committee.clone(),
            worker.store.clone(),
            rx_fairdag,
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

    /// Spawn all tasks responsible to handle messages from our primary.
    fn handle_primary_messages(
        &self,
        tx_fairdag: Sender<(Round, Vec<Certificate>)>,
    ) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

        let is_byzantine = self.is_byzantine;
        let byzantine_active = self.byzantine_active;

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            PrimaryReceiverHandler {
                byzantine_active,
                is_byzantine,
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
        rx_indirect: tokio::sync::mpsc::Receiver<IndirectTxEntry>,
    ) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let is_byzantine = self.is_byzantine;
        let byzantine_active = self.byzantine_active;

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            TxReceiverHandler { byzantine_active, is_byzantine, tx_batch_maker },
        );

        // FairDAG-RL: BatchMaker gets the shared tracker + rx_indirect channel.
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
            rx_indirect,
            self.is_byzantine,
            self.byzantine_active,
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
        tx_indirect: Sender<IndirectTxEntry>,
    ) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let is_byzantine = self.is_byzantine;
        let byzantine_active = self.byzantine_active;

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            WorkerReceiverHandler {
                byzantine_active,
                is_byzantine,
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

/// Handles incoming client transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    byzantine_active: bool,
    is_byzantine: bool,
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        
        if !self.is_byzantine || self.byzantine_active {
            self.tx_batch_maker
                .send(message.to_vec())
                .await
                .expect("Failed to send transaction");
        }

        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Handles incoming messages from other workers.
/// FairDAG-RL: records indirect tx arrivals in the shared tracker and
/// forwards direct entries to BatchMaker (one-hop propagation only).
#[derive(Clone)]
struct WorkerReceiverHandler {
    byzantine_active: bool,
    is_byzantine: bool,
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
    tracker: LocalOrderTracker,
    /// Channel to forward indirect tx entries to BatchMaker.
    tx_indirect: Sender<IndirectTxEntry>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        
        if !self.is_byzantine || self.byzantine_active {
            // Reply with an ACK.
            let _ = writer.send(Bytes::from("Ack")).await;

            match bincode::deserialize(&serialized) {
                Ok(WorkerMessage::Batch(ref direct_entries, ref _indirect_entries)) => {
                    // FairDAG-RL: Direct entries from the incoming batch —
                    // record in tracker AND forward to BatchMaker as indirect
                    // candidates for our own outgoing batches.
                    for (tx_bytes, _sender_oi) in direct_entries {
                        let tx_digest = extract_tx_digest(tx_bytes);
                        let local_oi = self.tracker.record(tx_digest);
                        let _ = self.tx_indirect.send((tx_digest, local_oi)).await;
                    }

                    // Indirect entries from the incoming batch: record in
                    // tracker for OI only. Do NOT re-forward — prevents
                    // multi-hop amplification.
                    for &(tx_digest, _sender_oi) in _indirect_entries {
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
        }

        Ok(())
    }
}

/// Handles incoming primary messages.
/// FairDAG-RL: dispatches ExecuteSubdag to the FairDagProcessor channel.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    byzantine_active: bool,
    is_byzantine: bool,
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
        
        if !self.is_byzantine || self.byzantine_active {
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
        }

        Ok(())
    }
}