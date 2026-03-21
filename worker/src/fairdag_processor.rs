// FairDAG-RL: FairDagProcessor
//
// Runs in the worker process where batch data is stored locally.
// Receives entire committed subdags from the primary (via GarbageCollector).
// For each subdag:
//   1. Reads batch bytes from local store for each cert's payload digests
//   2. Deserializes to extract (tx_bytes, ordering_indicator) pairs
//   3. Builds a CommittedSubdag with ordering entries
//   4. Feeds to FairnessLayer for dependency graph construction + ordering
//   5. Logs fair-ordered transactions for performance measurement

use crate::local_order_tracker::extract_tx_digest;
use crate::worker::WorkerMessage;
use config::Committee;
use crypto::PublicKey;
use fairdag_fairness::{
    CommittedSubdag, CommittedVertex, FairnessLayer, Round, TxDigest,
};
use log::{debug, error, info, warn};
use primary::Certificate;
use store::Store;
use tokio::sync::mpsc::Receiver;

pub struct FairDagProcessor {
    /// The persistent storage (worker-local, contains batch data).
    store: Store,
    /// The FairDAG-RL fairness layer.
    fairness_layer: FairnessLayer,
    /// Sorted committee keys for replica index lookup.
    sorted_keys: Vec<PublicKey>,
    /// Receives committed subdags: (leader_round, certificates).
    rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
}

impl FairDagProcessor {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
    ) {
        let n = committee.size();
        let f = (n - 1) / 3;

        let mut sorted_keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        sorted_keys.sort();

        let fairness_layer = FairnessLayer::new(sorted_keys.clone(), f);

        tokio::spawn(async move {
            Self {
                store,
                fairness_layer,
                sorted_keys,
                rx_committed_subdags,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((leader_round, certificates)) = self.rx_committed_subdags.recv().await {
            info!(
                "FairDagProcessor: received subdag for leader round {} with {} certs",
                leader_round,
                certificates.len()
            );

            // Build the CommittedSubdag by reading batches from local store.
            let subdag = self.extract_subdag(leader_round, &certificates).await;

            // Feed to fairness layer.
            let fair_ordered = self.fairness_layer.process_subdag(&subdag);

            // Log each fair-ordered transaction for the log parser to pick up.
            if !fair_ordered.is_empty() {
                info!(
                    "FairDAG: outputting {} fair-ordered transactions from leader round {}",
                    fair_ordered.len(),
                    leader_round
                );
                for tx_id in &fair_ordered {
                    info!("FairDAG-RL ordered transaction: {}", tx_id);
                }
            }
        }
    }

    /// Read batch data from the local store and extract ordering entries.
    async fn extract_subdag(
        &self,
        leader_round: Round,
        certificates: &[Certificate],
    ) -> CommittedSubdag {
        let mut vertices: Vec<CommittedVertex> = Vec::new();

        for cert in certificates {
            let author = cert.origin();
            let replica_index = self
                .sorted_keys
                .iter()
                .position(|k| *k == author)
                .expect("Certificate author not in committee");

            let mut ordering_entries: Vec<(TxDigest, u64)> = Vec::new();
            let num_batches = cert.header.payload.len();
            let mut batches_found = 0usize;
            let mut batches_missing = 0usize;
            let mut batches_deser_fail = 0usize;

            for batch_digest in cert.header.payload.keys() {
                match self.store.clone().read(batch_digest.to_vec()).await {
                    Ok(Some(serialized_batch)) => {
                        batches_found += 1;
                        match bincode::deserialize::<WorkerMessage>(&serialized_batch) {
                            Ok(WorkerMessage::Batch(batch_entries)) => {
                                for (tx_bytes, oi) in batch_entries {
                                    let tx_id = extract_tx_digest(&tx_bytes);
                                    ordering_entries.push((tx_id, oi));
                                }
                            }
                            Ok(_) => {
                                batches_deser_fail += 1;
                                warn!(
                                    "Unexpected message type in store for batch {:?}",
                                    batch_digest
                                );
                            }
                            Err(e) => {
                                batches_deser_fail += 1;
                                error!(
                                    "FairDagProcessor: deser_fail batch {:?}: {} (first 32 bytes: {:?})",
                                    batch_digest, e,
                                    &serialized_batch[..std::cmp::min(32, serialized_batch.len())]
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        batches_missing += 1;
                    }
                    Err(e) => {
                        error!("Store read error for batch {:?}: {}", batch_digest, e);
                    }
                }
            }

            debug!(
                "FairDagProcessor: cert round={} replica={} batches={} found={} missing={} fail={} entries={}",
                cert.round(), replica_index, num_batches, batches_found,
                batches_missing, batches_deser_fail, ordering_entries.len()
            );

            vertices.push(CommittedVertex {
                replica: author,
                replica_index,
                round: cert.round(),
                ordering_entries,
            });
        }

        // Sort vertices by round (ascending) as required by the protocol.
        vertices.sort_by_key(|v| v.round);

        CommittedSubdag {
            leader_round,
            vertices,
        }
    }
}