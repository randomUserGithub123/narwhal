use crate::local_order_tracker::extract_tx_digest;
use crate::worker::WorkerMessage;
use config::Committee;
use crypto::PublicKey;
use fairdag_fairness::{
    CommittedSubdag, CommittedVertex, FairnessLayer, Round, TxDigest,
};
use log::{error, info, warn};
use primary::Certificate;
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::Receiver;

pub struct FairDagProcessor {
    store: Store,
    fairness_layer: FairnessLayer,
    sorted_keys: Vec<PublicKey>,
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
        let mut subdag_count: u64 = 0;

        while let Some((leader_round, certificates)) = self.rx_committed_subdags.recv().await {
            subdag_count += 1;
            let total_start = Instant::now();

            info!(
                "FAIRDAG_TIMING: subdag #{} received: leader_round={} certs={}",
                subdag_count, leader_round, certificates.len()
            );

            // Step 1: Extract subdag (reads from store)
            let extract_start = Instant::now();
            let subdag = self.extract_subdag(leader_round, &certificates).await;
            let extract_ms = extract_start.elapsed().as_millis();

            let total_entries: usize = subdag.vertices.iter()
                .map(|v| v.ordering_entries.len())
                .sum();

            info!(
                "FAIRDAG_TIMING: subdag #{} extract done: {}ms, vertices={} total_entries={}",
                subdag_count, extract_ms, subdag.vertices.len(), total_entries
            );

            // Step 2: Process through fairness layer
            let process_start = Instant::now();
            let fair_ordered = self.fairness_layer.process_subdag(&subdag);
            let process_ms = process_start.elapsed().as_millis();

            let total_ms = total_start.elapsed().as_millis();

            info!(
                "FAIRDAG_TIMING: subdag #{} process done: {}ms, fair_ordered={} total_time={}ms",
                subdag_count, process_ms, fair_ordered.len(), total_ms
            );

            // Log each fair-ordered transaction.
            if !fair_ordered.is_empty() {
                info!(
                    "FairDAG: outputting {} fair-ordered transactions from leader round {}",
                    fair_ordered.len(), leader_round
                );
                for tx_id in &fair_ordered {
                    info!("FairDAG-RL ordered transaction: {}", tx_id);
                }
            }
        }
    }

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
            let mut batches_found = 0usize;
            let mut batches_missing = 0usize;

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
                                warn!("Unexpected message type for batch {:?}", batch_digest);
                            }
                            Err(e) => {
                                error!("Deser fail batch {:?}: {}", batch_digest, e);
                            }
                        }
                    }
                    Ok(None) => {
                        batches_missing += 1;
                    }
                    Err(e) => {
                        error!("Store read error batch {:?}: {}", batch_digest, e);
                    }
                }
            }

            info!(
                "FAIRDAG_TIMING: extract cert round={} replica={} batches found={} missing={} entries={}",
                cert.round(), replica_index, batches_found, batches_missing, ordering_entries.len()
            );

            vertices.push(CommittedVertex {
                replica: author,
                replica_index,
                round: cert.round(),
                ordering_entries,
            });
        }

        vertices.sort_by_key(|v| v.round);

        CommittedSubdag {
            leader_round,
            vertices,
        }
    }
}