// Copyright(C) FairDAG-RL Implementation.
// Modified for v5: Explicit Missing Edge Updates.
//
// Changes:
//   1. Extracts MissingEdgeUpdates from batches (decompresses lz4).
//   2. Passes them to the FairnessLayer alongside subdag data.
//   3. FairnessLayer sends MissingEdgeRequests back to BatchMaker via channel.

use crate::local_order_tracker::extract_tx_digest;
use crate::missing_edge_types::{
    FairnessToWorkerMessage, MissingEdgeUpdate,
};
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
use tokio::sync::mpsc::{Receiver, Sender};

pub struct FairDagProcessor {
    store: Store,
    fairness_layer: FairnessLayer,
    sorted_keys: Vec<PublicKey>,
    rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
    /// Channel to send MissingEdgeRequest / GraphResolved to BatchMaker.
    tx_fairness_to_worker: Sender<FairnessToWorkerMessage>,
}

impl FairDagProcessor {
    pub fn spawn(
        mut committee: Committee,
        store: Store,
        rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
        fault_threshold: u64,
        tx_fairness_to_worker: Sender<FairnessToWorkerMessage>,
    ) {
        let n = committee.size();
        let f = fault_threshold as usize;

        let mut sorted_keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        sorted_keys.sort();

        let gamma = committee.get_gamma().unwrap();

        let fairness_layer = FairnessLayer::new(
            sorted_keys.clone(),
            f,
            gamma,
        );

        let handle = tokio::spawn(async move {
            Self {
                store,
                fairness_layer,
                sorted_keys,
                rx_committed_subdags,
                tx_fairness_to_worker,
            }
            .run()
            .await;
        });

        tokio::spawn(async move {
            if let Err(e) = handle.await {
                error!("FATAL: FairDagProcessor panicked: {:?}", e);
                std::process::abort();
            }
        });
    }

    async fn run(&mut self) {
        let mut subdag_count: u64 = 0;
        let mut batch_count: u64 = 0;

        while let Some((leader_round, certificates)) = self.rx_committed_subdags.recv().await {
            let batch_start = Instant::now();

            // ─────────────────────────────────────────────────────────────────
            // Step 1: Drain all queued subdags into a batch.
            // ─────────────────────────────────────────────────────────────────
            let mut batch_raw: Vec<(Round, Vec<Certificate>)> = vec![(leader_round, certificates)];
            while let Ok((r, c)) = self.rx_committed_subdags.try_recv() {
                batch_raw.push((r, c));
            }

            let batch_size = batch_raw.len();
            batch_count += 1;

            info!(
                "FAIRDAG_TIMING: batch #{} draining: {} subdags queued",
                batch_count, batch_size
            );

            // ─────────────────────────────────────────────────────────────────
            // Step 2: Extract all subdags and their missing edge updates.
            // ─────────────────────────────────────────────────────────────────
            let extract_start = Instant::now();
            let mut subdags: Vec<CommittedSubdag> = Vec::with_capacity(batch_size);
            let mut all_edge_updates: Vec<MissingEdgeUpdate> = Vec::new();

            for (round, certs) in &batch_raw {
                subdag_count += 1;

                info!(
                    "FAIRDAG_TIMING: subdag #{} received: leader_round={} certs={}",
                    subdag_count, round, certs.len()
                );

                let sd_extract_start = Instant::now();
                let (subdag, edge_updates) = self.extract_subdag(*round, certs).await;
                let sd_extract_ms = sd_extract_start.elapsed().as_millis();

                let total_entries: usize = subdag
                    .vertices
                    .iter()
                    .map(|v| v.ordering_entries.len())
                    .sum();

                info!(
                    "FAIRDAG_TIMING: subdag #{} extract done: {}ms, vertices={} \
                     total_entries={} edge_updates={}",
                    subdag_count, sd_extract_ms, subdag.vertices.len(),
                    total_entries, edge_updates.len()
                );

                all_edge_updates.extend(edge_updates);
                subdags.push(subdag);
            }

            let extract_ms = extract_start.elapsed().as_millis();

            // ─────────────────────────────────────────────────────────────────
            // Step 3: Process subdags + explicit edge updates through fairness
            //         layer. The fairness layer handles:
            //         - Ingesting new OIs from subdags
            //         - Applying explicit edge updates
            //         - Constructing new graphs (independent, can be parallel)
            //         - Finalizing tournament graphs
            //         - Sending MissingEdgeRequests back to us
            // ─────────────────────────────────────────────────────────────────
            let process_start = Instant::now();

            let (fair_ordered, fairness_messages) = self.fairness_layer
                .process_subdag_batch_explicit(&subdags, &all_edge_updates);

            let process_ms = process_start.elapsed().as_millis();

            // Forward fairness messages to BatchMaker.
            for msg in fairness_messages {
                if let Err(e) = self.tx_fairness_to_worker.send(msg).await {
                    warn!(
                        "Failed to send fairness message to BatchMaker: {}",
                        e
                    );
                }
            }

            if !fair_ordered.is_empty() {
                info!(
                    "FairDAG: outputting {} fair-ordered transactions",
                    fair_ordered.len()
                );
                for tx_id in &fair_ordered {
                    info!("FairDAG-RL ordered transaction: {}", tx_id);
                }
            }

            let total_ms = batch_start.elapsed().as_millis();

            info!(
                "FAIRDAG_TIMING: batch #{} done: batch_size={} extract={}ms \
                 process={}ms total={}ms fair_ordered={} edge_updates_applied={}",
                batch_count, batch_size, extract_ms, process_ms, total_ms,
                fair_ordered.len(), all_edge_updates.len()
            );
        }
    }

    /// Extract a subdag from committed certificates.
    /// Also extracts any MissingEdgeUpdates embedded in batches.
    async fn extract_subdag(
        &self,
        leader_round: Round,
        certificates: &[Certificate],
    ) -> (CommittedSubdag, Vec<MissingEdgeUpdate>) {
        let mut vertices: Vec<CommittedVertex> = Vec::new();
        let mut edge_updates: Vec<MissingEdgeUpdate> = Vec::new();

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
                            Ok(WorkerMessage::Batch(batch_entries, compressed_update)) => {
                                for (tx_bytes, oi) in batch_entries {
                                    let tx_id = extract_tx_digest(&tx_bytes);
                                    ordering_entries.push((tx_id, oi));
                                }

                                // Extract lz4-compressed MissingEdgeUpdates.
                                if let Some(compressed) = compressed_update {
                                    match lz4_flex::decompress_size_prepended(&compressed) {
                                        Ok(bytes) => {
                                            match bincode::deserialize::<Vec<MissingEdgeUpdate>>(
                                                &bytes,
                                            ) {
                                                Ok(updates) => {
                                                    info!(
                                                        "FairDAG: extracted {} missing edge \
                                                         updates from batch",
                                                        updates.len()
                                                    );
                                                    edge_updates.extend(updates);
                                                }
                                                Err(e) => {
                                                    error!(
                                                        "Failed to deserialize \
                                                         MissingEdgeUpdate list: {}",
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to decompress MissingEdgeUpdate: {}",
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                            Ok(_) => {
                                warn!(
                                    "Unexpected message type for batch {:?}",
                                    batch_digest
                                );
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
                "FAIRDAG_TIMING: extract cert round={} replica={} batches \
                 found={} missing={} entries={}",
                cert.round(), replica_index, batches_found, batches_missing,
                ordering_entries.len()
            );

            vertices.push(CommittedVertex {
                replica: author,
                replica_index,
                round: cert.round(),
                ordering_entries,
            });
        }

        vertices.sort_by_key(|v| v.round);

        (
            CommittedSubdag {
                leader_round,
                vertices,
            },
            edge_updates,
        )
    }
}