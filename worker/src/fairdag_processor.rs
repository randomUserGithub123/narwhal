// FairDAG-RL v6: Explicit missing-edge update processor.
//
// Architecture:
//   1. Receive committed subdags from primary.
//   2. Extract batches from store in parallel (tokio::spawn per subdag).
//   3. Decompress lz4-compressed edge updates from committed batches.
//   4. Run FairPropose: build graph, identify missing edges.
//   5. Park graphs with missing edges; send MissingEdgeRequests to BatchMaker.
//   6. Collect MissingEdgeUpdate payloads (attributed per certificate author).
//   7. When n-f update sets collected for a parked graph, run FairUpdate.
//   8. FairFinalize: sequentially finalize graphs that are tournaments.
//
// No re-add: nodes after last solid in a finalized graph are discarded.

use crate::batch_maker::MissingEdgeRequest;
use crate::local_order_tracker::extract_tx_digest;
use crate::worker::{decompress_edge_updates, MissingEdgeUpdate, WorkerMessage};
use config::Committee;
use crypto::PublicKey;
use fairdag_fairness::{
    CommittedSubdag, CommittedVertex, FairnessLayer, Round, TxDigest,
};
use log::{error, info, warn};
use primary::Certificate;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct FairDagProcessor {
    store: Store,
    fairness_layer: FairnessLayer,
    sorted_keys: Vec<PublicKey>,
    rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
    rx_edge_updates: Receiver<(PublicKey, MissingEdgeUpdate)>,
    tx_missing_edges: Sender<MissingEdgeRequest>,

    /// Graphs waiting for n-f edge update sets.
    parked_graphs: HashMap<Round, ParkedGraphState>,
    /// Sequential finalization queue: rounds in creation order.
    finalization_queue: Vec<Round>,
    n: usize,
    f: usize,
}

struct ParkedGraphState {
    /// Tx digests involved in missing edges.
    missing_tx_digests: Vec<TxDigest>,
    /// Collected update sets: replica_index → orderings.
    collected_updates: HashMap<usize, Vec<(TxDigest, u64)>>,
    /// Whether MissingEdgeRequest has been sent.
    request_sent: bool,
}

impl FairDagProcessor {
    pub fn spawn(
        mut committee: Committee,
        store: Store,
        rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
        rx_edge_updates: Receiver<(PublicKey, MissingEdgeUpdate)>,
        tx_missing_edges: Sender<MissingEdgeRequest>,
        fault_threshold: u64,
    ) {
        let n = committee.size();
        let f = fault_threshold as usize;

        let mut sorted_keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        sorted_keys.sort();

        let gamma = committee.get_gamma().unwrap();
        let fairness_layer = FairnessLayer::new(sorted_keys.clone(), f, gamma);

        let handle = tokio::spawn(async move {
            Self {
                store,
                fairness_layer,
                sorted_keys,
                rx_committed_subdags,
                rx_edge_updates,
                tx_missing_edges,
                parked_graphs: HashMap::new(),
                finalization_queue: Vec::new(),
                n,
                f,
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

        loop {
            tokio::select! {
                // ─────────────────────────────────────────────────────
                // Path A: New committed subdag(s) from primary.
                // ─────────────────────────────────────────────────────
                Some((leader_round, certificates)) = self.rx_committed_subdags.recv() => {
                    let batch_start = Instant::now();

                    // Drain all queued subdags.
                    let mut batch_raw: Vec<(Round, Vec<Certificate>)> =
                        vec![(leader_round, certificates)];
                    while let Ok((r, c)) = self.rx_committed_subdags.try_recv() {
                        batch_raw.push((r, c));
                    }

                    let batch_size = batch_raw.len();
                    batch_count += 1;

                    info!(
                        "FAIRDAG_TIMING: batch #{} draining: {} subdags queued",
                        batch_count, batch_size
                    );

                    // ─────────────────────────────────────────────────
                    // Step 1: Parallel extraction (I/O-bound).
                    // ─────────────────────────────────────────────────
                    let extract_start = Instant::now();
                    let store_ref = self.store.clone();
                    let sorted_keys = Arc::new(self.sorted_keys.clone());

                    let mut handles = Vec::with_capacity(batch_size);
                    for (round, certs) in batch_raw {
                        let s = store_ref.clone();
                        let k = sorted_keys.clone();
                        handles.push(tokio::spawn(async move {
                            Self::extract_subdag_parallel(s, &k, round, certs).await
                        }));
                    }

                    let mut subdags: Vec<CommittedSubdag> = Vec::with_capacity(batch_size);
                    let mut all_extracted_updates: Vec<Vec<(PublicKey, MissingEdgeUpdate)>> =
                        Vec::with_capacity(batch_size);

                    for h in handles {
                        match h.await {
                            Ok((subdag, updates)) => {
                                subdags.push(subdag);
                                all_extracted_updates.push(updates);
                            }
                            Err(e) => {
                                error!("Subdag extraction task panicked: {:?}", e);
                            }
                        }
                    }
                    let extract_ms = extract_start.elapsed().as_millis();

                    // ─────────────────────────────────────────────────
                    // Step 2: Apply extracted edge updates to parked graphs.
                    // ─────────────────────────────────────────────────
                    for updates_batch in &all_extracted_updates {
                        for (author, update) in updates_batch {
                            self.apply_edge_update(author, update);
                        }
                    }

                    // ─────────────────────────────────────────────────
                    // Step 3: FairPropose for each subdag.
                    // ─────────────────────────────────────────────────
                    let process_start = Instant::now();

                    for subdag in &subdags {
                        subdag_count += 1;
                        let sd_start = Instant::now();

                        let propose_result = self.fairness_layer.ingest_and_propose(subdag);

                        info!(
                            "FAIRDAG_TIMING: subdag #{} propose done: {}ms, \
                             graph_nodes={} missing_pairs={}",
                            subdag_count, sd_start.elapsed().as_millis(),
                            propose_result.node_count, propose_result.missing_pair_count,
                        );

                        if propose_result.missing_pair_count > 0 {
                            self.park_graph(
                                subdag.leader_round,
                                &propose_result.missing_tx_digests,
                            ).await;
                        }

                        self.finalization_queue.push(subdag.leader_round);
                    }
                    let process_ms = process_start.elapsed().as_millis();

                    // ─────────────────────────────────────────────────
                    // Step 4: Check if any parked graph has n-f updates.
                    // ─────────────────────────────────────────────────
                    self.try_apply_collected_updates();

                    // ─────────────────────────────────────────────────
                    // Step 5: FairFinalize — sequential.
                    // ─────────────────────────────────────────────────
                    let finalize_start = Instant::now();
                    let fair_ordered = self.fair_finalize();
                    let finalize_ms = finalize_start.elapsed().as_millis();

                    let total_ms = batch_start.elapsed().as_millis();

                    info!(
                        "FAIRDAG_TIMING: batch #{} done: batch_size={} extract={}ms \
                         process={}ms finalize={}ms total={}ms fair_ordered={} \
                         parked_graphs={}",
                        batch_count, batch_size, extract_ms, process_ms,
                        finalize_ms, total_ms, fair_ordered.len(),
                        self.parked_graphs.len(),
                    );

                    if !fair_ordered.is_empty() {
                        for tx_id in &fair_ordered {
                            info!("FairDAG-RL ordered transaction: {}", tx_id);
                        }
                    }
                },

                // ─────────────────────────────────────────────────────
                // Path B: Real-time edge update from a committed batch.
                // ─────────────────────────────────────────────────────
                Some((author, update)) = self.rx_edge_updates.recv() => {
                    self.apply_edge_update(&author, &update);
                    self.try_apply_collected_updates();
                    let fair_ordered = self.fair_finalize();
                    if !fair_ordered.is_empty() {
                        for tx_id in &fair_ordered {
                            info!("FairDAG-RL ordered transaction: {}", tx_id);
                        }
                    }
                },
            }
        }
    }

    // =========================================================================
    // Park a graph and send MissingEdgeRequest
    // =========================================================================

    async fn park_graph(&mut self, leader_round: Round, missing_tx_digests: &[TxDigest]) {
        let state = self.parked_graphs.entry(leader_round).or_insert_with(|| {
            ParkedGraphState {
                missing_tx_digests: Vec::new(),
                collected_updates: HashMap::new(),
                request_sent: false,
            }
        });

        // Merge missing digests.
        let existing: HashSet<TxDigest> = state.missing_tx_digests.iter().copied().collect();
        for &d in missing_tx_digests {
            if !existing.contains(&d) {
                state.missing_tx_digests.push(d);
            }
        }

        if !state.request_sent && !state.missing_tx_digests.is_empty() {
            let request = MissingEdgeRequest {
                leader_round,
                tx_digests: state.missing_tx_digests.clone(),
            };

            info!(
                "FairDAG: sending MissingEdgeRequest for round {} with {} tx digests",
                leader_round, request.tx_digests.len()
            );

            if let Err(e) = self.tx_missing_edges.send(request).await {
                error!("Failed to send MissingEdgeRequest for round {}: {}", leader_round, e);
            }

            self.parked_graphs.get_mut(&leader_round).unwrap().request_sent = true;
        }
    }

    // =========================================================================
    // Apply a single edge update to parked graph state
    // =========================================================================

    fn apply_edge_update(&mut self, author: &PublicKey, update: &MissingEdgeUpdate) {
        let replica_index = match self.sorted_keys.iter().position(|k| k == author) {
            Some(idx) => idx,
            None => {
                warn!("Edge update from unknown replica for round {}", update.leader_round);
                return;
            }
        };

        let round = update.leader_round;

        let state = self.parked_graphs.entry(round).or_insert_with(|| {
            // Pre-park: update arrived before graph was parked.
            ParkedGraphState {
                missing_tx_digests: Vec::new(),
                collected_updates: HashMap::new(),
                request_sent: false,
            }
        });

        if state.collected_updates.contains_key(&replica_index) {
            return; // Already have this replica's update.
        }

        info!(
            "FairDAG: collected edge update for round {} from replica {} ({} orderings)",
            round, replica_index, update.orderings.len()
        );

        state.collected_updates.insert(replica_index, update.orderings.clone());
    }

    // =========================================================================
    // Check if any parked graph has n-f updates → apply FairUpdate
    // =========================================================================

    fn try_apply_collected_updates(&mut self) {
        let threshold = self.n - self.f;
        let mut rounds_ready: Vec<Round> = Vec::new();

        for (&round, state) in &self.parked_graphs {
            if state.collected_updates.len() >= threshold {
                rounds_ready.push(round);
            }
        }

        for round in rounds_ready {
            if let Some(state) = self.parked_graphs.get(&round) {
                info!(
                    "FairDAG: applying FairUpdate for round {} with {} update sets (threshold={})",
                    round, state.collected_updates.len(), threshold
                );

                let update_sets: Vec<(usize, &[(TxDigest, u64)])> = state
                    .collected_updates
                    .iter()
                    .map(|(&ri, ords)| (ri, ords.as_slice()))
                    .collect();

                self.fairness_layer.apply_explicit_edge_updates(round, &update_sets);
            }

            self.parked_graphs.remove(&round);
        }
    }

    // =========================================================================
    // FairFinalize: sequential in round order
    // =========================================================================

    fn fair_finalize(&mut self) -> Vec<TxDigest> {
        let mut all_ordered: Vec<TxDigest> = Vec::new();
        let mut new_queue: Vec<Round> = Vec::new();

        for &round in &self.finalization_queue {
            if self.fairness_layer.is_graph_finalized(round) {
                continue; // Already done.
            }

            if self.fairness_layer.is_graph_tournament(round) {
                let ordered = self.fairness_layer.finalize_graph(round);
                if !ordered.is_empty() {
                    info!(
                        "FairDAG: finalized {} transactions from round {}",
                        ordered.len(), round
                    );
                    all_ordered.extend(ordered);
                }
            } else {
                // Not a tournament — stop. Sequential constraint.
                new_queue.push(round);
                // Add all remaining rounds after this one.
                let idx = self.finalization_queue.iter()
                    .position(|&r| r == round)
                    .unwrap();
                new_queue.extend_from_slice(&self.finalization_queue[idx + 1..]);
                break;
            }
        }

        self.finalization_queue = new_queue;
        all_ordered
    }

    // =========================================================================
    // Parallel extraction: decompress lz4 edge updates from committed batches
    // =========================================================================

    async fn extract_subdag_parallel(
        store: Store,
        sorted_keys: &[PublicKey],
        leader_round: Round,
        certificates: Vec<Certificate>,
    ) -> (CommittedSubdag, Vec<(PublicKey, MissingEdgeUpdate)>) {
        let mut vertices: Vec<CommittedVertex> = Vec::new();
        let mut edge_updates: Vec<(PublicKey, MissingEdgeUpdate)> = Vec::new();

        for cert in &certificates {
            let author = cert.origin();
            let replica_index = sorted_keys
                .iter()
                .position(|k| *k == author)
                .expect("Certificate author not in committee");

            let mut ordering_entries: Vec<(TxDigest, u64)> = Vec::new();
            let mut batches_found = 0usize;
            let mut batches_missing = 0usize;

            for batch_digest in cert.header.payload.keys() {
                match store.clone().read(batch_digest.to_vec()).await {
                    Ok(Some(serialized_batch)) => {
                        batches_found += 1;
                        match bincode::deserialize::<WorkerMessage>(&serialized_batch) {
                            Ok(WorkerMessage::Batch(batch_entries, compressed_updates)) => {
                                // Extract regular tx entries.
                                for (tx_bytes, oi) in &batch_entries {
                                    let tx_id = extract_tx_digest(tx_bytes);
                                    ordering_entries.push((tx_id, *oi));
                                }

                                // Decompress lz4 edge updates.
                                let updates = decompress_edge_updates(&compressed_updates);
                                for update in updates {
                                    // Attributed to the certificate author.
                                    edge_updates.push((author, update));
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
                "FAIRDAG_TIMING: extract cert round={} replica={} batches found={} \
                 missing={} entries={} edge_updates={}",
                cert.round(), replica_index, batches_found,
                batches_missing, ordering_entries.len(), edge_updates.len()
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
            CommittedSubdag { leader_round, vertices },
            edge_updates,
        )
    }
}