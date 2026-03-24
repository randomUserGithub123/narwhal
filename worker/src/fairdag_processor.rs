// FairDAG-RL v5: Explicit missing-edge update processor.
//
// Architecture:
//   1. Receive committed subdags from primary.
//   2. Extract batches from store in parallel (I/O-bound, tokio tasks).
//   3. Run FairPropose: build dependency graph, identify missing edges.
//   4. Park graphs with missing edges; send MissingEdgeRequests to BatchMaker.
//   5. Collect EdgeUpdatePayloads from committed batches (attributed per-replica).
//   6. When n-f update sets collected for a parked graph, run FairUpdate.
//   7. FairFinalize: sequentially finalize graphs that are tournaments.
//
// Parallel construction: subdag extraction is spawned as independent tokio
// tasks. Multiple parked graphs are tracked concurrently. Finalization
// remains sequential (protocol requirement: round order).

use crate::batch_maker::MissingEdgeRequest;
use crate::local_order_tracker::extract_tx_digest;
use crate::worker::{EdgeUpdatePayload, SealedBatch, WorkerMessage};
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
    /// Receives edge update payloads extracted from committed batches.
    rx_edge_updates: Receiver<(PublicKey, EdgeUpdatePayload)>,
    /// Sends missing-edge requests to the local BatchMaker.
    tx_missing_edges: Sender<MissingEdgeRequest>,

    // =========================================================================
    // Parked graph tracking
    // =========================================================================
    /// Graphs waiting for explicit edge updates before they can become tournaments.
    /// Key: leader_round of the graph.
    parked_graphs: HashMap<Round, ParkedGraphState>,
    /// Sequential finalization queue: rounds in order of creation.
    finalization_queue: Vec<Round>,
    /// Number of replicas.
    n: usize,
    /// Fault threshold.
    f: usize,
}

/// State for a parked graph waiting for edge update sets.
struct ParkedGraphState {
    /// The set of tx digests that have missing edges in this graph.
    missing_tx_digests: Vec<TxDigest>,
    /// Collected update sets: maps replica_index → orderings.
    /// Each replica contributes at most one update set.
    collected_updates: HashMap<usize, Vec<(TxDigest, u64)>>,
    /// Whether we have already sent the MissingEdgeRequest for this graph.
    request_sent: bool,
}

impl FairDagProcessor {
    pub fn spawn(
        mut committee: Committee,
        store: Store,
        rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
        rx_edge_updates: Receiver<(PublicKey, EdgeUpdatePayload)>,
        tx_missing_edges: Sender<MissingEdgeRequest>,
        fault_threshold: u64,
    ) {
        let n = committee.size();
        let f = fault_threshold as usize;

        let mut sorted_keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        sorted_keys.sort();

        let gamma = committee.get_gamma().unwrap();

        let fairness_layer = FairnessLayer::new(sorted_keys.clone(), f, gamma);

        // Spawn with panic propagation.
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
                // ─────────────────────────────────────────────────────────
                // Path A: New committed subdag from primary.
                // ─────────────────────────────────────────────────────────
                Some((leader_round, certificates)) = self.rx_committed_subdags.recv() => {
                    let batch_start = Instant::now();

                    // Drain all queued subdags into a batch for pipelined extraction.
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

                    // ─────────────────────────────────────────────────────
                    // Step 1: Extract all subdags in parallel (I/O-bound).
                    // Each extraction is spawned as an independent tokio task.
                    // ─────────────────────────────────────────────────────
                    let extract_start = Instant::now();
                    let store = self.store.clone();
                    let sorted_keys = Arc::new(self.sorted_keys.clone());

                    let mut extraction_handles = Vec::with_capacity(batch_size);

                    for (round, certs) in batch_raw {
                        let store_clone = store.clone();
                        let keys_clone = sorted_keys.clone();

                        let handle = tokio::spawn(async move {
                            Self::extract_subdag_parallel(
                                store_clone,
                                &keys_clone,
                                round,
                                certs,
                            )
                            .await
                        });
                        extraction_handles.push(handle);
                    }

                    // Await all extraction tasks.
                    let mut subdags: Vec<CommittedSubdag> = Vec::with_capacity(batch_size);
                    let mut extracted_edge_updates: Vec<Vec<(PublicKey, EdgeUpdatePayload)>> =
                        Vec::with_capacity(batch_size);

                    for handle in extraction_handles {
                        match handle.await {
                            Ok((subdag, edge_updates)) => {
                                subdags.push(subdag);
                                extracted_edge_updates.push(edge_updates);
                            }
                            Err(e) => {
                                error!("Subdag extraction task panicked: {:?}", e);
                            }
                        }
                    }

                    let extract_ms = extract_start.elapsed().as_millis();

                    // ─────────────────────────────────────────────────────
                    // Step 2: Apply edge updates extracted from these batches
                    // to any parked graphs.
                    // ─────────────────────────────────────────────────────
                    for updates_batch in &extracted_edge_updates {
                        for (author, update) in updates_batch {
                            self.apply_edge_update(author, update);
                        }
                    }

                    // ─────────────────────────────────────────────────────
                    // Step 3: Process each subdag through FairPropose.
                    //
                    // Ingest → catchup → identify missing edges.
                    // Graphs with missing edges are parked.
                    // ─────────────────────────────────────────────────────
                    let process_start = Instant::now();

                    for (i, subdag) in subdags.iter().enumerate() {
                        subdag_count += 1;
                        let sd_start = Instant::now();

                        // Phase 1+2: ingest + catchup (no implicit weight update)
                        let propose_result = self.fairness_layer.ingest_and_propose(subdag);

                        let sd_num = subdag_count;

                        info!(
                            "FAIRDAG_TIMING: subdag #{} propose done: {}ms, \
                             graph_nodes={} missing_pairs={}",
                            sd_num,
                            sd_start.elapsed().as_millis(),
                            propose_result.node_count,
                            propose_result.missing_pair_count,
                        );

                        if propose_result.missing_pair_count > 0 {
                            // Graph has missing edges → park it and request updates.
                            self.park_graph(
                                subdag.leader_round,
                                &propose_result.missing_tx_digests,
                            )
                            .await;
                        }

                        // Add to finalization queue.
                        self.finalization_queue.push(subdag.leader_round);
                    }

                    let process_ms = process_start.elapsed().as_millis();

                    // ─────────────────────────────────────────────────────
                    // Step 4: Try to finalize parked graphs that now have
                    // enough updates (n-f sets).
                    // ─────────────────────────────────────────────────────
                    self.try_apply_collected_updates();

                    // ─────────────────────────────────────────────────────
                    // Step 5: FairFinalize — sequential finalization.
                    // ─────────────────────────────────────────────────────
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

                // ─────────────────────────────────────────────────────────
                // Path B: Edge update payload from a committed batch
                // (forwarded by WorkerReceiverHandler for real-time updates).
                // ─────────────────────────────────────────────────────────
                Some((author, update)) = self.rx_edge_updates.recv() => {
                    self.apply_edge_update(&author, &update);

                    // Check if any parked graph now has enough updates.
                    self.try_apply_collected_updates();

                    // Try to finalize.
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
        let state = self
            .parked_graphs
            .entry(leader_round)
            .or_insert_with(|| ParkedGraphState {
                missing_tx_digests: missing_tx_digests.to_vec(),
                collected_updates: HashMap::new(),
                request_sent: false,
            });

        // Merge any new missing tx digests (if called again for the same round).
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
                leader_round,
                request.tx_digests.len()
            );

            if let Err(e) = self.tx_missing_edges.send(request).await {
                error!(
                    "Failed to send MissingEdgeRequest for round {}: {}",
                    leader_round, e
                );
            }

            // Mark as sent so we don't send duplicate requests.
            self.parked_graphs
                .get_mut(&leader_round)
                .unwrap()
                .request_sent = true;
        }
    }

    // =========================================================================
    // Apply a single edge update to a parked graph's collected updates
    // =========================================================================

    fn apply_edge_update(&mut self, author: &PublicKey, update: &EdgeUpdatePayload) {
        let replica_index = match self.sorted_keys.iter().position(|k| k == author) {
            Some(idx) => idx,
            None => {
                warn!(
                    "Edge update from unknown replica {:?} for round {}",
                    author, update.leader_round
                );
                return;
            }
        };

        let round = update.leader_round;

        if let Some(state) = self.parked_graphs.get_mut(&round) {
            // Only accept if we haven't already received from this replica.
            if state.collected_updates.contains_key(&replica_index) {
                return;
            }

            info!(
                "FairDAG: received edge update for round {} from replica {} ({} orderings)",
                round, replica_index, update.orderings.len()
            );

            state
                .collected_updates
                .insert(replica_index, update.orderings.clone());
        } else {
            // Graph not parked (yet) — it might be parked later, or already
            // finalized. Store for potential future use if we want, or just
            // log and drop.
            //
            // To be safe, create a pre-parked state so updates arriving
            // before the graph is parked are not lost.
            info!(
                "FairDAG: received edge update for round {} (not yet parked), \
                 storing for later",
                round
            );
            let mut state = ParkedGraphState {
                missing_tx_digests: Vec::new(),
                collected_updates: HashMap::new(),
                request_sent: false,
            };
            state
                .collected_updates
                .insert(replica_index, update.orderings.clone());
            self.parked_graphs.insert(round, state);
        }
    }

    // =========================================================================
    // Check if any parked graph has n-f update sets; if so, apply FairUpdate
    // =========================================================================

    fn try_apply_collected_updates(&mut self) {
        let threshold = self.n - self.f;
        let mut rounds_to_update: Vec<Round> = Vec::new();

        for (&round, state) in &self.parked_graphs {
            if state.collected_updates.len() >= threshold {
                rounds_to_update.push(round);
            }
        }

        for round in rounds_to_update {
            if let Some(state) = self.parked_graphs.get(&round) {
                info!(
                    "FairDAG: applying FairUpdate for round {} with {} update sets (threshold={})",
                    round,
                    state.collected_updates.len(),
                    threshold
                );

                // Collect the update orderings to pass to the fairness layer.
                let update_sets: Vec<(usize, &[(TxDigest, u64)])> = state
                    .collected_updates
                    .iter()
                    .map(|(&ri, ords)| (ri, ords.as_slice()))
                    .collect();

                // Apply FairUpdate through the fairness layer.
                self.fairness_layer
                    .apply_explicit_edge_updates(round, &update_sets);
            }

            // Remove from parked — updates have been applied.
            // The graph may still not be a tournament if more pairs remain,
            // but re-requests will happen on the next processing cycle.
            self.parked_graphs.remove(&round);
        }
    }

    // =========================================================================
    // FairFinalize: sequential finalization in round order
    // =========================================================================

    fn fair_finalize(&mut self) -> Vec<TxDigest> {
        let mut all_ordered: Vec<TxDigest> = Vec::new();

        // Process finalization queue in order.
        // We must finalize sequentially: graph at round X can only finalize
        // if all graphs at rounds < X are already finalized.
        let mut new_queue: Vec<Round> = Vec::new();

        for &round in &self.finalization_queue {
            if self.fairness_layer.is_graph_finalized(round) {
                // Already finalized — skip.
                continue;
            }

            if self.fairness_layer.is_graph_tournament(round) {
                // Graph is a tournament — finalize it.
                let ordered = self.fairness_layer.finalize_graph(round);
                if !ordered.is_empty() {
                    info!(
                        "FairDAG: finalized {} transactions from round {}",
                        ordered.len(),
                        round
                    );
                    all_ordered.extend(ordered);
                }
            } else {
                // Not a tournament yet — keep in queue and stop.
                // Sequential constraint: can't finalize later rounds.
                new_queue.push(round);
                break;
            }
        }

        // Keep remaining rounds in queue (those after the first non-tournament).
        let stop_idx = self.finalization_queue.len() - new_queue.len();
        if !new_queue.is_empty() {
            // Extend with everything after the blocked round.
            let remaining_start = self
                .finalization_queue
                .iter()
                .position(|&r| r == new_queue[0])
                .unwrap_or(stop_idx);
            new_queue = self.finalization_queue[remaining_start..].to_vec();
        }
        self.finalization_queue = new_queue;

        all_ordered
    }

    // =========================================================================
    // Parallel subdag extraction (static method, spawned as tokio task)
    // =========================================================================

    async fn extract_subdag_parallel(
        store: Store,
        sorted_keys: &[PublicKey],
        leader_round: Round,
        certificates: Vec<Certificate>,
    ) -> (CommittedSubdag, Vec<(PublicKey, EdgeUpdatePayload)>) {
        let mut vertices: Vec<CommittedVertex> = Vec::new();
        let mut edge_updates: Vec<(PublicKey, EdgeUpdatePayload)> = Vec::new();

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
                            Ok(WorkerMessage::Batch(sealed_batch)) => {
                                // Extract regular tx entries.
                                for (tx_bytes, oi) in &sealed_batch.entries {
                                    let tx_id = extract_tx_digest(tx_bytes);
                                    ordering_entries.push((tx_id, oi.clone()));
                                }

                                // Extract edge update payloads (attributed to
                                // the certificate author = the replica that
                                // created this batch).
                                for update in sealed_batch.edge_updates {
                                    edge_updates.push((author, update));
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
                "FAIRDAG_TIMING: extract cert round={} replica={} batches found={} \
                 missing={} entries={}",
                cert.round(),
                replica_index,
                batches_found,
                batches_missing,
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

        let subdag = CommittedSubdag {
            leader_round,
            vertices,
        };

        (subdag, edge_updates)
    }
}