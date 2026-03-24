// FairDAG-RL v5: FairDagProcessor with explicit missing-edge updates
// and parallel graph construction support.
//
// Key changes from v4:
//   1. After graph construction, if there are missing edges, sends a
//      MissingEdgeRequest to the BatchMaker via tx_missing_edge channel.
//   2. Receives MissingEdgeContributions from other replicas via
//      rx_edge_contributions channel.
//   3. When n-f contributions are collected for a parked graph, runs
//      FairUpdate to resolve missing edges explicitly.
//   4. Subdag extraction is parallelized via tokio::spawn — the Store
//      is Arc-wrapped so cloning is cheap.
//   5. No implicit weight updates — graphs wait for explicit contributions.

use crate::batch_maker::{MissingEdgeContribution, MissingEdgeRequest};
use crate::local_order_tracker::{extract_tx_digest, TxDigest};
use crate::worker::WorkerMessage;
use config::Committee;
use crypto::PublicKey;
use fairdag_fairness::{CommittedSubdag, CommittedVertex, FairnessLayer, Round};
use log::{debug, error, info, warn};
use primary::Certificate;
use std::collections::HashMap;
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Tracks accumulated edge contributions for a single parked graph.
struct ParkedGraph {
    graph_round: Round,
    /// Number of distinct replica contributions received so far.
    contribution_count: usize,
    /// All contributions received, keyed by source.
    /// We use the oi_entries directly since we don't have replica ID in the
    /// contribution — we deduplicate by identical graph_round + content hash.
    contributions: Vec<MissingEdgeContribution>,
    /// Set of contribution hashes to avoid duplicates from the same replica.
    seen_hashes: std::collections::HashSet<u64>,
}

impl ParkedGraph {
    fn new(round: Round) -> Self {
        ParkedGraph {
            graph_round: round,
            contribution_count: 0,
            contributions: Vec::new(),
            seen_hashes: std::collections::HashSet::new(),
        }
    }

    /// Add a contribution. Returns true if this is a new (non-duplicate) contribution.
    fn add_contribution(&mut self, contrib: MissingEdgeContribution) -> bool {
        // Simple dedup: hash the OI entries to detect duplicates.
        let mut hash: u64 = 0;
        for (d, oi) in &contrib.oi_entries {
            hash = hash.wrapping_mul(6364136223846793005).wrapping_add(*d ^ *oi);
        }
        if self.seen_hashes.insert(hash) {
            self.contribution_count += 1;
            self.contributions.push(contrib);
            true
        } else {
            false
        }
    }
}

pub struct FairDagProcessor {
    store: Store,
    fairness_layer: FairnessLayer,
    sorted_keys: Vec<PublicKey>,
    rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,

    // =========================================================================
    // Explicit edge update channels and state
    // =========================================================================
    /// Send missing edge requests to BatchMaker.
    tx_missing_edge: Sender<MissingEdgeRequest>,
    /// Receive edge contributions from other workers (via WorkerReceiverHandler).
    rx_edge_contributions: Receiver<Vec<MissingEdgeContribution>>,
    /// Graphs waiting for n-f contributions before FairUpdate can run.
    parked_graphs: HashMap<Round, ParkedGraph>,
    /// Required number of contributions: n - f.
    required_contributions: usize,
}

impl FairDagProcessor {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
        fault_threshold: u64,
        tx_missing_edge: Sender<MissingEdgeRequest>,
        rx_edge_contributions: Receiver<Vec<MissingEdgeContribution>>,
    ) {
        let n = committee.size();
        let f = fault_threshold as usize;

        let mut sorted_keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        sorted_keys.sort();

        let gamma = committee.get_gamma().unwrap();

        let fairness_layer = FairnessLayer::new(sorted_keys.clone(), f, gamma);

        let required_contributions = n - f;

        let handle = tokio::spawn(async move {
            Self {
                store,
                fairness_layer,
                sorted_keys,
                rx_committed_subdags,
                tx_missing_edge,
                rx_edge_contributions,
                parked_graphs: HashMap::new(),
                required_contributions,
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
                // ─────────────────────────────────────────────────────────────
                // Branch 1: Process committed subdags.
                // ─────────────────────────────────────────────────────────────
                Some((leader_round, certificates)) = self.rx_committed_subdags.recv() => {
                    let batch_start = Instant::now();

                    // Drain all queued subdags into a batch.
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

                    // ─────────────────────────────────────────────────────────
                    // Step 1: Extract all subdags (parallel via tokio::spawn).
                    //
                    // The Store is Arc-wrapped internally, so cloning it is
                    // just an Arc increment — no data copy.
                    // ─────────────────────────────────────────────────────────
                    let extract_start = Instant::now();

                    let mut extraction_handles = Vec::with_capacity(batch_size);

                    for (round, certs) in batch_raw {
                        subdag_count += 1;
                        let store = self.store.clone(); // cheap Arc clone
                        let sorted_keys = self.sorted_keys.clone();

                        let handle = tokio::spawn(async move {
                            Self::extract_subdag_static(store, sorted_keys, round, &certs).await
                        });
                        extraction_handles.push(handle);
                    }

                    // Join all extraction tasks (preserving order).
                    let mut subdags: Vec<CommittedSubdag> = Vec::with_capacity(batch_size);
                    for handle in extraction_handles {
                        match handle.await {
                            Ok(subdag) => subdags.push(subdag),
                            Err(e) => {
                                error!("FATAL: subdag extraction task panicked: {:?}", e);
                                std::process::abort();
                            }
                        }
                    }

                    let extract_ms = extract_start.elapsed().as_millis();

                    // ─────────────────────────────────────────────────────────
                    // Step 2: Process all subdags through the fairness layer.
                    //
                    // Each graph is independent (no readd). After processing,
                    // check for missing edges and send MissingEdgeRequests.
                    // ─────────────────────────────────────────────────────────
                    let process_start = Instant::now();
                    let mut total_fair_ordered: usize = 0;

                    for subdag in &subdags {
                        let sd_process_start = Instant::now();
                        let (fair_ordered, missing_request) =
                            self.fairness_layer.process_subdag_explicit(subdag);
                        let sd_process_ms = sd_process_start.elapsed().as_millis();

                        info!(
                            "FAIRDAG_TIMING: subdag process done: {}ms, fair_ordered={} \
                             has_missing_edge_request={}",
                            sd_process_ms,
                            fair_ordered.len(),
                            missing_request.is_some(),
                        );

                        if !fair_ordered.is_empty() {
                            info!(
                                "FairDAG: outputting {} fair-ordered transactions from leader round {}",
                                fair_ordered.len(),
                                subdag.leader_round
                            );
                            for tx_id in &fair_ordered {
                                info!("FairDAG-RL ordered transaction: {}", tx_id);
                            }
                            total_fair_ordered += fair_ordered.len();
                        }

                        // If this graph has missing edges, send request to BatchMaker
                        // and park the graph for explicit updates.
                        if let Some((graph_round, needed_digests)) = missing_request {
                            info!(
                                "FairDAG: graph round {} has {} missing-edge txs — sending request to BatchMaker",
                                graph_round,
                                needed_digests.len()
                            );

                            let request = MissingEdgeRequest {
                                graph_round,
                                needed_tx_digests: needed_digests,
                            };

                            if let Err(e) = self.tx_missing_edge.send(request).await {
                                error!("Failed to send MissingEdgeRequest: {}", e);
                            }

                            self.parked_graphs
                                .entry(graph_round)
                                .or_insert_with(|| ParkedGraph::new(graph_round));
                        }
                    }

                    let process_ms = process_start.elapsed().as_millis();

                    // ─────────────────────────────────────────────────────────
                    // Step 3: Also drain any edge contributions that arrived
                    //         while we were processing.
                    // ─────────────────────────────────────────────────────────
                    self.drain_and_apply_contributions().await;

                    let total_ms = batch_start.elapsed().as_millis();

                    info!(
                        "FAIRDAG_TIMING: batch #{} done: batch_size={} extract={}ms process={}ms \
                         total={}ms fair_ordered={} parked_graphs={}",
                        batch_count, batch_size, extract_ms, process_ms, total_ms,
                        total_fair_ordered, self.parked_graphs.len(),
                    );
                },

                // ─────────────────────────────────────────────────────────────
                // Branch 2: Receive edge contributions from other replicas.
                // ─────────────────────────────────────────────────────────────
                Some(contributions) = self.rx_edge_contributions.recv() => {
                    for contrib in contributions {
                        self.apply_single_contribution(contrib);
                    }

                    // Check if any parked graph now has enough contributions.
                    self.try_resolve_parked_graphs();
                },
            }
        }
    }

    /// Drain all pending edge contributions from the channel and apply them.
    async fn drain_and_apply_contributions(&mut self) {
        while let Ok(contributions) = self.rx_edge_contributions.try_recv() {
            for contrib in contributions {
                self.apply_single_contribution(contrib);
            }
        }
        self.try_resolve_parked_graphs();
    }

    /// Apply a single edge contribution to the appropriate parked graph.
    fn apply_single_contribution(&mut self, contrib: MissingEdgeContribution) {
        let round = contrib.graph_round;

        if let Some(parked) = self.parked_graphs.get_mut(&round) {
            let is_new = parked.add_contribution(contrib);
            if is_new {
                debug!(
                    "FairDAG: received edge contribution for graph round {} ({}/{} needed)",
                    round, parked.contribution_count, self.required_contributions,
                );
            }
        } else {
            debug!(
                "FairDAG: received edge contribution for unknown/resolved graph round {} — ignoring",
                round
            );
        }
    }

    /// Check all parked graphs: if any has n-f contributions, run FairUpdate.
    fn try_resolve_parked_graphs(&mut self) {
        let mut resolved_rounds: Vec<Round> = Vec::new();

        for (round, parked) in &self.parked_graphs {
            if parked.contribution_count >= self.required_contributions {
                info!(
                    "FairDAG: graph round {} has {}/{} contributions — running FairUpdate",
                    round, parked.contribution_count, self.required_contributions
                );
                resolved_rounds.push(*round);
            }
        }

        for round in resolved_rounds {
            if let Some(parked) = self.parked_graphs.remove(&round) {
                let resolve_start = Instant::now();

                // Collect all OI entries from contributions into the format
                // expected by the fairness layer.
                let all_oi_sets: Vec<Vec<(TxDigest, u64)>> = parked
                    .contributions
                    .into_iter()
                    .map(|c| c.oi_entries)
                    .collect();

                let fair_ordered = self
                    .fairness_layer
                    .apply_fair_update(round, &all_oi_sets);

                let resolve_ms = resolve_start.elapsed().as_millis();

                info!(
                    "FairDAG: FairUpdate for graph round {} completed in {}ms — {} transactions ordered",
                    round, resolve_ms, fair_ordered.len()
                );

                for tx_id in &fair_ordered {
                    info!("FairDAG-RL ordered transaction (via FairUpdate): {}", tx_id);
                }
            }
        }
    }

    /// Static version of extract_subdag that can be called in a spawned task.
    /// Takes owned Store (cheap clone) and sorted_keys.
    async fn extract_subdag_static(
        store: Store,
        sorted_keys: Vec<PublicKey>,
        leader_round: Round,
        certificates: &[Certificate],
    ) -> CommittedSubdag {
        let mut vertices: Vec<CommittedVertex> = Vec::new();

        for cert in certificates {
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
                            Ok(WorkerMessage::Batch(batch_entries, _compressed)) => {
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