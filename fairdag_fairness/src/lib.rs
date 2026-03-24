// Copyright(C) FairDAG-RL Implementation — v5
//
// Implements the Fairness Layer with Themis-style explicit edge updates.
//
// Key changes from v4 (implicit missing-edge resolution):
//
//   1. NO IMPLICIT WEIGHT UPDATES: `update_weights_and_edges()` is removed.
//      Instead, missing edges are resolved via explicit `apply_fair_update()`
//      when n-f contributions arrive from other replicas.
//
//   2. NO READD: Each graph is fully independent. When a graph is finalized,
//      everything after the last solid SCC is DISCARDED — no shaded/blank
//      nodes are re-added to later graphs. This enables parallel graph
//      construction.
//
//   3. NEW PUBLIC API:
//      - `process_subdag_explicit()` → (ordered_txs, Option<missing_request>)
//        Processes a subdag and returns both the ordered txs (if graph is a
//        tournament) and an optional missing-edge request (if not).
//      - `apply_fair_update(graph_round, contributions)` → ordered_txs
//        Applies explicit edge contributions from n-f replicas to resolve
//        missing edges in a parked graph, following Themis's FairUpdate.
//
//   4. PARALLEL-READY: Since graphs are independent, the ingest phase can
//      be parallelized. The `process_subdag_explicit` method is designed to
//      be called sequentially (it mutates shared node state), but the
//      extraction phase (in FairDagProcessor) runs in parallel tokio tasks.
//
// Retained from v4: dense u32 indices, free-list recycling, nibble-packed
// weights, u32 counted masks (N ≤ 32), Tarjan SCC, Hamiltonian path.

use crypto::PublicKey;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

// =============================================================================
// Types
// =============================================================================

pub type TxDigest = u64;
pub type Round = u64;
pub type ReplicaIndex = usize;
pub type OrderingEntry = (TxDigest, u64);

// =============================================================================
// Constants
// =============================================================================

const NONE_LOCAL: u16 = u16::MAX;
const INITIAL_GRAPH_CAPACITY: usize = 16_384;

// =============================================================================
// CommittedVertex / CommittedSubdag
// =============================================================================

#[derive(Clone, Debug)]
pub struct CommittedVertex {
    pub replica: PublicKey,
    pub replica_index: ReplicaIndex,
    pub round: Round,
    pub ordering_entries: Vec<(TxDigest, u64)>,
}

#[derive(Clone, Debug)]
pub struct CommittedSubdag {
    pub leader_round: Round,
    pub vertices: Vec<CommittedVertex>,
}

// =============================================================================
// Node types
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeType {
    Blank,
    Shaded,
    Solid,
}

// =============================================================================
// Nibble-packed weight helpers
// =============================================================================

#[inline(always)]
fn get_weight(packed: &[u8], idx: usize) -> u8 {
    let b = packed[idx >> 1];
    if idx & 1 == 0 { b & 0x0F } else { b >> 4 }
}

#[inline(always)]
fn set_weight(packed: &mut [u8], idx: usize, value: u8) {
    let bi = idx >> 1;
    if idx & 1 == 0 {
        packed[bi] = (packed[bi] & 0xF0) | (value & 0x0F);
    } else {
        packed[bi] = (packed[bi] & 0x0F) | (value << 4);
    }
}

#[inline(always)]
fn inc_weight(packed: &mut [u8], idx: usize) {
    let bi = idx >> 1;
    if idx & 1 == 0 {
        let low = packed[bi] & 0x0F;
        if low < 15 { packed[bi] += 1; }
    } else {
        let high = packed[bi] >> 4;
        if high < 15 { packed[bi] += 0x10; }
    }
}

// =============================================================================
// Bitset helpers
// =============================================================================

#[inline(always)]
fn bit_get(bits: &[u64], idx: usize) -> bool {
    bits[idx >> 6] & (1u64 << (idx & 63)) != 0
}

#[inline(always)]
fn bit_set(bits: &mut [u64], idx: usize) {
    bits[idx >> 6] |= 1u64 << (idx & 63);
}

// =============================================================================
// Index helpers
// =============================================================================

#[inline(always)]
fn w_idx(i: u16, j: u16, cap: usize) -> usize {
    (i as usize) * cap + (j as usize)
}

#[inline(always)]
fn pair_idx(i: u16, j: u16, cap: usize) -> usize {
    let (a, b) = if i < j { (i, j) } else { (j, i) };
    (a as usize) * cap + (b as usize)
}

// =============================================================================
// TransactionNode
// =============================================================================

#[derive(Clone, Debug)]
pub struct TransactionNode {
    pub digest: TxDigest,
    pub dense_idx: u32,
    pub node_type: NodeType,
    pub committed_ois: Vec<Option<u64>>,
    pub committed_rounds: Vec<Option<Round>>,
    pub graph_index: Option<usize>,
}

impl TransactionNode {
    fn new(digest: TxDigest, dense_idx: u32, n: usize) -> Self {
        TransactionNode {
            digest,
            dense_idx,
            node_type: NodeType::Blank,
            committed_ois: vec![None; n],
            committed_rounds: vec![None; n],
            graph_index: None,
        }
    }

    #[inline]
    fn appearance_count(&self, up_to_round: Round) -> usize {
        self.committed_rounds
            .iter()
            .filter(|r| matches!(r, Some(r) if *r <= up_to_round))
            .count()
    }
}

// =============================================================================
// DependencyGraph
// =============================================================================

pub struct DependencyGraph {
    pub round: Round,
    pub node_count: usize,
    pub capacity: usize,
    pub local_to_global: Vec<u32>,
    pub global_to_local: Vec<u16>,
    pub weight: Vec<u8>,
    pub edges: Vec<Vec<u16>>,
    pub edge_pair_count: usize,
    pub has_edge_pair: Vec<u64>,
    pub counted: Vec<u32>,
    pub missing_pairs: Vec<(u16, u16)>,
    pub finalized: bool,
    pub final_order: Vec<TxDigest>,
    /// v5: Whether we have already sent a MissingEdgeRequest for this graph.
    pub missing_edge_request_sent: bool,
}

impl DependencyGraph {
    fn new(round: Round, capacity: usize) -> Self {
        let cap = capacity;
        let nibble_bytes = (cap * cap + 1) / 2;
        let bit_words = (cap * cap + 63) / 64;
        DependencyGraph {
            round,
            node_count: 0,
            capacity: cap,
            local_to_global: Vec::with_capacity(cap),
            global_to_local: Vec::new(),
            weight: vec![0u8; nibble_bytes],
            edges: (0..cap).map(|_| Vec::with_capacity(16)).collect(),
            edge_pair_count: 0,
            has_edge_pair: vec![0u64; bit_words],
            counted: vec![0u32; cap * cap],
            missing_pairs: Vec::new(),
            finalized: false,
            final_order: Vec::new(),
            missing_edge_request_sent: false,
        }
    }

    #[inline]
    fn ensure_global_capacity(&mut self, global_idx: u32) {
        let needed = global_idx as usize + 1;
        if needed > self.global_to_local.len() {
            self.global_to_local.resize(needed, NONE_LOCAL);
        }
    }

    fn add_node(&mut self, global_dense_idx: u32) -> u16 {
        self.ensure_global_capacity(global_dense_idx);
        let existing = self.global_to_local[global_dense_idx as usize];
        if existing != NONE_LOCAL {
            return existing;
        }
        let local_idx = self.node_count as u16;
        assert!(
            (local_idx as usize) < self.capacity,
            "FATAL: DependencyGraph capacity {} exceeded at node_count={}.",
            self.capacity, self.node_count
        );
        self.local_to_global.push(global_dense_idx);
        self.global_to_local[global_dense_idx as usize] = local_idx;
        self.node_count += 1;
        local_idx
    }

    #[inline]
    fn get_local(&self, global_dense_idx: u32) -> Option<u16> {
        let g = global_dense_idx as usize;
        if g < self.global_to_local.len() {
            let l = self.global_to_local[g];
            if l != NONE_LOCAL { Some(l) } else { None }
        } else {
            None
        }
    }

    #[inline]
    fn is_tournament(&self) -> bool {
        let m = self.node_count;
        if m < 2 { return m <= 1; }
        self.edge_pair_count == m * (m - 1) / 2
    }

    #[inline]
    fn has_edge(&self, li: u16, lj: u16) -> bool {
        bit_get(&self.has_edge_pair, pair_idx(li, lj, self.capacity))
    }

    fn add_edge(&mut self, from: u16, to: u16) -> bool {
        let pidx = pair_idx(from, to, self.capacity);
        if bit_get(&self.has_edge_pair, pidx) {
            return false;
        }
        bit_set(&mut self.has_edge_pair, pidx);
        self.edge_pair_count += 1;
        self.edges[from as usize].push(to);
        true
    }

    #[inline]
    fn get_weight_val(&self, li: u16, lj: u16) -> u8 {
        get_weight(&self.weight, w_idx(li, lj, self.capacity))
    }
    #[inline]
    fn set_weight_val(&mut self, li: u16, lj: u16, val: u8) {
        set_weight(&mut self.weight, w_idx(li, lj, self.capacity), val);
    }
    #[inline]
    fn inc_weight_val(&mut self, li: u16, lj: u16) {
        inc_weight(&mut self.weight, w_idx(li, lj, self.capacity));
    }

    fn release_memory(&mut self) {
        self.weight = Vec::new();
        self.has_edge_pair = Vec::new();
        self.counted = Vec::new();
        self.missing_pairs = Vec::new();
        for e in &mut self.edges {
            *e = Vec::new();
        }
        self.edges = Vec::new();
        self.global_to_local = Vec::new();
    }

    /// v5: Collect all unique tx digests involved in missing pairs.
    fn missing_edge_tx_digests(&self) -> Vec<u32> {
        let mut dense_set: HashSet<u32> = HashSet::new();
        for &(li, lj) in &self.missing_pairs {
            dense_set.insert(self.local_to_global[li as usize]);
            dense_set.insert(self.local_to_global[lj as usize]);
        }
        dense_set.into_iter().collect()
    }
}

// =============================================================================
// IngestResult
// =============================================================================

struct IngestResult {
    graph_idx: usize,
    newly_classified: Vec<u32>,
    round: Round,
}

// =============================================================================
// FairnessLayer
// =============================================================================

pub struct FairnessLayer {
    pub n: usize,
    pub f: usize,
    solid_threshold: usize,
    half_threshold: usize,

    digest_to_dense: HashMap<TxDigest, u32>,
    dense_to_digest: Vec<TxDigest>,
    next_dense_idx: u32,
    free_list: Vec<u32>,

    nodes: Vec<TransactionNode>,
    ordered_digests: HashSet<TxDigest>,

    graphs: Vec<DependencyGraph>,
    round_to_graph: HashMap<Round, usize>,

    output_sequence: Vec<TxDigest>,
    replica_indices: HashMap<PublicKey, ReplicaIndex>,

    use_hamiltonian_path: bool,

    subdag_count: u64,
    batch_count: u64,
}

impl FairnessLayer {
    pub fn new(committee_keys: Vec<PublicKey>, f: usize, gamma: f64) -> Self {
        let n = committee_keys.len();
        assert!(n <= 32, "FATAL: N={} exceeds 32 (counted bitmask is u32).", n);
        let solid_threshold = n - 2 * f;
        let non_blank_threshold =
            (((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0) * 1e10).round() / 1e10;
        let half_threshold = non_blank_threshold.floor() as usize;

        let replica_indices: HashMap<PublicKey, ReplicaIndex> = committee_keys
            .into_iter()
            .enumerate()
            .map(|(i, pk)| (pk, i))
            .collect();

        info!(
            "FairnessLayer v5 initialized: n={}, f={}, solid_threshold={}, half_threshold={}",
            n, f, solid_threshold, half_threshold
        );

        FairnessLayer {
            n,
            f,
            solid_threshold,
            half_threshold,
            digest_to_dense: HashMap::new(),
            dense_to_digest: Vec::new(),
            next_dense_idx: 0,
            free_list: Vec::new(),
            nodes: Vec::new(),
            ordered_digests: HashSet::new(),
            graphs: Vec::new(),
            round_to_graph: HashMap::new(),
            output_sequence: Vec::new(),
            replica_indices,
            use_hamiltonian_path: false,
            subdag_count: 0,
            batch_count: 0,
        }
    }

    // =========================================================================
    // Dense index management with recycling
    // =========================================================================

    fn get_or_create_dense(&mut self, digest: TxDigest) -> u32 {
        if let Some(&idx) = self.digest_to_dense.get(&digest) {
            return idx;
        }
        let idx = if let Some(recycled) = self.free_list.pop() {
            self.dense_to_digest[recycled as usize] = digest;
            self.nodes[recycled as usize] = TransactionNode::new(digest, recycled, self.n);
            recycled
        } else {
            let idx = self.next_dense_idx;
            if idx == u32::MAX {
                panic!(
                    "FATAL: Dense index overflow. free_list empty, live={}, ordered={}.",
                    self.digest_to_dense.len(),
                    self.ordered_digests.len()
                );
            }
            self.next_dense_idx = idx + 1;
            self.dense_to_digest.push(digest);
            self.nodes.push(TransactionNode::new(digest, idx, self.n));
            idx
        };
        self.digest_to_dense.insert(digest, idx);
        idx
    }

    fn recycle_ordered_tx(&mut self, digest: TxDigest) {
        self.ordered_digests.insert(digest);
        if let Some(dense) = self.digest_to_dense.remove(&digest) {
            self.dense_to_digest[dense as usize] = 0;
            self.nodes[dense as usize] = TransactionNode::new(0, dense, self.n);
            self.free_list.push(dense);
        }
    }

    // =========================================================================
    // PUBLIC API: Backward-compatible single subdag
    // =========================================================================

    pub fn process_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<TxDigest> {
        let (ordered, _missing) = self.process_subdag_explicit(subdag);
        ordered
    }

    // =========================================================================
    // PUBLIC API v5: Explicit missing-edge processing
    //
    // Returns:
    //   - Vec<TxDigest>: transactions that were fully ordered (graph was tournament)
    //   - Option<(Round, Vec<TxDigest>)>: if the graph has missing edges,
    //     returns (graph_round, needed_tx_digests) for the MissingEdgeRequest
    // =========================================================================

    pub fn process_subdag_explicit(
        &mut self,
        subdag: &CommittedSubdag,
    ) -> (Vec<TxDigest>, Option<(Round, Vec<TxDigest>)>) {
        let total_start = Instant::now();
        self.subdag_count += 1;
        let sd = self.subdag_count;

        let total_entries: usize = subdag
            .vertices
            .iter()
            .map(|v| v.ordering_entries.len())
            .sum();

        info!(
            "FAIRDAG_PERF: sd={} phase=start entries={} active_graphs={} live_dense={} \
             free_list={} ordered_total={}",
            sd,
            total_entries,
            self.graphs.iter().filter(|g| !g.finalized).count(),
            self.digest_to_dense.len(),
            self.free_list.len(),
            self.ordered_digests.len(),
        );

        // =====================================================================
        // Phase 1: Ingest subdag (create graph, update nodes, classify)
        // =====================================================================
        let t_ingest = Instant::now();
        let result = self.ingest_subdag(subdag);
        let t_ingest_done = t_ingest.elapsed();

        // =====================================================================
        // Phase 2: Compute catchup weights for newly classified nodes
        // =====================================================================
        let t_catchup = Instant::now();
        let (pairs, edges, missing) = if result.newly_classified.is_empty() {
            (0, 0, 0)
        } else {
            self.compute_catchup_weights_for_new_nodes(result.graph_idx, &result.newly_classified)
        };
        let t_catchup_done = t_catchup.elapsed();

        // =====================================================================
        // Phase 3: Try finalization (NO implicit weight update)
        // =====================================================================
        let t_finalize = Instant::now();
        let ordered = self.try_finalize_all_graphs();
        let t_finalize_done = t_finalize.elapsed();

        // =====================================================================
        // Phase 4: Check for missing edges in the current graph
        // =====================================================================
        let missing_request = self.check_for_missing_edge_request(result.graph_idx);

        let t_total = total_start.elapsed();

        info!(
            "FAIRDAG_PERF: sd={} phase=done total_us={} ingest_us={} catchup_us={} \
             finalize_us={} catchup_pairs={} catchup_edges={} catchup_missing={} \
             finalized={} has_missing_request={}",
            sd,
            t_total.as_micros(),
            t_ingest_done.as_micros(),
            t_catchup_done.as_micros(),
            t_finalize_done.as_micros(),
            pairs, edges, missing,
            ordered.len(),
            missing_request.is_some(),
        );

        (ordered, missing_request)
    }

    // =========================================================================
    // PUBLIC API v5: Apply explicit edge updates (Themis FairUpdate)
    //
    // Called when n-f MissingEdgeContributions have been collected for a
    // parked graph. Each contribution is a Vec<(TxDigest, u64)> — the OIs
    // from one replica for the txs involved in missing edges.
    //
    // Steps (following Themis FairUpdate, Fig. 2):
    //   1. For each pair (tx, tx') with no edge yet, compute weights using
    //      the contribution OIs.
    //   2. If weight reaches threshold, add the edge.
    //   3. Try finalization again.
    // =========================================================================

    pub fn apply_fair_update(
        &mut self,
        graph_round: Round,
        contributions: &[Vec<(TxDigest, u64)>],
    ) -> Vec<TxDigest> {
        let graph_idx = match self.round_to_graph.get(&graph_round) {
            Some(&idx) => idx,
            None => {
                warn!(
                    "FairUpdate: no graph found for round {} — ignoring",
                    graph_round
                );
                return Vec::new();
            }
        };

        if self.graphs[graph_idx].finalized {
            warn!(
                "FairUpdate: graph {} (round {}) already finalized — ignoring",
                graph_idx, graph_round
            );
            return Vec::new();
        }

        let update_start = Instant::now();

        // Build a map from TxDigest → Vec<u64> (one OI per contribution).
        let mut digest_ois: HashMap<TxDigest, Vec<u64>> = HashMap::new();
        for contrib_ois in contributions {
            for &(digest, oi) in contrib_ois {
                digest_ois.entry(digest).or_default().push(oi);
            }
        }

        // Process only the missing pairs.
        let ht = self.half_threshold;
        let missing_pairs: Vec<(u16, u16)> =
            self.graphs[graph_idx].missing_pairs.clone();
        let mut resolved: Vec<usize> = Vec::new();

        for (pair_pos, &(li, lj)) in missing_pairs.iter().enumerate() {
            if self.graphs[graph_idx].has_edge(li, lj) {
                resolved.push(pair_pos);
                continue;
            }

            let di_dense = self.graphs[graph_idx].local_to_global[li as usize];
            let dj_dense = self.graphs[graph_idx].local_to_global[lj as usize];
            let di_digest = self.nodes[di_dense as usize].digest;
            let dj_digest = self.nodes[dj_dense as usize].digest;

            // Get OIs from all contributions for this pair.
            let ois_i = digest_ois.get(&di_digest);
            let ois_j = digest_ois.get(&dj_digest);

            if let (Some(ois_i), Some(ois_j)) = (ois_i, ois_j) {
                // Count pairwise preferences from the contributions.
                let mut w_ij: usize = 0;
                let mut w_ji: usize = 0;

                // Each contribution is one replica's view. We pair up the
                // OIs from the same contribution index.
                let pairs_to_check = ois_i.len().min(ois_j.len());
                for k in 0..pairs_to_check {
                    if ois_i[k] < ois_j[k] {
                        w_ij += 1;
                    } else if ois_j[k] < ois_i[k] {
                        w_ji += 1;
                    }
                }

                // Also incorporate previously computed weights.
                let existing_w_ij = self.graphs[graph_idx].get_weight_val(li, lj) as usize;
                let existing_w_ji = self.graphs[graph_idx].get_weight_val(lj, li) as usize;
                w_ij += existing_w_ij;
                w_ji += existing_w_ji;

                // Update stored weights.
                self.graphs[graph_idx].set_weight_val(li, lj, w_ij.min(15) as u8);
                self.graphs[graph_idx].set_weight_val(lj, li, w_ji.min(15) as u8);

                // Check threshold (Themis FairUpdate: add edge if weight ≥ threshold).
                if w_ij >= ht || w_ji >= ht {
                    if w_ij >= w_ji {
                        self.graphs[graph_idx].add_edge(li, lj);
                    } else {
                        self.graphs[graph_idx].add_edge(lj, li);
                    }
                    resolved.push(pair_pos);
                }
            }
        }

        // Remove resolved pairs.
        resolved.sort_unstable();
        resolved.dedup();
        for &pos in resolved.iter().rev() {
            if pos < self.graphs[graph_idx].missing_pairs.len() {
                self.graphs[graph_idx].missing_pairs.swap_remove(pos);
            }
        }

        let update_ms = update_start.elapsed().as_millis();

        info!(
            "FairUpdate: graph {} (round {}) resolved {} pairs in {}ms, \
             remaining_missing={} is_tournament={}",
            graph_idx,
            graph_round,
            resolved.len(),
            update_ms,
            self.graphs[graph_idx].missing_pairs.len(),
            self.graphs[graph_idx].is_tournament(),
        );

        // Try finalization now.
        self.try_finalize_all_graphs()
    }

    // =========================================================================
    // Phase 1: Ingest a single subdag
    //
    // Creates graph, updates committed_ois, classifies nodes.
    // NO readd from prior graphs — each graph is independent.
    // =========================================================================

    fn ingest_subdag(&mut self, subdag: &CommittedSubdag) -> IngestResult {
        let r = subdag.leader_round;
        let sd = self.subdag_count;

        let total_entries: usize = subdag
            .vertices
            .iter()
            .map(|v| v.ordering_entries.len())
            .sum();
        info!(
            "FairnessLayer: processing subdag leader_round={} vertices={} total_entries={}",
            r,
            subdag.vertices.len(),
            total_entries
        );

        // Create graph — each graph is independent, no readd.
        let graph_idx = self.graphs.len();
        self.graphs
            .push(DependencyGraph::new(r, INITIAL_GRAPH_CAPACITY));
        self.round_to_graph.insert(r, graph_idx);

        // Update nodes
        let updated_nodes = self.update_nodes_from_subdag(subdag);

        // Classify
        let newly_classified = self.classify_and_add_nodes(r, graph_idx, &updated_nodes);

        info!(
            "FAIRDAG_PERF: sd={} phase=ingest round={} graph_idx={} \
             updated={} newly_classified={} graph_nodes={}",
            sd,
            r,
            graph_idx,
            updated_nodes.len(),
            newly_classified.len(),
            self.graphs[graph_idx].node_count,
        );

        IngestResult {
            graph_idx,
            newly_classified,
            round: r,
        }
    }

    // =========================================================================
    // Figure 8, Lines 3-10
    // =========================================================================

    fn update_nodes_from_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<u32> {
        let mut updated_set: HashSet<u32> = HashSet::new();
        let r = subdag.leader_round;

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;
            for &(d, oi) in &vertex.ordering_entries {
                if self.ordered_digests.contains(&d) {
                    continue;
                }
                let dense = self.get_or_create_dense(d);
                let node = &mut self.nodes[dense as usize];
                if node.committed_ois[i].is_none() {
                    node.committed_ois[i] = Some(oi);
                    node.committed_rounds[i] = Some(r);
                    updated_set.insert(dense);
                }
            }
        }

        let mut updated: Vec<u32> = updated_set.into_iter().collect();
        updated.sort_unstable();
        updated
    }

    // =========================================================================
    // Figure 8, Lines 11-18
    // =========================================================================

    fn classify_and_add_nodes(
        &mut self,
        r: Round,
        graph_idx: usize,
        updated_nodes: &[u32],
    ) -> Vec<u32> {
        let mut solid_count = 0usize;
        let mut shaded_count = 0usize;
        let mut blank_count = 0usize;
        let mut newly_classified: Vec<u32> = Vec::new();

        for &dense in updated_nodes {
            if self.nodes[dense as usize].node_type != NodeType::Blank {
                continue;
            }
            let ap = self.nodes[dense as usize].appearance_count(r);

            if ap >= self.solid_threshold {
                self.nodes[dense as usize].node_type = NodeType::Solid;
                self.nodes[dense as usize].graph_index = Some(graph_idx);
                self.graphs[graph_idx].add_node(dense);
                newly_classified.push(dense);
                solid_count += 1;
            } else if ap >= self.half_threshold {
                self.nodes[dense as usize].node_type = NodeType::Shaded;
                self.nodes[dense as usize].graph_index = Some(graph_idx);
                self.graphs[graph_idx].add_node(dense);
                newly_classified.push(dense);
                shaded_count += 1;
            } else {
                blank_count += 1;
                // v5: Blank nodes are NOT kept for readd — they are simply
                // left as blank. They'll be classified when a future subdag
                // brings enough OIs.
            }
        }

        info!(
            "FairnessLayer: classify round={} G[{}] solid={} shaded={} blank={} total_in_graph={}",
            r, graph_idx, solid_count, shaded_count, blank_count,
            self.graphs[graph_idx].node_count
        );
        newly_classified
    }

    // =========================================================================
    // Phase 2: Catchup weights + populate missing_pairs
    // =========================================================================

    fn compute_catchup_weights_for_new_nodes(
        &mut self,
        graph_idx: usize,
        newly_classified: &[u32],
    ) -> (usize, usize, usize) {
        if newly_classified.is_empty() {
            return (0, 0, 0);
        }

        let newly_set: HashSet<u32> = newly_classified.iter().copied().collect();
        let mut edges_added = 0usize;
        let mut weights_computed = 0usize;
        let mut missing_added = 0usize;
        let ht = self.half_threshold;

        for &d_dense in newly_classified {
            let d_local = self.graphs[graph_idx].get_local(d_dense).unwrap();
            let node_count = self.graphs[graph_idx].node_count;

            for li in 0..node_count {
                let d2_dense = self.graphs[graph_idx].local_to_global[li];
                if d2_dense == d_dense {
                    continue;
                }
                let d2_local = li as u16;

                if newly_set.contains(&d2_dense) && d_dense > d2_dense {
                    continue;
                }

                let (w12, w21) = self.calculate_pairwise_weight(d_dense, d2_dense);
                weights_computed += 1;

                self.graphs[graph_idx].set_weight_val(d_local, d2_local, w12 as u8);
                self.graphs[graph_idx].set_weight_val(d2_local, d_local, w21 as u8);

                let n = self.n;
                let mut mask: u32 = 0;
                for r in 0..n {
                    if self.nodes[d_dense as usize].committed_ois[r].is_some()
                        && self.nodes[d2_dense as usize].committed_ois[r].is_some()
                    {
                        mask |= 1u32 << r;
                    }
                }
                let pidx = pair_idx(d_local, d2_local, self.graphs[graph_idx].capacity);
                self.graphs[graph_idx].counted[pidx] = mask;

                if w12 >= ht || w21 >= ht {
                    if w12 >= w21 {
                        self.graphs[graph_idx].add_edge(d_local, d2_local);
                    } else {
                        self.graphs[graph_idx].add_edge(d2_local, d_local);
                    }
                    edges_added += 1;
                } else {
                    let (lmin, lmax) = if d_local < d2_local {
                        (d_local, d2_local)
                    } else {
                        (d2_local, d_local)
                    };
                    self.graphs[graph_idx].missing_pairs.push((lmin, lmax));
                    missing_added += 1;
                }
            }
        }

        (weights_computed, edges_added, missing_added)
    }

    // =========================================================================
    // Pairwise weight calculation
    // =========================================================================

    fn calculate_pairwise_weight(&self, dense1: u32, dense2: u32) -> (usize, usize) {
        let node1 = &self.nodes[dense1 as usize];
        let node2 = &self.nodes[dense2 as usize];
        let mut w12: usize = 0;
        let mut w21: usize = 0;
        for i in 0..self.n {
            if let (Some(oi1), Some(oi2)) = (node1.committed_ois[i], node2.committed_ois[i]) {
                if oi1 < oi2 {
                    w12 += 1;
                } else {
                    w21 += 1;
                }
            }
        }
        (w12, w21)
    }

    // =========================================================================
    // Check if a graph needs a MissingEdgeRequest
    //
    // Returns Some((graph_round, needed_tx_digests)) if the graph has missing
    // pairs and hasn't already sent a request.
    // =========================================================================

    fn check_for_missing_edge_request(
        &mut self,
        graph_idx: usize,
    ) -> Option<(Round, Vec<TxDigest>)> {
        if self.graphs[graph_idx].finalized {
            return None;
        }
        if self.graphs[graph_idx].missing_pairs.is_empty() {
            return None;
        }
        if self.graphs[graph_idx].missing_edge_request_sent {
            return None;
        }

        // Collect all tx digests involved in missing pairs.
        let dense_indices = self.graphs[graph_idx].missing_edge_tx_digests();
        let needed_digests: Vec<TxDigest> = dense_indices
            .iter()
            .map(|&d| self.nodes[d as usize].digest)
            .collect();

        self.graphs[graph_idx].missing_edge_request_sent = true;

        info!(
            "FairnessLayer: graph {} (round {}) has {} missing pairs involving {} unique txs — requesting explicit contributions",
            graph_idx,
            self.graphs[graph_idx].round,
            self.graphs[graph_idx].missing_pairs.len(),
            needed_digests.len(),
        );

        Some((self.graphs[graph_idx].round, needed_digests))
    }

    // =========================================================================
    // Phase 3: Finalization (sequential, round-increasing order)
    //
    // v5: No readd. Everything after last solid SCC is DISCARDED.
    // =========================================================================

    fn try_finalize_all_graphs(&mut self) -> Vec<TxDigest> {
        let mut newly_ordered: Vec<TxDigest> = Vec::new();

        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized {
                continue;
            }
            if self.graphs[g_idx].node_count == 0 {
                self.graphs[g_idx].finalized = true;
                continue;
            }

            if !self.graphs[g_idx].is_tournament() {
                info!(
                    "FAIRDAG_PERF: phase=finalize_blocked G[{}] round={} nodes={} \
                     edges={}/{} missing_pairs={}",
                    g_idx,
                    self.graphs[g_idx].round,
                    self.graphs[g_idx].node_count,
                    self.graphs[g_idx].edge_pair_count,
                    self.graphs[g_idx].node_count * (self.graphs[g_idx].node_count - 1) / 2,
                    self.graphs[g_idx].missing_pairs.len(),
                );
                break; // Must finalize in order — can't skip.
            }

            info!(
                "FairnessLayer: graph {} (round {}) is a tournament with {} nodes — finalizing",
                g_idx,
                self.graphs[g_idx].round,
                self.graphs[g_idx].node_count
            );

            let order = self.finalize_ordering(g_idx);
            newly_ordered.extend(order);
        }

        newly_ordered
    }

    fn finalize_ordering(&mut self, graph_idx: usize) -> Vec<TxDigest> {
        let node_count = self.graphs[graph_idx].node_count;

        let sccs = tarjan_scc_dense(node_count, &self.graphs[graph_idx].edges);
        let topo_order =
            topological_sort_sccs_dense(&sccs, &self.graphs[graph_idx].edges, node_count);

        let mut last_solid_pos: Option<usize> = None;
        for (pos, &scc_idx) in topo_order.iter().enumerate() {
            let has_solid = sccs[scc_idx].iter().any(|&li| {
                let dense = self.graphs[graph_idx].local_to_global[li as usize];
                self.nodes[dense as usize].node_type == NodeType::Solid
            });
            if has_solid {
                last_solid_pos = Some(pos);
            }
        }

        let mut ordered_digests: Vec<TxDigest> = Vec::new();
        let mut discarded_count: usize = 0;

        match last_solid_pos {
            Some(cutoff) => {
                for (pos, &scc_idx) in topo_order.iter().enumerate() {
                    let scc = &sccs[scc_idx];
                    if pos <= cutoff {
                        // Order this SCC.
                        let path = if self.use_hamiltonian_path {
                            hamiltonian_path_dense(scc, &self.graphs[graph_idx].edges)
                        } else {
                            let mut sorted = scc.clone();
                            sorted.sort_by_key(|&li| {
                                self.graphs[graph_idx].local_to_global[li as usize]
                            });
                            sorted
                        };
                        for &li in &path {
                            let dense = self.graphs[graph_idx].local_to_global[li as usize];
                            ordered_digests.push(self.nodes[dense as usize].digest);
                        }
                    } else {
                        // v5: DISCARD everything after last solid SCC.
                        // No readd to next graph. These nodes lose their
                        // classification and can be classified fresh when
                        // they appear in a future subdag (if ever).
                        for &li in scc {
                            let dense = self.graphs[graph_idx].local_to_global[li as usize];
                            // Reset the node to blank so it can be re-classified
                            // fresh in a future subdag.
                            self.nodes[dense as usize].node_type = NodeType::Blank;
                            self.nodes[dense as usize].graph_index = None;
                            discarded_count += 1;
                        }
                    }
                }
            }
            None => {
                warn!(
                    "FairnessLayer: graph {} tournament with no solid nodes — discarding all",
                    graph_idx
                );
                // Discard all nodes — reset to blank.
                for li in 0..node_count {
                    let dense = self.graphs[graph_idx].local_to_global[li];
                    self.nodes[dense as usize].node_type = NodeType::Blank;
                    self.nodes[dense as usize].graph_index = None;
                    discarded_count += 1;
                }
                self.graphs[graph_idx].finalized = true;
                self.graphs[graph_idx].release_memory();
                return Vec::new();
            }
        }

        self.graphs[graph_idx].finalized = true;
        self.graphs[graph_idx].final_order = ordered_digests.clone();
        self.output_sequence.extend(&ordered_digests);

        for &d in &ordered_digests {
            self.recycle_ordered_tx(d);
        }

        self.graphs[graph_idx].release_memory();

        // This log line is parsed by logs.py — DO NOT CHANGE FORMAT.
        info!(
            "FairnessLayer: finalized {} transactions from graph {} (round {}). Total ordered: {}",
            ordered_digests.len(),
            graph_idx,
            self.graphs[graph_idx].round,
            self.output_sequence.len()
        );

        if discarded_count > 0 {
            info!(
                "FairnessLayer: discarded {} post-solid nodes from graph {} (round {})",
                discarded_count, graph_idx, self.graphs[graph_idx].round
            );
        }

        ordered_digests
    }

    // =========================================================================
    // Public accessors
    // =========================================================================

    pub fn get_output_sequence(&self) -> &[TxDigest] {
        &self.output_sequence
    }

    pub fn pending_count(&self) -> usize {
        self.digest_to_dense.len()
    }

    pub fn replica_index(&self, pk: &PublicKey) -> Option<ReplicaIndex> {
        self.replica_indices.get(pk).copied()
    }
}

// =============================================================================
// Tarjan's SCC
// =============================================================================

fn tarjan_scc_dense(node_count: usize, edges: &[Vec<u16>]) -> Vec<Vec<u16>> {
    let mut dfn = vec![0i32; node_count];
    let mut low = vec![0i32; node_count];
    let mut on_stack = vec![false; node_count];
    let mut stack: Vec<u16> = Vec::with_capacity(node_count);
    let mut sccs: Vec<Vec<u16>> = Vec::new();
    let mut index_counter: i32 = 0;

    for start in 0..node_count {
        if dfn[start] != 0 {
            continue;
        }
        let mut dfs_stack: Vec<(u16, usize)> = Vec::new();
        let u = start as u16;
        index_counter += 1;
        dfn[start] = index_counter;
        low[start] = index_counter;
        stack.push(u);
        on_stack[start] = true;
        dfs_stack.push((u, 0));

        while let Some(&mut (v, ref mut ni)) = dfs_stack.last_mut() {
            let v_usize = v as usize;
            if *ni < edges[v_usize].len() {
                let w = edges[v_usize][*ni];
                *ni += 1;
                let w_usize = w as usize;
                if dfn[w_usize] == 0 {
                    index_counter += 1;
                    dfn[w_usize] = index_counter;
                    low[w_usize] = index_counter;
                    stack.push(w);
                    on_stack[w_usize] = true;
                    dfs_stack.push((w, 0));
                } else if on_stack[w_usize] && dfn[w_usize] < low[v_usize] {
                    low[v_usize] = dfn[w_usize];
                }
            } else {
                if low[v_usize] == dfn[v_usize] {
                    let mut scc: Vec<u16> = Vec::new();
                    loop {
                        let w = stack.pop().unwrap();
                        on_stack[w as usize] = false;
                        scc.push(w);
                        if w == v {
                            break;
                        }
                    }
                    scc.sort_unstable();
                    sccs.push(scc);
                }
                dfs_stack.pop();
                if let Some(&(parent, _)) = dfs_stack.last() {
                    let v_low = low[v_usize];
                    if v_low < low[parent as usize] {
                        low[parent as usize] = v_low;
                    }
                }
            }
        }
    }
    sccs.reverse();
    sccs
}

// =============================================================================
// Topological sort of SCCs
// =============================================================================

fn topological_sort_sccs_dense(
    sccs: &[Vec<u16>],
    edges: &[Vec<u16>],
    node_count: usize,
) -> Vec<usize> {
    let mut node_to_scc = vec![0usize; node_count];
    for (scc_idx, scc) in sccs.iter().enumerate() {
        for &node in scc {
            node_to_scc[node as usize] = scc_idx;
        }
    }

    let scc_n = sccs.len();
    let mut in_degree = vec![0usize; scc_n];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); scc_n];
    let mut seen: Vec<HashSet<usize>> = vec![HashSet::new(); scc_n];

    for u in 0..node_count {
        let su = node_to_scc[u];
        for &v16 in &edges[u] {
            let sv = node_to_scc[v16 as usize];
            if su != sv && seen[su].insert(sv) {
                adj[su].push(sv);
                in_degree[sv] += 1;
            }
        }
    }

    let mut ready: VecDeque<usize> = VecDeque::new();
    let mut initial: Vec<usize> = (0..scc_n).filter(|&i| in_degree[i] == 0).collect();
    initial.sort_unstable();
    for s in initial {
        ready.push_back(s);
    }

    let mut result: Vec<usize> = Vec::with_capacity(scc_n);
    while let Some(s) = ready.pop_front() {
        result.push(s);
        let mut new_ready: Vec<usize> = Vec::new();
        for &v in &adj[s] {
            in_degree[v] -= 1;
            if in_degree[v] == 0 {
                new_ready.push(v);
            }
        }
        new_ready.sort_unstable();
        for v in new_ready {
            ready.push_back(v);
        }
    }
    result
}

// =============================================================================
// Hamiltonian Path
// =============================================================================

fn hamiltonian_path_dense(scc: &[u16], edges: &[Vec<u16>]) -> Vec<u16> {
    if scc.len() <= 1 {
        return scc.to_vec();
    }

    let has_edge = |u: u16, v: u16| -> bool { edges[u as usize].contains(&v) };
    let mut sorted = scc.to_vec();
    sorted.sort_unstable();

    let mut path: VecDeque<u16> = VecDeque::new();
    path.push_back(sorted[0]);

    for &v in &sorted[1..] {
        if has_edge(v, *path.front().unwrap()) {
            path.push_front(v);
        } else if has_edge(*path.back().unwrap(), v) {
            path.push_back(v);
        } else {
            let mut inserted = false;
            for i in 0..path.len() - 1 {
                if has_edge(path[i], v) && has_edge(v, path[i + 1]) {
                    path.insert(i + 1, v);
                    inserted = true;
                    break;
                }
            }
            if !inserted {
                panic!(
                    "FATAL: Hamiltonian path insertion failed for node {} in SCC of size {}.",
                    v,
                    scc.len()
                );
            }
        }
    }
    path.into_iter().collect()
}