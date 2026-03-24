// Copyright(C) FairDAG-RL Implementation
// Implements the Fairness Layer of FairDAG-RL.
//
// v5: Explicit missing-edge updates.
//
// The processing pipeline is now split into explicit phases:
//
//   1. ingest_and_propose(subdag)
//      - Ingests OI data from committed subdag
//      - Builds dependency graph (FairPropose)
//      - Returns ProposeResult with missing edge info
//      - Does NOT implicitly resolve missing edges
//
//   2. apply_explicit_edge_updates(round, update_sets)
//      - Takes n-f update sets (replica_index, orderings)
//      - Applies FairUpdate: recomputes weights for missing pairs
//      - Adds edges where the updated weights meet the threshold
//
//   3. finalize_graph(round)
//      - FairFinalize for a single graph that is a tournament
//      - Condensation → topological sort → Hamiltonian path
//      - Returns ordered transactions
//
// Finalization is sequential: round order must be maintained.
// Blank nodes are discarded (only Solid/Shaded enter graphs).
// Hamiltonian path is controlled by the use_hamiltonian_path flag.

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
// ProposeResult: returned by ingest_and_propose
// =============================================================================

/// Result of FairPropose: describes the graph state after initial edge computation.
#[derive(Debug)]
pub struct ProposeResult {
    /// Number of (non-blank) nodes in the graph.
    pub node_count: usize,
    /// Number of tx pairs without edges (missing pairs).
    pub missing_pair_count: usize,
    /// All unique tx digests involved in missing pairs.
    /// This is the set of digests for which we need additional OIs.
    pub missing_tx_digests: Vec<TxDigest>,
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
        if existing != NONE_LOCAL { return existing; }
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
        } else { None }
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
        if bit_get(&self.has_edge_pair, pidx) { return false; }
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
        for e in &mut self.edges { *e = Vec::new(); }
        self.edges = Vec::new();
        self.global_to_local = Vec::new();
    }
}

// =============================================================================
// Work descriptor: output of ingest, input to catchup
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
    pending_readd: Vec<u32>,

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
            "FairnessLayer initialized: n={}, f={}, solid_threshold={}, half_threshold={}",
            n, f, solid_threshold, half_threshold
        );

        FairnessLayer {
            n, f, solid_threshold, half_threshold,
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
            pending_readd: Vec::new(),
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
    // PUBLIC API: Backward-compatible single/batch processing
    // (kept for compatibility; internally uses ingest_and_propose)
    // =========================================================================

    pub fn process_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<TxDigest> {
        self.process_subdag_batch(&[subdag.clone()])
    }

    pub fn process_subdag_batch(&mut self, subdags: &[CommittedSubdag]) -> Vec<TxDigest> {
        if subdags.is_empty() {
            return Vec::new();
        }

        for subdag in subdags {
            self.ingest_and_propose(subdag);
        }

        // In the backward-compatible path, do implicit weight updates.
        self.update_weights_and_edges_implicit();

        self.try_finalize_all_graphs()
    }

    // =========================================================================
    // PUBLIC API v5: Explicit update pipeline
    // =========================================================================

    /// Phase 1+2: Ingest subdag data and run FairPropose.
    ///
    /// Ingests committed OIs from the subdag vertices, classifies nodes
    /// (Blank/Shaded/Solid), builds the dependency graph, computes initial
    /// edge weights, and identifies missing pairs.
    ///
    /// Blank nodes are discarded (not added to the graph).
    ///
    /// Returns ProposeResult describing the graph state and which tx digests
    /// need additional OI data for missing edges.
    pub fn ingest_and_propose(&mut self, subdag: &CommittedSubdag) -> ProposeResult {
        self.subdag_count += 1;
        let result = self.ingest_subdag(subdag);

        // Catchup: compute initial weights for newly classified nodes.
        if !result.newly_classified.is_empty() {
            self.compute_catchup_weights_for_new_nodes(result.graph_idx, &result.newly_classified);
        }

        // Collect info about missing pairs.
        let graph = &self.graphs[result.graph_idx];
        let mut missing_digests_set: HashSet<TxDigest> = HashSet::new();

        for &(li, lj) in &graph.missing_pairs {
            let di = graph.local_to_global[li as usize];
            let dj = graph.local_to_global[lj as usize];
            missing_digests_set.insert(self.nodes[di as usize].digest);
            missing_digests_set.insert(self.nodes[dj as usize].digest);
        }

        let missing_tx_digests: Vec<TxDigest> = missing_digests_set.into_iter().collect();

        info!(
            "FairPropose: round={} G[{}] nodes={} edges={} missing_pairs={} \
             missing_tx_digests={}",
            subdag.leader_round,
            result.graph_idx,
            graph.node_count,
            graph.edge_pair_count,
            graph.missing_pairs.len(),
            missing_tx_digests.len(),
        );

        ProposeResult {
            node_count: graph.node_count,
            missing_pair_count: graph.missing_pairs.len(),
            missing_tx_digests,
        }
    }

    /// Phase 2.5: Apply explicit edge updates (FairUpdate algorithm).
    ///
    /// Takes n-f update sets for a parked graph at the given round.
    /// Each update set is (replica_index, orderings) where orderings
    /// are (tx_digest, oi) pairs — the replica's local ordering for
    /// the transactions involved in missing edges.
    ///
    /// For all tx pairs in the graph that don't have an edge yet:
    ///   - Recompute weights using the new orderings.
    ///   - If tx appears in >= n-2f update sets (solid in updates),
    ///     and weight >= threshold and weight >= reverse weight,
    ///     add the edge.
    ///
    /// After this call, check is_graph_tournament() to see if the graph
    /// is ready for finalization.
    pub fn apply_explicit_edge_updates(
        &mut self,
        round: Round,
        update_sets: &[(usize, &[(TxDigest, u64)])],
    ) {
        let graph_idx = match self.round_to_graph.get(&round) {
            Some(&idx) => idx,
            None => {
                warn!("apply_explicit_edge_updates: no graph for round {}", round);
                return;
            }
        };

        if self.graphs[graph_idx].finalized {
            return;
        }

        let n = self.n;
        let f = self.f;
        let ht = self.half_threshold as u8;
        let solid_in_updates_threshold = n - 2 * f; // n-2f

        info!(
            "FairUpdate: round={} G[{}] applying {} update sets, \
             current missing_pairs={}",
            round,
            graph_idx,
            update_sets.len(),
            self.graphs[graph_idx].missing_pairs.len(),
        );

        // Step 1: Ingest update orderings into node committed_ois.
        // This adds OI data from replicas that weren't in the original subdag.
        for &(replica_idx, orderings) in update_sets {
            for &(digest, oi) in orderings {
                if self.ordered_digests.contains(&digest) {
                    continue;
                }
                if let Some(&dense) = self.digest_to_dense.get(&digest) {
                    if self.nodes[dense as usize].committed_ois[replica_idx].is_none() {
                        self.nodes[dense as usize].committed_ois[replica_idx] = Some(oi);
                    }
                }
            }
        }

        // Step 2: Re-evaluate missing pairs with the new OI data.
        let cap = self.graphs[graph_idx].capacity;
        let num_missing = self.graphs[graph_idx].missing_pairs.len();
        let mut resolved: Vec<usize> = Vec::new();

        // Build a set of replicas that contributed update sets.
        let update_replica_set: HashSet<usize> =
            update_sets.iter().map(|&(ri, _)| ri).collect();

        for pair_pos in 0..num_missing {
            let (li, lj) = self.graphs[graph_idx].missing_pairs[pair_pos];

            if self.graphs[graph_idx].has_edge(li, lj) {
                resolved.push(pair_pos);
                continue;
            }

            let di = self.graphs[graph_idx].local_to_global[li as usize];
            let dj = self.graphs[graph_idx].local_to_global[lj as usize];
            let pidx = pair_idx(li, lj, cap);
            let mut counted_mask = self.graphs[graph_idx].counted[pidx];

            // Count how many update sets contain both tx_i and tx_j.
            // (FairUpdate condition: tx must appear in n-2f of Lupdates.)
            let di_digest = self.nodes[di as usize].digest;
            let dj_digest = self.nodes[dj as usize].digest;

            let mut di_in_updates = 0usize;
            let mut dj_in_updates = 0usize;
            for &(ri, orderings) in update_sets {
                if orderings.iter().any(|&(d, _)| d == di_digest) {
                    di_in_updates += 1;
                }
                if orderings.iter().any(|&(d, _)| d == dj_digest) {
                    dj_in_updates += 1;
                }
            }

            // FairUpdate condition: tx must be in n-2f of the update sets.
            let di_solid_in_updates = di_in_updates >= solid_in_updates_threshold;
            let dj_solid_in_updates = dj_in_updates >= solid_in_updates_threshold;

            // Recompute weights from ALL available committed_ois (original + updates).
            for r in 0..n {
                if counted_mask & (1u32 << r) != 0 {
                    continue;
                }
                if let (Some(oi_i), Some(oi_j)) = (
                    self.nodes[di as usize].committed_ois[r],
                    self.nodes[dj as usize].committed_ois[r],
                ) {
                    counted_mask |= 1u32 << r;
                    if oi_i < oi_j {
                        self.graphs[graph_idx].inc_weight_val(li, lj);
                    } else {
                        self.graphs[graph_idx].inc_weight_val(lj, li);
                    }
                }
            }
            self.graphs[graph_idx].counted[pidx] = counted_mask;

            let w_fwd = self.graphs[graph_idx].get_weight_val(li, lj);
            let w_rev = self.graphs[graph_idx].get_weight_val(lj, li);

            // FairUpdate edge condition:
            //   - At least one tx is solid in updates (n-2f threshold)
            //   - Weight meets threshold (n(1-γ)+f+1)
            //   - Weight >= reverse weight (direction is decided)
            let can_add_edge = (di_solid_in_updates || dj_solid_in_updates)
                && (w_fwd >= ht || w_rev >= ht);

            if can_add_edge {
                if w_fwd >= w_rev {
                    self.graphs[graph_idx].add_edge(li, lj);
                } else {
                    self.graphs[graph_idx].add_edge(lj, li);
                }
                resolved.push(pair_pos);
            }
        }

        // Remove resolved pairs.
        resolved.sort_unstable();
        for &pos in resolved.iter().rev() {
            self.graphs[graph_idx].missing_pairs.swap_remove(pos);
        }

        info!(
            "FairUpdate: round={} G[{}] resolved {} pairs, remaining={}",
            round,
            graph_idx,
            resolved.len(),
            self.graphs[graph_idx].missing_pairs.len(),
        );
    }

    /// Check whether the graph at the given round is a tournament.
    pub fn is_graph_tournament(&self, round: Round) -> bool {
        match self.round_to_graph.get(&round) {
            Some(&idx) => {
                if idx < self.graphs.len() {
                    self.graphs[idx].is_tournament()
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Check whether the graph at the given round is finalized.
    pub fn is_graph_finalized(&self, round: Round) -> bool {
        match self.round_to_graph.get(&round) {
            Some(&idx) => {
                if idx < self.graphs.len() {
                    self.graphs[idx].finalized
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Finalize a single graph (FairFinalize for one block).
    /// The caller must ensure sequential ordering: only call this after
    /// all prior graphs are finalized.
    ///
    /// Returns the fair-ordered sequence of TxDigests.
    pub fn finalize_graph(&mut self, round: Round) -> Vec<TxDigest> {
        let graph_idx = match self.round_to_graph.get(&round) {
            Some(&idx) => idx,
            None => {
                warn!("finalize_graph: no graph for round {}", round);
                return Vec::new();
            }
        };

        if self.graphs[graph_idx].finalized {
            return Vec::new();
        }

        if self.graphs[graph_idx].node_count == 0 {
            self.graphs[graph_idx].finalized = true;
            return Vec::new();
        }

        if !self.graphs[graph_idx].is_tournament() {
            warn!(
                "finalize_graph: round {} is NOT a tournament (edges={}/{})",
                round,
                self.graphs[graph_idx].edge_pair_count,
                self.graphs[graph_idx].node_count * (self.graphs[graph_idx].node_count - 1) / 2
            );
            return Vec::new();
        }

        self.finalize_ordering(graph_idx)
    }

    /// Get the missing tx digests for a graph at a given round.
    pub fn get_missing_tx_digests(&self, round: Round) -> Vec<TxDigest> {
        let graph_idx = match self.round_to_graph.get(&round) {
            Some(&idx) => idx,
            None => return Vec::new(),
        };

        let graph = &self.graphs[graph_idx];
        let mut digests: HashSet<TxDigest> = HashSet::new();

        for &(li, lj) in &graph.missing_pairs {
            let di = graph.local_to_global[li as usize];
            let dj = graph.local_to_global[lj as usize];
            digests.insert(self.nodes[di as usize].digest);
            digests.insert(self.nodes[dj as usize].digest);
        }

        digests.into_iter().collect()
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

    // =========================================================================
    // Ingest a single subdag (fast, sequential)
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

        // Create graph.
        let graph_idx = self.graphs.len();
        self.graphs
            .push(DependencyGraph::new(r, INITIAL_GRAPH_CAPACITY));
        self.round_to_graph.insert(r, graph_idx);

        // Process pending readd from prior finalizations.
        // self.process_pending_readd(graph_idx);

        // Update nodes.
        let updated_nodes = self.update_nodes_from_subdag(subdag);

        // Classify (blank nodes are discarded — only Solid/Shaded enter graph).
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
    // Update nodes from subdag (Figure 8, Lines 3-10)
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
    // Classify and add nodes (Figure 8, Lines 11-18)
    // Blank nodes are DISCARDED — only Solid/Shaded enter the graph.
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
                // Blank — DISCARDED from this graph.
                blank_count += 1;
            }
        }

        info!(
            "FairnessLayer: classify round={} G[{}] solid={} shaded={} blank(discarded)={} \
             total_in_graph={}",
            r,
            graph_idx,
            solid_count,
            shaded_count,
            blank_count,
            self.graphs[graph_idx].node_count
        );
        newly_classified
    }

    // =========================================================================
    // Catchup weights + populate missing_pairs
    // Returns (pairs_computed, edges_added, missing_added)
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
        let n = self.n;
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

                let mut mask: u32 = 0;
                for r in 0..n {
                    if self.nodes[d_dense as usize].committed_ois[r].is_some()
                        && self.nodes[d2_dense as usize].committed_ois[r].is_some()
                    {
                        mask |= 1u32 << r;
                    }
                }
                let pidx = pair_idx(
                    d_local,
                    d2_local,
                    self.graphs[graph_idx].capacity,
                );
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
    // Implicit weight update (backward-compatible path only)
    // NOT used by the explicit update pipeline.
    // =========================================================================

    fn update_weights_and_edges_implicit(&mut self) -> (usize, usize, usize) {
        let n = self.n;
        let ht = self.half_threshold as u8;
        let mut stat_checked: usize = 0;
        let mut stat_incr: usize = 0;
        let mut stat_resolved: usize = 0;

        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized || self.graphs[g_idx].missing_pairs.is_empty() {
                continue;
            }

            let cap = self.graphs[g_idx].capacity;
            let num_missing = self.graphs[g_idx].missing_pairs.len();
            let mut resolved: Vec<usize> = Vec::new();

            for pair_pos in 0..num_missing {
                let (li, lj) = self.graphs[g_idx].missing_pairs[pair_pos];

                if self.graphs[g_idx].has_edge(li, lj) {
                    resolved.push(pair_pos);
                    continue;
                }

                stat_checked += 1;

                let di = self.graphs[g_idx].local_to_global[li as usize];
                let dj = self.graphs[g_idx].local_to_global[lj as usize];
                let pidx = pair_idx(li, lj, cap);
                let mut counted_mask = self.graphs[g_idx].counted[pidx];

                for r in 0..n {
                    if counted_mask & (1u32 << r) != 0 {
                        continue;
                    }
                    if let (Some(oi_i), Some(oi_j)) = (
                        self.nodes[di as usize].committed_ois[r],
                        self.nodes[dj as usize].committed_ois[r],
                    ) {
                        counted_mask |= 1u32 << r;
                        if oi_i < oi_j {
                            self.graphs[g_idx].inc_weight_val(li, lj);
                        } else {
                            self.graphs[g_idx].inc_weight_val(lj, li);
                        }
                        stat_incr += 1;
                    }
                }

                self.graphs[g_idx].counted[pidx] = counted_mask;

                let w_fwd = self.graphs[g_idx].get_weight_val(li, lj);
                let w_rev = self.graphs[g_idx].get_weight_val(lj, li);

                if w_fwd >= ht || w_rev >= ht {
                    if w_fwd >= w_rev {
                        self.graphs[g_idx].add_edge(li, lj);
                    } else {
                        self.graphs[g_idx].add_edge(lj, li);
                    }
                    resolved.push(pair_pos);
                    stat_resolved += 1;
                }
            }

            resolved.sort_unstable();
            for &pos in resolved.iter().rev() {
                self.graphs[g_idx].missing_pairs.swap_remove(pos);
            }
        }

        (stat_checked, stat_incr, stat_resolved)
    }

    // =========================================================================
    // Finalization (sequential, round-increasing order)
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
                break; // Sequential constraint: can't finalize later rounds.
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

        // Find last SCC position containing a solid transaction.
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
        let mut to_readd: Vec<u32> = Vec::new();

        match last_solid_pos {
            Some(cutoff) => {
                for (pos, &scc_idx) in topo_order.iter().enumerate() {
                    let scc = &sccs[scc_idx];
                    if pos <= cutoff {
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
                        // Nodes after the last solid SCC → re-add to next graph.
                        for &li in scc {
                            let dense = self.graphs[graph_idx].local_to_global[li as usize];
                            to_readd.push(dense);
                        }
                    }
                }
            }
            None => {
                warn!(
                    "FairnessLayer: graph {} tournament with no solid nodes — deferring",
                    graph_idx
                );
                return Vec::new();
            }
        }

        self.graphs[graph_idx].finalized = true;
        self.graphs[graph_idx].final_order = ordered_digests.clone();
        self.output_sequence.extend(&ordered_digests);

        for &d in &ordered_digests {
            self.recycle_ordered_tx(d);
        }

        if !to_readd.is_empty() {
            let next_graph_idx = self.find_next_unfinalized_graph(graph_idx);
            match next_graph_idx {
                Some(next_idx) => {
                    self.readd_nodes_to_graph(to_readd, next_idx);
                }
                None => {
                    for &dense in &to_readd {
                        self.nodes[dense as usize].node_type = NodeType::Blank;
                        self.nodes[dense as usize].graph_index = None;
                    }
                    self.pending_readd.extend(to_readd);
                }
            }
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

        ordered_digests
    }

    fn find_next_unfinalized_graph(&self, after_idx: usize) -> Option<usize> {
        for idx in (after_idx + 1)..self.graphs.len() {
            if !self.graphs[idx].finalized {
                return Some(idx);
            }
        }
        None
    }

    // =========================================================================
    // Re-add nodes to graph (also populates missing_pairs)
    // =========================================================================

    fn readd_nodes_to_graph(&mut self, to_readd: Vec<u32>, target_graph_idx: usize) {
        let r_prime = self.graphs[target_graph_idx].round;
        let n = self.n;
        let ht = self.half_threshold;

        for &dense in &to_readd {
            let ap = self.nodes[dense as usize].appearance_count(r_prime);

            if ap >= self.solid_threshold {
                self.nodes[dense as usize].node_type = NodeType::Solid;
            } else if ap >= ht {
                self.nodes[dense as usize].node_type = NodeType::Shaded;
            } else {
                // Blank — discarded.
                self.nodes[dense as usize].node_type = NodeType::Blank;
                self.nodes[dense as usize].graph_index = None;
                self.pending_readd.push(dense);
                continue;
            }

            self.nodes[dense as usize].graph_index = Some(target_graph_idx);
            let d_local = self.graphs[target_graph_idx].add_node(dense);

            let node_count = self.graphs[target_graph_idx].node_count;
            let cap = self.graphs[target_graph_idx].capacity;

            for li in 0..node_count {
                let d2_dense = self.graphs[target_graph_idx].local_to_global[li];
                if d2_dense == dense {
                    continue;
                }
                let d2_local = li as u16;

                let (w12, w21) = self.calculate_pairwise_weight(dense, d2_dense);

                self.graphs[target_graph_idx].set_weight_val(d_local, d2_local, w12 as u8);
                self.graphs[target_graph_idx].set_weight_val(d2_local, d_local, w21 as u8);

                let mut mask: u32 = 0;
                for r in 0..n {
                    if self.nodes[dense as usize].committed_ois[r].is_some()
                        && self.nodes[d2_dense as usize].committed_ois[r].is_some()
                    {
                        mask |= 1u32 << r;
                    }
                }
                let pidx = pair_idx(d_local, d2_local, cap);
                self.graphs[target_graph_idx].counted[pidx] = mask;

                if w12 >= ht || w21 >= ht {
                    if !self.graphs[target_graph_idx].has_edge(d_local, d2_local) {
                        if w12 >= w21 {
                            self.graphs[target_graph_idx].add_edge(d_local, d2_local);
                        } else {
                            self.graphs[target_graph_idx].add_edge(d2_local, d_local);
                        }
                    }
                } else {
                    let (lmin, lmax) = if d_local < d2_local {
                        (d_local, d2_local)
                    } else {
                        (d2_local, d_local)
                    };
                    self.graphs[target_graph_idx]
                        .missing_pairs
                        .push((lmin, lmax));
                }
            }
        }
    }

    // =========================================================================
    // Pending re-add processing
    // =========================================================================

    fn process_pending_readd(&mut self, graph_idx: usize) {
        if self.pending_readd.is_empty() {
            return;
        }

        let pending: Vec<u32> = self
            .pending_readd
            .drain(..)
            .filter(|&d| !self.ordered_digests.contains(&self.nodes[d as usize].digest))
            .collect();

        if !pending.is_empty() {
            self.readd_nodes_to_graph(pending, graph_idx);
        }
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
// Hamiltonian Path in strongly connected tournament
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