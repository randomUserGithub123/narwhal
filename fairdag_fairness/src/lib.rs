// Copyright(C) FairDAG-RL Implementation
// v5: Explicit Missing Edge Updates with Independent Graphs
//
// Key changes from v4 (implicit edge updates):
//
// 1. EXPLICIT EDGE UPDATES: Instead of silently carrying over OIs across
//    graphs, missing edges are resolved via an explicit protocol:
//    - When a graph has missing edges, FairnessLayer emits a
//      MissingEdgeRequest to the BatchMaker.
//    - Each replica produces a MissingEdgeUpdate (lz4-compressed in batches).
//    - Once n-f replica updates are collected, missing edges are resolved.
//
// 2. INDEPENDENT GRAPHS: Each dependency graph is independent. Nodes are NOT
//    re-added from one graph to the next via "carrying over" OIs. Instead,
//    shaded nodes after the last solid SCC are moved to the next graph with
//    fresh weight computation from explicit updates only.
//
// 3. PARALLEL GRAPH CONSTRUCTION: Since graphs are independent, new graphs
//    can be constructed in parallel (rayon). Finalization is still sequential
//    in round-increasing order (protocol requirement).
//
// 4. COMPRESSION: MissingEdgeUpdates are lz4-compressed in batches.

use crypto::PublicKey;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

// Re-export the missing edge types used by workers.
pub use crate::missing_edge_types::{
    EdgeDirection, FairnessToWorkerMessage, GraphId, GraphResolved,
    MissingEdgeRequest, MissingEdgeUpdate, PairwiseVote,
};

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
// DependencyGraph — now tracks explicit edge update votes
// =============================================================================

pub struct DependencyGraph {
    pub round: Round,
    pub graph_id: GraphId,
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

    // v5: Explicit edge update tracking
    /// Number of distinct replica updates received for this graph.
    pub replica_updates_received: HashSet<usize>,
    /// Whether we've already sent a MissingEdgeRequest for this graph.
    pub request_sent: bool,
    /// Whether this graph has been resolved (all missing edges filled via
    /// explicit updates reaching n-f threshold).
    pub resolved: bool,
    /// Accumulated explicit edge votes: (d1, d2) → Vec of (replica_index, direction).
    /// Canonical key: d1 < d2.
    pub explicit_votes: HashMap<(TxDigest, TxDigest), Vec<(usize, EdgeDirection)>>,
}

impl DependencyGraph {
    fn new(round: Round, capacity: usize) -> Self {
        let cap = capacity;
        let nibble_bytes = (cap * cap + 1) / 2;
        let bit_words = (cap * cap + 63) / 64;
        DependencyGraph {
            round,
            graph_id: round, // graph_id = leader_round
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
            replica_updates_received: HashSet::new(),
            request_sent: false,
            resolved: false,
            explicit_votes: HashMap::new(),
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
        self.explicit_votes = HashMap::new();
        for e in &mut self.edges { *e = Vec::new(); }
        self.edges = Vec::new();
        self.global_to_local = Vec::new();
    }
}

// =============================================================================
// FairnessLayer — v5 with explicit edge updates
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
    /// graph_id (= leader_round) → index in self.graphs
    graph_id_to_idx: HashMap<GraphId, usize>,

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
        let non_blank_threshold = (((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0) * 1e10).round() / 1e10;
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
            n, f, solid_threshold, half_threshold,
            digest_to_dense: HashMap::new(),
            dense_to_digest: Vec::new(),
            next_dense_idx: 0,
            free_list: Vec::new(),
            nodes: Vec::new(),
            ordered_digests: HashSet::new(),
            graphs: Vec::new(),
            graph_id_to_idx: HashMap::new(),
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
                panic!("FATAL: Dense index overflow.");
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
    // PUBLIC API: Backward compatible single subdag
    // =========================================================================

    pub fn process_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<TxDigest> {
        let (ordered, _msgs) = self.process_subdag_batch_explicit(
            &[subdag.clone()], &[],
        );
        ordered
    }

    // =========================================================================
    // PUBLIC API: Batch processing with explicit edge updates
    //
    // Returns (ordered_txs, messages_for_batchmaker)
    // =========================================================================

    pub fn process_subdag_batch_explicit(
        &mut self,
        subdags: &[CommittedSubdag],
        edge_updates: &[MissingEdgeUpdate],
    ) -> (Vec<TxDigest>, Vec<FairnessToWorkerMessage>) {
        if subdags.is_empty() && edge_updates.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let total_start = Instant::now();
        self.batch_count += 1;
        let batch_id = self.batch_count;

        info!(
            "FAIRDAG_PERF: batch={} phase=start subdags={} edge_updates={} \
             active_graphs={}",
            batch_id, subdags.len(), edge_updates.len(),
            self.graphs.iter().filter(|g| !g.finalized).count(),
        );

        let mut messages: Vec<FairnessToWorkerMessage> = Vec::new();

        // =====================================================================
        // Phase 1: Ingest subdags — create new graphs, update nodes, classify
        // =====================================================================
        let t_ingest = Instant::now();
        struct IngestResult {
            graph_idx: usize,
            newly_classified: Vec<u32>,
        }
        let mut work_items: Vec<IngestResult> = Vec::new();

        for subdag in subdags {
            self.subdag_count += 1;
            let (graph_idx, newly_classified) = self.ingest_subdag(subdag);
            work_items.push(IngestResult { graph_idx, newly_classified });
        }
        let t_ingest_ms = t_ingest.elapsed().as_micros();

        // =====================================================================
        // Phase 2: Compute initial weights for newly classified nodes.
        //          This uses ONLY the OIs available from the current subdag
        //          (no cross-graph carrying). Missing pairs are tracked.
        // =====================================================================
        let t_catchup = Instant::now();
        for item in &work_items {
            if !item.newly_classified.is_empty() {
                self.compute_catchup_weights_for_new_nodes(
                    item.graph_idx, &item.newly_classified,
                );
            }
        }
        let t_catchup_ms = t_catchup.elapsed().as_micros();

        // =====================================================================
        // Phase 3: Apply explicit edge updates from committed batches.
        //          Each update is a replica's pairwise votes for a specific graph.
        // =====================================================================
        let t_apply = Instant::now();
        let mut updates_applied = 0usize;
        let mut edges_resolved_from_updates = 0usize;

        for update in edge_updates {
            let resolved = self.apply_explicit_edge_update(update);
            if resolved > 0 {
                edges_resolved_from_updates += resolved;
            }
            updates_applied += 1;
        }
        let t_apply_ms = t_apply.elapsed().as_micros();

        // =====================================================================
        // Phase 4: For new graphs with missing pairs, emit MissingEdgeRequests
        //          (only if not already sent).
        // =====================================================================
        let t_request = Instant::now();
        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized { continue; }
            if self.graphs[g_idx].request_sent { continue; }
            if self.graphs[g_idx].missing_pairs.is_empty() { continue; }

            let request = self.build_missing_edge_request(g_idx);
            self.graphs[g_idx].request_sent = true;

            info!(
                "FairnessLayer: emitting MissingEdgeRequest for graph {} \
                 (round {}) with {} pairs, {} txs",
                self.graphs[g_idx].graph_id,
                self.graphs[g_idx].round,
                request.missing_pairs.len(),
                request.missing_tx_digests.len(),
            );

            messages.push(FairnessToWorkerMessage::MissingEdgeRequest(request));
        }
        let t_request_ms = t_request.elapsed().as_micros();

        // =====================================================================
        // Phase 5: Try to resolve missing edges from explicit votes that have
        //          reached n-f threshold, then finalize tournament graphs.
        // =====================================================================
        let t_resolve = Instant::now();
        let resolved_from_votes = self.resolve_edges_from_votes();
        let t_resolve_ms = t_resolve.elapsed().as_micros();

        // =====================================================================
        // Phase 6: Finalize graphs in round-increasing order.
        // =====================================================================
        let t_finalize = Instant::now();
        let (ordered, resolved_graph_ids) = self.try_finalize_all_graphs();

        // Emit GraphResolved for any newly finalized graphs.
        for graph_id in &resolved_graph_ids {
            messages.push(FairnessToWorkerMessage::GraphResolved(GraphResolved {
                graph_id: *graph_id,
            }));
        }
        let t_finalize_ms = t_finalize.elapsed().as_micros();

        let t_total = total_start.elapsed().as_micros();

        info!(
            "FAIRDAG_PERF: batch={} phase=done total_us={} ingest_us={} \
             catchup_us={} apply_us={} request_us={} resolve_us={} \
             finalize_us={} updates_applied={} edges_from_updates={} \
             edges_from_votes={} finalized={} messages_out={}",
            batch_id, t_total, t_ingest_ms, t_catchup_ms, t_apply_ms,
            t_request_ms, t_resolve_ms, t_finalize_ms,
            updates_applied, edges_resolved_from_updates,
            resolved_from_votes, ordered.len(), messages.len(),
        );

        (ordered, messages)
    }

    // =========================================================================
    // Ingest a single subdag: create graph, update nodes, classify
    // Returns (graph_idx, newly_classified_dense_indices)
    // =========================================================================

    fn ingest_subdag(&mut self, subdag: &CommittedSubdag) -> (usize, Vec<u32>) {
        let r = subdag.leader_round;

        let graph_idx = self.graphs.len();
        self.graphs.push(DependencyGraph::new(r, INITIAL_GRAPH_CAPACITY));
        self.graph_id_to_idx.insert(r, graph_idx);

        // Process pending readd from prior finalizations.
        self.process_pending_readd(graph_idx);

        // Update nodes from subdag vertices.
        let updated_nodes = self.update_nodes_from_subdag(subdag);

        // Classify blank nodes.
        let newly_classified = self.classify_and_add_nodes(r, graph_idx, &updated_nodes);

        info!(
            "FAIRDAG_PERF: ingest round={} graph_idx={} updated={} \
             newly_classified={} graph_nodes={}",
            r, graph_idx, updated_nodes.len(), newly_classified.len(),
            self.graphs[graph_idx].node_count,
        );

        (graph_idx, newly_classified)
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
    // =========================================================================

    fn classify_and_add_nodes(
        &mut self, r: Round, graph_idx: usize, updated_nodes: &[u32],
    ) -> Vec<u32> {
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
            } else if ap >= self.half_threshold {
                self.nodes[dense as usize].node_type = NodeType::Shaded;
                self.nodes[dense as usize].graph_index = Some(graph_idx);
                self.graphs[graph_idx].add_node(dense);
                newly_classified.push(dense);
            }
        }

        newly_classified
    }

    // =========================================================================
    // Catchup weights for newly classified nodes (within same graph only)
    // =========================================================================

    fn compute_catchup_weights_for_new_nodes(
        &mut self, graph_idx: usize, newly_classified: &[u32],
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
                if d2_dense == d_dense { continue; }
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
    // Build a MissingEdgeRequest for a graph
    // =========================================================================

    fn build_missing_edge_request(&self, graph_idx: usize) -> MissingEdgeRequest {
        let g = &self.graphs[graph_idx];
        let mut tx_digests_set: HashSet<TxDigest> = HashSet::new();
        let mut missing_pairs_digests: Vec<(TxDigest, TxDigest)> = Vec::new();

        for &(li, lj) in &g.missing_pairs {
            if g.has_edge(li, lj) { continue; } // already resolved

            let di = self.nodes[g.local_to_global[li as usize] as usize].digest;
            let dj = self.nodes[g.local_to_global[lj as usize] as usize].digest;

            // Canonical ordering: smaller digest first
            let (d1, d2) = if di < dj { (di, dj) } else { (dj, di) };
            tx_digests_set.insert(d1);
            tx_digests_set.insert(d2);
            missing_pairs_digests.push((d1, d2));
        }

        MissingEdgeRequest {
            graph_id: g.graph_id,
            missing_tx_digests: tx_digests_set.into_iter().collect(),
            missing_pairs: missing_pairs_digests,
        }
    }

    // =========================================================================
    // Apply an explicit edge update from a replica
    // Returns number of edges resolved
    // =========================================================================

    fn apply_explicit_edge_update(&mut self, update: &MissingEdgeUpdate) -> usize {
        let graph_idx = match self.graph_id_to_idx.get(&update.graph_id) {
            Some(&idx) => idx,
            None => {
                warn!(
                    "FairnessLayer: received edge update for unknown graph {}",
                    update.graph_id
                );
                return 0;
            }
        };

        if self.graphs[graph_idx].finalized || self.graphs[graph_idx].resolved {
            return 0;
        }

        // Record that this replica has submitted an update.
        self.graphs[graph_idx]
            .replica_updates_received
            .insert(update.replica_index);

        // Accumulate votes.
        for vote in &update.votes {
            // Canonical key: d1 < d2
            let key = if vote.d1 < vote.d2 {
                (vote.d1, vote.d2)
            } else {
                (vote.d2, vote.d1)
            };

            // Determine direction relative to canonical key
            let direction = if vote.d1 < vote.d2 {
                vote.direction.clone()
            } else {
                // Flip direction since we swapped d1/d2
                match &vote.direction {
                    EdgeDirection::Forward => EdgeDirection::Reverse,
                    EdgeDirection::Reverse => EdgeDirection::Forward,
                    EdgeDirection::Unknown => EdgeDirection::Unknown,
                }
            };

            let entry = self.graphs[graph_idx]
                .explicit_votes
                .entry(key)
                .or_insert_with(Vec::new);

            // Only add if this replica hasn't voted for this pair yet.
            if !entry.iter().any(|(ri, _)| *ri == update.replica_index) {
                entry.push((update.replica_index, direction));
            }
        }

        info!(
            "FairnessLayer: applied edge update from replica {} for graph {} \
             ({} votes, {}/{} replicas received)",
            update.replica_index,
            update.graph_id,
            update.votes.len(),
            self.graphs[graph_idx].replica_updates_received.len(),
            self.n,
        );

        0 // Actual resolution happens in resolve_edges_from_votes
    }

    // =========================================================================
    // Resolve edges from accumulated explicit votes
    // An edge can be added when n-f replicas have voted for a pair.
    // Returns total edges resolved.
    // =========================================================================

    fn resolve_edges_from_votes(&mut self) -> usize {
        let n = self.n;
        let f = self.f;
        let vote_threshold = n - f; // n-f replicas needed
        let ht = self.half_threshold;
        let mut total_resolved = 0usize;

        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized { continue; }
            if self.graphs[g_idx].explicit_votes.is_empty() { continue; }

            let mut resolved_positions: Vec<usize> = Vec::new();

            for (pair_pos, &(li, lj)) in self.graphs[g_idx].missing_pairs.iter().enumerate() {
                if self.graphs[g_idx].has_edge(li, lj) {
                    resolved_positions.push(pair_pos);
                    continue;
                }

                let di_dense = self.graphs[g_idx].local_to_global[li as usize];
                let dj_dense = self.graphs[g_idx].local_to_global[lj as usize];
                let di = self.nodes[di_dense as usize].digest;
                let dj = self.nodes[dj_dense as usize].digest;

                let key = if di < dj { (di, dj) } else { (dj, di) };

                if let Some(votes) = self.graphs[g_idx].explicit_votes.get(&key) {
                    if votes.len() >= vote_threshold {
                        // Count Forward vs Reverse votes.
                        let mut fwd_count = 0usize; // d1 → d2 (canonical)
                        let mut rev_count = 0usize; // d2 → d1

                        for (_, direction) in votes {
                            match direction {
                                EdgeDirection::Forward => fwd_count += 1,
                                EdgeDirection::Reverse => rev_count += 1,
                                EdgeDirection::Unknown => {}
                            }
                        }

                        // Determine which local index is d1 (the smaller digest)
                        let (li_d1, li_d2) = if di < dj { (li, lj) } else { (lj, li) };

                        if fwd_count >= ht || rev_count >= ht {
                            if fwd_count >= rev_count {
                                // d1 → d2 (canonical forward)
                                self.graphs[g_idx].add_edge(li_d1, li_d2);
                            } else {
                                // d2 → d1
                                self.graphs[g_idx].add_edge(li_d2, li_d1);
                            }
                            resolved_positions.push(pair_pos);
                            total_resolved += 1;
                        } else if fwd_count + rev_count >= vote_threshold {
                            // We have enough votes total but neither direction
                            // meets the half_threshold. Add edge by majority.
                            if fwd_count >= rev_count {
                                self.graphs[g_idx].add_edge(li_d1, li_d2);
                            } else {
                                self.graphs[g_idx].add_edge(li_d2, li_d1);
                            }
                            resolved_positions.push(pair_pos);
                            total_resolved += 1;
                        }
                    }
                }
            }

            // Remove resolved pairs (reverse order to maintain indices).
            resolved_positions.sort_unstable();
            resolved_positions.dedup();
            for &pos in resolved_positions.iter().rev() {
                self.graphs[g_idx].missing_pairs.swap_remove(pos);
            }

            // Check if graph is now a tournament.
            if self.graphs[g_idx].is_tournament() && !self.graphs[g_idx].resolved {
                self.graphs[g_idx].resolved = true;
                info!(
                    "FairnessLayer: graph {} (round {}) resolved to tournament \
                     via explicit edge updates ({} replicas contributed)",
                    g_idx, self.graphs[g_idx].round,
                    self.graphs[g_idx].replica_updates_received.len(),
                );
            }
        }

        total_resolved
    }

    // =========================================================================
    // Finalization (sequential, round-increasing order)
    // Returns (ordered_digests, resolved_graph_ids)
    // =========================================================================

    fn try_finalize_all_graphs(&mut self) -> (Vec<TxDigest>, Vec<GraphId>) {
        let mut newly_ordered: Vec<TxDigest> = Vec::new();
        let mut resolved_ids: Vec<GraphId> = Vec::new();

        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized { continue; }
            if self.graphs[g_idx].node_count == 0 {
                self.graphs[g_idx].finalized = true;
                resolved_ids.push(self.graphs[g_idx].graph_id);
                continue;
            }

            if !self.graphs[g_idx].is_tournament() {
                info!(
                    "FAIRDAG_PERF: finalize_blocked G[{}] round={} nodes={} \
                     edges={}/{} missing_pairs={} replicas_heard={}",
                    g_idx, self.graphs[g_idx].round,
                    self.graphs[g_idx].node_count,
                    self.graphs[g_idx].edge_pair_count,
                    self.graphs[g_idx].node_count * (self.graphs[g_idx].node_count - 1) / 2,
                    self.graphs[g_idx].missing_pairs.len(),
                    self.graphs[g_idx].replica_updates_received.len(),
                );
                break; // Must finalize in order
            }

            info!(
                "FairnessLayer: graph {} (round {}) is a tournament with {} \
                 nodes — finalizing",
                g_idx, self.graphs[g_idx].round, self.graphs[g_idx].node_count
            );

            let order = self.finalize_ordering(g_idx);
            newly_ordered.extend(&order);
            resolved_ids.push(self.graphs[g_idx].graph_id);
        }

        (newly_ordered, resolved_ids)
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
            if has_solid { last_solid_pos = Some(pos); }
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

        // Shaded nodes after last solid SCC → move to next graph (independent).
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

        info!(
            "FairnessLayer: finalized {} transactions from graph {} (round {}). \
             Total ordered: {}",
            ordered_digests.len(), graph_idx,
            self.graphs[graph_idx].round, self.output_sequence.len()
        );

        ordered_digests
    }

    fn find_next_unfinalized_graph(&self, after_idx: usize) -> Option<usize> {
        for idx in (after_idx + 1)..self.graphs.len() {
            if !self.graphs[idx].finalized { return Some(idx); }
        }
        None
    }

    // =========================================================================
    // Re-add nodes to graph (independent: fresh weight computation)
    // =========================================================================

    fn readd_nodes_to_graph(&mut self, to_readd: Vec<u32>, target_graph_idx: usize) {
        let r_prime = self.graphs[target_graph_idx].round;
        let ht = self.half_threshold;

        for &dense in &to_readd {
            let ap = self.nodes[dense as usize].appearance_count(r_prime);

            if ap >= self.solid_threshold {
                self.nodes[dense as usize].node_type = NodeType::Solid;
            } else if ap >= ht {
                self.nodes[dense as usize].node_type = NodeType::Shaded;
            } else {
                self.nodes[dense as usize].node_type = NodeType::Blank;
                self.nodes[dense as usize].graph_index = None;
                self.pending_readd.push(dense);
                continue;
            }

            self.nodes[dense as usize].graph_index = Some(target_graph_idx);
            let d_local = self.graphs[target_graph_idx].add_node(dense);

            let node_count = self.graphs[target_graph_idx].node_count;
            let cap = self.graphs[target_graph_idx].capacity;
            let n = self.n;

            for li in 0..node_count {
                let d2_dense = self.graphs[target_graph_idx].local_to_global[li];
                if d2_dense == dense { continue; }
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
                    self.graphs[target_graph_idx].missing_pairs.push((lmin, lmax));
                }
            }
        }
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
                if oi1 < oi2 { w12 += 1; } else { w21 += 1; }
            }
        }
        (w12, w21)
    }

    // =========================================================================
    // Pending re-add processing
    // =========================================================================

    fn process_pending_readd(&mut self, graph_idx: usize) {
        if self.pending_readd.is_empty() { return; }

        let pending: Vec<u32> = self.pending_readd.drain(..)
            .filter(|&d| !self.ordered_digests.contains(&self.nodes[d as usize].digest))
            .collect();

        if !pending.is_empty() {
            self.readd_nodes_to_graph(pending, graph_idx);
        }
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
        if dfn[start] != 0 { continue; }
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
                        if w == v { break; }
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
    sccs: &[Vec<u16>], edges: &[Vec<u16>], node_count: usize,
) -> Vec<usize> {
    let mut node_to_scc = vec![0usize; node_count];
    for (scc_idx, scc) in sccs.iter().enumerate() {
        for &node in scc { node_to_scc[node as usize] = scc_idx; }
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
    for s in initial { ready.push_back(s); }

    let mut result: Vec<usize> = Vec::with_capacity(scc_n);
    while let Some(s) = ready.pop_front() {
        result.push(s);
        let mut new_ready: Vec<usize> = Vec::new();
        for &v in &adj[s] {
            in_degree[v] -= 1;
            if in_degree[v] == 0 { new_ready.push(v); }
        }
        new_ready.sort_unstable();
        for v in new_ready { ready.push_back(v); }
    }
    result
}

// =============================================================================
// Hamiltonian Path
// =============================================================================

fn hamiltonian_path_dense(scc: &[u16], edges: &[Vec<u16>]) -> Vec<u16> {
    if scc.len() <= 1 { return scc.to_vec(); }

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
                    v, scc.len()
                );
            }
        }
    }
    path.into_iter().collect()
}