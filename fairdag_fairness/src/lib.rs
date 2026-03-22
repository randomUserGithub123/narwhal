// Copyright(C) FairDAG-RL Implementation
// Implements the Fairness Layer of FairDAG-RL (Sections 6.1–6.3 of the paper).
//
// Option C optimization for fair benchmarking against global_order.rs:
//   - Dense u16 index mapping (TxDigest → u16) eliminates hash overhead
//   - Nibble-packed weight matrices (matching global_order.rs representation)
//   - Adjacency-list edges with bitset existence check
//   - Pre-allocated Tarjan arrays
//   - Vec-based committed_ois/rounds (indexed by ReplicaIndex, size = N)
//   - u8 bitmask for counted_replicas (supports N ≤ 8)
//
// Multi-graph logic, catchup weights, re-add mechanism, and tournament
// checking are preserved exactly from the protocol (Figures 7, 8, 11).
//
// Protocol references in comments refer to:
//   Figure 7:  Transaction dissemination and DAG vertex proposal
//   Figure 8:  Dependency graph construction
//   Figure 11: Ordering finalization

use crypto::PublicKey;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

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

/// Sentinel value for "no local index assigned".
const NONE_LOCAL: u16 = u16::MAX;

/// Initial capacity for per-graph dense arrays. Graphs with more nodes will
/// panic. For N=4 experiments with moderate throughput, 2048 is generous.
const INITIAL_GRAPH_CAPACITY: usize = 2048;

// =============================================================================
// CommittedVertex / CommittedSubdag — input from consensus (unchanged)
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
// Node types (Section 6.2 — Adding nodes)
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeType {
    Blank,
    Shaded,
    Solid,
}

// =============================================================================
// Nibble-packed weight helpers (matching global_order.rs representation)
//
// Each weight value is a 4-bit nibble (0–15). Two nibbles per byte.
// Index 2k → low nibble of byte k.  Index 2k+1 → high nibble of byte k.
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
// Bitset helpers for has_edge_pair
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

/// Directed weight index: weight(i → j) in a matrix of width `cap`.
#[inline(always)]
fn w_idx(i: u16, j: u16, cap: usize) -> usize {
    (i as usize) * cap + (j as usize)
}

/// Symmetric pair index: for has_edge_pair and counted_replicas.
/// Always uses (min, max) ordering so (i,j) and (j,i) map to the same slot.
#[inline(always)]
fn pair_idx(i: u16, j: u16, cap: usize) -> usize {
    let (a, b) = if i < j { (i, j) } else { (j, i) };
    (a as usize) * cap + (b as usize)
}

// =============================================================================
// TransactionNode (Section 6.2)
//
// Optimized: committed_ois and committed_rounds are fixed-size Vecs indexed
// by ReplicaIndex, replacing HashMap<ReplicaIndex, _>. With N=4 this is a
// tight 4-element array — no hashing, no allocation.
// =============================================================================

#[derive(Clone, Debug)]
pub struct TransactionNode {
    pub digest: TxDigest,
    pub dense_idx: u16,
    pub node_type: NodeType,
    /// committed_ois[i] = ordering indicator from replica i (None if not yet received).
    pub committed_ois: Vec<Option<u64>>,
    /// committed_rounds[i] = the leader round in which replica i's OI was committed.
    pub committed_rounds: Vec<Option<Round>>,
    /// Index into FairnessLayer.graphs — the graph this node belongs to.
    pub graph_index: Option<usize>,
}

impl TransactionNode {
    fn new(digest: TxDigest, dense_idx: u16, n: usize) -> Self {
        TransactionNode {
            digest,
            dense_idx,
            node_type: NodeType::Blank,
            committed_ois: vec![None; n],
            committed_rounds: vec![None; n],
            graph_index: None,
        }
    }

    /// ap(d, r) := |{i | node(d).committed_rounds[i] <= r}|
    #[inline]
    fn appearance_count(&self, up_to_round: Round) -> usize {
        self.committed_rounds
            .iter()
            .filter(|r| matches!(r, Some(r) if *r <= up_to_round))
            .count()
    }
}

// =============================================================================
// DependencyGraph (Section 6.2)
//
// Optimized: per-graph local dense indices with:
//   - Nibble-packed weight matrix (cap × cap nibbles)
//   - Bitset for O(1) undirected-edge existence check
//   - Adjacency lists (Vec<Vec<u16>>) for directed edges
//   - u8 bitmask for counted_replicas (per symmetric pair)
// =============================================================================

pub struct DependencyGraph {
    pub round: Round,

    // --- Node management (local dense indices) ---
    pub node_count: usize,
    pub capacity: usize,
    /// local_idx → global dense idx.
    pub local_to_global: Vec<u16>,
    /// global dense idx → local_idx (NONE_LOCAL if not in this graph).
    /// Grows dynamically to cover the max global dense idx seen.
    pub global_to_local: Vec<u16>,

    // --- Weights: nibble-packed, cap × cap ---
    pub weight: Vec<u8>,

    // --- Edges ---
    /// Directed adjacency lists using local indices.
    pub edges: Vec<Vec<u16>>,
    /// Number of undirected pairs that have a directed edge.
    pub edge_pair_count: usize,
    /// Bitset: bit at pair_idx(i,j) is set if ANY directed edge exists
    /// between local i and local j.
    pub has_edge_pair: Vec<u64>,

    // --- Counted replicas ---
    /// u8 bitmask per symmetric pair: bit r is set if replica r's weight
    /// contribution has been counted for this pair.  Indexed by pair_idx.
    pub counted: Vec<u8>,

    // --- State ---
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
            counted: vec![0u8; cap * cap],
            finalized: false,
            final_order: Vec::new(),
        }
    }

    /// Ensure global_to_local is large enough for `global_idx`.
    #[inline]
    fn ensure_global_capacity(&mut self, global_idx: u16) {
        let needed = global_idx as usize + 1;
        if needed > self.global_to_local.len() {
            self.global_to_local.resize(needed, NONE_LOCAL);
        }
    }

    /// Add a node (by global dense idx) to this graph. Returns the local idx.
    fn add_node(&mut self, global_dense_idx: u16) -> u16 {
        self.ensure_global_capacity(global_dense_idx);

        let existing = self.global_to_local[global_dense_idx as usize];
        if existing != NONE_LOCAL {
            return existing;
        }

        let local_idx = self.node_count as u16;
        assert!(
            (local_idx as usize) < self.capacity,
            "DependencyGraph capacity {} exceeded — increase INITIAL_GRAPH_CAPACITY",
            self.capacity
        );

        self.local_to_global.push(global_dense_idx);
        self.global_to_local[global_dense_idx as usize] = local_idx;
        self.node_count += 1;
        local_idx
    }

    /// Look up the local index for a global dense idx.
    #[inline]
    fn get_local(&self, global_dense_idx: u16) -> Option<u16> {
        let g = global_dense_idx as usize;
        if g < self.global_to_local.len() {
            let l = self.global_to_local[g];
            if l != NONE_LOCAL { Some(l) } else { None }
        } else {
            None
        }
    }

    /// Tournament: m nodes ⇒ m*(m-1)/2 undirected edge pairs.
    #[inline]
    fn is_tournament(&self) -> bool {
        let m = self.node_count;
        if m < 2 { return m <= 1; }
        self.edge_pair_count == m * (m - 1) / 2
    }

    /// O(1) check: does any directed edge exist between local i and local j?
    #[inline]
    fn has_edge(&self, li: u16, lj: u16) -> bool {
        bit_get(&self.has_edge_pair, pair_idx(li, lj, self.capacity))
    }

    /// Add a directed edge from → to. Returns false if pair already has an edge.
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

    #[inline]
    fn get_counted(&self, li: u16, lj: u16) -> u8 {
        self.counted[pair_idx(li, lj, self.capacity)]
    }

    #[inline]
    fn set_counted(&mut self, li: u16, lj: u16, val: u8) {
        self.counted[pair_idx(li, lj, self.capacity)] = val;
    }

    /// Free heavy allocations after finalization to limit memory growth.
    fn release_memory(&mut self) {
        self.weight = Vec::new();
        self.has_edge_pair = Vec::new();
        self.counted = Vec::new();
        for e in &mut self.edges {
            *e = Vec::new();
        }
        self.edges = Vec::new();
        self.global_to_local = Vec::new();
    }
}

// =============================================================================
// FairnessLayer
// =============================================================================

pub struct FairnessLayer {
    pub n: usize,
    pub f: usize,
    solid_threshold: usize,
    half_threshold: usize,

    // --- Dense index mapping ---
    digest_to_dense: HashMap<TxDigest, u16>,
    dense_to_digest: Vec<TxDigest>,
    next_dense_idx: u16,

    // --- Node storage (indexed by dense idx) ---
    nodes: Vec<TransactionNode>,
    /// Fast ordered-check by dense idx.
    ordered: Vec<bool>,

    // --- Graphs ---
    graphs: Vec<DependencyGraph>,
    round_to_graph: HashMap<Round, usize>,

    output_sequence: Vec<TxDigest>,
    replica_indices: HashMap<PublicKey, ReplicaIndex>,

    use_hamiltonian_path: bool,
    /// Dense indices of nodes waiting to be re-added to the next graph.
    pending_readd: Vec<u16>,
}

impl FairnessLayer {
    pub fn new(committee_keys: Vec<PublicKey>, f: usize) -> Self {
        let n = committee_keys.len();
        let solid_threshold = n - f;
        let half_threshold = (n - f + 1) / 2;

        let replica_indices: HashMap<PublicKey, ReplicaIndex> = committee_keys
            .into_iter()
            .enumerate()
            .map(|(i, pk)| (pk, i))
            .collect();

        info!(
            "FairnessLayer initialized: n={}, f={}, solid_threshold={}, \
             half_threshold={}",
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
            nodes: Vec::new(),
            ordered: Vec::new(),
            graphs: Vec::new(),
            round_to_graph: HashMap::new(),
            output_sequence: Vec::new(),
            replica_indices,
            use_hamiltonian_path: false,
            pending_readd: Vec::new(),
        }
    }

    // =========================================================================
    // Dense index management
    // =========================================================================

    fn get_or_create_dense(&mut self, digest: TxDigest) -> u16 {
        if let Some(&idx) = self.digest_to_dense.get(&digest) {
            return idx;
        }
        let idx = self.next_dense_idx;
        self.next_dense_idx = self
            .next_dense_idx
            .checked_add(1)
            .expect("Dense index overflow (>65535 unique transactions)");
        self.digest_to_dense.insert(digest, idx);
        self.dense_to_digest.push(digest);
        self.nodes
            .push(TransactionNode::new(digest, idx, self.n));
        self.ordered.push(false);
        idx
    }

    // =========================================================================
    // Figure 8, Lines 1-2: On receive Ar
    // =========================================================================

    pub fn process_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<TxDigest> {
        let r = subdag.leader_round;

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

        // Figure 8, Line 2: Gr := NewGraph(), graphs.push(Gr)
        let graph_idx = self.graphs.len();
        self.graphs
            .push(DependencyGraph::new(r, INITIAL_GRAPH_CAPACITY));
        self.round_to_graph.insert(r, graph_idx);

        // Process pending re-add nodes from a prior finalization.
        self.process_pending_readd(graph_idx);

        // Figure 8, Lines 3-10
        let updated_nodes = self.update_nodes_from_subdag(subdag);

        // Figure 8, Lines 11-18
        let newly_classified = self.classify_and_add_nodes(r, graph_idx, &updated_nodes);

        // LIVENESS FIX: catch-up weights for newly classified nodes.
        self.compute_catchup_weights_for_new_nodes(graph_idx, &newly_classified);

        // Figure 8, Lines 19-39
        self.update_weights_and_edges(subdag);

        // Log graph states.
        for (gi, g) in self.graphs.iter().enumerate() {
            if !g.finalized && g.node_count > 0 {
                let expected = if g.node_count > 1 {
                    g.node_count * (g.node_count - 1) / 2
                } else {
                    0
                };
                info!(
                    "DIAG graph_state: G[{}] round={} nodes={} edges={}/{} is_tournament={}",
                    gi, g.round, g.node_count, g.edge_pair_count, expected, g.is_tournament()
                );
            }
        }

        // Figure 11, Line 41: OrderFinalization()
        self.try_finalize_all_graphs()
    }

    // =========================================================================
    // Figure 8, Lines 3-10: Find nodes updated with Ar
    // =========================================================================

    fn update_nodes_from_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<u16> {
        let mut updated_set: HashSet<u16> = HashSet::new();
        let r = subdag.leader_round;

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;
            for &(d, oi) in &vertex.ordering_entries {
                let dense = self.get_or_create_dense(d);
                if self.ordered[dense as usize] {
                    continue;
                }
                let node = &mut self.nodes[dense as usize];
                if node.committed_ois[i].is_none() {
                    node.committed_ois[i] = Some(oi);
                    node.committed_rounds[i] = Some(r);
                    updated_set.insert(dense);
                }
            }
        }

        let mut updated: Vec<u16> = updated_set.into_iter().collect();
        updated.sort_unstable();

        info!(
            "FairnessLayer: update_nodes round={} updated={}",
            r,
            updated.len()
        );
        updated
    }

    // =========================================================================
    // Figure 8, Lines 11-18: Classify nodes and add to Gr
    // =========================================================================

    fn classify_and_add_nodes(
        &mut self,
        r: Round,
        graph_idx: usize,
        updated_nodes: &[u16],
    ) -> Vec<u16> {
        let mut solid_count = 0usize;
        let mut shaded_count = 0usize;
        let mut blank_count = 0usize;
        let mut newly_classified: Vec<u16> = Vec::new();

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
    // LIVENESS FIX: Catch-up weight computation for newly classified nodes
    // =========================================================================

    fn compute_catchup_weights_for_new_nodes(
        &mut self,
        graph_idx: usize,
        newly_classified: &[u16],
    ) {
        if newly_classified.is_empty() {
            return;
        }

        let newly_set: HashSet<u16> = newly_classified.iter().copied().collect();
        let mut edges_added = 0usize;
        let mut weights_computed = 0usize;
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

                // For pairs between two newly classified: compute once (d < d2).
                if newly_set.contains(&d2_dense) && d_dense > d2_dense {
                    continue;
                }

                let (w12, w21) = self.calculate_pairwise_weight(d_dense, d2_dense);
                weights_computed += 1;

                self.graphs[graph_idx].set_weight_val(d_local, d2_local, w12 as u8);
                self.graphs[graph_idx].set_weight_val(d2_local, d_local, w21 as u8);

                // Build counted bitmask.
                let mut mask: u8 = 0;
                for r in 0..n {
                    if self.nodes[d_dense as usize].committed_ois[r].is_some()
                        && self.nodes[d2_dense as usize].committed_ois[r].is_some()
                    {
                        mask |= 1u8 << r;
                    }
                }
                self.graphs[graph_idx].set_counted(d_local, d2_local, mask);

                // Add edge if threshold met.
                if w12 >= ht || w21 >= ht {
                    if !self.graphs[graph_idx].has_edge(d_local, d2_local) {
                        if w12 >= w21 {
                            self.graphs[graph_idx].add_edge(d_local, d2_local);
                        } else {
                            self.graphs[graph_idx].add_edge(d2_local, d_local);
                        }
                        edges_added += 1;
                    }
                }
            }
        }

        info!(
            "FairnessLayer: catchup weights for {} new nodes in G[{}]: \
             pairs_computed={} edges_added={}",
            newly_classified.len(),
            graph_idx,
            weights_computed,
            edges_added
        );
    }

    // =========================================================================
    // Figure 8, Lines 19-39: Update weights and add edges
    // =========================================================================

    fn update_weights_and_edges(&mut self, subdag: &CommittedSubdag) {
        let mut addable_edges: Vec<(u16, u16, usize)> = Vec::new();

        let mut stat_pairs_checked: usize = 0;
        let mut stat_pairs_skipped_counted: usize = 0;
        let mut stat_pairs_skipped_edge: usize = 0;
        let mut stat_weights_incremented: usize = 0;
        let ht = self.half_threshold as u8;

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;

            for &(d, _oi) in &vertex.ordering_entries {
                let d_dense = match self.digest_to_dense.get(&d) {
                    Some(&idx) => idx,
                    None => continue,
                };
                if self.ordered[d_dense as usize] {
                    continue;
                }

                let g_idx = match self.nodes[d_dense as usize].graph_index {
                    Some(idx) => idx,
                    None => continue,
                };

                let d_oi = match self.nodes[d_dense as usize].committed_ois[i] {
                    Some(oi) => oi,
                    None => continue,
                };

                let d_local = self.graphs[g_idx].get_local(d_dense).unwrap();
                let node_count = self.graphs[g_idx].node_count;
                let cap = self.graphs[g_idx].capacity;

                for li in 0..node_count {
                    let d2_dense = self.graphs[g_idx].local_to_global[li];
                    if d2_dense == d_dense {
                        continue;
                    }
                    let d2_local = li as u16;

                    let d2_oi = match self.nodes[d2_dense as usize].committed_ois[i] {
                        Some(oi) => oi,
                        None => continue,
                    };

                    stat_pairs_checked += 1;

                    // Skip if edge already exists.
                    if self.graphs[g_idx].has_edge(d_local, d2_local) {
                        stat_pairs_skipped_edge += 1;
                        continue;
                    }

                    // Check counted bitmask.
                    let pidx = pair_idx(d_local, d2_local, cap);
                    let counted_mask = self.graphs[g_idx].counted[pidx];
                    if counted_mask & (1u8 << i) != 0 {
                        stat_pairs_skipped_counted += 1;
                        continue;
                    }
                    self.graphs[g_idx].counted[pidx] = counted_mask | (1u8 << i);

                    // Figure 8, Lines 27-30
                    if d_oi < d2_oi {
                        self.graphs[g_idx].inc_weight_val(d_local, d2_local);
                    } else {
                        self.graphs[g_idx].inc_weight_val(d2_local, d_local);
                    }
                    stat_weights_incremented += 1;

                    // Figure 8, Lines 31-32
                    let w_fwd = self.graphs[g_idx].get_weight_val(d_local, d2_local);
                    let w_rev = self.graphs[g_idx].get_weight_val(d2_local, d_local);
                    if w_fwd >= ht || w_rev >= ht {
                        let (lmin, lmax) = if d_local < d2_local {
                            (d_local, d2_local)
                        } else {
                            (d2_local, d_local)
                        };
                        addable_edges.push((lmin, lmax, g_idx));
                    }
                }
            }
        }

        // Figure 8, Lines 33-39
        let mut edges_added = 0usize;
        for &(l_min, l_max, g_idx) in &addable_edges {
            if self.graphs[g_idx].has_edge(l_min, l_max) {
                continue;
            }

            let w_fwd = self.graphs[g_idx].get_weight_val(l_min, l_max);
            let w_rev = self.graphs[g_idx].get_weight_val(l_max, l_min);

            if w_fwd >= w_rev {
                self.graphs[g_idx].add_edge(l_min, l_max);
            } else {
                self.graphs[g_idx].add_edge(l_max, l_min);
            }
            edges_added += 1;
        }

        info!(
            "FairnessLayer: weights pairs_checked={} skipped_counted={} skipped_edge={} \
             incremented={} edges_added={} half_threshold={}",
            stat_pairs_checked,
            stat_pairs_skipped_counted,
            stat_pairs_skipped_edge,
            stat_weights_incremented,
            edges_added,
            self.half_threshold
        );
    }

    // =========================================================================
    // Figure 11: OrderFinalization
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
                break;
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

        if !newly_ordered.is_empty() {
            info!(
                "FairnessLayer: finalized {} transactions this round",
                newly_ordered.len()
            );
        }

        newly_ordered
    }

    /// Figure 11, Lines 4-29: Finalize ordering for a tournament graph.
    fn finalize_ordering(&mut self, graph_idx: usize) -> Vec<TxDigest> {
        let node_count = self.graphs[graph_idx].node_count;

        // Figure 11, Line 5: Tarjan SCC
        let sccs = tarjan_scc_dense(node_count, &self.graphs[graph_idx].edges);

        // Figure 11, Line 6: Topological sort
        let topo_order =
            topological_sort_sccs_dense(&sccs, &self.graphs[graph_idx].edges, node_count);

        // Figure 11, Line 7: last SCC with a solid node
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
        let mut to_readd: Vec<u16> = Vec::new();

        match last_solid_pos {
            Some(cutoff) => {
                for (pos, &scc_idx) in topo_order.iter().enumerate() {
                    let scc = &sccs[scc_idx];
                    if pos <= cutoff {
                        // Figure 11, Line 9: Hamilton path (or lex fallback)
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
                            let dense =
                                self.graphs[graph_idx].local_to_global[li as usize];
                            ordered_digests.push(self.nodes[dense as usize].digest);
                        }
                    } else {
                        for &li in scc {
                            let dense =
                                self.graphs[graph_idx].local_to_global[li as usize];
                            to_readd.push(dense);
                        }
                    }
                }
            }
            None => {
                warn!(
                    "FairnessLayer: graph {} is a tournament but has no solid nodes — deferring all",
                    graph_idx
                );
                return Vec::new();
            }
        }

        // Mark finalized.
        self.graphs[graph_idx].finalized = true;
        self.graphs[graph_idx].final_order = ordered_digests.clone();

        for &d in &ordered_digests {
            let dense = self.digest_to_dense[&d];
            self.ordered[dense as usize] = true;
        }
        self.output_sequence.extend(&ordered_digests);

        // =====================================================================
        // Figure 11, Lines 13-29: Re-add shaded nodes
        // =====================================================================

        if !to_readd.is_empty() {
            let next_graph_idx = self.find_next_unfinalized_graph(graph_idx);

            match next_graph_idx {
                Some(next_idx) => {
                    info!(
                        "FairnessLayer: re-adding {} shaded nodes from graph {} to graph {} (round {})",
                        to_readd.len(), graph_idx, next_idx, self.graphs[next_idx].round
                    );
                    self.readd_nodes_to_graph(to_readd, next_idx);
                }
                None => {
                    info!(
                        "FairnessLayer: deferring {} shaded nodes from graph {} (no next graph yet)",
                        to_readd.len(), graph_idx
                    );
                    for &dense in &to_readd {
                        self.nodes[dense as usize].node_type = NodeType::Blank;
                        self.nodes[dense as usize].graph_index = None;
                    }
                    self.pending_readd.extend(to_readd);
                }
            }
        }

        // Release heavy graph memory.
        self.graphs[graph_idx].release_memory();

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
    // Figure 11, Lines 14-29: Re-add nodes to graph Gr'
    // =========================================================================

    fn readd_nodes_to_graph(&mut self, to_readd: Vec<u16>, target_graph_idx: usize) {
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
                // Per protocol: do NOT add to graph, defer to pending_readd.
                self.nodes[dense as usize].node_type = NodeType::Blank;
                self.nodes[dense as usize].graph_index = None;
                self.pending_readd.push(dense);
                continue;
            }

            self.nodes[dense as usize].graph_index = Some(target_graph_idx);
            let d_local = self.graphs[target_graph_idx].add_node(dense);

            // Calculate weights and edges with all existing nodes.
            let node_count = self.graphs[target_graph_idx].node_count;
            for li in 0..node_count {
                let d2_dense = self.graphs[target_graph_idx].local_to_global[li];
                if d2_dense == dense {
                    continue;
                }
                let d2_local = li as u16;

                let (w12, w21) = self.calculate_pairwise_weight(dense, d2_dense);

                self.graphs[target_graph_idx].set_weight_val(d_local, d2_local, w12 as u8);
                self.graphs[target_graph_idx].set_weight_val(d2_local, d_local, w21 as u8);

                // Set counted bitmask so incremental updates don't double-count.
                let mut mask: u8 = 0;
                for r in 0..n {
                    if self.nodes[dense as usize].committed_ois[r].is_some()
                        && self.nodes[d2_dense as usize].committed_ois[r].is_some()
                    {
                        mask |= 1u8 << r;
                    }
                }
                self.graphs[target_graph_idx].set_counted(d_local, d2_local, mask);

                // Add edge if threshold met.
                if w12 >= ht || w21 >= ht {
                    if !self.graphs[target_graph_idx].has_edge(d_local, d2_local) {
                        if w12 >= w21 {
                            self.graphs[target_graph_idx].add_edge(d_local, d2_local);
                        } else {
                            self.graphs[target_graph_idx].add_edge(d2_local, d_local);
                        }
                    }
                }
            }
        }
    }

    // =========================================================================
    // Pairwise weight calculation
    // =========================================================================

    fn calculate_pairwise_weight(&self, dense1: u16, dense2: u16) -> (usize, usize) {
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
    // Pending re-add processing
    // =========================================================================

    fn process_pending_readd(&mut self, graph_idx: usize) {
        if self.pending_readd.is_empty() {
            return;
        }

        let pending: Vec<u16> = self
            .pending_readd
            .drain(..)
            .filter(|&d| !self.ordered[d as usize])
            .collect();

        if !pending.is_empty() {
            info!(
                "FairnessLayer: processing {} pending re-add nodes into graph {} (round {})",
                pending.len(),
                graph_idx,
                self.graphs[graph_idx].round
            );
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
        self.nodes
            .iter()
            .filter(|n| !self.ordered[n.dense_idx as usize])
            .count()
    }

    pub fn replica_index(&self, pk: &PublicKey) -> Option<ReplicaIndex> {
        self.replica_indices.get(pk).copied()
    }
}

// =============================================================================
// Tarjan's SCC — iterative, pre-allocated arrays, local dense indices
//
// Produces SCCs in topological order (reversed from standard Tarjan output).
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
                    let p_usize = parent as usize;
                    if v_low < low[p_usize] {
                        low[p_usize] = v_low;
                    }
                }
            }
        }
    }

    sccs.reverse();
    sccs
}

// =============================================================================
// Topological sort of SCCs (Kahn's algorithm, deterministic tie-breaking)
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
// Hamiltonian Path in a tournament SCC (Figure 11, Line 9)
//
// Standard greedy insertion algorithm. Uses adjacency-list edge lookup.
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
                warn!(
                    "Hamiltonian path: could not find insertion point for {} — appending",
                    v
                );
                path.push_back(v);
            }
        }
    }

    path.into_iter().collect()
}