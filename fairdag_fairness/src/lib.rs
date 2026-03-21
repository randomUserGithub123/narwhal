// Copyright(C) FairDAG-RL Implementation
// Implements the Fairness Layer of FairDAG-RL (Sections 6.1–6.3 of the paper).
//
// This module receives committed subdags from the DAG/consensus layer and:
//   1. Constructs dependency graphs with pairwise ordering weights
//   2. Classifies transaction nodes as solid/shaded/blank
//   3. Adds directed edges when weights reach the quorum threshold
//   4. Finalizes ordering when a dependency graph becomes a tournament
//      (via SCC condensation + topological sort)

use crypto::PublicKey;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

// =============================================================================
// Types
// =============================================================================

/// A transaction digest represented as a u64 unique identifier.
pub type TxDigest = u64;

/// A DAG round number.
pub type Round = u64;

/// Index identifying a replica (derived from sorted PublicKey order).
pub type ReplicaIndex = usize;

/// An ordering entry: (tx_digest, ordering_indicator_value).
pub type OrderingEntry = (TxDigest, u64);

// =============================================================================
// Vertex — a single vertex in a committed subdag
// =============================================================================

/// A committed vertex from the DAG layer, carrying its local ordering slice.
#[derive(Clone, Debug)]
pub struct CommittedVertex {
    /// The replica (author) of this vertex.
    pub replica: PublicKey,
    /// The replica's index (position in sorted committee keys).
    pub replica_index: ReplicaIndex,
    /// The round of this vertex.
    pub round: Round,
    /// The local ordering entries: (tx_digest, ordering_indicator).
    pub ordering_entries: Vec<(TxDigest, u64)>,
}

/// A committed subdag A_r — the set of vertices newly committed when
/// leader L_r is committed.
#[derive(Clone, Debug)]
pub struct CommittedSubdag {
    /// The leader round that triggered this commit.
    pub leader_round: Round,
    /// All vertices in this subdag, sorted by round (ascending).
    pub vertices: Vec<CommittedVertex>,
}

// =============================================================================
// Node types (Section 6.2 — "Adding nodes")
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeType {
    /// Has not been added to any dependency graph yet.
    Blank,
    /// ap(d,r) >= (n-f)/2 but < n-f
    Shaded,
    /// ap(d,r) >= n-f
    Solid,
}

// =============================================================================
// TransactionNode — per-digest metadata (Figure 8, Lines 3-10)
// =============================================================================

#[derive(Clone, Debug)]
pub struct TransactionNode {
    /// The transaction digest.
    pub digest: TxDigest,
    /// Node classification.
    pub node_type: NodeType,
    /// committed_ois[i] = ordering indicator from replica i for this tx.
    /// None if replica i has not yet committed an OI for this tx.
    pub committed_ois: HashMap<ReplicaIndex, u64>,
    /// committed_rounds[i] = the round in which replica i's OI was committed.
    pub committed_rounds: HashMap<ReplicaIndex, Round>,
    /// The index of the dependency graph this node belongs to (None if blank).
    pub graph_index: Option<usize>,
}

impl TransactionNode {
    fn new(digest: TxDigest) -> Self {
        TransactionNode {
            digest,
            node_type: NodeType::Blank,
            committed_ois: HashMap::new(),
            committed_rounds: HashMap::new(),
            graph_index: None,
        }
    }

    /// ap(d, r) = number of ordering indicators committed by round r.
    fn appearance_count(&self, up_to_round: Round) -> usize {
        self.committed_rounds
            .values()
            .filter(|&&r| r <= up_to_round)
            .count()
    }
}

// =============================================================================
// DependencyGraph — one per committed leader round (Figure 8, Line 2)
// =============================================================================

#[derive(Clone, Debug)]
pub struct DependencyGraph {
    /// The leader round that created this graph.
    pub round: Round,
    /// Set of transaction digest nodes in this graph.
    pub nodes: HashSet<TxDigest>,
    /// Pairwise weights: weight[(d1, d2)] = number of replicas preferring d1 before d2.
    pub weights: HashMap<(TxDigest, TxDigest), usize>,
    /// Directed edges in the dependency graph.
    pub edges: HashSet<(TxDigest, TxDigest)>,
    /// Whether ordering has been finalized for this graph.
    pub finalized: bool,
    /// The finalized ordering (populated after finalization).
    pub final_order: Vec<TxDigest>,
}

impl DependencyGraph {
    fn new(round: Round) -> Self {
        DependencyGraph {
            round,
            nodes: HashSet::new(),
            weights: HashMap::new(),
            edges: HashSet::new(),
            finalized: false,
            final_order: Vec::new(),
        }
    }

    /// Check if the graph is a tournament: every pair of nodes has exactly one directed edge.
    fn is_tournament(&self) -> bool {
        let n = self.nodes.len();
        if n < 2 {
            return n <= 1; // 0 or 1 nodes → trivially a tournament
        }
        // A tournament on n nodes has exactly n*(n-1)/2 edges
        let expected_edges = n * (n - 1) / 2;
        if self.edges.len() != expected_edges {
            return false;
        }
        // Verify: for every pair, exactly one direction exists
        let nodes_vec: Vec<TxDigest> = self.nodes.iter().cloned().collect();
        for i in 0..nodes_vec.len() {
            for j in (i + 1)..nodes_vec.len() {
                let a = nodes_vec[i];
                let b = nodes_vec[j];
                let has_ab = self.edges.contains(&(a, b));
                let has_ba = self.edges.contains(&(b, a));
                if !(has_ab ^ has_ba) {
                    return false;
                }
            }
        }
        true
    }
}

// =============================================================================
// FairnessLayer — the main orchestrator (Figure 8)
// =============================================================================

pub struct FairnessLayer {
    /// Total number of replicas (n).
    pub n: usize,
    /// Maximum number of faulty replicas (f).
    pub f: usize,
    /// Threshold for solid nodes: n - f.
    solid_threshold: usize,
    /// Threshold for shaded nodes and edge weights: ceil((n - f) / 2).
    half_threshold: usize,

    /// All transaction nodes, keyed by digest.
    /// Persists across rounds — nodes accumulate committed_ois over time.
    nodes: HashMap<TxDigest, TransactionNode>,

    /// All dependency graphs, indexed sequentially.
    graphs: Vec<DependencyGraph>,

    /// Mapping from leader round → graph index.
    round_to_graph: HashMap<Round, usize>,

    /// Set of transaction digests that have already been output in final ordering.
    ordered_digests: HashSet<TxDigest>,

    /// The complete output sequence of ordered transactions.
    output_sequence: Vec<TxDigest>,

    /// Mapping from PublicKey → ReplicaIndex.
    replica_indices: HashMap<PublicKey, ReplicaIndex>,
}

impl FairnessLayer {
    /// Create a new FairnessLayer.
    ///
    /// `committee_keys` should be the sorted list of all replica public keys.
    /// `f` is the maximum number of Byzantine/crash-faulty replicas.
    pub fn new(committee_keys: Vec<PublicKey>, f: usize) -> Self {
        let n = committee_keys.len();
        let solid_threshold = n - f;                        // n - f
        let half_threshold = (n - f + 1) / 2;              // ceil((n-f)/2)

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
            n,
            f,
            solid_threshold,
            half_threshold,
            nodes: HashMap::new(),
            graphs: Vec::new(),
            round_to_graph: HashMap::new(),
            ordered_digests: HashSet::new(),
            output_sequence: Vec::new(),
            replica_indices,
        }
    }

    /// Process a committed subdag A_r.
    /// This is the main entry point, called each time a leader is committed.
    /// Returns any newly finalized transaction orderings.
    ///
    /// Implements Figure 8 of the paper.
    pub fn process_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<TxDigest> {
        let r = subdag.leader_round;

        // Log every vertex's contents
        for (vi, vertex) in subdag.vertices.iter().enumerate() {
            info!(
                "DIAG subdag r={}: vertex[{}] replica_idx={} round={} entries={}",
                r, vi, vertex.replica_index, vertex.round, vertex.ordering_entries.len()
            );
            // Log first few entries for inspection
            for (ei, (d, oi)) in vertex.ordering_entries.iter().take(5).enumerate() {
                info!(
                    "DIAG subdag r={}: vertex[{}] entry[{}] tx_digest={} oi={}",
                    r, vi, ei, d, oi
                );
            }
        }

        // Line 2: G_r := NewGraph(), graphs.push(G_r)
        let graph_idx = self.graphs.len();
        self.graphs.push(DependencyGraph::new(r));
        self.round_to_graph.insert(r, graph_idx);

        // Lines 3-10: Update nodes with ordering info from A_r
        let updated_nodes = self.update_nodes_from_subdag(subdag);

        // Lines 11-18: Classify blank nodes and add non-blank ones to G_r
        self.classify_and_add_nodes(r, graph_idx, &updated_nodes);

        // Lines 19-39: Update weights and add edges
        self.update_weights_and_edges(subdag, graph_idx);

        // Log graph state after processing
        for (gi, g) in self.graphs.iter().enumerate() {
            if !g.finalized && !g.nodes.is_empty() {
                let total_edges_possible = if g.nodes.len() > 1 {
                    g.nodes.len() * (g.nodes.len() - 1) / 2
                } else {
                    0
                };
                info!(
                    "DIAG graph_state: G[{}] round={} nodes={} edges={}/{} weights={} is_tournament={}",
                    gi, g.round, g.nodes.len(), g.edges.len(),
                    total_edges_possible, g.weights.len(), g.is_tournament()
                );
            }
        }

        info!(
            "DIAG totals: total_nodes_tracked={} total_graphs={} ordered_so_far={}",
            self.nodes.len(), self.graphs.len(), self.output_sequence.len()
        );

        // Line 40-41: Check all graphs for tournament completion and finalize
        self.try_finalize_all_graphs()
    }

    // =========================================================================
    // Lines 3-10: Update nodes with committed ordering info
    // =========================================================================

    fn update_nodes_from_subdag(&mut self, subdag: &CommittedSubdag) -> HashSet<TxDigest> {
        let mut updated_nodes: HashSet<TxDigest> = HashSet::new();
        let r = subdag.leader_round;

        let mut total_entries = 0usize;
        let mut new_ois = 0usize;
        let mut skipped_ordered = 0usize;
        let mut skipped_dup = 0usize;

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;

            for &(d, oi) in &vertex.ordering_entries {
                total_entries += 1;

                if self.ordered_digests.contains(&d) {
                    skipped_ordered += 1;
                    continue;
                }

                let node = self.nodes.entry(d).or_insert_with(|| TransactionNode::new(d));

                if !node.committed_ois.contains_key(&i) {
                    node.committed_ois.insert(i, oi);
                    node.committed_rounds.insert(i, r);
                    updated_nodes.insert(d);
                    new_ois += 1;
                } else {
                    skipped_dup += 1;
                }
            }
        }

        info!(
            "DIAG update_nodes r={}: total_entries={} new_ois={} skipped_ordered={} skipped_dup={} updated_nodes={}",
            r, total_entries, new_ois, skipped_ordered, skipped_dup, updated_nodes.len()
        );

        // Log a few sample nodes with their OI counts
        let mut sample_count = 0;
        for &d in updated_nodes.iter() {
            if sample_count >= 5 { break; }
            if let Some(node) = self.nodes.get(&d) {
                info!(
                    "DIAG sample_node tx={} committed_ois_count={} ois={:?}",
                    d, node.committed_ois.len(), node.committed_ois
                );
                sample_count += 1;
            }
        }

        updated_nodes
    }

    // =========================================================================
    // Lines 11-18: Classify nodes and add to dependency graph
    // =========================================================================

    fn classify_and_add_nodes(
        &mut self,
        r: Round,
        graph_idx: usize,
        updated_nodes: &HashSet<TxDigest>,
    ) {
        let mut newly_added: Vec<TxDigest> = Vec::new();
        let mut solid_count = 0usize;
        let mut shaded_count = 0usize;
        let mut blank_count = 0usize;
        let mut already_classified = 0usize;

        for &d in updated_nodes {
            let node = self.nodes.get(&d).unwrap();
            if node.node_type != NodeType::Blank {
                already_classified += 1;
                continue;
            }

            let ap = node.appearance_count(r);

            if ap >= self.solid_threshold {
                newly_added.push(d);
                let node = self.nodes.get_mut(&d).unwrap();
                node.node_type = NodeType::Solid;
                node.graph_index = Some(graph_idx);
                solid_count += 1;
            } else if ap >= self.half_threshold {
                newly_added.push(d);
                let node = self.nodes.get_mut(&d).unwrap();
                node.node_type = NodeType::Shaded;
                node.graph_index = Some(graph_idx);
                shaded_count += 1;
            } else {
                blank_count += 1;
            }
        }

        for &d in &newly_added {
            self.graphs[graph_idx].nodes.insert(d);
        }

        info!(
            "DIAG classify r={} G[{}]: solid={} shaded={} blank={} already_classified={} total_in_graph={}",
            r, graph_idx, solid_count, shaded_count, blank_count, already_classified,
            self.graphs[graph_idx].nodes.len()
        );

        // Log a few classified nodes with their ap counts
        let mut sample_count = 0;
        for &d in newly_added.iter() {
            if sample_count >= 3 { break; }
            if let Some(node) = self.nodes.get(&d) {
                info!(
                    "DIAG classified tx={} type={:?} ap={} committed_ois={:?}",
                    d, node.node_type, node.appearance_count(r), node.committed_ois
                );
                sample_count += 1;
            }
        }
    }

    // =========================================================================
    // Lines 19-39: Update weights between nodes and add edges
    // =========================================================================

    fn update_weights_and_edges(
        &mut self,
        subdag: &CommittedSubdag,
        _current_graph_idx: usize,
    ) {
        let mut addable_edges: HashSet<(TxDigest, TxDigest)> = HashSet::new();

        let mut stat_vertices = 0usize;
        let mut stat_entries = 0usize;
        let mut stat_skip_ordered = 0usize;
        let mut stat_skip_no_graph = 0usize;
        let mut stat_skip_no_d_oi = 0usize;
        let mut stat_d2_checked = 0usize;
        let mut stat_skip_no_d2_oi = 0usize;
        let mut stat_weight_incremented = 0usize;
        let mut stat_max_weight: usize = 0;

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;
            stat_vertices += 1;

            for &(d, _oi) in &vertex.ordering_entries {
                stat_entries += 1;

                if self.ordered_digests.contains(&d) {
                    stat_skip_ordered += 1;
                    continue;
                }

                // Line 24: G' := node(d).G
                let g_idx = match self.nodes.get(&d).and_then(|n| n.graph_index) {
                    Some(idx) => idx,
                    None => {
                        stat_skip_no_graph += 1;
                        continue;
                    }
                };

                // Line 25: d_oi := node(d).committed_ois[i]
                let d_oi = match self.nodes.get(&d).and_then(|n| n.committed_ois.get(&i)) {
                    Some(&oi) => oi,
                    None => {
                        stat_skip_no_d_oi += 1;
                        continue;
                    }
                };

                let graph_nodes: Vec<TxDigest> =
                    self.graphs[g_idx].nodes.iter().cloned().collect();

                for d2 in graph_nodes {
                    if d2 == d {
                        continue;
                    }
                    stat_d2_checked += 1;

                    let d2_oi = match self.nodes.get(&d2).and_then(|n| n.committed_ois.get(&i)) {
                        Some(&oi) => oi,
                        None => {
                            stat_skip_no_d2_oi += 1;
                            continue;
                        }
                    };

                    if d_oi < d2_oi {
                        *self.graphs[g_idx]
                            .weights
                            .entry((d, d2))
                            .or_insert(0) += 1;
                    } else {
                        *self.graphs[g_idx]
                            .weights
                            .entry((d2, d))
                            .or_insert(0) += 1;
                    }
                    stat_weight_incremented += 1;

                    let w_d_d2 = *self.graphs[g_idx].weights.get(&(d, d2)).unwrap_or(&0);
                    let w_d2_d = *self.graphs[g_idx].weights.get(&(d2, d)).unwrap_or(&0);
                    let max_w = w_d_d2.max(w_d2_d);
                    if max_w > stat_max_weight {
                        stat_max_weight = max_w;
                    }

                    if w_d_d2 >= self.half_threshold || w_d2_d >= self.half_threshold {
                        if !self.graphs[g_idx].edges.contains(&(d, d2))
                            && !self.graphs[g_idx].edges.contains(&(d2, d))
                        {
                            addable_edges.insert((d, d2));
                        }
                    }
                }
            }
        }

        info!(
            "DIAG weights: vertices={} entries={} skip_ordered={} skip_no_graph={} skip_no_d_oi={} \
             d2_checked={} skip_no_d2_oi={} weight_incremented={} max_weight={} half_threshold={} addable_edges={}",
            stat_vertices, stat_entries, stat_skip_ordered, stat_skip_no_graph,
            stat_skip_no_d_oi, stat_d2_checked, stat_skip_no_d2_oi,
            stat_weight_incremented, stat_max_weight, self.half_threshold, addable_edges.len()
        );

        // Lines 33-39: Add edges
        let mut edges_added = 0usize;
        for (d, d2) in addable_edges {
            let g_idx = match self.nodes.get(&d).and_then(|n| n.graph_index) {
                Some(idx) => idx,
                None => continue,
            };

            if self.graphs[g_idx].edges.contains(&(d, d2))
                || self.graphs[g_idx].edges.contains(&(d2, d))
            {
                continue;
            }

            let w_d_d2 = *self.graphs[g_idx].weights.get(&(d, d2)).unwrap_or(&0);
            let w_d2_d = *self.graphs[g_idx].weights.get(&(d2, d)).unwrap_or(&0);

            if w_d_d2 >= w_d2_d {
                self.graphs[g_idx].edges.insert((d, d2));
            } else {
                self.graphs[g_idx].edges.insert((d2, d));
            }
            edges_added += 1;
        }

        if edges_added > 0 {
            info!("DIAG: added {} edges this round", edges_added);
        }
    }

    // =========================================================================
    // Line 40-41: Ordering Finalization
    // =========================================================================

    /// Finalize graphs IN ORDER. G_i must be finalized before G_{i+1}.
    ///
    /// This is required because finalizing G_i can re-add shaded transactions
    /// (those behind the last solid SCC) as blank nodes into later graphs.
    /// If we finalized G_{i+1} first, we'd miss those re-added transactions.
    ///
    /// So we walk the graph list from the oldest non-finalized graph forward:
    ///   - If it's a tournament → finalize it (may cascade: its re-adds could
    ///     complete a later graph, so keep going)
    ///   - If it's NOT a tournament → STOP. Nothing after it can be finalized yet.
    ///   - If it's empty (no nodes) → treat as finalized, skip past it.
    fn try_finalize_all_graphs(&mut self) -> Vec<TxDigest> {
        let mut newly_ordered: Vec<TxDigest> = Vec::new();

        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized {
                continue; // already done, move to next
            }
            if self.graphs[g_idx].nodes.is_empty() {
                // Empty graph (no nodes were classified into it).
                // Mark as finalized so we don't block on it.
                self.graphs[g_idx].finalized = true;
                continue;
            }
            if !self.graphs[g_idx].is_tournament() {
                // This graph is not yet complete — STOP.
                // Cannot finalize any later graph until this one is done.
                break;
            }

            info!(
                "FairnessLayer: graph {} (round {}) is a tournament with {} nodes — finalizing",
                g_idx,
                self.graphs[g_idx].round,
                self.graphs[g_idx].nodes.len()
            );

            let order = self.finalize_ordering(g_idx);
            newly_ordered.extend(order);
            // Continue to the next graph — the finalization above may have
            // re-added txs that completed a later graph, so check it too.
        }

        if !newly_ordered.is_empty() {
            info!(
                "FairnessLayer: finalized {} transactions in this round",
                newly_ordered.len()
            );
        }

        newly_ordered
    }

    /// Finalize the ordering of a tournament dependency graph.
    ///
    /// Section 6.3: Condense into SCCs, topological sort, prune after last
    /// SCC containing at least one solid transaction.
    fn finalize_ordering(&mut self, graph_idx: usize) -> Vec<TxDigest> {
        let graph = &self.graphs[graph_idx];
        let nodes: Vec<TxDigest> = graph.nodes.iter().cloned().collect();
        let edges: Vec<(TxDigest, TxDigest)> = graph.edges.iter().cloned().collect();

        // Step 1: Find SCCs using Kosaraju's algorithm
        let sccs = kosaraju_scc(&nodes, &edges);

        // Step 2: Build condensation DAG + topological sort of SCCs
        let topo_order = topological_sort_sccs(&sccs, &edges);

        // Step 3: Find the last SCC (in topological order) containing a solid node
        let mut last_solid_scc_pos: Option<usize> = None;
        for (pos, scc_idx) in topo_order.iter().enumerate() {
            let scc = &sccs[*scc_idx];
            let has_solid = scc.iter().any(|d| {
                self.nodes
                    .get(d)
                    .map_or(false, |n| n.node_type == NodeType::Solid)
            });
            if has_solid {
                last_solid_scc_pos = Some(pos);
            }
        }

        // Step 4: Output transactions up to (and including) the last solid SCC
        let mut ordered: Vec<TxDigest> = Vec::new();
        let mut to_readd: Vec<TxDigest> = Vec::new(); // txs after last solid SCC

        match last_solid_scc_pos {
            Some(cutoff_pos) => {
                for (pos, scc_idx) in topo_order.iter().enumerate() {
                    let scc = &sccs[*scc_idx];
                    if pos <= cutoff_pos {
                        // Output these transactions in a deterministic order within the SCC
                        let mut scc_sorted = scc.clone();
                        scc_sorted.sort();
                        ordered.extend(scc_sorted);
                    } else {
                        // Re-add to later dependency graphs
                        to_readd.extend(scc.iter());
                    }
                }
            }
            None => {
                // No solid node found — do not finalize, wait for more data
                warn!(
                    "FairnessLayer: graph {} is a tournament but has no solid nodes — deferring",
                    graph_idx
                );
                return Vec::new();
            }
        }

        // Mark graph as finalized
        self.graphs[graph_idx].finalized = true;
        self.graphs[graph_idx].final_order = ordered.clone();

        // Record ordered digests
        for &d in &ordered {
            self.ordered_digests.insert(d);
        }
        self.output_sequence.extend(ordered.iter());

        // Re-add deferred transactions as blank nodes for future graphs
        for d in to_readd {
            if let Some(node) = self.nodes.get_mut(&d) {
                node.node_type = NodeType::Blank;
                node.graph_index = None;
                debug!("FairnessLayer: re-adding tx {} as blank for future graphs", d);
            }
        }

        info!(
            "FairnessLayer: finalized {} transactions from graph {} (round {}). Total ordered: {}",
            ordered.len(),
            graph_idx,
            self.graphs[graph_idx].round,
            self.output_sequence.len()
        );

        ordered
    }

    // =========================================================================
    // Public accessors
    // =========================================================================

    /// Get the full output sequence so far.
    pub fn get_output_sequence(&self) -> &[TxDigest] {
        &self.output_sequence
    }

    /// Get the number of pending (unordered) transactions.
    pub fn pending_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| !self.ordered_digests.contains(&n.digest))
            .count()
    }

    /// Get replica index for a public key.
    pub fn replica_index(&self, pk: &PublicKey) -> Option<ReplicaIndex> {
        self.replica_indices.get(pk).copied()
    }
}

// =============================================================================
// Graph algorithms
// =============================================================================

/// Kosaraju's algorithm for finding SCCs.
fn kosaraju_scc(nodes: &[TxDigest], edges: &[(TxDigest, TxDigest)]) -> Vec<Vec<TxDigest>> {
    // Build adjacency lists
    let mut adj: HashMap<TxDigest, Vec<TxDigest>> = HashMap::new();
    let mut adj_rev: HashMap<TxDigest, Vec<TxDigest>> = HashMap::new();

    for &n in nodes {
        adj.entry(n).or_default();
        adj_rev.entry(n).or_default();
    }
    for &(u, v) in edges {
        adj.entry(u).or_default().push(v);
        adj_rev.entry(v).or_default().push(u);
    }

    // Pass 1: DFS on original graph, record finish order
    let mut visited: HashSet<TxDigest> = HashSet::new();
    let mut finish_order: Vec<TxDigest> = Vec::new();

    // Process nodes in deterministic order
    let mut sorted_nodes = nodes.to_vec();
    sorted_nodes.sort();

    for &start in &sorted_nodes {
        if !visited.contains(&start) {
            dfs_iterative(&adj, start, &mut visited, &mut finish_order);
        }
    }

    // Pass 2: DFS on reversed graph in reverse finish order
    let mut visited2: HashSet<TxDigest> = HashSet::new();
    let mut sccs: Vec<Vec<TxDigest>> = Vec::new();

    for &node in finish_order.iter().rev() {
        if !visited2.contains(&node) {
            let mut component: Vec<TxDigest> = Vec::new();
            let mut stack = vec![node];
            while let Some(n) = stack.pop() {
                if visited2.insert(n) {
                    component.push(n);
                    if let Some(neighbors) = adj_rev.get(&n) {
                        for &neighbor in neighbors {
                            if !visited2.contains(&neighbor) {
                                stack.push(neighbor);
                            }
                        }
                    }
                }
            }
            component.sort(); // deterministic ordering within SCC
            sccs.push(component);
        }
    }

    sccs
}

/// Iterative DFS that records finish order.
fn dfs_iterative(
    adj: &HashMap<TxDigest, Vec<TxDigest>>,
    start: TxDigest,
    visited: &mut HashSet<TxDigest>,
    finish_order: &mut Vec<TxDigest>,
) {
    // Use explicit stack with state tracking
    let mut stack: Vec<(TxDigest, bool)> = vec![(start, false)];

    while let Some((node, processed)) = stack.pop() {
        if processed {
            finish_order.push(node);
            continue;
        }
        if !visited.insert(node) {
            continue;
        }
        // Push node again to record finish time after processing children
        stack.push((node, true));

        if let Some(neighbors) = adj.get(&node) {
            // Process in reverse order for determinism
            let mut sorted_neighbors = neighbors.clone();
            sorted_neighbors.sort();
            for &neighbor in sorted_neighbors.iter().rev() {
                if !visited.contains(&neighbor) {
                    stack.push((neighbor, false));
                }
            }
        }
    }
}

/// Topological sort of SCCs in the condensation DAG.
/// Returns indices into the `sccs` vector in topological order.
fn topological_sort_sccs(
    sccs: &[Vec<TxDigest>],
    edges: &[(TxDigest, TxDigest)],
) -> Vec<usize> {
    // Map each node to its SCC index
    let mut node_to_scc: HashMap<TxDigest, usize> = HashMap::new();
    for (scc_idx, scc) in sccs.iter().enumerate() {
        for &node in scc {
            node_to_scc.insert(node, scc_idx);
        }
    }

    // Build condensation DAG
    let num_sccs = sccs.len();
    let mut in_degree: Vec<usize> = vec![0; num_sccs];
    let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); num_sccs];

    for &(u, v) in edges {
        if let (Some(&su), Some(&sv)) = (node_to_scc.get(&u), node_to_scc.get(&v)) {
            if su != sv && adj[su].insert(sv) {
                in_degree[sv] += 1;
            }
        }
    }

    // Kahn's algorithm
    let mut queue: VecDeque<usize> = VecDeque::new();
    for i in 0..num_sccs {
        if in_degree[i] == 0 {
            queue.push_back(i);
        }
    }

    // Use BTreeMap-based priority for determinism (process smaller SCC indices first)
    let mut result: Vec<usize> = Vec::new();
    let mut ready: BTreeMap<usize, ()> = BTreeMap::new();
    for i in 0..num_sccs {
        if in_degree[i] == 0 {
            ready.insert(i, ());
        }
    }

    while let Some((&scc_idx, _)) = ready.iter().next() {
        ready.remove(&scc_idx);
        result.push(scc_idx);

        for &neighbor in &adj[scc_idx] {
            in_degree[neighbor] -= 1;
            if in_degree[neighbor] == 0 {
                ready.insert(neighbor, ());
            }
        }
    }

    result
}