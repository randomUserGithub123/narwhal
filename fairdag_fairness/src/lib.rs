// Copyright(C) FairDAG-RL Implementation
// Implements the Fairness Layer of FairDAG-RL (Sections 6.1–6.3 of the paper).
//
// Protocol references in comments refer to:
//   Figure 7:  Transaction dissemination and DAG vertex proposal
//   Figure 8:  Dependency graph construction
//   Figure 11: Ordering finalization

use crypto::PublicKey;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

// =============================================================================
// Types
// =============================================================================

pub type TxDigest = u64;
pub type Round = u64;
pub type ReplicaIndex = usize;
pub type OrderingEntry = (TxDigest, u64);

// =============================================================================
// CommittedVertex / CommittedSubdag — input from consensus
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
    /// Not yet added to any dependency graph.
    Blank,
    /// ap(d, r) >= (n-f)/2 but < n-f.
    Shaded,
    /// ap(d, r) >= n-f.
    Solid,
}

// =============================================================================
// TransactionNode (Section 6.2)
//
// Each transaction digest d is associated with a node that stores:
//   - type: Blank | Shaded | Solid
//   - committed_ois: vector of committed ordering indicators indexed by replica
//   - committed_rounds: the rounds in which each committed_oi was committed
//   - G: the graph to which the node is added
// =============================================================================

#[derive(Clone, Debug)]
pub struct TransactionNode {
    pub digest: TxDigest,
    pub node_type: NodeType,
    /// committed_ois[i] = ordering indicator from replica i.
    pub committed_ois: HashMap<ReplicaIndex, u64>,
    /// committed_rounds[i] = the leader round in which replica i's OI was committed.
    pub committed_rounds: HashMap<ReplicaIndex, Round>,
    /// Index into FairnessLayer.graphs — the graph this node belongs to.
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

    /// ap(d, r) := |{i | node(d).committed_rounds[i] <= r}|
    fn appearance_count(&self, up_to_round: Round) -> usize {
        self.committed_rounds
            .values()
            .filter(|&&r| r <= up_to_round)
            .count()
    }
}

// =============================================================================
// DependencyGraph (Section 6.2)
//
// Each dependency graph contains:
//   - nodes: the set of transaction digest nodes
//   - weight: mapping of pairs of nodes to their weights
//   - edges: directed edges representing inferred ordering constraints
// =============================================================================

#[derive(Clone, Debug)]
pub struct DependencyGraph {
    pub round: Round,
    pub nodes: HashSet<TxDigest>,
    pub weights: HashMap<(TxDigest, TxDigest), usize>,
    pub edges: HashSet<(TxDigest, TxDigest)>,
    pub finalized: bool,
    pub final_order: Vec<TxDigest>,
    /// Safety guard: tracks which replicas have already been counted for each
    /// normalized pair (min(d1,d2), max(d1,d2)). This prevents double-counting
    /// when a replica has multiple vertices in the same subdag (spanning
    /// different rounds). The pseudocode does not include this because it
    /// implicitly assumes each (replica, tx) pair appears in exactly one vertex
    /// per subdag, but in practice a subdag can span many rounds.
    pub counted_replicas: HashMap<(TxDigest, TxDigest), HashSet<ReplicaIndex>>,
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
            counted_replicas: HashMap::new(),
        }
    }

    /// A tournament on n nodes has exactly n*(n-1)/2 directed edges
    /// (one per unordered pair).
    fn is_tournament(&self) -> bool {
        let n = self.nodes.len();
        if n < 2 {
            return n <= 1;
        }
        let expected_edges = n * (n - 1) / 2;
        self.edges.len() == expected_edges
    }
}

// =============================================================================
// FairnessLayer
// =============================================================================

pub struct FairnessLayer {
    pub n: usize,
    pub f: usize,
    /// n - f: threshold for solid classification.
    solid_threshold: usize,
    /// ceil((n - f) / 2): threshold for shaded classification and edge addition.
    half_threshold: usize,

    /// Global map from tx digest → TransactionNode.
    nodes: HashMap<TxDigest, TransactionNode>,
    /// Ordered list of dependency graphs. Figure 8 Line 2: graphs := [].
    graphs: Vec<DependencyGraph>,
    /// Mapping from leader round → index in self.graphs.
    round_to_graph: HashMap<Round, usize>,
    /// Set of digests that have been finalized/ordered.
    ordered_digests: HashSet<TxDigest>,
    /// The cumulative final transaction ordering.
    output_sequence: Vec<TxDigest>,
    /// Mapping from PublicKey → ReplicaIndex.
    replica_indices: HashMap<PublicKey, ReplicaIndex>,

    /// Whether to use Hamiltonian path (true) or lexicographic sort (false)
    /// for linearizing SCCs during ordering finalization (Figure 11, Line 9).
    use_hamiltonian_path: bool,

    /// Nodes waiting to be re-added to the next graph that gets created.
    /// This handles the case where finalization produces leftover shaded
    /// nodes (Figure 11, Lines 13-29) but no next graph exists yet.
    pending_readd: Vec<TxDigest>,
}

impl FairnessLayer {
    pub fn new(
        committee_keys: Vec<PublicKey>,
        f: usize,
    ) -> Self {
        let n = committee_keys.len();
        let solid_threshold = n - f;
        // ceil((n - f) / 2) — this is the >= threshold used in the protocol.
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
            nodes: HashMap::new(),
            graphs: Vec::new(),
            round_to_graph: HashMap::new(),
            ordered_digests: HashSet::new(),
            output_sequence: Vec::new(),
            replica_indices,
            use_hamiltonian_path: false,
            pending_readd: Vec::new(),
        }
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
        self.graphs.push(DependencyGraph::new(r));
        self.round_to_graph.insert(r, graph_idx);

        // Process any pending re-add nodes from a prior finalization
        // (these were leftover shaded nodes that had no next graph at the time).
        self.process_pending_readd(graph_idx);

        // Figure 8, Lines 3-10: Update nodes from subdag.
        let updated_nodes = self.update_nodes_from_subdag(subdag);

        // Figure 8, Lines 11-18: Classify and add nodes to Gr.
        let newly_classified = self.classify_and_add_nodes(r, graph_idx, &updated_nodes);

        // LIVENESS FIX: Compute full pairwise weights for newly classified
        // nodes using ALL accumulated committed_ois — not just the current
        // subdag's vertices. This closes the gap where a replica committed
        // both members of a pair in a previous subdag (when one or both were
        // still blank), and the incremental weight update will never revisit
        // that pair from that replica's perspective.
        self.compute_catchup_weights_for_new_nodes(graph_idx, &newly_classified);

        // Figure 8, Lines 19-39: Incremental weight update from current
        // subdag's vertices. This handles edges between nodes that were
        // ALREADY in older graphs and receive new weight data now.
        self.update_weights_and_edges(subdag);

        // Log graph states.
        for (gi, g) in self.graphs.iter().enumerate() {
            if !g.finalized && !g.nodes.is_empty() {
                let expected = if g.nodes.len() > 1 {
                    g.nodes.len() * (g.nodes.len() - 1) / 2
                } else {
                    0
                };
                info!(
                    "DIAG graph_state: G[{}] round={} nodes={} edges={}/{} is_tournament={}",
                    gi,
                    g.round,
                    g.nodes.len(),
                    g.edges.len(),
                    expected,
                    g.is_tournament()
                );
            }
        }

        // Figure 11, Line 41: OrderFinalization()
        self.try_finalize_all_graphs()
    }

    // =========================================================================
    // Figure 8, Lines 3-10: Find nodes updated with Ar
    //
    //   for v ∈ Ar do
    //     i := v.replica_id
    //     for (d, oi) ∈ (v.dgs, v.ois) do
    //       node(d).committed_ois[i] := oi
    //       node(d).committed_rounds[i] := r
    //       updated_nodes.insert(d)
    // =========================================================================

    fn update_nodes_from_subdag(&mut self, subdag: &CommittedSubdag) -> HashSet<TxDigest> {
        let mut updated_nodes: HashSet<TxDigest> = HashSet::new();
        let r = subdag.leader_round;

        // Figure 8, Line 5: for v ∈ Ar do
        for vertex in &subdag.vertices {
            // Figure 8, Line 6: i := v.replica_id
            let i = vertex.replica_index;
            // Figure 8, Line 7: for (d, oi) ∈ (v.dgs, v.ois) do
            for &(d, oi) in &vertex.ordering_entries {
                // Footnote 2: skip already-ordered transactions.
                if self.ordered_digests.contains(&d) {
                    continue;
                }
                let node = self
                    .nodes
                    .entry(d)
                    .or_insert_with(|| TransactionNode::new(d));
                // Figure 8, Lines 8-9: Only record first OI from each replica.
                if !node.committed_ois.contains_key(&i) {
                    node.committed_ois.insert(i, oi);   // Line 8
                    node.committed_rounds.insert(i, r);  // Line 9
                    updated_nodes.insert(d);              // Line 10
                }
            }
        }

        info!(
            "FairnessLayer: update_nodes round={} updated={}",
            r,
            updated_nodes.len()
        );
        updated_nodes
    }

    // =========================================================================
    // Figure 8, Lines 11-18: Classify nodes and add to Gr
    //
    //   for d ∈ updated_nodes do
    //     if node(d).type = blank then
    //       if ap(d, r) >= n-f then
    //         node(d).type := solid; Gr.nodes.add(node(d))
    //       else if ap(d, r) >= (n-f)/2 then
    //         node(d).type := shaded; Gr.nodes.add(node(d))
    // =========================================================================

    fn classify_and_add_nodes(
        &mut self,
        r: Round,
        graph_idx: usize,
        updated_nodes: &HashSet<TxDigest>,
    ) -> Vec<TxDigest> {
        let mut solid_count = 0usize;
        let mut shaded_count = 0usize;
        let mut blank_count = 0usize;
        let mut newly_classified: Vec<TxDigest> = Vec::new();

        // Figure 8, Line 12: for d ∈ updated_nodes do
        for &d in updated_nodes {
            let node = self.nodes.get(&d).unwrap();
            // Figure 8, Line 13: if node(d).type = blank then
            if node.node_type != NodeType::Blank {
                continue;
            }
            // Figure 8, Line 14: ap(d, r)
            let ap = node.appearance_count(r);

            // Figure 8, Lines 15-18
            if ap >= self.solid_threshold {
                // Line 16: node(d).type := solid; Gr.nodes.add(node(d))
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Solid;
                self.nodes.get_mut(&d).unwrap().graph_index = Some(graph_idx);
                self.graphs[graph_idx].nodes.insert(d);
                newly_classified.push(d);
                solid_count += 1;
            } else if ap >= self.half_threshold {
                // Line 18: node(d).type := shaded; Gr.nodes.add(node(d))
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Shaded;
                self.nodes.get_mut(&d).unwrap().graph_index = Some(graph_idx);
                self.graphs[graph_idx].nodes.insert(d);
                newly_classified.push(d);
                shaded_count += 1;
            } else {
                blank_count += 1;
            }
        }

        info!(
            "FairnessLayer: classify round={} G[{}] solid={} shaded={} blank={} total_in_graph={}",
            r,
            graph_idx,
            solid_count,
            shaded_count,
            blank_count,
            self.graphs[graph_idx].nodes.len()
        );

        newly_classified
    }

    // =========================================================================
    // LIVENESS FIX: Catch-up weight computation for newly classified nodes
    //
    // When a node d transitions from blank → solid/shaded and is added to Gr,
    // its committed_ois may contain OIs from PREVIOUS subdags whose vertices
    // have already been processed and will never appear in future subdags.
    //
    // The protocol's incremental weight update (Figure 8 Lines 21-32) only
    // processes the CURRENT subdag's vertices, so pairs where both nodes had
    // their OI from a given replica committed in a previous subdag will never
    // receive that replica's weight contribution — causing permanent edge
    // deficits and liveness failure.
    //
    // Fix: for each newly classified node, compute full pairwise weights
    // against all existing nodes in the graph using ALL accumulated
    // committed_ois. This is the same approach used by readd_nodes_to_graph.
    // =========================================================================

    fn compute_catchup_weights_for_new_nodes(
        &mut self,
        graph_idx: usize,
        newly_classified: &[TxDigest],
    ) {
        if newly_classified.is_empty() {
            return;
        }

        let newly_set: HashSet<TxDigest> = newly_classified.iter().cloned().collect();
        let mut edges_added = 0usize;
        let mut weights_computed = 0usize;

        for &d in newly_classified {
            // Compare d against all OTHER nodes already in the graph.
            let existing_nodes: Vec<TxDigest> = self.graphs[graph_idx]
                .nodes
                .iter()
                .filter(|&&d2| d2 != d)
                .cloned()
                .collect();

            for d2 in existing_nodes {
                // For pairs between two newly classified nodes, only compute
                // once: when d < d2 (avoid duplicate computation).
                if newly_set.contains(&d2) && d > d2 {
                    continue;
                }

                // Compute full weights from ALL committed_ois.
                let (w_d_d2, w_d2_d) = self.calculate_pairwise_weight(d, d2);
                weights_computed += 1;

                // Store the computed weights (overwrite any partial values from
                // incremental updates — the full computation is authoritative).
                self.graphs[graph_idx].weights.insert((d, d2), w_d_d2);
                self.graphs[graph_idx].weights.insert((d2, d), w_d2_d);

                // Mark all replicas that contributed as counted (so the
                // incremental update doesn't double-count).
                let pair = (d.min(d2), d.max(d2));
                let node1 = self.nodes.get(&d).unwrap();
                let node2 = self.nodes.get(&d2).unwrap();
                let replica_set = self.graphs[graph_idx]
                    .counted_replicas
                    .entry(pair)
                    .or_default();
                for (&i, _) in &node1.committed_ois {
                    if node2.committed_ois.contains_key(&i) {
                        replica_set.insert(i);
                    }
                }

                // Add edge if threshold met and no edge exists yet.
                if w_d_d2 >= self.half_threshold || w_d2_d >= self.half_threshold {
                    if self.graphs[graph_idx].edges.contains(&(d, d2))
                        || self.graphs[graph_idx].edges.contains(&(d2, d))
                    {
                        continue;
                    }

                    if w_d_d2 >= w_d2_d {
                        self.graphs[graph_idx].edges.insert((d, d2));
                    } else {
                        self.graphs[graph_idx].edges.insert((d2, d));
                    }
                    edges_added += 1;
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
    //
    // Lines 19-32 (weight update):
    //   addable_edges := {}
    //   for v ∈ Ar do                             // each vertex
    //     i := v.replica_id                        // replica index
    //     for (d, oi) ∈ (v.dgs, v.ois) do         // each entry
    //       G' := node(d).G                        // graph containing d
    //       d_oi := node(d).committed_ois[i]
    //       for node(d2) ∈ G'.nodes do             // each node in same graph
    //         if d_oi < node(d2).committed_ois[i] then
    //           increment G'.weight[(d, d2)]
    //         else
    //           increment G'.weight[(d2, d)]
    //         if either weight reaches threshold (n-f)/2 then
    //           addable_edges.insert(d, d2)
    //
    // Lines 33-39 (edge addition):
    //   for (d, d2) ∈ addable_edges do
    //     G := node(d).G
    //     if G.weight[(d, d2)] >= G.weight[(d2, d)] then
    //       G.edges.add(e(d, d2))
    //     else
    //       G.edges.add(e(d2, d))
    // =========================================================================

    fn update_weights_and_edges(&mut self, subdag: &CommittedSubdag) {
        let mut addable_edges: HashSet<(TxDigest, TxDigest)> = HashSet::new();

        let mut stat_pairs_checked: usize = 0;
        let mut stat_pairs_skipped_counted: usize = 0;
        let mut stat_pairs_skipped_edge: usize = 0;
        let mut stat_weights_incremented: usize = 0;

        // Figure 8, Line 21: for v ∈ Ar do (vertices in round-increasing order)
        for vertex in &subdag.vertices {
            // Figure 8, Line 22: i := v.replica_id
            let i = vertex.replica_index;

            // Figure 8, Line 23: for (d, oi) ∈ (v.dgs, v.ois) do
            for &(d, _oi) in &vertex.ordering_entries {
                // Footnote 2: skip already-ordered transactions.
                if self.ordered_digests.contains(&d) {
                    continue;
                }

                // Figure 8, Line 24: G' := node(d).G
                let g_idx = match self.nodes.get(&d).and_then(|n| n.graph_index) {
                    Some(idx) => idx,
                    None => continue, // node not yet in any graph (still blank)
                };

                // Figure 8, Line 25: d_oi := node(d).committed_ois[i]
                let d_oi = match self.nodes.get(&d).and_then(|n| n.committed_ois.get(&i)) {
                    Some(&oi) => oi,
                    None => continue, // replica i has no OI for d
                };

                // Collect (d2, d2_oi) pairs from the graph to avoid borrow issues.
                let graph_nodes: Vec<TxDigest> =
                    self.graphs[g_idx].nodes.iter().cloned().collect();

                // Figure 8, Line 26: for node(d2) ∈ G'.nodes do
                for d2 in graph_nodes {
                    if d2 == d {
                        continue;
                    }

                    // d2 must have committed_ois[i] for this comparison.
                    let d2_oi = match self
                        .nodes
                        .get(&d2)
                        .and_then(|n| n.committed_ois.get(&i))
                    {
                        Some(&oi) => oi,
                        None => continue,
                    };

                    stat_pairs_checked += 1;

                    // Normalize pair for dedup tracking: always (smaller, larger).
                    let pair = (d.min(d2), d.max(d2));

                    // Skip if this pair already has an edge.
                    if self.graphs[g_idx].edges.contains(&(d, d2))
                        || self.graphs[g_idx].edges.contains(&(d2, d))
                    {
                        stat_pairs_skipped_edge += 1;
                        continue;
                    }

                    // Safety guard: skip if this replica was already counted for
                    // this pair (prevents double-counting across multiple vertices
                    // from the same replica within one subdag).
                    let replica_set = self.graphs[g_idx]
                        .counted_replicas
                        .entry(pair)
                        .or_default();
                    if !replica_set.insert(i) {
                        stat_pairs_skipped_counted += 1;
                        continue;
                    }

                    // Figure 8, Lines 27-30: Compare OIs and increment weights.
                    // Protocol: if d_oi < d2_oi then increment weight(d, d2)
                    //           else increment weight(d2, d)
                    if d_oi < d2_oi {
                        // Line 28: increment G'.weight[(d, d2)]
                        *self.graphs[g_idx].weights.entry((d, d2)).or_insert(0) += 1;
                    } else {
                        // Line 30: increment G'.weight[(d2, d)]
                        // (includes the d_oi == d2_oi case per protocol's else branch)
                        *self.graphs[g_idx].weights.entry((d2, d)).or_insert(0) += 1;
                    }
                    stat_weights_incremented += 1;

                    // Figure 8, Lines 31-32: Check if either weight reaches
                    // threshold (n-f)/2 → add to addable_edges.
                    let w_fwd =
                        *self.graphs[g_idx].weights.get(&(pair.0, pair.1)).unwrap_or(&0);
                    let w_rev =
                        *self.graphs[g_idx].weights.get(&(pair.1, pair.0)).unwrap_or(&0);

                    if w_fwd >= self.half_threshold || w_rev >= self.half_threshold {
                        addable_edges.insert(pair);
                    }
                }
            }
        }

        // =====================================================================
        // Figure 8, Lines 33-39: Add edges based on majority preference
        //
        //   for (d, d2) ∈ addable_edges do
        //     G := node(d).G
        //     if G.weight[(d, d2)] >= G.weight[(d2, d)] then
        //       G.edges.add(e(d, d2))
        //     else
        //       G.edges.add(e(d2, d))
        // =====================================================================

        let mut edges_added = 0usize;

        // Figure 8, Line 34: for (d, d2) ∈ addable_edges do
        for (d1, d2) in &addable_edges {
            let d1 = *d1;
            let d2 = *d2;

            // Figure 8, Line 35: G := node(d).G
            let g_idx = match self.nodes.get(&d1).and_then(|n| n.graph_index) {
                Some(idx) => idx,
                None => continue,
            };

            // Skip if edge already exists (from a previous subdag or re-add).
            if self.graphs[g_idx].edges.contains(&(d1, d2))
                || self.graphs[g_idx].edges.contains(&(d2, d1))
            {
                continue;
            }

            let w_12 = *self.graphs[g_idx].weights.get(&(d1, d2)).unwrap_or(&0);
            let w_21 = *self.graphs[g_idx].weights.get(&(d2, d1)).unwrap_or(&0);

            // Figure 8, Lines 36-39
            if w_12 >= w_21 {
                // Line 37: G.edges.add(e(d, d2))
                self.graphs[g_idx].edges.insert((d1, d2));
            } else {
                // Line 39: G.edges.add(e(d2, d))
                self.graphs[g_idx].edges.insert((d2, d1));
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
    //
    //   while Gr := graphs.Front() do
    //     if Gr is a tournament then
    //       graphs.Pop()
    //       Gc := Tarjan_SCC(Gr)
    //       [S1, S2, ..., Ss] := Topologically_Sorted(Gc)
    //       last := max{j | ∃node ∈ Sj, node.type = solid}
    //       for j = 1, 2, ..., last do
    //         pj := Hamilton_Path(Sj)
    //         Append pj to final ordering
    //         for node(d) ∈ pj do ordered_nodes.add(node(d))
    //       Gr' := graphs.Front()
    //       for j = last+1, ..., s do
    //         for node(d) ∈ Sj do
    //           <re-classify and re-add to Gr'>
    //     else
    //       break
    // =========================================================================

    fn try_finalize_all_graphs(&mut self) -> Vec<TxDigest> {
        let mut newly_ordered: Vec<TxDigest> = Vec::new();

        // Figure 11, Line 2: while Gr := graphs.Front() do
        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized {
                continue;
            }
            if self.graphs[g_idx].nodes.is_empty() {
                self.graphs[g_idx].finalized = true;
                continue;
            }

            // Figure 11, Line 3: if Gr is a tournament then
            if !self.graphs[g_idx].is_tournament() {
                // Figure 11, Lines 30-31: else break
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
        let graph = &self.graphs[graph_idx];
        let nodes: Vec<TxDigest> = graph.nodes.iter().cloned().collect();
        let edges: Vec<(TxDigest, TxDigest)> = graph.edges.iter().cloned().collect();

        // Figure 11, Line 5: Gc := Tarjan_SCC(Gr)
        let sccs = tarjan_scc(&nodes, &edges);

        // Figure 11, Line 6: [S1, S2, ..., Ss] := Topologically_Sorted(Gc)
        let topo_order = topological_sort_sccs(&sccs, &edges);

        // Figure 11, Line 7: last := max{j | ∃node ∈ Sj, node.type = solid}
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

        let mut ordered: Vec<TxDigest> = Vec::new();
        let mut to_readd: Vec<TxDigest> = Vec::new();

        match last_solid_scc_pos {
            Some(cutoff_pos) => {
                // Figure 11, Lines 8-12: for j = 1, 2, ..., last
                for (pos, scc_idx) in topo_order.iter().enumerate() {
                    let scc = &sccs[*scc_idx];
                    if pos <= cutoff_pos {
                        // Figure 11, Line 9: pj := Hamilton_Path(Sj)
                        let path = if self.use_hamiltonian_path {
                            // Build the edge set restricted to this SCC.
                            let scc_set: HashSet<TxDigest> = scc.iter().cloned().collect();
                            let scc_edges: HashSet<(TxDigest, TxDigest)> = edges
                                .iter()
                                .filter(|(u, v)| scc_set.contains(u) && scc_set.contains(v))
                                .cloned()
                                .collect();
                            hamiltonian_path(scc, &scc_edges)
                        } else {
                            // Fallback: lexicographic sort by digest value.
                            let mut scc_sorted = scc.clone();
                            scc_sorted.sort();
                            scc_sorted
                        };

                        // Figure 11, Line 10: Append pj to final ordering
                        ordered.extend(&path);

                        // Figure 11, Lines 11-12:
                        //   for node(d) ∈ pj do ordered_nodes.add(node(d))
                        // (handled below when we insert into ordered_digests)
                    } else {
                        // Figure 11, Lines 14 onwards: SCCs after last
                        to_readd.extend(scc.iter());
                    }
                }
            }
            None => {
                // No solid nodes in the tournament — defer entirely.
                warn!(
                    "FairnessLayer: graph {} is a tournament but has no solid nodes — deferring all",
                    graph_idx
                );
                return Vec::new();
            }
        }

        // Mark graph as finalized.
        self.graphs[graph_idx].finalized = true;
        self.graphs[graph_idx].final_order = ordered.clone();

        // Record ordered digests.
        for &d in &ordered {
            self.ordered_digests.insert(d);
        }
        self.output_sequence.extend(ordered.iter());

        // =====================================================================
        // Figure 11, Lines 13-29: Re-add shaded nodes to the next graph Gr'
        //
        //   Gr' := graphs.Front()
        //   for j = last+1, ..., s do
        //     for node(d) ∈ Sj do
        //       ap(d, r') := |{i | node(d).committed_rounds[i] <= r'}|
        //       if ap(d, r') >= n-f then node(d).type = solid
        //       else if ap(d, r') >= (n-f)/2 then node(d).type = shaded
        //       Gr'.nodes.add(node(d))
        //       for node(d2) ∈ Gr'.nodes do
        //         Calculate weights and add edges if threshold met
        // =====================================================================

        if !to_readd.is_empty() {
            // Figure 11, Line 13: Gr' := graphs.Front() — the next non-finalized graph.
            let next_graph_idx = self.find_next_unfinalized_graph(graph_idx);

            match next_graph_idx {
                Some(next_idx) => {
                    info!(
                        "FairnessLayer: re-adding {} shaded nodes from graph {} to graph {} (round {})",
                        to_readd.len(),
                        graph_idx,
                        next_idx,
                        self.graphs[next_idx].round
                    );
                    self.readd_nodes_to_graph(to_readd, next_idx);
                }
                None => {
                    // No next graph exists yet — store for later.
                    info!(
                        "FairnessLayer: deferring {} shaded nodes from graph {} (no next graph yet)",
                        to_readd.len(),
                        graph_idx
                    );
                    for d in &to_readd {
                        if let Some(node) = self.nodes.get_mut(d) {
                            node.node_type = NodeType::Blank;
                            node.graph_index = None;
                        }
                    }
                    self.pending_readd.extend(to_readd);
                }
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

    /// Find the next non-finalized graph after the given index.
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
    //
    //   for node(d) ∈ Sj do
    //     ap(d, r') := |{i | node(d).committed_rounds[i] <= r'}|
    //     if ap(d, r') >= n-f then node(d).type = solid
    //     else if ap(d, r') >= (n-f)/2 then node(d).type = shaded
    //     Gr'.nodes.add(node(d))
    //     for node(d2) ∈ Gr'.nodes do
    //       Calculate Gr'.weights[(d, d2)]
    //       Calculate Gr'.weights[(d2, d)]
    //       if Gr'.weights[(d, d2)] >= (n-f)/2
    //          OR Gr'.weights[(d2, d)] >= (n-f)/2 then
    //         if Gr'.weights[(d, d2)] >= Gr'.weights[(d2, d)] then
    //           Gr'.edges.add(e(d, d2))
    //         else
    //           Gr'.edges.add(e(d2, d))
    // =========================================================================

    fn readd_nodes_to_graph(&mut self, to_readd: Vec<TxDigest>, target_graph_idx: usize) {
        let r_prime = self.graphs[target_graph_idx].round;

        // Figure 11, Lines 15-16: For each node(d) to re-add
        for d in &to_readd {
            let d = *d;

            // Figure 11, Line 16: ap(d, r')
            let ap = self.nodes.get(&d).unwrap().appearance_count(r_prime);

            // Figure 11, Lines 17-20: Classify
            if ap >= self.solid_threshold {
                // Line 18: node(d).type = solid
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Solid;
            } else if ap >= self.half_threshold {
                // Line 20: node(d).type = shaded
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Shaded;
            } else {
                // Still below threshold — classify as shaded anyway for re-add
                // (the protocol re-adds all leftover nodes from finalized SCCs).
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Shaded;
            }

            // Figure 11, Line 21: Gr'.nodes.add(node(d))
            self.nodes.get_mut(&d).unwrap().graph_index = Some(target_graph_idx);
            self.graphs[target_graph_idx].nodes.insert(d);

            // Figure 11, Lines 22-29: Calculate weights and edges with all
            // existing nodes in Gr'.
            // Collect existing nodes first to avoid borrow conflict.
            let existing_nodes: Vec<TxDigest> = self.graphs[target_graph_idx]
                .nodes
                .iter()
                .filter(|&&d2| d2 != d)
                .cloned()
                .collect();

            for d2 in existing_nodes {
                // Figure 11, Lines 23-24: Calculate weights from ALL committed OIs.
                let (w_d_d2, w_d2_d) = self.calculate_pairwise_weight(d, d2);

                // Store the computed weights.
                self.graphs[target_graph_idx]
                    .weights
                    .insert((d, d2), w_d_d2);
                self.graphs[target_graph_idx]
                    .weights
                    .insert((d2, d), w_d2_d);

                // Figure 11, Lines 25-29: Add edge if threshold met.
                if w_d_d2 >= self.half_threshold || w_d2_d >= self.half_threshold {
                    // Skip if edge already exists.
                    if self.graphs[target_graph_idx].edges.contains(&(d, d2))
                        || self.graphs[target_graph_idx].edges.contains(&(d2, d))
                    {
                        continue;
                    }

                    // Figure 11, Lines 26-29
                    if w_d_d2 >= w_d2_d {
                        // Line 27: Gr'.edges.add(e(d, d2))
                        self.graphs[target_graph_idx].edges.insert((d, d2));
                    } else {
                        // Line 29: Gr'.edges.add(e(d2, d))
                        self.graphs[target_graph_idx].edges.insert((d2, d));
                    }
                }
            }
        }
    }

    /// Calculate the full pairwise weight between two transaction nodes
    /// based on all committed ordering indicators.
    ///
    /// weight(d1, d2) = |{i : node(d1).committed_ois[i] < node(d2).committed_ois[i]}|
    fn calculate_pairwise_weight(&self, d1: TxDigest, d2: TxDigest) -> (usize, usize) {
        let node1 = self.nodes.get(&d1).unwrap();
        let node2 = self.nodes.get(&d2).unwrap();
        let mut w12: usize = 0;
        let mut w21: usize = 0;

        for (&i, &oi1) in &node1.committed_ois {
            if let Some(&oi2) = node2.committed_ois.get(&i) {
                // Match protocol: if oi1 < oi2 then w12++, else w21++.
                if oi1 < oi2 {
                    w12 += 1;
                } else {
                    w21 += 1;
                }
            }
        }

        (w12, w21)
    }

    /// Process pending re-add nodes into a newly created graph.
    /// Called at the start of process_subdag, before the new subdag's
    /// nodes are classified.
    fn process_pending_readd(&mut self, graph_idx: usize) {
        if self.pending_readd.is_empty() {
            return;
        }

        let pending: Vec<TxDigest> = self.pending_readd.drain(..).collect();
        // Filter out any that have been ordered in the meantime.
        let pending: Vec<TxDigest> = pending
            .into_iter()
            .filter(|d| !self.ordered_digests.contains(d))
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

    pub fn get_output_sequence(&self) -> &[TxDigest] {
        &self.output_sequence
    }

    pub fn pending_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| !self.ordered_digests.contains(&n.digest))
            .count()
    }

    pub fn replica_index(&self, pk: &PublicKey) -> Option<ReplicaIndex> {
        self.replica_indices.get(pk).copied()
    }
}

// =============================================================================
// Graph algorithms
// =============================================================================

// =============================================================================
// Tarjan's SCC (Figure 11, Line 5)
//
// Iterative implementation to avoid stack overflow on large graphs.
// Produces SCCs; the returned order is topological (reversed from the
// standard Tarjan output which is reverse-topological).
// =============================================================================

fn tarjan_scc(nodes: &[TxDigest], edges: &[(TxDigest, TxDigest)]) -> Vec<Vec<TxDigest>> {
    // Build adjacency list.
    let mut adj: HashMap<TxDigest, Vec<TxDigest>> = HashMap::new();
    for &n in nodes {
        adj.entry(n).or_default();
    }
    for &(u, v) in edges {
        adj.entry(u).or_default().push(v);
    }
    // Sort adjacency lists for deterministic output.
    for list in adj.values_mut() {
        list.sort();
    }

    let mut index_counter: usize = 0;
    let mut stack: Vec<TxDigest> = Vec::new();
    let mut on_stack: HashSet<TxDigest> = HashSet::new();
    let mut index_of: HashMap<TxDigest, usize> = HashMap::new();
    let mut lowlink: HashMap<TxDigest, usize> = HashMap::new();
    let mut sccs: Vec<Vec<TxDigest>> = Vec::new();

    // Process nodes in sorted order for determinism.
    let mut sorted_nodes = nodes.to_vec();
    sorted_nodes.sort();

    for &start in &sorted_nodes {
        if index_of.contains_key(&start) {
            continue;
        }

        // Iterative DFS.
        // Each frame: (node, neighbor_list_position).
        let mut dfs_stack: Vec<(TxDigest, usize)> = Vec::new();

        // Initialize start node.
        index_of.insert(start, index_counter);
        lowlink.insert(start, index_counter);
        index_counter += 1;
        stack.push(start);
        on_stack.insert(start);
        dfs_stack.push((start, 0));

        while !dfs_stack.is_empty() {
            let (v, ni) = *dfs_stack.last().unwrap();
            let neighbors = adj.get(&v).cloned().unwrap_or_default();

            if ni < neighbors.len() {
                let w = neighbors[ni];
                // Advance the neighbor index.
                dfs_stack.last_mut().unwrap().1 += 1;

                if !index_of.contains_key(&w) {
                    // w has not been visited — push it.
                    index_of.insert(w, index_counter);
                    lowlink.insert(w, index_counter);
                    index_counter += 1;
                    stack.push(w);
                    on_stack.insert(w);
                    dfs_stack.push((w, 0));
                } else if on_stack.contains(&w) {
                    // w is on the stack — update v's lowlink.
                    let v_low = lowlink[&v];
                    let w_idx = index_of[&w];
                    if w_idx < v_low {
                        lowlink.insert(v, w_idx);
                    }
                }
            } else {
                // All neighbors of v have been processed.
                // Check if v is a root of an SCC.
                if lowlink[&v] == index_of[&v] {
                    let mut scc: Vec<TxDigest> = Vec::new();
                    loop {
                        let w = stack.pop().unwrap();
                        on_stack.remove(&w);
                        scc.push(w);
                        if w == v {
                            break;
                        }
                    }
                    scc.sort(); // Sort within SCC for determinism.
                    sccs.push(scc);
                }

                // Pop v from the DFS stack.
                dfs_stack.pop();

                // Propagate lowlink to parent.
                if let Some(&(parent, _)) = dfs_stack.last() {
                    let v_low = lowlink[&v];
                    let p_low = lowlink[&parent];
                    if v_low < p_low {
                        lowlink.insert(parent, v_low);
                    }
                }
            }
        }
    }

    // Tarjan's produces SCCs in reverse topological order.
    // Reverse to get topological order.
    sccs.reverse();
    sccs
}

// =============================================================================
// Topological sort of SCCs (Figure 11, Line 6)
//
// Returns the topologically sorted SCC indices. Uses Kahn's algorithm
// with a BTreeMap for deterministic tie-breaking.
// =============================================================================

fn topological_sort_sccs(
    sccs: &[Vec<TxDigest>],
    edges: &[(TxDigest, TxDigest)],
) -> Vec<usize> {
    let mut node_to_scc: HashMap<TxDigest, usize> = HashMap::new();
    for (scc_idx, scc) in sccs.iter().enumerate() {
        for &node in scc {
            node_to_scc.insert(node, scc_idx);
        }
    }

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

    let mut ready: BTreeMap<usize, ()> = BTreeMap::new();
    for i in 0..num_sccs {
        if in_degree[i] == 0 {
            ready.insert(i, ());
        }
    }

    let mut result: Vec<usize> = Vec::new();
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

// =============================================================================
// Hamiltonian Path in a tournament SCC (Figure 11, Line 9)
//
// Every tournament has a Hamiltonian path. We use the standard greedy
// insertion algorithm:
//   1. Start with a path containing one node.
//   2. For each remaining node v:
//      a. If edge(v → head): prepend v.
//      b. Else if edge(tail → v): append v.
//      c. Else find position i where edge(path[i] → v) and
//         edge(v → path[i+1]), and insert v there.
// =============================================================================

fn hamiltonian_path(
    nodes: &[TxDigest],
    edges: &HashSet<(TxDigest, TxDigest)>,
) -> Vec<TxDigest> {
    if nodes.is_empty() {
        return Vec::new();
    }
    if nodes.len() == 1 {
        return nodes.to_vec();
    }

    // Sort nodes for deterministic starting point.
    let mut sorted_nodes = nodes.to_vec();
    sorted_nodes.sort();

    let mut path: VecDeque<TxDigest> = VecDeque::new();
    path.push_back(sorted_nodes[0]);

    for &v in &sorted_nodes[1..] {
        // Case a: edge from v to head of path → prepend.
        if edges.contains(&(v, *path.front().unwrap())) {
            path.push_front(v);
        }
        // Case b: edge from tail of path to v → append.
        else if edges.contains(&(*path.back().unwrap(), v)) {
            path.push_back(v);
        }
        // Case c: find insertion point.
        else {
            let mut inserted = false;
            for i in 0..path.len() - 1 {
                if edges.contains(&(path[i], v)) && edges.contains(&(v, path[i + 1])) {
                    path.insert(i + 1, v);
                    inserted = true;
                    break;
                }
            }
            if !inserted {
                // Should never happen in a valid tournament, but handle gracefully.
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