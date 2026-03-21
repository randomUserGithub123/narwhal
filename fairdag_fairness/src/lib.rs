// Copyright(C) FairDAG-RL Implementation
// Implements the Fairness Layer of FairDAG-RL (Sections 6.1–6.3 of the paper).

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
// Node types
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeType {
    Blank,
    Shaded,
    Solid,
}

// =============================================================================
// TransactionNode
// =============================================================================

#[derive(Clone, Debug)]
pub struct TransactionNode {
    pub digest: TxDigest,
    pub node_type: NodeType,
    pub committed_ois: HashMap<ReplicaIndex, u64>,
    pub committed_rounds: HashMap<ReplicaIndex, Round>,
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

    fn appearance_count(&self, up_to_round: Round) -> usize {
        self.committed_rounds
            .values()
            .filter(|&&r| r <= up_to_round)
            .count()
    }
}

// =============================================================================
// DependencyGraph
// =============================================================================

#[derive(Clone, Debug)]
pub struct DependencyGraph {
    pub round: Round,
    pub nodes: HashSet<TxDigest>,
    pub weights: HashMap<(TxDigest, TxDigest), usize>,
    pub edges: HashSet<(TxDigest, TxDigest)>,
    pub finalized: bool,
    pub final_order: Vec<TxDigest>,
    /// Tracks which replicas have already been counted for each normalized pair.
    /// Key: (min(d1,d2), max(d1,d2)). Prevents double-counting across subdags.
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
    /// (one per unordered pair). Just check the count.
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
    solid_threshold: usize,
    half_threshold: usize,

    nodes: HashMap<TxDigest, TransactionNode>,
    graphs: Vec<DependencyGraph>,
    round_to_graph: HashMap<Round, usize>,
    ordered_digests: HashSet<TxDigest>,
    output_sequence: Vec<TxDigest>,
    replica_indices: HashMap<PublicKey, ReplicaIndex>,
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

    pub fn process_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<TxDigest> {
        let r = subdag.leader_round;

        let total_entries: usize = subdag.vertices.iter()
            .map(|v| v.ordering_entries.len()).sum();
        info!(
            "FairnessLayer: processing subdag leader_round={} vertices={} total_entries={}",
            r, subdag.vertices.len(), total_entries
        );

        let graph_idx = self.graphs.len();
        self.graphs.push(DependencyGraph::new(r));
        self.round_to_graph.insert(r, graph_idx);

        let updated_nodes = self.update_nodes_from_subdag(subdag);
        self.classify_and_add_nodes(r, graph_idx, &updated_nodes);
        self.update_weights_and_edges(subdag);

        // Log graph states
        for (gi, g) in self.graphs.iter().enumerate() {
            if !g.finalized && !g.nodes.is_empty() {
                let expected = if g.nodes.len() > 1 {
                    g.nodes.len() * (g.nodes.len() - 1) / 2
                } else { 0 };
                info!(
                    "DIAG graph_state: G[{}] round={} nodes={} edges={}/{} is_tournament={}",
                    gi, g.round, g.nodes.len(), g.edges.len(), expected, g.is_tournament()
                );
            }
        }

        self.try_finalize_all_graphs()
    }

    // =========================================================================
    // Lines 3-10: Update nodes
    // =========================================================================

    fn update_nodes_from_subdag(&mut self, subdag: &CommittedSubdag) -> HashSet<TxDigest> {
        let mut updated_nodes: HashSet<TxDigest> = HashSet::new();
        let r = subdag.leader_round;

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;
            for &(d, oi) in &vertex.ordering_entries {
                if self.ordered_digests.contains(&d) {
                    continue;
                }
                let node = self.nodes.entry(d).or_insert_with(|| TransactionNode::new(d));
                if !node.committed_ois.contains_key(&i) {
                    node.committed_ois.insert(i, oi);
                    node.committed_rounds.insert(i, r);
                    updated_nodes.insert(d);
                }
            }
        }

        info!(
            "FairnessLayer: update_nodes round={} updated={}",
            r, updated_nodes.len()
        );
        updated_nodes
    }

    // =========================================================================
    // Lines 11-18: Classify nodes
    // =========================================================================

    fn classify_and_add_nodes(
        &mut self,
        r: Round,
        graph_idx: usize,
        updated_nodes: &HashSet<TxDigest>,
    ) {
        let mut solid_count = 0usize;
        let mut shaded_count = 0usize;
        let mut blank_count = 0usize;

        for &d in updated_nodes {
            let node = self.nodes.get(&d).unwrap();
            if node.node_type != NodeType::Blank {
                continue;
            }
            let ap = node.appearance_count(r);

            if ap >= self.solid_threshold {
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Solid;
                self.nodes.get_mut(&d).unwrap().graph_index = Some(graph_idx);
                self.graphs[graph_idx].nodes.insert(d);
                solid_count += 1;
            } else if ap >= self.half_threshold {
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Shaded;
                self.nodes.get_mut(&d).unwrap().graph_index = Some(graph_idx);
                self.graphs[graph_idx].nodes.insert(d);
                shaded_count += 1;
            } else {
                blank_count += 1;
            }
        }

        info!(
            "FairnessLayer: classify round={} G[{}] solid={} shaded={} blank={} total_in_graph={}",
            r, graph_idx, solid_count, shaded_count, blank_count,
            self.graphs[graph_idx].nodes.len()
        );
    }

    // =========================================================================
    // Lines 19-39: OPTIMIZED weight update + edge addition
    //
    // Key optimizations vs naive implementation:
    //   1. Group entries by (graph, replica) — build node list ONCE per group
    //   2. Track counted_replicas per pair — each replica counted at most once
    //   3. Skip pairs that already have an edge
    //   4. No allocations inside the inner loop
    // =========================================================================

    fn update_weights_and_edges(&mut self, subdag: &CommittedSubdag) {
        let mut addable_edges: Vec<(TxDigest, TxDigest)> = Vec::new();

        // Phase 1: Group subdag entries by (graph_idx, replica_index).
        // Deduplicate tx digests within each group.
        let mut groups: HashMap<(usize, ReplicaIndex), HashSet<TxDigest>> = HashMap::new();

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;
            for &(d, _oi) in &vertex.ordering_entries {
                if self.ordered_digests.contains(&d) {
                    continue;
                }
                if let Some(g_idx) = self.nodes.get(&d).and_then(|n| n.graph_index) {
                    groups.entry((g_idx, i)).or_default().insert(d);
                }
            }
        }

        let mut stat_pairs_checked: usize = 0;
        let mut stat_pairs_skipped_counted: usize = 0;
        let mut stat_pairs_skipped_edge: usize = 0;
        let mut stat_weights_incremented: usize = 0;

        // Phase 2: For each (graph, replica), compare pairs.
        for (&(g_idx, replica_idx), new_txs) in &groups {
            // Build list of ALL nodes in this graph that have committed_ois[replica_idx].
            // Done ONCE per (graph, replica) — not per entry.
            let nodes_with_oi: Vec<(TxDigest, u64)> = self.graphs[g_idx]
                .nodes
                .iter()
                .filter_map(|&d| {
                    self.nodes
                        .get(&d)
                        .and_then(|n| n.committed_ois.get(&replica_idx).map(|&oi| (d, oi)))
                })
                .collect();

            // For each new tx from this replica, compare against all nodes with OI.
            for &d in new_txs {
                let d_oi = match self.nodes.get(&d).and_then(|n| n.committed_ois.get(&replica_idx))
                {
                    Some(&oi) => oi,
                    None => continue,
                };

                for &(d2, d2_oi) in &nodes_with_oi {
                    if d2 == d {
                        continue;
                    }

                    // Normalize pair: always (smaller, larger) for dedup tracking.
                    let pair = (d.min(d2), d.max(d2));
                    stat_pairs_checked += 1;

                    // Skip if this pair already has an edge.
                    if self.graphs[g_idx].edges.contains(&(d, d2))
                        || self.graphs[g_idx].edges.contains(&(d2, d))
                    {
                        stat_pairs_skipped_edge += 1;
                        continue;
                    }

                    // Skip if this replica was already counted for this pair.
                    let replica_set = self.graphs[g_idx]
                        .counted_replicas
                        .entry(pair)
                        .or_default();
                    if !replica_set.insert(replica_idx) {
                        // Already counted — skip.
                        stat_pairs_skipped_counted += 1;
                        continue;
                    }

                    // Increment weight based on OI comparison.
                    if d_oi < d2_oi {
                        *self.graphs[g_idx].weights.entry((d, d2)).or_insert(0) += 1;
                    } else if d2_oi < d_oi {
                        *self.graphs[g_idx].weights.entry((d2, d)).or_insert(0) += 1;
                    }
                    // Equal OIs: no preference, don't increment either direction.
                    stat_weights_incremented += 1;

                    // Check if either direction reached threshold.
                    let w_fwd = *self.graphs[g_idx].weights.get(&(pair.0, pair.1)).unwrap_or(&0);
                    let w_rev = *self.graphs[g_idx].weights.get(&(pair.1, pair.0)).unwrap_or(&0);

                    if w_fwd >= self.half_threshold || w_rev >= self.half_threshold {
                        addable_edges.push(pair);
                    }
                }
            }
        }

        // Phase 3: Add edges based on majority preference.
        let mut edges_added = 0usize;
        for (d1, d2) in addable_edges {
            let g_idx = match self.nodes.get(&d1).and_then(|n| n.graph_index) {
                Some(idx) => idx,
                None => continue,
            };

            if self.graphs[g_idx].edges.contains(&(d1, d2))
                || self.graphs[g_idx].edges.contains(&(d2, d1))
            {
                continue;
            }

            let w_12 = *self.graphs[g_idx].weights.get(&(d1, d2)).unwrap_or(&0);
            let w_21 = *self.graphs[g_idx].weights.get(&(d2, d1)).unwrap_or(&0);

            if w_12 >= w_21 {
                self.graphs[g_idx].edges.insert((d1, d2));
            } else {
                self.graphs[g_idx].edges.insert((d2, d1));
            }
            edges_added += 1;
        }

        info!(
            "FairnessLayer: weights pairs_checked={} skipped_counted={} skipped_edge={} \
             incremented={} edges_added={} half_threshold={}",
            stat_pairs_checked, stat_pairs_skipped_counted, stat_pairs_skipped_edge,
            stat_weights_incremented, edges_added, self.half_threshold
        );
    }

    // =========================================================================
    // Ordering Finalization — sequential, in-order
    // =========================================================================

    fn try_finalize_all_graphs(&mut self) -> Vec<TxDigest> {
        let mut newly_ordered: Vec<TxDigest> = Vec::new();

        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized {
                continue;
            }
            if self.graphs[g_idx].nodes.is_empty() {
                self.graphs[g_idx].finalized = true;
                continue;
            }
            if !self.graphs[g_idx].is_tournament() {
                break;
            }

            info!(
                "FairnessLayer: graph {} (round {}) is a tournament with {} nodes — finalizing",
                g_idx, self.graphs[g_idx].round, self.graphs[g_idx].nodes.len()
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

    fn finalize_ordering(&mut self, graph_idx: usize) -> Vec<TxDigest> {
        let graph = &self.graphs[graph_idx];
        let nodes: Vec<TxDigest> = graph.nodes.iter().cloned().collect();
        let edges: Vec<(TxDigest, TxDigest)> = graph.edges.iter().cloned().collect();

        let sccs = kosaraju_scc(&nodes, &edges);
        let topo_order = topological_sort_sccs(&sccs, &edges);

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
                for (pos, scc_idx) in topo_order.iter().enumerate() {
                    let scc = &sccs[*scc_idx];
                    if pos <= cutoff_pos {
                        let mut scc_sorted = scc.clone();
                        scc_sorted.sort();
                        ordered.extend(scc_sorted);
                    } else {
                        to_readd.extend(scc.iter());
                    }
                }
            }
            None => {
                warn!(
                    "FairnessLayer: graph {} is a tournament but has no solid nodes — deferring",
                    graph_idx
                );
                return Vec::new();
            }
        }

        self.graphs[graph_idx].finalized = true;
        self.graphs[graph_idx].final_order = ordered.clone();

        for &d in &ordered {
            self.ordered_digests.insert(d);
        }
        self.output_sequence.extend(ordered.iter());

        for d in to_readd {
            if let Some(node) = self.nodes.get_mut(&d) {
                node.node_type = NodeType::Blank;
                node.graph_index = None;
            }
        }

        info!(
            "FairnessLayer: finalized {} transactions from graph {} (round {}). Total ordered: {}",
            ordered.len(), graph_idx, self.graphs[graph_idx].round, self.output_sequence.len()
        );

        ordered
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

fn kosaraju_scc(nodes: &[TxDigest], edges: &[(TxDigest, TxDigest)]) -> Vec<Vec<TxDigest>> {
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

    let mut visited: HashSet<TxDigest> = HashSet::new();
    let mut finish_order: Vec<TxDigest> = Vec::new();

    let mut sorted_nodes = nodes.to_vec();
    sorted_nodes.sort();

    for &start in &sorted_nodes {
        if !visited.contains(&start) {
            dfs_iterative(&adj, start, &mut visited, &mut finish_order);
        }
    }

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
            component.sort();
            sccs.push(component);
        }
    }

    sccs
}

fn dfs_iterative(
    adj: &HashMap<TxDigest, Vec<TxDigest>>,
    start: TxDigest,
    visited: &mut HashSet<TxDigest>,
    finish_order: &mut Vec<TxDigest>,
) {
    let mut stack: Vec<(TxDigest, bool)> = vec![(start, false)];

    while let Some((node, processed)) = stack.pop() {
        if processed {
            finish_order.push(node);
            continue;
        }
        if !visited.insert(node) {
            continue;
        }
        stack.push((node, true));

        if let Some(neighbors) = adj.get(&node) {
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