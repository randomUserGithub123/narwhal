// Copyright(C) FairDAG-RL Implementation
// Implements the Fairness Layer of FairDAG-RL (Sections 6.1-6.3 of the paper).
//
// OPTIMIZED version:
//   - Vec<u64> for committed_ois/committed_rounds (array access vs HashMap)
//   - Pre-built per-(graph, replica) OI tables in incremental weight update
//   - Pre-collected OI vectors in catchup to avoid repeated lookups
//   - node_vec for cache-friendly iteration
//   - Comprehensive per-phase timing (FAIRNESS_TIMING logs)

use crypto::PublicKey;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

pub type TxDigest = u64;
pub type Round = u64;
pub type ReplicaIndex = usize;
pub type OrderingEntry = (TxDigest, u64);

/// Sentinel value meaning "no OI from this replica".
const OI_NONE: u64 = u64::MAX;
/// Sentinel value meaning "no committed round from this replica".
const ROUND_NONE: u64 = u64::MAX;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeType { Blank, Shaded, Solid }

// =============================================================================
// TransactionNode - Vec-based OIs for O(1) access
// =============================================================================

#[derive(Clone, Debug)]
pub struct TransactionNode {
    pub digest: TxDigest,
    pub node_type: NodeType,
    /// committed_ois[i] = OI from replica i, or OI_NONE if not yet committed.
    pub committed_ois: Vec<u64>,
    /// committed_rounds[i] = leader round when replica i's OI was committed, or ROUND_NONE.
    pub committed_rounds: Vec<u64>,
    pub graph_index: Option<usize>,
}

impl TransactionNode {
    fn new(digest: TxDigest, n_replicas: usize) -> Self {
        TransactionNode {
            digest,
            node_type: NodeType::Blank,
            committed_ois: vec![OI_NONE; n_replicas],
            committed_rounds: vec![ROUND_NONE; n_replicas],
            graph_index: None,
        }
    }

    #[inline]
    fn appearance_count(&self, up_to_round: Round) -> usize {
        self.committed_rounds.iter()
            .filter(|&&r| r != ROUND_NONE && r <= up_to_round)
            .count()
    }

    /// Collect (replica_index, oi) for all replicas that have committed.
    #[inline]
    fn active_ois(&self) -> Vec<(ReplicaIndex, u64)> {
        self.committed_ois.iter().enumerate()
            .filter(|(_, &oi)| oi != OI_NONE)
            .map(|(i, &oi)| (i, oi))
            .collect()
    }
}

// =============================================================================
// DependencyGraph
// =============================================================================

#[derive(Clone, Debug)]
pub struct DependencyGraph {
    pub round: Round,
    pub nodes: HashSet<TxDigest>,
    pub node_vec: Vec<TxDigest>,
    pub weights: HashMap<(TxDigest, TxDigest), usize>,
    pub edges: HashSet<(TxDigest, TxDigest)>,
    pub finalized: bool,
    pub final_order: Vec<TxDigest>,
    pub counted_replicas: HashMap<(TxDigest, TxDigest), HashSet<ReplicaIndex>>,
}

impl DependencyGraph {
    fn new(round: Round) -> Self {
        DependencyGraph {
            round, nodes: HashSet::new(), node_vec: Vec::new(),
            weights: HashMap::new(), edges: HashSet::new(),
            finalized: false, final_order: Vec::new(),
            counted_replicas: HashMap::new(),
        }
    }

    #[inline]
    fn insert_node(&mut self, d: TxDigest) {
        if self.nodes.insert(d) { self.node_vec.push(d); }
    }

    #[inline]
    fn has_edge(&self, d1: TxDigest, d2: TxDigest) -> bool {
        self.edges.contains(&(d1, d2)) || self.edges.contains(&(d2, d1))
    }

    #[inline]
    fn is_tournament(&self) -> bool {
        let n = self.nodes.len();
        if n < 2 { return n <= 1; }
        self.edges.len() == n * (n - 1) / 2
    }
}

// =============================================================================
// FairnessLayer
// =============================================================================

pub struct FairnessLayer {
    pub n: usize,
    pub f: usize,
    n_replicas: usize,
    solid_threshold: usize,
    half_threshold: usize,
    nodes: HashMap<TxDigest, TransactionNode>,
    graphs: Vec<DependencyGraph>,
    round_to_graph: HashMap<Round, usize>,
    ordered_digests: HashSet<TxDigest>,
    output_sequence: Vec<TxDigest>,
    replica_indices: HashMap<PublicKey, ReplicaIndex>,
    use_hamiltonian_path: bool,
    pending_readd: Vec<TxDigest>,
}

impl FairnessLayer {
    pub fn new(committee_keys: Vec<PublicKey>, f: usize) -> Self {
        let n = committee_keys.len();
        let solid_threshold = n - f;
        let half_threshold = (n - f + 1) / 2;
        let replica_indices: HashMap<PublicKey, ReplicaIndex> = committee_keys
            .into_iter().enumerate().map(|(i, pk)| (pk, i)).collect();

        info!("FairnessLayer: n={} f={} solid={} half={}",
            n, f, solid_threshold, half_threshold);

        FairnessLayer {
            n, f, n_replicas: n, solid_threshold, half_threshold,
            nodes: HashMap::new(), graphs: Vec::new(),
            round_to_graph: HashMap::new(), ordered_digests: HashSet::new(),
            output_sequence: Vec::new(), replica_indices,
            use_hamiltonian_path: false, pending_readd: Vec::new(),
        }
    }

    pub fn process_subdag(&mut self, subdag: &CommittedSubdag) -> Vec<TxDigest> {
        let t_total = Instant::now();
        let r = subdag.leader_round;
        let total_entries: usize = subdag.vertices.iter().map(|v| v.ordering_entries.len()).sum();

        let graph_idx = self.graphs.len();
        self.graphs.push(DependencyGraph::new(r));
        self.round_to_graph.insert(r, graph_idx);

        let t0 = Instant::now();
        self.process_pending_readd(graph_idx);
        let t0_ns = t0.elapsed().as_nanos();

        let t1 = Instant::now();
        let updated = self.update_nodes_from_subdag(subdag);
        let t1_ns = t1.elapsed().as_nanos();

        let t2 = Instant::now();
        let newly = self.classify_and_add_nodes(r, graph_idx, &updated);
        let t2_ns = t2.elapsed().as_nanos();

        let t3 = Instant::now();
        self.compute_catchup_weights(&newly, graph_idx);
        let t3_ns = t3.elapsed().as_nanos();

        let t4 = Instant::now();
        self.update_weights_and_edges(subdag);
        let t4_ns = t4.elapsed().as_nanos();

        for (gi, g) in self.graphs.iter().enumerate() {
            if !g.finalized && !g.nodes.is_empty() {
                let exp = if g.nodes.len() > 1 { g.nodes.len() * (g.nodes.len() - 1) / 2 } else { 0 };
                info!("DIAG graph_state: G[{}] round={} nodes={} edges={}/{} is_tournament={}",
                    gi, g.round, g.nodes.len(), g.edges.len(), exp, g.is_tournament());
            }
        }

        let t5 = Instant::now();
        let result = self.try_finalize_all_graphs();
        let t5_ns = t5.elapsed().as_nanos();

        info!("FAIRNESS_TIMING: round={} entries={} updated={} new_classified={} \
             readd_ns={} update_ns={} classify_ns={} catchup_ns={} \
             incr_wt_ns={} finalize_ns={} total_ns={} ordered={}",
            r, total_entries, updated.len(), newly.len(),
            t0_ns, t1_ns, t2_ns, t3_ns, t4_ns, t5_ns,
            t_total.elapsed().as_nanos(), result.len());

        result
    }

    fn update_nodes_from_subdag(&mut self, subdag: &CommittedSubdag) -> HashSet<TxDigest> {
        let mut updated: HashSet<TxDigest> = HashSet::new();
        let r = subdag.leader_round;
        let n_rep = self.n_replicas;

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;
            for &(d, oi) in &vertex.ordering_entries {
                if self.ordered_digests.contains(&d) { continue; }
                let node = self.nodes.entry(d).or_insert_with(|| TransactionNode::new(d, n_rep));
                if node.committed_ois[i] == OI_NONE {
                    node.committed_ois[i] = oi;
                    node.committed_rounds[i] = r;
                    updated.insert(d);
                }
            }
        }
        updated
    }

    fn classify_and_add_nodes(&mut self, r: Round, graph_idx: usize, updated: &HashSet<TxDigest>) -> Vec<TxDigest> {
        let mut newly: Vec<TxDigest> = Vec::new();
        for &d in updated {
            if self.nodes.get(&d).unwrap().node_type != NodeType::Blank { continue; }
            let ap = self.nodes.get(&d).unwrap().appearance_count(r);
            if ap >= self.solid_threshold {
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Solid;
                self.nodes.get_mut(&d).unwrap().graph_index = Some(graph_idx);
                self.graphs[graph_idx].insert_node(d);
                newly.push(d);
            } else if ap >= self.half_threshold {
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Shaded;
                self.nodes.get_mut(&d).unwrap().graph_index = Some(graph_idx);
                self.graphs[graph_idx].insert_node(d);
                newly.push(d);
            }
        }
        newly
    }

    // =========================================================================
    // LIVENESS FIX: Catch-up weight computation
    // Uses direct Vec indexing on committed_ois[i] (O(1) vs HashMap ~30ns)
    // =========================================================================

    fn compute_catchup_weights(&mut self, newly: &[TxDigest], graph_idx: usize) {
        if newly.is_empty() { return; }
        let newly_set: HashSet<TxDigest> = newly.iter().cloned().collect();
        let snapshot: Vec<TxDigest> = self.graphs[graph_idx].node_vec.clone();
        let mut ea = 0usize;
        let mut wc = 0usize;

        for &d in newly {
            let d_ois = self.nodes.get(&d).unwrap().active_ois();
            for &d2 in &snapshot {
                if d2 == d { continue; }
                if newly_set.contains(&d2) && d > d2 { continue; }

                let n2_ois = &self.nodes.get(&d2).unwrap().committed_ois;
                let mut w12 = 0usize;
                let mut w21 = 0usize;
                for &(i, oi1) in &d_ois {
                    let oi2 = n2_ois[i]; // O(1) array access
                    if oi2 != OI_NONE {
                        if oi1 < oi2 { w12 += 1; } else { w21 += 1; }
                    }
                }
                wc += 1;

                self.graphs[graph_idx].weights.insert((d, d2), w12);
                self.graphs[graph_idx].weights.insert((d2, d), w21);

                let pair = (d.min(d2), d.max(d2));
                let rs = self.graphs[graph_idx].counted_replicas.entry(pair).or_default();
                for &(i, _) in &d_ois {
                    if n2_ois[i] != OI_NONE { rs.insert(i); }
                }

                if w12 >= self.half_threshold || w21 >= self.half_threshold {
                    if !self.graphs[graph_idx].has_edge(d, d2) {
                        if w12 >= w21 { self.graphs[graph_idx].edges.insert((d, d2)); }
                        else { self.graphs[graph_idx].edges.insert((d2, d)); }
                        ea += 1;
                    }
                }
            }
        }
        info!("FAIRNESS_TIMING: catchup G[{}] new_nodes={} pairs={} edges_added={}",
            graph_idx, newly.len(), wc, ea);
    }

    // =========================================================================
    // Incremental weight update (Figure 8, Lines 19-39)
    //
    // KEY OPTIMIZATION: For each (graph, replica), pre-build an OI lookup
    // table ONCE, then iterate entries against it. This replaces ~921k
    // individual node lookups with ~2k table-build lookups + fast iteration.
    // =========================================================================

    fn update_weights_and_edges(&mut self, subdag: &CommittedSubdag) {
        let mut addable: HashSet<(TxDigest, TxDigest)> = HashSet::new();
        let mut sp = 0usize; let mut sc = 0usize; let mut se = 0usize; let mut si = 0usize;

        for vertex in &subdag.vertices {
            let i = vertex.replica_index;

            // OPTIMIZATION: Pre-build per-graph OI table for replica i.
            // Built lazily on first access per graph within this vertex.
            let mut oi_cache: HashMap<usize, Vec<(TxDigest, u64)>> = HashMap::new();

            for &(d, _) in &vertex.ordering_entries {
                if self.ordered_digests.contains(&d) { continue; }
                let g_idx = match self.nodes.get(&d).and_then(|n| n.graph_index) {
                    Some(idx) => idx, None => continue,
                };
                let d_oi = self.nodes.get(&d).unwrap().committed_ois[i];
                if d_oi == OI_NONE { continue; }

                // Build or reuse the OI table for (g_idx, replica i)
                let table = oi_cache.entry(g_idx).or_insert_with(|| {
                    self.graphs[g_idx].node_vec.iter().filter_map(|&d2| {
                        let oi2 = self.nodes.get(&d2).unwrap().committed_ois[i];
                        if oi2 != OI_NONE { Some((d2, oi2)) } else { None }
                    }).collect()
                });

                for &(d2, d2_oi) in table.iter() {
                    if d2 == d { continue; }
                    sp += 1;
                    let pair = (d.min(d2), d.max(d2));

                    if self.graphs[g_idx].has_edge(d, d2) { se += 1; continue; }

                    let rs = self.graphs[g_idx].counted_replicas.entry(pair).or_default();
                    if !rs.insert(i) { sc += 1; continue; }

                    if d_oi < d2_oi {
                        *self.graphs[g_idx].weights.entry((d, d2)).or_insert(0) += 1;
                    } else {
                        *self.graphs[g_idx].weights.entry((d2, d)).or_insert(0) += 1;
                    }
                    si += 1;

                    let wf = *self.graphs[g_idx].weights.get(&(pair.0, pair.1)).unwrap_or(&0);
                    let wr = *self.graphs[g_idx].weights.get(&(pair.1, pair.0)).unwrap_or(&0);
                    if wf >= self.half_threshold || wr >= self.half_threshold {
                        addable.insert(pair);
                    }
                }
            }
        }

        let mut ea = 0usize;
        for &(d1, d2) in &addable {
            let g_idx = match self.nodes.get(&d1).and_then(|n| n.graph_index) {
                Some(idx) => idx, None => continue,
            };
            if self.graphs[g_idx].has_edge(d1, d2) { continue; }
            let w12 = *self.graphs[g_idx].weights.get(&(d1, d2)).unwrap_or(&0);
            let w21 = *self.graphs[g_idx].weights.get(&(d2, d1)).unwrap_or(&0);
            if w12 >= w21 { self.graphs[g_idx].edges.insert((d1, d2)); }
            else { self.graphs[g_idx].edges.insert((d2, d1)); }
            ea += 1;
        }

        info!("FAIRNESS_TIMING: incr_wt pairs={} skip_counted={} skip_edge={} incr={} edges_added={}",
            sp, sc, se, si, ea);
    }

    // =========================================================================
    // OrderFinalization (Figure 11)
    // =========================================================================

    fn try_finalize_all_graphs(&mut self) -> Vec<TxDigest> {
        let mut out: Vec<TxDigest> = Vec::new();
        for g_idx in 0..self.graphs.len() {
            if self.graphs[g_idx].finalized { continue; }
            if self.graphs[g_idx].nodes.is_empty() {
                self.graphs[g_idx].finalized = true; continue;
            }
            if !self.graphs[g_idx].is_tournament() { break; }

            let t = Instant::now();
            let order = self.finalize_ordering(g_idx);
            info!("FAIRNESS_TIMING: finalize G[{}] nodes={} ordered={} time={}ns",
                g_idx, self.graphs[g_idx].nodes.len(), order.len(), t.elapsed().as_nanos());
            out.extend(order);
        }
        out
    }

    fn finalize_ordering(&mut self, gi: usize) -> Vec<TxDigest> {
        let t0 = Instant::now();
        let nodes: Vec<TxDigest> = self.graphs[gi].node_vec.clone();
        let edges: Vec<(TxDigest, TxDigest)> = self.graphs[gi].edges.iter().cloned().collect();

        let t_scc = Instant::now();
        let sccs = tarjan_scc(&nodes, &edges);
        let scc_ns = t_scc.elapsed().as_nanos();

        let t_topo = Instant::now();
        let topo = topological_sort_sccs(&sccs, &edges);
        let topo_ns = t_topo.elapsed().as_nanos();

        let mut last_solid: Option<usize> = None;
        for (pos, &si) in topo.iter().enumerate() {
            if sccs[si].iter().any(|d| self.nodes.get(d).map_or(false, |n| n.node_type == NodeType::Solid)) {
                last_solid = Some(pos);
            }
        }

        let mut ordered: Vec<TxDigest> = Vec::new();
        let mut to_readd: Vec<TxDigest> = Vec::new();

        match last_solid {
            Some(cutoff) => {
                for (pos, &si) in topo.iter().enumerate() {
                    let scc = &sccs[si];
                    if pos <= cutoff {
                        let path = if self.use_hamiltonian_path {
                            let ss: HashSet<TxDigest> = scc.iter().cloned().collect();
                            let se: HashSet<(TxDigest, TxDigest)> = edges.iter()
                                .filter(|(u, v)| ss.contains(u) && ss.contains(v))
                                .cloned().collect();
                            hamiltonian_path(scc, &se)
                        } else {
                            let mut s = scc.clone(); s.sort(); s
                        };
                        ordered.extend(&path);
                    } else {
                        to_readd.extend(scc.iter());
                    }
                }
            }
            None => {
                warn!("FairnessLayer: graph {} no solid nodes, deferring", gi);
                return Vec::new();
            }
        }

        self.graphs[gi].finalized = true;
        self.graphs[gi].final_order = ordered.clone();
        for &d in &ordered { self.ordered_digests.insert(d); }
        self.output_sequence.extend(ordered.iter());

        let to_readd_len = to_readd.len();

        if !to_readd.is_empty() {
            match self.find_next_unfinalized_graph(gi) {
                Some(ni) => self.readd_nodes_to_graph(to_readd, ni),
                None => {
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

        info!("FAIRNESS_TIMING: finalize_detail G[{}] nodes={} sccs={} scc_ns={} topo_ns={} \
             ordered={} readd={} total_ns={}",
            gi, nodes.len(), sccs.len(), scc_ns, topo_ns,
            ordered.len(), to_readd_len, t0.elapsed().as_nanos());

        ordered
    }

    fn find_next_unfinalized_graph(&self, after: usize) -> Option<usize> {
        ((after + 1)..self.graphs.len()).find(|&i| !self.graphs[i].finalized)
    }

    fn readd_nodes_to_graph(&mut self, to_readd: Vec<TxDigest>, tgi: usize) {
        let rp = self.graphs[tgi].round;
        for d in &to_readd {
            let d = *d;
            let ap = self.nodes.get(&d).unwrap().appearance_count(rp);
            if ap >= self.solid_threshold {
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Solid;
            } else {
                self.nodes.get_mut(&d).unwrap().node_type = NodeType::Shaded;
            }
            self.nodes.get_mut(&d).unwrap().graph_index = Some(tgi);
            self.graphs[tgi].insert_node(d);

            let d_ois = self.nodes.get(&d).unwrap().active_ois();
            let existing: Vec<TxDigest> = self.graphs[tgi].node_vec.iter()
                .filter(|&&x| x != d).cloned().collect();

            for d2 in existing {
                let n2 = &self.nodes.get(&d2).unwrap().committed_ois;
                let mut w12 = 0usize; let mut w21 = 0usize;
                for &(i, oi1) in &d_ois {
                    let oi2 = n2[i];
                    if oi2 != OI_NONE {
                        if oi1 < oi2 { w12 += 1; } else { w21 += 1; }
                    }
                }
                self.graphs[tgi].weights.insert((d, d2), w12);
                self.graphs[tgi].weights.insert((d2, d), w21);
                if w12 >= self.half_threshold || w21 >= self.half_threshold {
                    if !self.graphs[tgi].has_edge(d, d2) {
                        if w12 >= w21 { self.graphs[tgi].edges.insert((d, d2)); }
                        else { self.graphs[tgi].edges.insert((d2, d)); }
                    }
                }
            }
        }
    }

    fn process_pending_readd(&mut self, gi: usize) {
        if self.pending_readd.is_empty() { return; }
        let p: Vec<TxDigest> = self.pending_readd.drain(..)
            .filter(|d| !self.ordered_digests.contains(d)).collect();
        if !p.is_empty() { self.readd_nodes_to_graph(p, gi); }
    }

    pub fn get_output_sequence(&self) -> &[TxDigest] { &self.output_sequence }

    pub fn pending_count(&self) -> usize {
        self.nodes.values().filter(|n| !self.ordered_digests.contains(&n.digest)).count()
    }

    pub fn replica_index(&self, pk: &PublicKey) -> Option<ReplicaIndex> {
        self.replica_indices.get(pk).copied()
    }
}

// =============================================================================
// Tarjan's SCC (iterative)
// =============================================================================

fn tarjan_scc(nodes: &[TxDigest], edges: &[(TxDigest, TxDigest)]) -> Vec<Vec<TxDigest>> {
    let mut adj: HashMap<TxDigest, Vec<TxDigest>> = HashMap::new();
    for &n in nodes { adj.entry(n).or_default(); }
    for &(u, v) in edges { adj.entry(u).or_default().push(v); }
    for list in adj.values_mut() { list.sort(); }

    let mut ic: usize = 0;
    let mut stack: Vec<TxDigest> = Vec::new();
    let mut on_stack: HashSet<TxDigest> = HashSet::new();
    let mut idx_of: HashMap<TxDigest, usize> = HashMap::new();
    let mut lowlink: HashMap<TxDigest, usize> = HashMap::new();
    let mut sccs: Vec<Vec<TxDigest>> = Vec::new();

    let mut sn = nodes.to_vec(); sn.sort();

    for &start in &sn {
        if idx_of.contains_key(&start) { continue; }
        let mut ds: Vec<(TxDigest, usize)> = Vec::new();
        idx_of.insert(start, ic); lowlink.insert(start, ic); ic += 1;
        stack.push(start); on_stack.insert(start); ds.push((start, 0));

        while !ds.is_empty() {
            let (v, ni) = *ds.last().unwrap();
            let nb = adj.get(&v).cloned().unwrap_or_default();
            if ni < nb.len() {
                let w = nb[ni];
                ds.last_mut().unwrap().1 += 1;
                if !idx_of.contains_key(&w) {
                    idx_of.insert(w, ic); lowlink.insert(w, ic); ic += 1;
                    stack.push(w); on_stack.insert(w); ds.push((w, 0));
                } else if on_stack.contains(&w) {
                    let vl = lowlink[&v]; let wi = idx_of[&w];
                    if wi < vl { lowlink.insert(v, wi); }
                }
            } else {
                if lowlink[&v] == idx_of[&v] {
                    let mut scc: Vec<TxDigest> = Vec::new();
                    loop {
                        let w = stack.pop().unwrap(); on_stack.remove(&w); scc.push(w);
                        if w == v { break; }
                    }
                    scc.sort(); sccs.push(scc);
                }
                ds.pop();
                if let Some(&(p, _)) = ds.last() {
                    let vl = lowlink[&v]; let pl = lowlink[&p];
                    if vl < pl { lowlink.insert(p, vl); }
                }
            }
        }
    }
    sccs.reverse(); sccs
}

fn topological_sort_sccs(sccs: &[Vec<TxDigest>], edges: &[(TxDigest, TxDigest)]) -> Vec<usize> {
    let mut n2s: HashMap<TxDigest, usize> = HashMap::new();
    for (i, scc) in sccs.iter().enumerate() { for &n in scc { n2s.insert(n, i); } }
    let num = sccs.len();
    let mut ind: Vec<usize> = vec![0; num];
    let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); num];
    for &(u, v) in edges {
        if let (Some(&su), Some(&sv)) = (n2s.get(&u), n2s.get(&v)) {
            if su != sv && adj[su].insert(sv) { ind[sv] += 1; }
        }
    }
    let mut rdy: BTreeMap<usize, ()> = BTreeMap::new();
    for i in 0..num { if ind[i] == 0 { rdy.insert(i, ()); } }
    let mut res: Vec<usize> = Vec::new();
    while let Some((&i, _)) = rdy.iter().next() {
        rdy.remove(&i); res.push(i);
        for &nb in &adj[i] { ind[nb] -= 1; if ind[nb] == 0 { rdy.insert(nb, ()); } }
    }
    res
}

fn hamiltonian_path(nodes: &[TxDigest], edges: &HashSet<(TxDigest, TxDigest)>) -> Vec<TxDigest> {
    if nodes.len() <= 1 { return nodes.to_vec(); }
    let mut s = nodes.to_vec(); s.sort();
    let mut p: VecDeque<TxDigest> = VecDeque::new();
    p.push_back(s[0]);
    for &v in &s[1..] {
        if edges.contains(&(v, *p.front().unwrap())) { p.push_front(v); }
        else if edges.contains(&(*p.back().unwrap(), v)) { p.push_back(v); }
        else {
            let mut ins = false;
            for i in 0..p.len()-1 {
                if edges.contains(&(p[i], v)) && edges.contains(&(v, p[i+1])) {
                    p.insert(i+1, v); ins = true; break;
                }
            }
            if !ins { warn!("Hamiltonian: could not insert {}", v); p.push_back(v); }
        }
    }
    p.into_iter().collect()
}