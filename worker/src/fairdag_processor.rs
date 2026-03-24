// FairDAG-RL with explicit FairUpdate (Themis-style).
//
// Replaces the stateful FairnessLayer with:
//   - Stateless per-subdag dependency graph construction
//   - Explicit FairUpdate: missing edges resolved via directed-edge votes
//     embedded in batches by BatchMaker and extracted on commit
//   - Parked graphs awaiting quorum of n-f votes
//   - Sequential finalization by sub_dag_id
//
// NOTE: Requires `lz4_flex` dependency in worker crate Cargo.toml.

use crate::local_order_tracker::extract_tx_digest;
use crate::worker::{FairProposeMessage, FairUpdateVote, WorkerMessage};
use config::Committee;
use crypto::PublicKey;
use log::{debug, error, info, warn};
use lz4_flex::decompress_size_prepended;
use primary::{Certificate, Round};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// TxDigest matching local_order_tracker.rs.
type TxDigest = u64;

// =========================================================================
// Edge compression helpers (lz4 + varint + delta)
// =========================================================================

fn unpack_and_decompress_edges(compressed: &[u8], expected_count: usize) -> Vec<u32> {
    if compressed.is_empty() || expected_count == 0 {
        return vec![];
    }

    let deltas = match decompress_size_prepended(compressed) {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to decompress edges: {}", e);
            return vec![];
        }
    };

    let mut edges: Vec<u32> = Vec::with_capacity(expected_count);
    let mut prev = 0u32;
    let mut i = 0;

    while i < deltas.len() && edges.len() < expected_count {
        let mut delta = 0u32;
        let mut shift = 0;
        loop {
            if i >= deltas.len() {
                break;
            }
            let b = deltas[i];
            i += 1;
            delta |= ((b & 0x7F) as u32) << shift;
            if b < 0x80 {
                break;
            }
            shift += 7;
            if shift > 28 {
                error!("Varint overflow during edge decoding");
                return edges;
            }
        }
        prev = prev.wrapping_add(delta);
        edges.push(prev);
    }

    edges
}

// =========================================================================
// Graph algorithm helpers
// =========================================================================

/// Tarjan SCC on a subgraph defined by `active` nodes.
/// Returns (sccs, scc_id) where scc_id[node] = index into sccs.
fn tarjan_scc(adj: &[Vec<usize>], active: &[usize], k: usize) -> (Vec<Vec<usize>>, Vec<i32>) {
    let mut dfn = vec![0i32; k];
    let mut low = vec![0i32; k];
    let mut on_stack = vec![false; k];
    let mut scc_id = vec![-1i32; k];
    let mut sccs: Vec<Vec<usize>> = Vec::new();
    let mut index_counter: i32 = 0;
    let mut stack: Vec<usize> = Vec::new();

    fn strongconnect(
        u: usize,
        index_counter: &mut i32,
        stack: &mut Vec<usize>,
        on_stack: &mut [bool],
        dfn: &mut [i32],
        low: &mut [i32],
        adj: &[Vec<usize>],
        scc_id: &mut [i32],
        sccs: &mut Vec<Vec<usize>>,
    ) {
        *index_counter += 1;
        dfn[u] = *index_counter;
        low[u] = *index_counter;
        stack.push(u);
        on_stack[u] = true;

        for &v in &adj[u] {
            if dfn[v] == 0 {
                strongconnect(v, index_counter, stack, on_stack, dfn, low, adj, scc_id, sccs);
                if low[v] < low[u] {
                    low[u] = low[v];
                }
            } else if on_stack[v] && dfn[v] < low[u] {
                low[u] = dfn[v];
            }
        }

        if low[u] == dfn[u] {
            let mut comp = Vec::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack[w] = false;
                scc_id[w] = sccs.len() as i32;
                comp.push(w);
                if w == u {
                    break;
                }
            }
            sccs.push(comp);
        }
    }

    for &u in active {
        if dfn[u] == 0 {
            strongconnect(
                u,
                &mut index_counter,
                &mut stack,
                &mut on_stack,
                &mut dfn,
                &mut low,
                adj,
                &mut scc_id,
                &mut sccs,
            );
        }
    }

    (sccs, scc_id)
}

/// Topological sort of the SCC condensation graph.
fn topo_sort_condensation(
    sccs: &[Vec<usize>],
    adj: &[Vec<usize>],
    scc_id: &[i32],
    active: &[usize],
    is_non_blank: &[bool],
) -> Vec<usize> {
    let scc_n = sccs.len();
    let mut gc: Vec<Vec<usize>> = vec![Vec::new(); scc_n];
    let mut indegree: Vec<usize> = vec![0; scc_n];

    for &u in active {
        let su = scc_id[u];
        if su < 0 {
            continue;
        }
        let su = su as usize;
        for &v in &adj[u] {
            if !is_non_blank[v] {
                continue;
            }
            let sv = scc_id[v];
            if sv < 0 || su == sv as usize {
                continue;
            }
            gc[su].push(sv as usize);
        }
    }

    for u in 0..scc_n {
        gc[u].sort_unstable();
        gc[u].dedup();
        for &v in &gc[u] {
            indegree[v] += 1;
        }
    }

    let mut topo: Vec<usize> = Vec::with_capacity(scc_n);
    let mut q: VecDeque<usize> = VecDeque::new();
    for s in 0..scc_n {
        if indegree[s] == 0 {
            q.push_back(s);
        }
    }
    while let Some(u) = q.pop_front() {
        topo.push(u);
        for &v in &gc[u] {
            if indegree[v] > 0 {
                indegree[v] -= 1;
                if indegree[v] == 0 {
                    q.push_back(v);
                }
            }
        }
    }

    topo
}

/// Find a Hamiltonian cycle in a strongly connected tournament.
fn find_hamiltonian_cycle(
    nodes: &[usize],
    has_edge: &impl Fn(usize, usize) -> bool,
) -> Vec<usize> {
    if nodes.len() <= 1 {
        return nodes.to_vec();
    }
    if nodes.len() == 2 {
        return if has_edge(nodes[0], nodes[1]) {
            vec![nodes[0], nodes[1]]
        } else {
            vec![nodes[1], nodes[0]]
        };
    }

    let mut cycle: Vec<usize> = if has_edge(nodes[0], nodes[1]) {
        vec![nodes[0], nodes[1]]
    } else {
        vec![nodes[1], nodes[0]]
    };

    for &node in &nodes[2..] {
        let m = cycle.len();
        let mut inserted = false;
        for j in 0..m {
            let next = (j + 1) % m;
            if has_edge(cycle[j], node) && has_edge(node, cycle[next]) {
                cycle.insert(j + 1, node);
                inserted = true;
                break;
            }
        }
        if !inserted {
            // Fallback — shouldn't happen for SC tournament.
            cycle.push(node);
        }
    }

    cycle
}

/// Order transactions within an SCC using Hamiltonian cycle.
/// For the final SCC, rotates so a solid tx is last.
fn order_scc(
    comp: &[usize],
    is_final_scc: bool,
    is_solid: &[bool],
    has_edge: &impl Fn(usize, usize) -> bool,
) -> Vec<usize> {
    if comp.len() <= 1 {
        return comp.to_vec();
    }

    let mut cycle = find_hamiltonian_cycle(comp, has_edge);

    if is_final_scc {
        // Rotate so a solid tx is LAST (pick smallest-index solid tx, deterministic).
        if let Some(solid_pos) = cycle
            .iter()
            .enumerate()
            .filter(|(_, &tx)| is_solid[tx])
            .min_by_key(|(_, &tx)| tx)
            .map(|(pos, _)| pos)
        {
            let start = (solid_pos + 1) % cycle.len();
            let mut rotated = Vec::with_capacity(cycle.len());
            rotated.extend_from_slice(&cycle[start..]);
            rotated.extend_from_slice(&cycle[..start]);
            cycle = rotated;
        }
    }

    cycle
}

// =========================================================================
// Graph construction result
// =========================================================================

enum GraphResult {
    /// No non-blank txs or no solid anchor — nothing to finalize.
    Empty,
    /// Graph is a tournament (no missing edges). Contains finalized local indices.
    Finalized(Vec<usize>),
    /// Graph has missing edges — needs FairUpdate votes.
    NeedsFairUpdate {
        /// All non-blank vertex indices from first SCC through anchor.
        vertex_indices: Vec<usize>,
        /// Existing directed edges (from, to) in local index space.
        edges: Vec<(usize, usize)>,
        /// Missing edge pairs, packed as pair_key (min << 16 | max).
        missing_edges: Vec<u32>,
        /// Deduplicated vertices involved in missing edges.
        missing_edge_vertices: Vec<u16>,
        /// Solid classification per local index (up to k).
        is_solid: Vec<bool>,
    },
}

/// Build a dependency graph from local orderings extracted from a committed subdag.
///
/// `indices_sets`: one entry per replica-vertex, each a sequence of local tx indices
///                 ordered by OI (lower OI = earlier position).
/// `k`: total number of unique tx digests (size of local index space).
/// `non_blank_threshold`: minimum support for a tx to be non-blank.
/// `solid_threshold`: minimum support for a tx to be solid.
fn build_and_analyze_graph(
    indices_sets: &[Vec<usize>],
    k: usize,
    non_blank_threshold: u8,
    solid_threshold: u8,
) -> GraphResult {
    if k == 0 || indices_sets.is_empty() {
        return GraphResult::Empty;
    }

    // ─── Step 1: Compute support per tx, classify ────────────────────────
    let mut support = vec![0u8; k];
    for order in indices_sets {
        for &tx in order {
            support[tx] = support[tx].saturating_add(1);
        }
    }

    let mut is_non_blank = vec![false; k];
    let mut is_solid = vec![false; k];
    for i in 0..k {
        if support[i] >= non_blank_threshold {
            is_non_blank[i] = true;
        }
        if support[i] >= solid_threshold {
            is_solid[i] = true;
        }
    }

    let active: Vec<usize> = (0..k).filter(|&u| is_non_blank[u]).collect();
    if active.is_empty() {
        return GraphResult::Empty;
    }

    // ─── Step 2: Compute pairwise weights ────────────────────────────────
    // weight[i * k + j] = number of replicas where tx i appears before tx j.
    let mut weight = vec![0u16; k * k];

    for order in indices_sets {
        let len = order.len();
        for from_pos in 0..len {
            let from = order[from_pos];
            if !is_non_blank[from] {
                continue;
            }
            for to_pos in (from_pos + 1)..len {
                let to = order[to_pos];
                if !is_non_blank[to] {
                    continue;
                }
                weight[from * k + to] += 1;
            }
        }
    }

    // ─── Step 3: Add edges ───────────────────────────────────────────────
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); k];
    let t_edge = non_blank_threshold as u16;

    for &u in &active {
        for &v in &active {
            if u >= v {
                continue;
            }
            let kuv = weight[u * k + v];
            let kvu = weight[v * k + u];

            if kuv < t_edge && kvu < t_edge {
                continue; // Missing edge.
            }

            if kuv >= kvu {
                adj[u].push(v);
            } else {
                adj[v].push(u);
            }
        }
    }

    // ─── Step 4: SCC decomposition + topo sort ───────────────────────────
    let (sccs, scc_id) = tarjan_scc(&adj, &active, k);
    if sccs.is_empty() {
        return GraphResult::Empty;
    }

    let topo = topo_sort_condensation(&sccs, &adj, &scc_id, &active, &is_non_blank);

    // ─── Step 5: Find anchor (last SCC containing a solid tx) ────────────
    let mut anchor_idx: Option<usize> = None;
    for (idx, &scc_index) in topo.iter().enumerate() {
        let comp = &sccs[scc_index];
        if comp.iter().any(|&tx| is_solid[tx]) {
            anchor_idx = Some(idx);
        }
    }

    let anchor = match anchor_idx {
        Some(a) => a,
        None => return GraphResult::Empty,
    };

    // ─── Step 6: Collect all vertices from first SCC through anchor ──────
    let mut graph_vertices: Vec<usize> = Vec::new();
    let mut in_graph = vec![false; k];
    for topo_pos in 0..=anchor {
        let comp = &sccs[topo[topo_pos]];
        for &tx in comp {
            graph_vertices.push(tx);
            in_graph[tx] = true;
        }
    }

    // ─── Step 7: Find missing edges among graph vertices ─────────────────
    // Only shaded pairs can have missing edges.
    let shaded_in_graph: Vec<usize> = graph_vertices
        .iter()
        .copied()
        .filter(|&u| !is_solid[u])
        .collect();

    let mut missing_edges: Vec<u32> = Vec::new();
    for i in 0..shaded_in_graph.len() {
        let u = shaded_in_graph[i];
        for j in (i + 1)..shaded_in_graph.len() {
            let v = shaded_in_graph[j];
            let kuv = weight[u * k + v];
            let kvu = weight[v * k + u];
            if kuv < t_edge && kvu < t_edge {
                let (a, b) = if u < v { (u, v) } else { (v, u) };
                missing_edges.push(((a as u32) << 16) | (b as u32));
            }
        }
    }

    // ─── Step 8: Finalize or park ────────────────────────────────────────
    if missing_edges.is_empty() {
        // Tournament — finalize the entire graph.
        let has_edge_fn = |u: usize, v: usize| -> bool {
            let kuv = weight[u * k + v];
            let kvu = weight[v * k + u];
            if kuv < t_edge && kvu < t_edge {
                return false;
            }
            kuv >= kvu
        };

        let mut finalized: Vec<usize> = Vec::new();
        for (idx, &scc_index) in topo.iter().enumerate() {
            if idx > anchor {
                break;
            }
            let comp = &sccs[scc_index];
            let is_final = idx == anchor;
            let ordered = order_scc(comp, is_final, &is_solid, &has_edge_fn);
            finalized.extend(ordered);
        }

        GraphResult::Finalized(finalized)
    } else {
        // Collect existing directed edges within the graph.
        let mut edge_list: Vec<(usize, usize)> = Vec::new();
        for &u in &graph_vertices {
            for &v in &adj[u] {
                if in_graph[v] {
                    edge_list.push((u, v));
                }
            }
        }

        // Deduplicate vertices involved in missing edges.
        let mut missing_edge_vertices: Vec<u16> = Vec::new();
        for &packed in &missing_edges {
            let u = (packed >> 16) as u16;
            let v = (packed & 0xFFFF) as u16;
            missing_edge_vertices.push(u);
            missing_edge_vertices.push(v);
        }
        missing_edge_vertices.sort_unstable();
        missing_edge_vertices.dedup();

        GraphResult::NeedsFairUpdate {
            vertex_indices: graph_vertices,
            edges: edge_list,
            missing_edges,
            missing_edge_vertices,
            is_solid,
        }
    }
}

// =========================================================================
// Parked graph
// =========================================================================

struct ParkedGraph {
    /// All non-blank vertex indices (local index space) from first SCC through anchor.
    vertex_indices: Vec<usize>,
    /// Existing directed edges (from, to) in local index space.
    edges: Vec<(usize, usize)>,
    /// Missing edge pairs, packed as (min << 16 | max).
    missing_edges: Vec<u32>,
    /// Local index → TxDigest mapping for the entire subdag.
    index_to_digest: Vec<TxDigest>,
    /// Solid classification per local index.
    is_solid: Vec<bool>,
}

// =========================================================================
// FairDagProcessor
// =========================================================================

pub struct FairDagProcessor {
    store: Store,
    sorted_keys: Vec<PublicKey>,

    // Thresholds (computed from committee config).
    n: usize,
    f: usize,
    non_blank_threshold: u8,
    solid_threshold: u8,

    rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,

    /// Channel to notify BatchMaker of missing edges.
    tx_fair_propose: Sender<FairProposeMessage>,

    /// Parked graphs awaiting FairUpdate votes, keyed by sub_dag_id.
    parked_graphs: HashMap<u64, ParkedGraph>,

    /// Pending FairUpdate votes: sub_dag_id → (replica PublicKey → directed edges).
    pending_votes: HashMap<u64, HashMap<PublicKey, Vec<u32>>>,

    /// Monotonically increasing sub-dag counter (1-indexed).
    sub_dag_count: u64,

    /// Next sub_dag_id to output in sequential order.
    next_to_finalize: u64,

    /// Results ready to finalize, keyed by sub_dag_id.
    /// Value is the ordered list of TxDigests.
    ready_to_finalize: BTreeMap<u64, Vec<TxDigest>>,
}

impl FairDagProcessor {
    pub fn spawn(
        mut committee: Committee,
        store: Store,
        rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
        fault_threshold: u64,
        tx_fair_propose: Sender<FairProposeMessage>,
    ) {
        let n = committee.size();
        let f = fault_threshold as usize;

        let mut sorted_keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        sorted_keys.sort();

        let gamma = committee.get_gamma().unwrap();

        let non_blank_threshold =
            ((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0).floor() as u8;
        let solid_threshold = (n - 2 * f) as u8;

        info!(
            "FairDagProcessor: n={}, f={}, gamma={}, non_blank_threshold={}, solid_threshold={}",
            n, f, gamma, non_blank_threshold, solid_threshold
        );

        // Spawn with panic propagation.
        let handle = tokio::spawn(async move {
            Self {
                store,
                sorted_keys,
                n,
                f,
                non_blank_threshold,
                solid_threshold,
                rx_committed_subdags,
                tx_fair_propose,
                parked_graphs: HashMap::new(),
                pending_votes: HashMap::new(),
                sub_dag_count: 0,
                next_to_finalize: 1, // 1-indexed
                ready_to_finalize: BTreeMap::new(),
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

    // =====================================================================
    // Main loop
    // =====================================================================

    async fn run(&mut self) {
        while let Some((leader_round, certificates)) = self.rx_committed_subdags.recv().await {
            let batch_start = Instant::now();

            // Drain all queued subdags into a batch.
            let mut batch_raw: Vec<(Round, Vec<Certificate>)> =
                vec![(leader_round, certificates)];
            while let Ok((r, c)) = self.rx_committed_subdags.try_recv() {
                batch_raw.push((r, c));
            }

            let batch_size = batch_raw.len();
            info!(
                "FAIRDAG_TIMING: processing {} subdags in batch",
                batch_size
            );

            for (round, certs) in batch_raw {
                self.sub_dag_count += 1;
                let sub_dag_id = self.sub_dag_count;
                let sd_start = Instant::now();

                // Extract ordering entries + FairUpdate votes from committed batches.
                let (indices_sets, index_to_digest, extracted_votes) =
                    self.extract_subdag_and_votes(round, &certs).await;

                let k = index_to_digest.len();
                let extract_ms = sd_start.elapsed().as_millis();

                info!(
                    "FAIRDAG_TIMING: sub_dag_id={} extract done: {}ms, k={}, replicas={}, votes_found={}",
                    sub_dag_id, extract_ms, k, indices_sets.len(), extracted_votes.len()
                );

                // Aggregate extracted votes into pending_votes.
                for (author, vote) in &extracted_votes {
                    let decompressed =
                        unpack_and_decompress_edges(&vote.directed_edges_compressed, vote.edge_count);
                    if !decompressed.is_empty() {
                        debug!(
                            "FairUpdate: extracted {} votes for sub_dag_id={} from {:?}",
                            decompressed.len(),
                            vote.sub_dag_id,
                            author
                        );
                        self.pending_votes
                            .entry(vote.sub_dag_id)
                            .or_default()
                            .insert(*author, decompressed);
                    }
                }

                // Check if any parked graphs now have quorum.
                self.try_resolve_parked_graphs().await;

                // Build dependency graph for this subdag.
                self.process_new_subdag(sub_dag_id, &indices_sets, k, index_to_digest)
                    .await;

                // Output finalized results in sequential order.
                self.try_finalize_sequential();
            }

            info!(
                "FAIRDAG_TIMING: batch done in {}ms, next_to_finalize={}, parked={}",
                batch_start.elapsed().as_millis(),
                self.next_to_finalize,
                self.parked_graphs.len()
            );
        }
    }

    // =====================================================================
    // Subdag extraction
    // =====================================================================

    /// Extract per-replica ordering sequences and FairUpdate votes from
    /// committed certificates/batches.
    ///
    /// Returns:
    ///   - indices_sets: one ordered list of local tx indices per replica-vertex
    ///   - index_to_digest: local index → TxDigest
    ///   - extracted_votes: (author, FairUpdateVote) from embedded vote payloads
    async fn extract_subdag_and_votes(
        &self,
        _leader_round: Round,
        certificates: &[Certificate],
    ) -> (Vec<Vec<usize>>, Vec<TxDigest>, Vec<(PublicKey, FairUpdateVote)>) {
        // Group ordering entries by replica, merge across rounds.
        // Key: replica_index, Value: Vec<(tx_digest, oi)>
        let mut per_replica: HashMap<usize, Vec<(TxDigest, u64)>> = HashMap::new();
        let mut extracted_votes: Vec<(PublicKey, FairUpdateVote)> = Vec::new();

        for cert in certificates {
            let author = cert.origin();
            let replica_index = self
                .sorted_keys
                .iter()
                .position(|k| *k == author)
                .expect("Certificate author not in committee");

            for batch_digest in cert.header.payload.keys() {
                match self.store.clone().read(batch_digest.to_vec()).await {
                    Ok(Some(serialized_batch)) => {
                        match bincode::deserialize::<WorkerMessage>(&serialized_batch) {
                            Ok(WorkerMessage::Batch(batch_entries, votes)) => {
                                let entries =
                                    per_replica.entry(replica_index).or_default();
                                for (tx_bytes, oi) in batch_entries {
                                    let tx_id = extract_tx_digest(&tx_bytes);
                                    entries.push((tx_id, oi));
                                }
                                // Extract FairUpdate votes.
                                for vote in votes {
                                    extracted_votes.push((author, vote));
                                }
                            }
                            Ok(_) => {
                                warn!("Unexpected WorkerMessage type for batch {:?}", batch_digest);
                            }
                            Err(e) => {
                                error!("Deser fail batch {:?}: {}", batch_digest, e);
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("Batch {:?} not found in store", batch_digest);
                    }
                    Err(e) => {
                        error!("Store read error batch {:?}: {}", batch_digest, e);
                    }
                }
            }
        }

        // Deduplicate per replica: sort by OI, keep first occurrence of each digest.
        for entries in per_replica.values_mut() {
            entries.sort_by_key(|&(_, oi)| oi);
            let mut seen = HashSet::new();
            entries.retain(|(digest, _)| seen.insert(*digest));
        }

        // Build global index space.
        let mut digest_to_index: HashMap<TxDigest, usize> = HashMap::new();
        let mut index_to_digest: Vec<TxDigest> = Vec::new();

        // Sort replicas for deterministic ordering.
        let mut replica_indices: Vec<usize> = per_replica.keys().copied().collect();
        replica_indices.sort_unstable();

        let mut indices_sets: Vec<Vec<usize>> = Vec::new();

        for &ri in &replica_indices {
            let entries = &per_replica[&ri];
            let mut order: Vec<usize> = Vec::with_capacity(entries.len());

            for &(tx_digest, _oi) in entries {
                let idx = *digest_to_index.entry(tx_digest).or_insert_with(|| {
                    let i = index_to_digest.len();
                    index_to_digest.push(tx_digest);
                    i
                });
                order.push(idx);
            }

            if !order.is_empty() {
                indices_sets.push(order);
            }
        }

        (indices_sets, index_to_digest, extracted_votes)
    }

    // =====================================================================
    // New subdag processing
    // =====================================================================

    async fn process_new_subdag(
        &mut self,
        sub_dag_id: u64,
        indices_sets: &[Vec<usize>],
        k: usize,
        index_to_digest: Vec<TxDigest>,
    ) {
        let start = Instant::now();

        let result = build_and_analyze_graph(
            indices_sets,
            k,
            self.non_blank_threshold,
            self.solid_threshold,
        );

        match result {
            GraphResult::Empty => {
                info!(
                    "sub_dag_id={}: empty graph (no non-blank or no solid anchor), {}ms",
                    sub_dag_id,
                    start.elapsed().as_millis()
                );
                // Mark as ready with empty result.
                self.ready_to_finalize.insert(sub_dag_id, vec![]);
            }
            GraphResult::Finalized(local_order) => {
                let tx_order: Vec<TxDigest> =
                    local_order.iter().map(|&i| index_to_digest[i]).collect();
                info!(
                    "sub_dag_id={}: finalized immediately, {} txs, {}ms",
                    sub_dag_id,
                    tx_order.len(),
                    start.elapsed().as_millis()
                );
                self.ready_to_finalize.insert(sub_dag_id, tx_order);
            }
            GraphResult::NeedsFairUpdate {
                vertex_indices,
                edges,
                missing_edges,
                missing_edge_vertices,
                is_solid,
            } => {
                info!(
                    "sub_dag_id={}: parking graph, {} vertices, {} edges, {} missing, {}ms",
                    sub_dag_id,
                    vertex_indices.len(),
                    edges.len(),
                    missing_edges.len(),
                    start.elapsed().as_millis()
                );

                // Build the notification for BatchMaker.
                let vertices_with_digests: Vec<(u16, u64)> = missing_edge_vertices
                    .iter()
                    .map(|&v| (v, index_to_digest[v as usize]))
                    .collect();

                // Park the graph.
                self.parked_graphs.insert(
                    sub_dag_id,
                    ParkedGraph {
                        vertex_indices,
                        edges,
                        missing_edges: missing_edges.clone(),
                        index_to_digest,
                        is_solid,
                    },
                );

                // Notify BatchMaker.
                if let Err(e) = self
                    .tx_fair_propose
                    .send((sub_dag_id, vertices_with_digests, missing_edges))
                    .await
                {
                    error!(
                        "sub_dag_id={}: failed to send FairPropose to BatchMaker: {}",
                        sub_dag_id, e
                    );
                }
            }
        }
    }

    // =====================================================================
    // FairUpdate: resolve parked graphs
    // =====================================================================

    async fn try_resolve_parked_graphs(&mut self) {
        let quorum = self.n - self.f;

        // Find parked graphs that now have enough votes.
        let ready_ids: Vec<u64> = self
            .parked_graphs
            .keys()
            .filter(|id| {
                self.pending_votes
                    .get(id)
                    .map(|v| v.len() >= quorum)
                    .unwrap_or(false)
            })
            .copied()
            .collect();

        for sub_dag_id in ready_ids {
            self.apply_fair_update(sub_dag_id).await;
        }
    }

    async fn apply_fair_update(&mut self, sub_dag_id: u64) {
        let start = Instant::now();

        let parked = match self.parked_graphs.remove(&sub_dag_id) {
            Some(p) => p,
            None => return,
        };

        let votes = match self.pending_votes.remove(&sub_dag_id) {
            Some(v) => v,
            None => {
                // Shouldn't happen if we checked quorum, but be safe.
                self.parked_graphs.insert(sub_dag_id, parked);
                return;
            }
        };

        info!(
            "sub_dag_id={}: applying FairUpdate with {} replica votes, {} missing edges",
            sub_dag_id,
            votes.len(),
            parked.missing_edges.len()
        );

        let k = parked.index_to_digest.len();

        // Build edge set from existing edges.
        let mut edge_set: HashSet<(usize, usize)> = parked.edges.iter().copied().collect();

        // Tally directed votes per missing edge pair.
        // Also track how many replicas voted on edges involving each vertex
        // (the "tx ∈_{n-2f} L_updates" condition).
        let mut vote_weight: HashMap<(usize, usize), u16> = HashMap::new();
        let mut tx_author_count: HashMap<usize, HashSet<usize>> = HashMap::new();

        for (author_idx, (_author, directed_edges)) in votes.iter().enumerate() {
            for &packed in directed_edges {
                let from = (packed >> 16) as usize;
                let to = (packed & 0xFFFF) as usize;
                *vote_weight.entry((from, to)).or_insert(0) += 1;
                tx_author_count
                    .entry(from)
                    .or_default()
                    .insert(author_idx);
                tx_author_count
                    .entry(to)
                    .or_default()
                    .insert(author_idx);
            }
        }

        let t_edge = self.non_blank_threshold as u16;
        let t_solid = self.solid_threshold as u16;
        let mut new_edges_count = 0;

        for &packed in &parked.missing_edges {
            let u = (packed >> 16) as usize;
            let v = (packed & 0xFFFF) as usize;

            // Skip if edge was already resolved somehow.
            if edge_set.contains(&(u, v)) || edge_set.contains(&(v, u)) {
                continue;
            }

            let kuv = vote_weight.get(&(u, v)).copied().unwrap_or(0);
            let kvu = vote_weight.get(&(v, u)).copied().unwrap_or(0);

            let u_in_enough = tx_author_count
                .get(&u)
                .map(|s| s.len() as u16 >= t_solid)
                .unwrap_or(false);
            let v_in_enough = tx_author_count
                .get(&v)
                .map(|s| s.len() as u16 >= t_solid)
                .unwrap_or(false);

            // Per FairUpdate: direction with higher weight wins,
            // but source must be in enough updates and weight must meet threshold.
            if kuv >= kvu {
                if u_in_enough && kuv >= t_edge {
                    edge_set.insert((u, v));
                    new_edges_count += 1;
                }
            } else {
                if v_in_enough && kvu >= t_edge {
                    edge_set.insert((v, u));
                    new_edges_count += 1;
                }
            }
        }

        info!(
            "sub_dag_id={}: FairUpdate added {} new edges, total edges={}, {}ms",
            sub_dag_id,
            new_edges_count,
            edge_set.len(),
            start.elapsed().as_millis()
        );

        // Build adjacency from edge_set for SCC/finalization.
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); k];
        for &(u, v) in &edge_set {
            adj[u].push(v);
        }

        let is_non_blank: Vec<bool> = {
            let mut nb = vec![false; k];
            for &v in &parked.vertex_indices {
                nb[v] = true;
            }
            nb
        };

        // SCC decomposition on the updated graph.
        let (sccs, scc_id) =
            tarjan_scc(&adj, &parked.vertex_indices, k);

        let topo = topo_sort_condensation(
            &sccs,
            &adj,
            &scc_id,
            &parked.vertex_indices,
            &is_non_blank,
        );

        // Finalize via Hamiltonian path per SCC.
        let has_edge_fn = |u: usize, v: usize| -> bool { edge_set.contains(&(u, v)) };

        let mut finalized_indices: Vec<usize> = Vec::new();
        for (idx, &scc_index) in topo.iter().enumerate() {
            let comp = &sccs[scc_index];
            // Determine if this is the "last" SCC for rotation purposes.
            // Use: last SCC in topo order that contains a solid tx.
            let is_final = {
                let mut last_solid = 0;
                for (j, &si) in topo.iter().enumerate() {
                    if sccs[si].iter().any(|&tx| parked.is_solid[tx]) {
                        last_solid = j;
                    }
                }
                idx == last_solid
            };
            let ordered = order_scc(comp, is_final, &parked.is_solid, &has_edge_fn);
            finalized_indices.extend(ordered);
        }

        let tx_order: Vec<TxDigest> = finalized_indices
            .iter()
            .map(|&i| parked.index_to_digest[i])
            .collect();

        info!(
            "sub_dag_id={}: FairUpdate+Finalize complete, {} txs, {}ms",
            sub_dag_id,
            tx_order.len(),
            start.elapsed().as_millis()
        );

        self.ready_to_finalize.insert(sub_dag_id, tx_order);

        // Send cleanup signal to BatchMaker.
        let _ = self.tx_fair_propose.send((sub_dag_id, vec![], vec![])).await;
    }

    // =====================================================================
    // Sequential finalization
    // =====================================================================

    fn try_finalize_sequential(&mut self) {
        loop {
            let id = self.next_to_finalize;
            if let Some(tx_order) = self.ready_to_finalize.remove(&id) {
                if !tx_order.is_empty() {
                    info!(
                        "FairDAG: FINALIZED sub_dag_id={}, {} transactions",
                        id,
                        tx_order.len()
                    );
                    for tx_id in &tx_order {
                        info!("FairDAG-RL ordered transaction: {}", tx_id);
                    }
                } else {
                    debug!("FairDAG: sub_dag_id={} finalized (empty)", id);
                }

                self.next_to_finalize += 1;
            } else {
                break;
            }
        }
    }
}