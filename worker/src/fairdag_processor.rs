// FairDAG-RL with explicit FairUpdate (Themis-style).
//
// Cross-subdag dedup: tracks (replica_index, tx_digest) pairs already seen.
// If replica X reported tx_digest Y in sub-dag 1, Y is dropped from
// replica X's entries in sub-dag 2 (same tx shouldn't contribute twice
// from the same replica).

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

type TxDigest = u64;

// =========================================================================
// Edge compression helpers
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
// Nibble-packed weight helpers
// =========================================================================

#[inline(always)]
fn get_weight(packed: &[u8], idx: usize) -> u8 {
    let b = packed[idx >> 1];
    if idx & 1 == 0 { b & 0x0F } else { b >> 4 }
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

// =========================================================================
// Iterative Tarjan SCC
// =========================================================================

fn tarjan_scc_iterative(node_count: usize, edges: &[Vec<u16>]) -> Vec<Vec<u16>> {
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

// =========================================================================
// Topological sort of SCCs
// =========================================================================

fn topological_sort_sccs(sccs: &[Vec<u16>], edges: &[Vec<u16>], node_count: usize) -> Vec<usize> {
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

// =========================================================================
// Hamiltonian path
// =========================================================================

fn hamiltonian_path(scc: &[u16], edges: &[Vec<u16>]) -> Vec<u16> {
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
            if !inserted { path.push_back(v); }
        }
    }
    path.into_iter().collect()
}

// =========================================================================
// Graph construction result
// =========================================================================

enum GraphResult {
    Empty { sub_dag_id: u64 },
    Finalized { sub_dag_id: u64, tx_order: Vec<TxDigest> },
    NeedsFairUpdate {
        sub_dag_id: u64,
        vertex_indices: Vec<u16>,
        existing_edges: Vec<(u16, u16)>,
        missing_edges: Vec<u32>,
        missing_edge_vertices: Vec<u16>,
        index_to_digest: Vec<TxDigest>,
        is_solid: Vec<bool>,
    },
}

struct FairUpdateResult {
    sub_dag_id: u64,
    tx_order: Vec<TxDigest>,
}

// =========================================================================
// Stateless graph construction (runs on spawn_blocking)
// =========================================================================

fn build_graph(
    sub_dag_id: u64,
    indices_sets: Vec<Vec<usize>>,
    k: usize,
    index_to_digest: Vec<TxDigest>,
    non_blank_threshold: u8,
    solid_threshold: u8,
) -> GraphResult {
    let start = Instant::now();

    if k == 0 || indices_sets.is_empty() {
        return GraphResult::Empty { sub_dag_id };
    }

    let t_edge = non_blank_threshold;

    let mut support = vec![0u8; k];
    for order in &indices_sets {
        for &tx in order { support[tx] = support[tx].saturating_add(1); }
    }

    let mut is_non_blank = vec![false; k];
    let mut is_solid = vec![false; k];
    for i in 0..k {
        if support[i] >= non_blank_threshold { is_non_blank[i] = true; }
        if support[i] >= solid_threshold { is_solid[i] = true; }
    }

    let active: Vec<usize> = (0..k).filter(|&u| is_non_blank[u]).collect();
    if active.is_empty() {
        return GraphResult::Empty { sub_dag_id };
    }

    let nibble_bytes = (k * k + 1) / 2;
    let mut weight = vec![0u8; nibble_bytes];

    #[inline(always)]
    fn w_idx(i: usize, j: usize, k: usize) -> usize { i * k + j }

    for order in &indices_sets {
        let len = order.len();
        for from_pos in 0..len {
            let from = order[from_pos];
            if !is_non_blank[from] { continue; }
            for to_pos in (from_pos + 1)..len {
                let to = order[to_pos];
                if !is_non_blank[to] { continue; }
                inc_weight(&mut weight, w_idx(from, to, k));
            }
        }
    }

    let active_count = active.len();
    let mut orig_to_dense = vec![u16::MAX; k];
    let mut dense_to_orig: Vec<usize> = Vec::with_capacity(active_count);
    for (di, &ai) in active.iter().enumerate() {
        orig_to_dense[ai] = di as u16;
        dense_to_orig.push(ai);
    }

    let mut edges: Vec<Vec<u16>> = (0..active_count).map(|_| Vec::with_capacity(16)).collect();
    for i in 0..active_count {
        let u = dense_to_orig[i];
        for j in (i + 1)..active_count {
            let v = dense_to_orig[j];
            let kuv = get_weight(&weight, w_idx(u, v, k));
            let kvu = get_weight(&weight, w_idx(v, u, k));
            if kuv < t_edge && kvu < t_edge { continue; }
            if kuv >= kvu { edges[i].push(j as u16); } else { edges[j].push(i as u16); }
        }
    }

    let sccs = tarjan_scc_iterative(active_count, &edges);
    if sccs.is_empty() { return GraphResult::Empty { sub_dag_id }; }

    let topo = topological_sort_sccs(&sccs, &edges, active_count);

    let mut anchor_idx: Option<usize> = None;
    for (idx, &scc_index) in topo.iter().enumerate() {
        if sccs[scc_index].iter().any(|&di| is_solid[dense_to_orig[di as usize]]) {
            anchor_idx = Some(idx);
        }
    }

    let anchor = match anchor_idx {
        Some(a) => a,
        None => return GraphResult::Empty { sub_dag_id },
    };

    let mut in_graph = vec![false; active_count];
    for topo_pos in 0..=anchor {
        for &di in &sccs[topo[topo_pos]] { in_graph[di as usize] = true; }
    }

    let shaded_in_graph: Vec<u16> = (0..active_count as u16)
        .filter(|&di| in_graph[di as usize] && !is_solid[dense_to_orig[di as usize]])
        .collect();

    let mut missing_edges: Vec<u32> = Vec::new();
    for i in 0..shaded_in_graph.len() {
        let u = shaded_in_graph[i];
        let u_orig = dense_to_orig[u as usize];
        for j in (i + 1)..shaded_in_graph.len() {
            let v = shaded_in_graph[j];
            let v_orig = dense_to_orig[v as usize];
            let kuv = get_weight(&weight, w_idx(u_orig, v_orig, k));
            let kvu = get_weight(&weight, w_idx(v_orig, u_orig, k));
            if kuv < t_edge && kvu < t_edge {
                let (a, b) = if u < v { (u, v) } else { (v, u) };
                missing_edges.push(((a as u32) << 16) | (b as u32));
            }
        }
    }

    if missing_edges.is_empty() {
        let mut finalized: Vec<TxDigest> = Vec::new();
        for (idx, &scc_index) in topo.iter().enumerate() {
            if idx > anchor { break; }
            let path = hamiltonian_path(&sccs[scc_index], &edges);
            for &di in &path {
                finalized.push(index_to_digest[dense_to_orig[di as usize]]);
            }
        }

        info!(
            "FAIRDAG_TIMING: sub_dag_id={} FINALIZED: k={} active={} txs={} total={}ns",
            sub_dag_id, k, active_count, finalized.len(), start.elapsed().as_nanos()
        );

        GraphResult::Finalized { sub_dag_id, tx_order: finalized }
    } else {
        let mut vertex_indices: Vec<u16> = Vec::new();
        let mut existing_edges_list: Vec<(u16, u16)> = Vec::new();
        for topo_pos in 0..=anchor {
            for &di in &sccs[topo[topo_pos]] { vertex_indices.push(di); }
        }
        for &di in &vertex_indices {
            for &dv in &edges[di as usize] {
                if in_graph[dv as usize] { existing_edges_list.push((di, dv)); }
            }
        }

        let mut missing_edge_vertices: Vec<u16> = Vec::new();
        for &packed in &missing_edges {
            missing_edge_vertices.push((packed >> 16) as u16);
            missing_edge_vertices.push((packed & 0xFFFF) as u16);
        }
        missing_edge_vertices.sort_unstable();
        missing_edge_vertices.dedup();

        let is_solid_dense: Vec<bool> = (0..active_count)
            .map(|di| is_solid[dense_to_orig[di]]).collect();
        let dense_to_digest: Vec<TxDigest> = (0..active_count)
            .map(|di| index_to_digest[dense_to_orig[di]]).collect();

        info!(
            "FAIRDAG_TIMING: sub_dag_id={} PARKED: k={} active={} vertices={} \
             existing={} missing={} total={}ns",
            sub_dag_id, k, active_count, vertex_indices.len(),
            existing_edges_list.len(), missing_edges.len(), start.elapsed().as_nanos()
        );

        GraphResult::NeedsFairUpdate {
            sub_dag_id, vertex_indices,
            existing_edges: existing_edges_list,
            missing_edges, missing_edge_vertices,
            index_to_digest: dense_to_digest,
            is_solid: is_solid_dense,
        }
    }
}

// =========================================================================
// FairUpdate application (runs on spawn_blocking)
// =========================================================================

fn apply_fair_update(
    sub_dag_id: u64,
    vertex_indices: Vec<u16>,
    existing_edges: Vec<(u16, u16)>,
    missing_edges: Vec<u32>,
    index_to_digest: Vec<TxDigest>,
    is_solid: Vec<bool>,
    votes: HashMap<PublicKey, Vec<u32>>,
    non_blank_threshold: u8,
    solid_threshold: u8,
) -> FairUpdateResult {
    let start = Instant::now();
    let k = index_to_digest.len();

    let mut edge_set: HashSet<(u16, u16)> = existing_edges.into_iter().collect();

    let mut vote_weight: HashMap<(u16, u16), u16> = HashMap::new();
    let mut tx_author_count: HashMap<u16, HashSet<usize>> = HashMap::new();

    for (author_idx, (_author, directed_edges)) in votes.iter().enumerate() {
        for &packed in directed_edges {
            let from = (packed >> 16) as u16;
            let to = (packed & 0xFFFF) as u16;
            *vote_weight.entry((from, to)).or_insert(0) += 1;
            tx_author_count.entry(from).or_default().insert(author_idx);
            tx_author_count.entry(to).or_default().insert(author_idx);
        }
    }

    let t_edge = non_blank_threshold as u16;
    let t_solid = solid_threshold as u16;
    let mut new_edges_count = 0;

    for &packed in &missing_edges {
        let u = (packed >> 16) as u16;
        let v = (packed & 0xFFFF) as u16;

        if edge_set.contains(&(u, v)) || edge_set.contains(&(v, u)) { continue; }

        let kuv = vote_weight.get(&(u, v)).copied().unwrap_or(0);
        let kvu = vote_weight.get(&(v, u)).copied().unwrap_or(0);

        let u_in_enough = tx_author_count.get(&u).map(|s| s.len() as u16 >= t_solid).unwrap_or(false);
        let v_in_enough = tx_author_count.get(&v).map(|s| s.len() as u16 >= t_solid).unwrap_or(false);

        if kuv >= kvu {
            if u_in_enough && kuv >= t_edge { edge_set.insert((u, v)); new_edges_count += 1; }
        } else {
            if v_in_enough && kvu >= t_edge { edge_set.insert((v, u)); new_edges_count += 1; }
        }
    }

    let active_count = vertex_indices.len();
    let mut remap = vec![u16::MAX; k];
    let mut unmap: Vec<u16> = Vec::with_capacity(active_count);
    for (i, &v) in vertex_indices.iter().enumerate() {
        remap[v as usize] = i as u16;
        unmap.push(v);
    }

    let mut scc_edges: Vec<Vec<u16>> = (0..active_count).map(|_| Vec::with_capacity(16)).collect();
    for &(u, v) in &edge_set {
        let ru = remap[u as usize];
        let rv = remap[v as usize];
        if ru != u16::MAX && rv != u16::MAX { scc_edges[ru as usize].push(rv); }
    }

    let sccs = tarjan_scc_iterative(active_count, &scc_edges);
    let topo = topological_sort_sccs(&sccs, &scc_edges, active_count);

    let mut finalized: Vec<TxDigest> = Vec::new();
    for &scc_index in &topo {
        let path = hamiltonian_path(&sccs[scc_index], &scc_edges);
        for &ri in &path {
            finalized.push(index_to_digest[unmap[ri as usize] as usize]);
        }
    }

    info!(
        "FAIRDAG_TIMING: sub_dag_id={} FairUpdate: new_edges={} finalized={} {}ms",
        sub_dag_id, new_edges_count, finalized.len(), start.elapsed().as_millis()
    );

    FairUpdateResult { sub_dag_id, tx_order: finalized }
}

// =========================================================================
// Parked graph
// =========================================================================

struct ParkedGraph {
    vertex_indices: Vec<u16>,
    existing_edges: Vec<(u16, u16)>,
    missing_edges: Vec<u32>,
    index_to_digest: Vec<TxDigest>,
    is_solid: Vec<bool>,
}

// =========================================================================
// FairDagProcessor
// =========================================================================

pub struct FairDagProcessor {
    store: Store,
    sorted_keys: Vec<PublicKey>,

    n: usize,
    f: usize,
    non_blank_threshold: u8,
    solid_threshold: u8,

    rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
    tx_fair_propose: Sender<FairProposeMessage>,

    rx_graph_results: tokio::sync::mpsc::Receiver<GraphResult>,
    tx_graph_results: tokio::sync::mpsc::Sender<GraphResult>,

    rx_update_results: tokio::sync::mpsc::Receiver<FairUpdateResult>,
    tx_update_results: tokio::sync::mpsc::Sender<FairUpdateResult>,

    parked_graphs: HashMap<u64, ParkedGraph>,
    pending_votes: HashMap<u64, HashMap<PublicKey, Vec<u32>>>,

    sub_dag_count: u64,
    next_to_finalize: u64,
    ready_to_finalize: BTreeMap<u64, Vec<TxDigest>>,

    /// Cross-subdag dedup: tracks (replica_index, tx_digest) pairs already
    /// seen in any previous subdag. If the same (replica, digest) appears
    /// again in a later subdag, it is dropped during extraction.
    seen_replica_tx: HashSet<(usize, TxDigest)>,
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

        let (tx_graph_results, rx_graph_results) = tokio::sync::mpsc::channel(1024);
        let (tx_update_results, rx_update_results) = tokio::sync::mpsc::channel(1024);

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
                rx_graph_results,
                tx_graph_results,
                rx_update_results,
                tx_update_results,
                parked_graphs: HashMap::new(),
                pending_votes: HashMap::new(),
                sub_dag_count: 0,
                next_to_finalize: 1,
                ready_to_finalize: BTreeMap::new(),
                seen_replica_tx: HashSet::new(),
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
    // Main async loop
    // =====================================================================

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some((leader_round, certificates)) = self.rx_committed_subdags.recv() => {
                    self.sub_dag_count += 1;
                    let sub_dag_id = self.sub_dag_count;
                    let extract_start = Instant::now();

                    let (indices_sets, index_to_digest, extracted_votes) =
                        self.extract_subdag_and_votes(leader_round, &certificates).await;

                    let k = index_to_digest.len();
                    info!(
                        "FAIRDAG_TIMING: sub_dag_id={} extract: {}ms k={} replicas={} votes={}",
                        sub_dag_id, extract_start.elapsed().as_millis(),
                        k, indices_sets.len(), extracted_votes.len()
                    );

                    for (author, vote) in extracted_votes {
                        let decompressed = unpack_and_decompress_edges(
                            &vote.directed_edges_compressed, vote.edge_count,
                        );
                        if !decompressed.is_empty() {
                            self.pending_votes
                                .entry(vote.sub_dag_id)
                                .or_default()
                                .insert(author, decompressed);
                        }
                    }

                    self.try_resolve_parked_graphs().await;

                    let tx = self.tx_graph_results.clone();
                    let nbt = self.non_blank_threshold;
                    let st = self.solid_threshold;

                    tokio::task::spawn_blocking(move || {
                        let result = build_graph(sub_dag_id, indices_sets, k, index_to_digest, nbt, st);
                        let _ = tx.blocking_send(result);
                    });
                },

                Some(result) = self.rx_graph_results.recv() => {
                    match result {
                        GraphResult::Empty { sub_dag_id } => {
                            debug!("sub_dag_id={}: empty graph", sub_dag_id);
                            self.ready_to_finalize.insert(sub_dag_id, vec![]);
                        }
                        GraphResult::Finalized { sub_dag_id, tx_order } => {
                            info!(
                                "sub_dag_id={}: finalized immediately, {} txs",
                                sub_dag_id, tx_order.len()
                            );
                            self.ready_to_finalize.insert(sub_dag_id, tx_order);
                        }
                        GraphResult::NeedsFairUpdate {
                            sub_dag_id, vertex_indices, existing_edges,
                            missing_edges, missing_edge_vertices,
                            index_to_digest, is_solid,
                        } => {
                            info!(
                                "sub_dag_id={}: parking, {} vertices, {} missing edges",
                                sub_dag_id, vertex_indices.len(), missing_edges.len()
                            );

                            let vertices_with_digests: Vec<(u16, u64)> = missing_edge_vertices
                                .iter()
                                .map(|&v| (v, index_to_digest[v as usize]))
                                .collect();

                            self.parked_graphs.insert(sub_dag_id, ParkedGraph {
                                vertex_indices, existing_edges,
                                missing_edges: missing_edges.clone(),
                                index_to_digest, is_solid,
                            });

                            let _ = self.tx_fair_propose
                                .send((sub_dag_id, vertices_with_digests, missing_edges))
                                .await;
                        }
                    }
                    self.try_finalize_sequential();
                },

                Some(result) = self.rx_update_results.recv() => {
                    info!(
                        "sub_dag_id={}: FairUpdate complete, {} txs",
                        result.sub_dag_id, result.tx_order.len()
                    );
                    self.ready_to_finalize.insert(result.sub_dag_id, result.tx_order);
                    let _ = self.tx_fair_propose
                        .send((result.sub_dag_id, vec![], vec![]))
                        .await;
                    self.try_finalize_sequential();
                },
            }
        }
    }

    // =====================================================================
    // Subdag extraction — deduplicates (replica, tx_digest) across subdags
    // =====================================================================

    async fn extract_subdag_and_votes(
        &mut self,
        _leader_round: Round,
        certificates: &[Certificate],
    ) -> (Vec<Vec<usize>>, Vec<TxDigest>, Vec<(PublicKey, FairUpdateVote)>) {
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
                            Ok(WorkerMessage::Batch(direct_entries, indirect_entries, votes)) => {
                                let entries = per_replica.entry(replica_index).or_default();

                                for (tx_bytes, oi) in direct_entries {
                                    let tx_id = extract_tx_digest(&tx_bytes);
                                    // Drop if this (replica, tx) was seen in a previous subdag.
                                    if self.seen_replica_tx.insert((replica_index, tx_id)) {
                                        entries.push((tx_id, oi));
                                    }
                                }

                                for (tx_digest, oi) in indirect_entries {
                                    // Drop if this (replica, tx) was seen in a previous subdag.
                                    if self.seen_replica_tx.insert((replica_index, tx_digest)) {
                                        entries.push((tx_digest, oi));
                                    }
                                }

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

        // Dedup within this subdag: sort by OI, keep first occurrence.
        for entries in per_replica.values_mut() {
            entries.sort_by_key(|&(_, oi)| oi);
            let mut seen = HashSet::new();
            entries.retain(|(digest, _)| seen.insert(*digest));
        }

        // Build global index space.
        let mut digest_to_index: HashMap<TxDigest, usize> = HashMap::new();
        let mut index_to_digest: Vec<TxDigest> = Vec::new();

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
    // FairUpdate resolution
    // =====================================================================

    async fn try_resolve_parked_graphs(&mut self) {
        let quorum = self.n - self.f;

        let ready_ids: Vec<u64> = self
            .parked_graphs
            .keys()
            .filter(|id| {
                self.pending_votes.get(id).map(|v| v.len() >= quorum).unwrap_or(false)
            })
            .copied()
            .collect();

        for sub_dag_id in ready_ids {
            let parked = match self.parked_graphs.remove(&sub_dag_id) {
                Some(p) => p,
                None => continue,
            };
            let votes = match self.pending_votes.remove(&sub_dag_id) {
                Some(v) => v,
                None => { self.parked_graphs.insert(sub_dag_id, parked); continue; }
            };

            info!(
                "sub_dag_id={}: spawning FairUpdate, {} votes, {} missing edges",
                sub_dag_id, votes.len(), parked.missing_edges.len()
            );

            let tx = self.tx_update_results.clone();
            let nbt = self.non_blank_threshold;
            let st = self.solid_threshold;

            tokio::task::spawn_blocking(move || {
                let result = apply_fair_update(
                    sub_dag_id, parked.vertex_indices, parked.existing_edges,
                    parked.missing_edges, parked.index_to_digest, parked.is_solid,
                    votes, nbt, st,
                );
                let _ = tx.blocking_send(result);
            });
        }
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
                        id, tx_order.len()
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