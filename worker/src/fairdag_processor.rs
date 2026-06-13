// ===========================================================================
// IMPLICIT MISSING-EDGE RESOLUTION VARIANT  (ablation build).
//
// This is Herring's processor with ONE thing changed: parked graphs are resolved
// IMPLICITLY from a global ordering-indicator (OI) pool — exactly like FairDAG-RL's
// update_weights_and_edges — instead of from explicit FairUpdate votes. The parallel
// pipeline below (run_phase1 / run_phase23 / Tarjan) is UNCHANGED: the dominant
// O(n·V^2) weight matrix still runs on the thread pool, fully parallel across
// in-flight subdags. Only resolution changes. Purpose: measure the cost of the
// implicit choice (paper W1/W2). It is expected to lose at high load / large N,
// because implicit resolution is a SERIAL cross-graph sweep whose cost grows with
// the parked backlog, whereas explicit resolution is per-graph-independent/parallel.
//
// Replace ONLY this file. The worker (batch_maker.rs), worker.rs, the channels, and
// every spawn site are left untouched: the processor simply never sends FairPropose,
// so the worker's pending_fair_proposals stay empty and it emits an empty votes vec,
// which this processor ignores.
//
// Removed vs explicit: pending_votes, try_resolve_parked_graphs, apply_fair_update,
// FairUpdateResult, the rx/tx_update_results channel, the FairPropose send.
// Added: ImplicitResolver (OI pool + cross-graph sweep), finalize_edges, finalize_swept.
//
// The explicit-only vote codec (unpack_and_decompress_edges) and the lz4/FairUpdateVote
// imports have been removed. The worker still SENDS the (now always-empty) votes field
// in WorkerMessage::Batch; this processor matches and ignores it (no enum change needed).
// NOTE: not compiled in this environment — review before building.
// ===========================================================================
//
// FairDAG-RL with explicit FairUpdate (Themis-style) — Option D + cumulative-chain fix.
//
// === Why this file exists ===
//
// Plain Option D (claim-only-solids + sync-barrier on immediate-prior K_kept) has a
// residual race that violates single_graph when subdags are parked:
//
//   r:    parked, K_r computed, chain signal fires (K_r broadcast to r+1).
//   r+1:  awaits K_r, discards correctly, signals K_{r+1} (which excludes K_r's txs
//         because r+1 already discarded them).
//   r+2:  commits BEFORE r's GraphResult reaches the main loop, so proposed_txs has
//         not yet absorbed K_r. r+2's snapshot includes shaded txs from K_r.
//         r+2 awaits K_{r+1} — does NOT contain K_r's txs — so r+2's discard misses
//         K_r. K_r's shaded txs land in r+2's active set and enter G_{r+2}.
//
// Result: X ∈ G_r ∩ G_{r+2}, single_graph violated. The HashSet at finalization
// masks the duplicate at the output but the SCC structure of G_{r+2} has been
// computed against a transaction that was already finalized elsewhere, which can
// pull other transactions into the wrong batch.
//
// === Fix ===
//
// The chain carries the CUMULATIVE in-flight K_kept (Arc<HashSet>) instead of the
// per-subdag K. Each subdag r's task:
//   1. Awaits prior_cumulative = K_1 ∪ ... ∪ K_{r-1}.
//   2. Uses prior_cumulative as the discard set in phase 2/3.
//   3. Computes own K_r.
//   4. Signals new_cumulative = prior_cumulative ∪ K_r to subdag r+1.
//
// This guarantees that any subdag s > r whose snapshot was extracted before r's
// OnResult fired sees K_r in its discard, regardless of how many parked subdags sit
// between r and s.
//
// The cumulative set is not actively pruned (entries stay even after their owning
// subdag's OnResult has fired and the txs have moved to proposed_txs). This is
// memory-bounded by the depth of in-flight subdags and is harmless because
// extract_subdag_and_votes already excludes proposed_txs from the snapshot, so a
// stale entry in the cumulative set can never match an active vertex.
//
// IMPORTANT: this is exploratory. Correctness has NOT been formally verified.

use crate::local_order_tracker::extract_tx_digest;
use crate::worker::{FairProposeMessage, WorkerMessage};
use config::Committee;
use crypto::PublicKey;
use log::{debug, error, info, warn};
use primary::{Certificate, Round};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

type TxDigest = u64;

/// Cumulative in-flight K_kept passed along the chain. Cheap to clone (Arc bump).
type CumulativeKKept = Arc<HashSet<TxDigest>>;

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
// Phase 1 result: support classification and weight matrix
// =========================================================================

struct Phase1Result {
    sub_dag_id: u64,
    k: usize,
    is_non_blank: Vec<bool>,
    is_solid: Vec<bool>,
    weight: Vec<u8>, // nibble-packed
    index_to_digest: Vec<TxDigest>,
    elapsed_ns: u128,
}

fn run_phase1(
    sub_dag_id: u64,
    indices_sets: Vec<Vec<usize>>,
    k: usize,
    index_to_digest: Vec<TxDigest>,
    non_blank_threshold: u8,
    solid_threshold: u8,
) -> Option<Phase1Result> {
    let start = Instant::now();

    if k == 0 || indices_sets.is_empty() {
        return None;
    }

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

    if !is_non_blank.iter().any(|&b| b) {
        return None;
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

    Some(Phase1Result {
        sub_dag_id,
        k,
        is_non_blank,
        is_solid,
        weight,
        index_to_digest,
        elapsed_ns: start.elapsed().as_nanos(),
    })
}

// =========================================================================
// Phase 2 + 3: Tarjan, anchor, truncation, missing-edge selection
// (runs after sync barrier with prior subdag's cumulative K_kept)
// =========================================================================

enum GraphResult {
    Empty { sub_dag_id: u64 },
    Finalized {
        sub_dag_id: u64,
        tx_order: Vec<TxDigest>,
        /// OWN K_r only (not cumulative). Used by main loop to update proposed_txs.
        k_kept: HashSet<TxDigest>,
    },
    NeedsFairUpdate {
        sub_dag_id: u64,
        vertex_indices: Vec<u16>,
        existing_edges: Vec<(u16, u16)>,
        missing_edges: Vec<u32>,
        missing_edge_vertices: Vec<u16>,
        index_to_digest: Vec<TxDigest>,
        is_solid: Vec<bool>,
        /// OWN K_r only.
        k_kept: HashSet<TxDigest>,
    },
}

// (FairUpdateResult removed — implicit resolution finalizes inline on the main task.)

fn run_phase23(
    p1: Phase1Result,
    prior_cumulative: CumulativeKKept,
    non_blank_threshold: u8,
    cumulative_sender: oneshot::Sender<CumulativeKKept>,
) -> GraphResult {
    let start = Instant::now();
    let Phase1Result {
        sub_dag_id,
        k,
        is_non_blank,
        is_solid,
        weight,
        index_to_digest,
        elapsed_ns: phase1_ns,
    } = p1;

    let t_edge = non_blank_threshold;

    #[inline(always)]
    fn w_idx(i: usize, j: usize, k: usize) -> usize { i * k + j }

    // Active set: non-blank AND not in cumulative discard from prior in-flight subdags.
    let active: Vec<usize> = (0..k)
        .filter(|&u| is_non_blank[u] && !prior_cumulative.contains(&index_to_digest[u]))
        .collect();

    if active.is_empty() {
        // No own contribution; forward prior cumulative unchanged so the chain stays intact.
        let _ = cumulative_sender.send(prior_cumulative);
        return GraphResult::Empty { sub_dag_id };
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
    if sccs.is_empty() {
        let _ = cumulative_sender.send(prior_cumulative);
        return GraphResult::Empty { sub_dag_id };
    }

    let topo = topological_sort_sccs(&sccs, &edges, active_count);

    let mut anchor_idx: Option<usize> = None;
    for (idx, &scc_index) in topo.iter().enumerate() {
        if sccs[scc_index].iter().any(|&di| is_solid[dense_to_orig[di as usize]]) {
            anchor_idx = Some(idx);
        }
    }

    let anchor = match anchor_idx {
        Some(a) => a,
        None => {
            let _ = cumulative_sender.send(prior_cumulative);
            return GraphResult::Empty { sub_dag_id };
        }
    };

    let mut in_graph = vec![false; active_count];
    for topo_pos in 0..=anchor {
        for &di in &sccs[topo[topo_pos]] { in_graph[di as usize] = true; }
    }

    // Own K_r: digests of vertices that entered G_r (in_graph).
    let mut k_kept: HashSet<TxDigest> = HashSet::new();
    for di in 0..active_count {
        if in_graph[di] {
            k_kept.insert(index_to_digest[dense_to_orig[di]]);
        }
    }

    // Build new cumulative = prior_cumulative ∪ k_kept and signal it EARLY so the next
    // subdag's phase 2/3 can start in parallel with this subdag's missing-edge phase.
    //
    // We clone prior_cumulative's contents into a fresh HashSet because the next subdag's
    // discard set is structurally the union of all in-flight K's; we cannot share storage
    // because each chain link extends by k_kept. The Arc itself is still cheap to ship.
    let mut new_cumulative: HashSet<TxDigest> =
        HashSet::with_capacity(prior_cumulative.len() + k_kept.len());
    new_cumulative.extend(prior_cumulative.iter().copied());
    for &tx in &k_kept { new_cumulative.insert(tx); }
    let _ = cumulative_sender.send(Arc::new(new_cumulative));

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
            "FAIRDAG_TIMING: sub_dag_id={} FINALIZED: k={} active={} txs={} \
             cumulative_discard={} phase1={}ns phase23={}ns",
            sub_dag_id, k, active_count, finalized.len(), prior_cumulative.len(),
            phase1_ns, start.elapsed().as_nanos()
        );

        GraphResult::Finalized { sub_dag_id, tx_order: finalized, k_kept }
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
             existing={} missing={} cumulative_discard={} phase1={}ns phase23={}ns",
            sub_dag_id, k, active_count, vertex_indices.len(),
            existing_edges_list.len(), missing_edges.len(), prior_cumulative.len(),
            phase1_ns, start.elapsed().as_nanos()
        );

        GraphResult::NeedsFairUpdate {
            sub_dag_id, vertex_indices,
            existing_edges: existing_edges_list,
            missing_edges, missing_edge_vertices,
            index_to_digest: dense_to_digest,
            is_solid: is_solid_dense,
            k_kept,
        }
    }
}

// =========================================================================
// Implicit missing-edge resolution  (replaces the explicit FairUpdate path)
//
// A parked graph's missing edge (u,v) is resolved using ordering indicators that
// arrive in LATER committed subdags, pulled from a global, UNPRUNED OI pool. This is
// the FairDAG-RL mechanism (update_weights_and_edges), but with Herring's threshold
// τ = n(1-γ)+f+1 and Herring's correctness model (n >= 4f+1, no weak edges).
//
// Why this is the costly choice: resolution is a &mut sweep over ALL parked graphs
// against one shared pool, re-paid every commit. The explicit path instead resolves
// each parked graph independently and in parallel on its own thread. Holding the
// parallel weight pipeline constant, this swap is the entire A/B of the ablation.
// =========================================================================

/// A parked graph awaiting implicit resolution, in DENSE active-index space (exactly
/// what run_phase23 emits for the NeedsFairUpdate case), plus incremental per-pair
/// resolution state so a replica's OI is counted at most once across sweeps.
struct ImplicitParkedGraph {
    vertex_indices: Vec<u16>,        // in-graph dense indices (for the finalize remap)
    existing_edges: Vec<(u16, u16)>, // directions fixed before parking (phase 2/3)
    resolved_edges: Vec<(u16, u16)>, // directions discovered implicitly here
    index_to_digest: Vec<TxDigest>,  // dense index -> tx digest
    missing_pairs: Vec<(u16, u16)>,  // (li, lj) with li < lj, still unresolved
    counted: Vec<u32>,               // per-pair replica bitmask (requires N <= 32)
    w_fwd: Vec<u8>,                  // # replicas with oi(li) < oi(lj)
    w_rev: Vec<u8>,                  // # replicas with oi(lj) < oi(li)
}

impl ImplicitParkedGraph {
    fn is_complete(&self) -> bool {
        self.missing_pairs.is_empty()
    }
    /// Completed edge set = pre-park edges + implicitly resolved edges.
    fn all_edges(&self) -> Vec<(u16, u16)> {
        let mut e = self.existing_edges.clone();
        e.extend_from_slice(&self.resolved_edges);
        e
    }
}

/// Owns the global OI pool and the parked-graph registry. Replaces pending_votes,
/// try_resolve_parked_graphs, apply_fair_update, and the worker FairUpdate channel.
struct ImplicitResolver {
    n: usize,
    tau: u8, // Herring's non-blank threshold τ = floor(n(1-γ)) + f + 1.
    /// committed_ois[digest][replica] = Some(oi) once seen in SOME committed subdag.
    /// UNPRUNED: every later commit may add the evidence that closes an old pair.
    ois: HashMap<TxDigest, Vec<Option<u64>>>,
    parked: HashMap<u64, ImplicitParkedGraph>,
}

impl ImplicitResolver {
    fn new(n: usize, tau: u8) -> Self {
        assert!(
            n <= 32,
            "ImplicitResolver uses a u32 replica bitmask; N must be <= 32"
        );
        Self { n, tau, ois: HashMap::new(), parked: HashMap::new() }
    }

    /// Feed one committed subdag's ordering evidence into the pool. First-observation
    /// per (tx, replica) wins (OIs are monotone per replica under the LOI rule).
    fn ingest_ois(&mut self, entries: &[(TxDigest, usize, u64)]) {
        let n = self.n; // hoist so the or_insert_with closure captures a Copy local, not `self`
        for &(digest, r, oi) in entries {
            let slot = self.ois.entry(digest).or_insert_with(|| vec![None; n]);
            if slot[r].is_none() {
                slot[r] = Some(oi);
            }
        }
    }

    fn park(&mut self, sub_dag_id: u64, g: ImplicitParkedGraph) {
        self.parked.insert(sub_dag_id, g);
    }

    /// Resolve missing pairs of ALL parked graphs from the current OI pool. Returns
    /// sub_dag_ids that became complete this sweep. Cost O( sum_g |missing_g| * n ).
    /// This is the serial cross-graph cost the explicit design avoids.
    fn resolve_sweep(&mut self) -> Vec<u64> {
        let n = self.n;
        let tau = self.tau;
        let mut completed: Vec<u64> = Vec::new();

        for (sid, g) in self.parked.iter_mut() {
            if g.missing_pairs.is_empty() {
                continue;
            }
            let mut done: Vec<usize> = Vec::new();

            for pos in 0..g.missing_pairs.len() {
                let (li, lj) = g.missing_pairs[pos];
                let di = g.index_to_digest[li as usize];
                let dj = g.index_to_digest[lj as usize];

                let (oi_i, oi_j) = match (self.ois.get(&di), self.ois.get(&dj)) {
                    (Some(a), Some(b)) => (a, b),
                    _ => continue,
                };

                let mut mask = g.counted[pos];
                for r in 0..n {
                    if mask & (1u32 << r) != 0 {
                        continue;
                    }
                    if let (Some(a), Some(b)) = (oi_i[r], oi_j[r]) {
                        mask |= 1u32 << r;
                        if a < b {
                            g.w_fwd[pos] += 1;
                        } else {
                            g.w_rev[pos] += 1;
                        }
                    }
                }
                g.counted[pos] = mask;

                if g.w_fwd[pos] >= tau || g.w_rev[pos] >= tau {
                    if g.w_fwd[pos] >= g.w_rev[pos] {
                        g.resolved_edges.push((li, lj));
                    } else {
                        g.resolved_edges.push((lj, li));
                    }
                    done.push(pos);
                }
            }

            done.sort_unstable();
            for &pos in done.iter().rev() {
                g.missing_pairs.swap_remove(pos);
                g.counted.swap_remove(pos);
                g.w_fwd.swap_remove(pos);
                g.w_rev.swap_remove(pos);
            }

            if g.missing_pairs.is_empty() {
                completed.push(*sid);
            }
        }

        completed
    }

    fn take_completed(&mut self, sub_dag_id: u64) -> Option<ImplicitParkedGraph> {
        let complete = self
            .parked
            .get(&sub_dag_id)
            .map_or(false, |g| g.is_complete());
        if complete {
            self.parked.remove(&sub_dag_id)
        } else {
            None
        }
    }

    fn parked_len(&self) -> usize {
        self.parked.len()
    }
}

/// Finalize a completed parked graph: Tarjan -> topo -> Hamiltonian over the full
/// edge set, mapped back to digests. This is exactly the tail of the old
/// apply_fair_update, factored out so the implicit path reuses identical finalization.
fn finalize_edges(
    vertex_indices: &[u16],
    edges: &[(u16, u16)],
    index_to_digest: &[TxDigest],
) -> Vec<TxDigest> {
    let k = index_to_digest.len();
    let edge_set: HashSet<(u16, u16)> = edges.iter().copied().collect();

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
        if ru != u16::MAX && rv != u16::MAX {
            scc_edges[ru as usize].push(rv);
        }
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
    finalized
}

// =========================================================================
// FairDagProcessor (cumulative chain + implicit resolution)
// =========================================================================

pub struct FairDagProcessor {
    store: Store,
    sorted_keys: Vec<PublicKey>,

    non_blank_threshold: u8,
    solid_threshold: u8,

    rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,

    rx_graph_results: tokio::sync::mpsc::Receiver<GraphResult>,
    tx_graph_results: tokio::sync::mpsc::Sender<GraphResult>,

    /// OI pool + parked-graph registry. Implicit resolution lives entirely here;
    /// there is no FairUpdate vote state and no worker round-trip.
    resolver: ImplicitResolver,

    sub_dag_count: u64,
    next_to_finalize: u64,
    ready_to_finalize: BTreeMap<u64, Vec<TxDigest>>,

    pending_support: HashMap<TxDigest, HashSet<usize>>,
    pending_orderings: Vec<Vec<TxDigest>>,
    proposed_txs: HashSet<TxDigest>,

    /// Solids claimed at dispatch (per subdag). Released on result arrival.
    /// Shaded txs are deliberately NOT claimed — they remain visible to concurrent
    /// subdags' snapshots so cross-subdag edge weights are computed correctly. The
    /// cumulative-K chain handles single_graph for shaded.
    claimed_solids: HashMap<u64, HashSet<TxDigest>>,

    finalized_txs: HashSet<TxDigest>,
}

impl FairDagProcessor {
    pub fn spawn(
        mut committee: Committee,
        store: Store,
        rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
        fault_threshold: u64,
        // Kept in the signature so the call site is unchanged; dropped immediately.
        // The implicit build never sends FairPropose, so the worker's receiver simply
        // sees a closed channel and that select branch stays disabled.
        _tx_fair_propose: Sender<FairProposeMessage>,
    ) {
        let n = committee.size();
        let f = fault_threshold as usize;

        let mut sorted_keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        sorted_keys.sort();

        let gamma = committee.get_gamma().unwrap();

        // Paper formula: τ = n(1 − γ) + f + 1.
        let non_blank_threshold =
            ((n as f64) * (1.0 - gamma) + (f as f64) + 1.0).floor() as u8;
        let solid_threshold = (n - 2 * f) as u8;

        info!(
            "FairDagProcessor [IMPLICIT resolution]: n={}, f={}, gamma={}, τ={}, τ_s={}",
            n, f, gamma, non_blank_threshold, solid_threshold
        );

        let (tx_graph_results, rx_graph_results) = tokio::sync::mpsc::channel(1024);

        let handle = tokio::spawn(async move {
            Self {
                store,
                sorted_keys,
                non_blank_threshold,
                solid_threshold,
                rx_committed_subdags,
                rx_graph_results,
                tx_graph_results,
                resolver: ImplicitResolver::new(n, non_blank_threshold),
                sub_dag_count: 0,
                next_to_finalize: 1,
                ready_to_finalize: BTreeMap::new(),
                pending_support: HashMap::new(),
                pending_orderings: vec![Vec::new(); n],
                proposed_txs: HashSet::new(),
                claimed_solids: HashMap::new(),
                finalized_txs: HashSet::new(),
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
        // Genesis: pre-fired sender so subdag 1's task starts with an empty cumulative
        // discard set.
        let (genesis_tx, mut prior_cumulative_rx): (
            oneshot::Sender<CumulativeKKept>,
            oneshot::Receiver<CumulativeKKept>,
        ) = oneshot::channel();
        let _ = genesis_tx.send(Arc::new(HashSet::new()));

        loop {
            tokio::select! {

                Some((leader_round, certificates)) = self.rx_committed_subdags.recv() => {
                    self.sub_dag_count += 1;
                    let sub_dag_id = self.sub_dag_count;
                    let extract_start = Instant::now();

                    let (indices_sets, index_to_digest, oi_entries) =
                        self.extract_subdag(leader_round, &certificates).await;

                    let k = index_to_digest.len();
                    info!(
                        "FAIRDAG_TIMING: sub_dag_id={} extract: {}ms k={} replicas={} oi_entries={}",
                        sub_dag_id, extract_start.elapsed().as_millis(),
                        k, indices_sets.len(), oi_entries.len()
                    );

                    // Implicit resolution: fold this subdag's OIs into the global pool,
                    // then sweep ALL parked graphs (earlier subdags may now resolve from
                    // this subdag's evidence). This sweep is the serial cross-graph cost.
                    self.resolver.ingest_ois(&oi_entries);
                    self.finalize_swept();

                    // Claim only solids in this snapshot. Shaded stays visible.
                    self.compute_and_record_solid_claim(sub_dag_id, &indices_sets, &index_to_digest);

                    // Prepare next link in the cumulative chain.
                    let (cumul_tx, cumul_rx) = oneshot::channel::<CumulativeKKept>();
                    let current_prior_rx = std::mem::replace(&mut prior_cumulative_rx, cumul_rx);
                    // current_prior_rx receives the PREVIOUS subdag's cumulative.

                    let tx = self.tx_graph_results.clone();
                    let nbt = self.non_blank_threshold;
                    let st = self.solid_threshold;

                    // Task: phase 1 (parallel) → await prior cumulative → phase 2/3
                    // (which extends and forwards the cumulative ASAP, before completing).
                    tokio::spawn(async move {
                        let p1_opt = tokio::task::spawn_blocking(move || {
                            run_phase1(sub_dag_id, indices_sets, k, index_to_digest, nbt, st)
                        }).await.expect("phase1 panicked");

                        let prior_cumulative: CumulativeKKept = match current_prior_rx.await {
                            Ok(set) => set,
                            Err(_) => {
                                // Prior task's sender dropped — chain is broken. Aborting
                                // here is the only safe choice; falling back to an empty
                                // set silently violates single_graph.
                                error!(
                                    "FATAL sub_dag_id={}: prior cumulative sender dropped, \
                                     single_graph cannot be guaranteed",
                                    sub_dag_id
                                );
                                std::process::abort();
                            }
                        };

                        let result = match p1_opt {
                            Some(p1) => tokio::task::spawn_blocking(move || {
                                run_phase23(p1, prior_cumulative, nbt, cumul_tx)
                            }).await.expect("phase23 panicked"),
                            None => {
                                // No own contribution; forward prior cumulative unchanged
                                // so the next subdag's task isn't blocked.
                                let _ = cumul_tx.send(prior_cumulative);
                                GraphResult::Empty { sub_dag_id }
                            }
                        };

                        let _ = tx.send(result).await;
                    });
                },

                Some(result) = self.rx_graph_results.recv() => {
                    match result {
                        GraphResult::Empty { sub_dag_id } => {
                            debug!("sub_dag_id={}: empty graph", sub_dag_id);
                            self.release_solid_claim(sub_dag_id);
                            self.ready_to_finalize.insert(sub_dag_id, vec![]);
                        }
                        GraphResult::Finalized { sub_dag_id, tx_order, k_kept } => {
                            info!(
                                "sub_dag_id={}: finalized immediately, {} txs",
                                sub_dag_id, tx_order.len()
                            );
                            self.confirm_proposed(&k_kept);
                            self.release_solid_claim(sub_dag_id);
                            self.ready_to_finalize.insert(sub_dag_id, tx_order);
                        }
                        GraphResult::NeedsFairUpdate {
                            sub_dag_id, vertex_indices, existing_edges,
                            missing_edges, missing_edge_vertices: _,
                            index_to_digest, is_solid: _, k_kept,
                        } => {
                            info!(
                                "sub_dag_id={}: parking (implicit), {} vertices, {} missing edges",
                                sub_dag_id, vertex_indices.len(), missing_edges.len()
                            );

                            self.confirm_proposed(&k_kept);
                            self.release_solid_claim(sub_dag_id);

                            // Unpack dense-index missing pairs. run_phase23 packs each as
                            // ((a as u32) << 16) | (b as u32) with a < b.
                            let mut missing_pairs: Vec<(u16, u16)> =
                                Vec::with_capacity(missing_edges.len());
                            for &p in &missing_edges {
                                let a = (p >> 16) as u16;
                                let b = (p & 0xFFFF) as u16;
                                missing_pairs.push(if a < b { (a, b) } else { (b, a) });
                            }
                            let m = missing_pairs.len();

                            self.resolver.park(sub_dag_id, ImplicitParkedGraph {
                                vertex_indices,
                                existing_edges,
                                resolved_edges: Vec::new(),
                                index_to_digest,
                                missing_pairs,
                                counted: vec![0u32; m],
                                w_fwd: vec![0u8; m],
                                w_rev: vec![0u8; m],
                            });

                            // The just-parked graph may already be resolvable from the
                            // pool accumulated so far.
                            self.finalize_swept();
                        }
                    }
                    self.try_finalize_sequential();
                },
            }
        }
    }

    fn confirm_proposed(&mut self, included: &HashSet<TxDigest>) {
        if included.is_empty() { return; }
        for tx in included {
            self.proposed_txs.insert(*tx);
            self.pending_support.remove(tx);
        }
        for ordering in self.pending_orderings.iter_mut() {
            ordering.retain(|tx| !included.contains(tx));
        }
    }

    fn release_solid_claim(&mut self, sub_dag_id: u64) {
        self.claimed_solids.remove(&sub_dag_id);
    }

    /// Run one implicit resolution sweep and finalize any graphs that just completed.
    /// Completed orders enter `ready_to_finalize`; `try_finalize_sequential` then drains
    /// them in commit order, preserving the Emit serialization point.
    fn finalize_swept(&mut self) {
        let completed = self.resolver.resolve_sweep();
        for sid in completed {
            if let Some(g) = self.resolver.take_completed(sid) {
                let order =
                    finalize_edges(&g.vertex_indices, &g.all_edges(), &g.index_to_digest);
                info!(
                    "sub_dag_id={}: implicitly resolved, {} txs (parked_backlog={})",
                    sid, order.len(), self.resolver.parked_len()
                );
                self.ready_to_finalize.insert(sid, order);
            }
        }
        self.try_finalize_sequential();
    }

    // =====================================================================
    // Subdag extraction
    // =====================================================================

    /// Extract this subdag's per-replica orderings AND every (tx, replica, oi)
    /// observation for the implicit OI pool.
    ///
    /// CRUCIAL DIFFERENCE vs the snapshot path: each observed OI is pushed into
    /// `oi_entries` BEFORE the `proposed_txs` skip. The skip only governs the active
    /// snapshot (which must exclude already-graphed txs); the OI pool must keep
    /// accumulating evidence for already-parked txs, otherwise their missing edges
    /// could never reach threshold. (This is exactly the trap behind the FairDAG-RL
    /// B.1 liveness bug: OIs deposited while a tx is excluded must still be counted.)
    async fn extract_subdag(
        &mut self,
        _leader_round: Round,
        certificates: &[Certificate],
    ) -> (Vec<Vec<usize>>, Vec<TxDigest>, Vec<(TxDigest, usize, u64)>) {
        let mut new_per_replica: HashMap<usize, Vec<(TxDigest, u64)>> = HashMap::new();
        let mut oi_entries: Vec<(TxDigest, usize, u64)> = Vec::new();

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
                            // Third field (FairUpdate votes) is ignored in the implicit build.
                            Ok(WorkerMessage::Batch(direct_entries, indirect_entries, _votes)) => {
                                for (tx_bytes, oi) in direct_entries {
                                    let tx_id = extract_tx_digest(&tx_bytes);
                                    // Pool first — unconditionally, even if proposed/parked.
                                    oi_entries.push((tx_id, replica_index, oi));
                                    if self.proposed_txs.contains(&tx_id) { continue; }
                                    let supp = self.pending_support.entry(tx_id).or_default();
                                    if supp.insert(replica_index) {
                                        new_per_replica
                                            .entry(replica_index)
                                            .or_default()
                                            .push((tx_id, oi));
                                    }
                                }
                                for (tx_id, oi) in indirect_entries {
                                    // Pool first — unconditionally.
                                    oi_entries.push((tx_id, replica_index, oi));
                                    if self.proposed_txs.contains(&tx_id) { continue; }
                                    let supp = self.pending_support.entry(tx_id).or_default();
                                    if supp.insert(replica_index) {
                                        new_per_replica
                                            .entry(replica_index)
                                            .or_default()
                                            .push((tx_id, oi));
                                    }
                                }
                            }
                            Ok(_) => warn!("Unexpected WorkerMessage type for batch {:?}", batch_digest),
                            Err(e) => error!("Deser fail batch {:?}: {}", batch_digest, e),
                        }
                    }
                    Ok(None) => debug!("Batch {:?} not found in store", batch_digest),
                    Err(e) => error!("Store read error batch {:?}: {}", batch_digest, e),
                }
            }
        }

        for (replica_index, mut new_entries) in new_per_replica {
            new_entries.sort_by_key(|&(_, oi)| oi);
            for (tx_id, _) in new_entries {
                self.pending_orderings[replica_index].push(tx_id);
            }
        }

        // Snapshot exclusion at extract time = proposed ∪ all-claimed-solids.
        // Shaded txs from in-flight subdags ARE included in the snapshot; the cumulative
        // chain inside phase 2/3 discards them from the active set under the sync barrier.
        let mut excluded: HashSet<TxDigest> = self.proposed_txs.clone();
        for claim_set in self.claimed_solids.values() {
            for tx in claim_set { excluded.insert(*tx); }
        }

        let mut digest_to_index: HashMap<TxDigest, usize> = HashMap::new();
        let mut index_to_digest: Vec<TxDigest> = Vec::new();
        let mut indices_sets: Vec<Vec<usize>> = Vec::new();

        for ordering in self.pending_orderings.iter() {
            if ordering.is_empty() { continue; }
            let mut order: Vec<usize> = Vec::with_capacity(ordering.len());
            for &tx_digest in ordering {
                if excluded.contains(&tx_digest) { continue; }
                let idx = *digest_to_index.entry(tx_digest).or_insert_with(|| {
                    let i = index_to_digest.len();
                    index_to_digest.push(tx_digest);
                    i
                });
                order.push(idx);
            }
            if !order.is_empty() { indices_sets.push(order); }
        }

        (indices_sets, index_to_digest, oi_entries)
    }

    /// Claim only SOLID transactions in this subdag's snapshot.
    /// Solids are guaranteed to enter G_r (anchor rule), so claiming them eagerly
    /// preserves single_graph for the common case while leaving shaded txs visible
    /// to concurrent subdags' tasks (which discard them via the cumulative chain).
    fn compute_and_record_solid_claim(
        &mut self,
        sub_dag_id: u64,
        indices_sets: &[Vec<usize>],
        index_to_digest: &[TxDigest],
    ) {
        let k = index_to_digest.len();
        let mut support = vec![0u8; k];
        for order in indices_sets {
            for &idx in order { support[idx] = support[idx].saturating_add(1); }
        }

        let mut claim: HashSet<TxDigest> = HashSet::new();
        for (idx, &cnt) in support.iter().enumerate() {
            if cnt >= self.solid_threshold {
                claim.insert(index_to_digest[idx]);
            }
        }

        self.claimed_solids.insert(sub_dag_id, claim);
    }

    // =====================================================================
    // FairUpdate resolution
    // =====================================================================

    // =====================================================================
    // Sequential finalization
    // =====================================================================

    fn try_finalize_sequential(&mut self) {
        loop {
            let id = self.next_to_finalize;
            if let Some(tx_order) = self.ready_to_finalize.remove(&id) {
                let mut count = 0usize;
                for tx_id in &tx_order {
                    if self.finalized_txs.insert(*tx_id) {
                        info!("FairDAG-RL ordered transaction: {}", tx_id);
                        count += 1;
                    }
                }
                if count > 0 {
                    info!(
                        "FairDAG: FINALIZED sub_dag_id={}, {} unique transactions ({} total, {} duplicates skipped)",
                        id, count, tx_order.len(), tx_order.len() - count
                    );
                } else {
                    debug!("FairDAG: sub_dag_id={} finalized (empty or all duplicates)", id);
                }
                self.next_to_finalize += 1;
            } else {
                break;
            }
        }
    }
}