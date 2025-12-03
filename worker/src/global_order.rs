use primary::Round;
use store::Store;
// Copyright(C) Facebook, Inc. and its affiliates.
use tokio::sync::mpsc::Receiver;
use tokio::task;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use crypto::{Digest, PublicKey};

use crate::batch_maker::Batch;

const MAX_TX: usize = 10_000;

struct GlobalOrderState {

    tx_digest_to_index: HashMap<Vec<u8>, usize>,
    index_to_tx_digest: Vec<Vec<u8>>,
    free_indices: Vec<usize>,
}

impl GlobalOrderState {

    fn new() -> Self {

        let tx_digest_to_index = HashMap::with_capacity(MAX_TX);
        let index_to_tx_digest = Vec::with_capacity(MAX_TX);

        GlobalOrderState {
            tx_digest_to_index,
            index_to_tx_digest,
            free_indices: Vec::new(),
        }
    }

    fn index_for_digest(&mut self, digest: Vec<u8>) -> usize {

        if let Some(&idx) = self.tx_digest_to_index.get(&digest) {
            return idx;
        }

        let idx = if let Some(free_idx) = self.free_indices.pop() {
            self.index_to_tx_digest[free_idx] = digest.clone();
            free_idx
        }else{
            let idx = self.index_to_tx_digest.len();
            self.index_to_tx_digest.push(digest.clone());
            idx
        };

        self.tx_digest_to_index.insert(digest.clone(), idx);

        idx
    }

    fn remove_tx(&mut self, idx: usize) {
        if let Some(tx_digest) = self.index_to_tx_digest.get(idx) {
            self.tx_digest_to_index.remove(tx_digest); 
            self.index_to_tx_digest[idx].clear();
            self.free_indices.push(idx);
        }
    }

}

#[cfg(test)]
#[path = "tests/global_order_tests.rs"]
mod global_order_tests;

pub struct GlobalOrder {

    // The persistent storage.
    store: Store,
    
    rx_local_orders: Receiver<(PublicKey, Digest, Batch)>,
    rx_header_update: Receiver<(PublicKey, Round, Vec<Digest>)>,
    rx_consensus_update: Receiver<Vec<(Round, Vec<PublicKey>)>>,

    n: u64,
    f: u64,
    gamma: f64,
    non_blank_threshold: u16,
    solid_threshold: u16,

    state: Arc<Mutex<GlobalOrderState>>,
    author_to_lo_digests: HashMap<PublicKey, Vec<Digest>>,
    digest_to_local_order: HashMap<Digest, Vec<Vec<u8>>>,
    author_round_boundaries: HashMap<PublicKey, Vec<(Round, usize, usize)>>,

    tx_utig_results: tokio::sync::mpsc::Sender<Vec<usize>>,
    rx_utig_results: tokio::sync::mpsc::Receiver<Vec<usize>>,

}

impl GlobalOrder {

    pub fn new(
        store: Store,
        rx_local_orders: Receiver<(PublicKey, Digest, Batch)>,
        rx_header_update: Receiver<(PublicKey, Round, Vec<Digest>)>,
        rx_consensus_update: Receiver<Vec<(Round, Vec<PublicKey>)>>,
        n: u64,
        f: u64,
        gamma: f64,
    ) -> Self {
        let state = Arc::new(Mutex::new(GlobalOrderState::new()));

        let non_blank_threshold =
            ((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0).floor() as u16;
        let solid_threshold = (n - 2 * f) as u16;

        let (tx_utig_results, rx_utig_results) = tokio::sync::mpsc::channel(1024);

        GlobalOrder {
            store,
            rx_local_orders,
            rx_header_update,
            rx_consensus_update,
            n,
            f,
            gamma,
            non_blank_threshold,
            solid_threshold,
            state,
            author_to_lo_digests: HashMap::new(),
            digest_to_local_order: HashMap::new(),
            author_round_boundaries: HashMap::new(),
            rx_utig_results,
            tx_utig_results,
        }
    }

    pub fn start(self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    async fn run(mut self) {

        loop {
            tokio::select! {

                Some((author, lo_digest, local_order)) = self.rx_local_orders.recv() => {

                    self.author_to_lo_digests
                        .entry(author)
                        .or_default()
                        .push(lo_digest.clone());
                    
                    self.digest_to_local_order
                        .insert(lo_digest, local_order);

                },
                Some((author, round, lo_digests)) = self.rx_header_update.recv() => {
                    
                    let author_local_orders = self.author_to_lo_digests
                        .get(&author)
                        .expect("Header arrived for author, but we have not received LocalOrders?");

                    let author_round_boundary = self.author_round_boundaries
                        .entry(author.clone())
                        .or_default();

                    let prev_boundary_opt = author_round_boundary
                        .iter()
                        .rev()
                        .find(|(r, _start, _end)| *r < round);

                    let search_start = match prev_boundary_opt {
                        Some((_prev_round, _prev_start, prev_end)) => prev_end + 1,
                        None => 0,
                    };

                    let target_len = lo_digests.len();
                    let lo_set: HashSet<Digest> = lo_digests.into_iter().collect();
                    let mut found: Option<(usize, usize)> = None;
                    let n = author_local_orders.len();

                    'outer: for i in search_start..=n - target_len {
                        
                        if !lo_set.contains(&author_local_orders[i]) {
                            continue;
                        }

                        let mut remaining = lo_set.clone();
                        let mut ok = true;

                        for j in i..i + target_len {
                            let d = &author_local_orders[j];
                            if !remaining.remove(d) {
                                ok = false;
                                break;
                            }
                        }

                        if ok && remaining.is_empty() {
                            let start_idx = i;
                            let end_idx = i + target_len - 1;
                            found = Some((start_idx, end_idx));
                            break 'outer;
                        }
                    }

                    match found {
                        Some((start_idx, end_idx)) => {
                            author_round_boundary.push((round, start_idx, end_idx));
                        }
                        None => {
                            log::warn!(
                                "rx_header_update: could not find contiguous block for author {:?}, round {} \
                                starting from index {}, with {} digests",
                                author,
                                round,
                                search_start,
                                target_len
                            );
                        }
                    }

                },
                Some(sub_dag) = self.rx_consensus_update.recv() => {

                    log::info!("Received sub-dag : {:?}", sub_dag);

                    let start_time = Instant::now();

                    let mut author_to_lo_digests_subdag: HashMap<PublicKey, Vec<Digest>> = HashMap::new();

                    for (round, authors) in sub_dag.iter() {
                        for author in authors {

                            let round_boundaries_opt = self.author_round_boundaries.get(author);
                            if round_boundaries_opt.is_none() {
                                log::warn!("Missing round boundaries for author {:?} at round {}", author, round);
                                continue;
                            }

                            let round_boundaries = round_boundaries_opt.unwrap();
                            let boundary_opt = round_boundaries.iter().find(|(r, _, _)| r == round);

                            if boundary_opt.is_none() {
                                log::warn!(
                                    "No boundary recorded for author {:?} in round {}",
                                    author, round
                                );
                                continue;
                            }

                            let (_r, start_idx, end_idx) = *boundary_opt.unwrap();

                            let author_local_orders_opt = self.author_to_lo_digests.get(author);
                            if author_local_orders_opt.is_none() {
                                log::warn!("Author {:?} not found in author_to_lo_digests", author);
                                continue;
                            }

                            let author_local_orders = author_local_orders_opt.unwrap();
                            if end_idx >= author_local_orders.len() {
                                log::warn!(
                                    "Invalid boundary ({},{}) for author {:?} - only {} local orders",
                                    start_idx,
                                    end_idx,
                                    author,
                                    author_local_orders.len()
                                );
                                continue;
                            }

                            let lo_slice = &author_local_orders[start_idx..=end_idx];

                            for lo_digest in lo_slice {
                                author_to_lo_digests_subdag
                                    .entry(*author)
                                    .or_default()
                                    .push(lo_digest.clone());
                            }

                        }
                    }

                    let t1 = start_time.elapsed().as_nanos();
                    log::info!(
                        "t1 : {}", t1
                    );

                    let mut indices_sets: Vec<Vec<usize>> = Vec::new();
                    {
                        let mut g = self.state.lock().expect("Failed to get lock");

                        for (_, lo_digests) in author_to_lo_digests_subdag.iter() {
                            let mut author_indices: Vec<usize> = Vec::new();
                            for lo_digest in lo_digests {

                                let local_order = self.digest_to_local_order
                                    .get(lo_digest)
                                    .expect("Not able to retrieve local_order");
                                for tx_digest in local_order {
                                    author_indices.push(g.index_for_digest(tx_digest.clone()));
                                }

                            }
                            indices_sets.push(author_indices);
                        }

                    }

                    let t2 = start_time.elapsed().as_nanos() - t1;
                    log::info!(
                        "t2 : {}", t2
                    );

                    let non_blank_threshold = self.non_blank_threshold;
                    let solid_threshold = self.solid_threshold;
                    let tx_utig_results = self.tx_utig_results.clone();

                    task::spawn_blocking(move || {
                        run_utig(indices_sets, non_blank_threshold, solid_threshold, tx_utig_results);
                    });

                    log::info!(
                        "spawn_blocking overhead: {} ns",
                        start_time.elapsed().as_nanos() - t2
                    );

                },
                Some(final_order) = self.rx_utig_results.recv() => {
                    let mut g = self.state.lock().expect("Failed to get lock");
                    for idx in &final_order {
                        g.remove_tx(*idx);
                    }
                }

            }
        }
    }
}


fn run_utig(
    indices_sets: Vec<Vec<usize>>,  // GLOBAL indices
    non_blank_threshold: u16,
    solid_threshold: u16,
    tx_utig_results: tokio::sync::mpsc::Sender<Vec<usize>>, // returns GLOBAL indices
) {
    let start_total = Instant::now();

    // 1) Build the set of active global indices and a compact mapping.
    let mut global_to_local: HashMap<usize, usize> = HashMap::new();
    let mut local_to_global: Vec<usize> = Vec::new();

    for indices in &indices_sets {
        for &g_idx in indices {
            if !global_to_local.contains_key(&g_idx) {
                let local_idx = local_to_global.len();
                local_to_global.push(g_idx);
                global_to_local.insert(g_idx, local_idx);
            }
        }
    }

    let k = local_to_global.len();
    log::info!(
        "unique txs in sub_dag: {}", k
    );
    if k == 0 {
        log::info!("run_utig: empty sub-dag, nothing to do");
        return;
    }

    // Map each indices_set to local indices.
    let local_indices_sets: Vec<Vec<usize>> = indices_sets
        .into_iter()
        .map(|indices| {
            indices
                .into_iter()
                .map(|g_idx| *global_to_local.get(&g_idx).unwrap())
                .collect()
        })
        .collect();

    let t_map = start_total.elapsed().as_nanos();
    log::info!("UTIG: mapping global->local took {} ns", t_map);

    // 2) Allocate UTIG structures sized by k (NOT MAX_TX).
    let mut support: Vec<u16> = vec![0u16; k];
    let mut is_non_blank: Vec<bool> = vec![false; k];
    let mut is_solid: Vec<bool> = vec![false; k];
    let mut dirty_nodes: Vec<usize> = Vec::new();
    let mut dirty_pairs: Vec<(usize, usize)> = Vec::new();

    // dense k×k matrix, but k is just "txs in this sub-dag", typically << MAX_TX
    let mut weight: Vec<u16> = vec![0u16; k * k];
    let mut edges: Vec<Vec<usize>> = vec![Vec::new(); k];

    #[inline]
    fn w_idx(i: usize, j: usize, k: usize) -> usize {
        i * k + j
    }

    let t_alloc = start_total.elapsed().as_nanos() - t_map;
    log::info!(
        "t_alloc: {}", t_alloc
    );

    // 3) Phase 2: support, weights, dirty_nodes/pairs
    for indices in &local_indices_sets {
        for (pos, &i) in indices.iter().enumerate() {
            let old_sup = support[i];
            let new_sup = old_sup.saturating_add(1);
            support[i] = new_sup;

            if !is_non_blank[i] && new_sup >= non_blank_threshold {
                is_non_blank[i] = true;
                dirty_nodes.push(i);
            }

            if !is_solid[i] && new_sup >= solid_threshold {
                is_solid[i] = true;
            }

            for &j in &indices[pos + 1..] {
                let idx_ij = w_idx(i, j, k);
                let old_w = weight[idx_ij];
                let new_w = old_w.saturating_add(1);
                weight[idx_ij] = new_w;

                if old_w < non_blank_threshold && new_w >= non_blank_threshold {
                    let (a, b) = if i < j { (i, j) } else { (j, i) };
                    dirty_pairs.push((a, b));
                }
            }
        }
    }

    let t_weights = start_total.elapsed().as_nanos() - t_alloc - t_map;
    log::info!("UTIG: weights+support took {} ns", t_weights);

    // 4) Orientation predicate and edges
    let predicate_p = |u: usize, v: usize, k: usize, weight: &Vec<u16>, t_edge: u16| -> bool {
        let w_uv = weight[w_idx(u, v, k)];
        let w_vu = weight[w_idx(v, u, k)];

        if w_uv < t_edge {
            return false;
        }
        if w_vu < t_edge {
            return true;
        }
        if w_uv > w_vu {
            return true;
        }
        if w_uv < w_vu {
            return false;
        }
        u < v
    };

    for &(u, v) in &dirty_pairs {
        if !is_non_blank[u] || !is_non_blank[v] {
            continue;
        }

        let w_uv = weight[w_idx(u, v, k)];
        let w_vu = weight[w_idx(v, u, k)];
        if w_uv == 0 && w_vu == 0 {
            continue;
        }

        let u_to_v = predicate_p(u, v, k, &weight, non_blank_threshold);
        let v_to_u = predicate_p(v, u, k, &weight, non_blank_threshold);

        if u_to_v {
            edges[u].push(v);
        } else if v_to_u {
            edges[v].push(u);
        }
    }

    let t_edges = start_total.elapsed().as_nanos() - t_map - t_alloc - t_weights;
    log::info!("UTIG: edges/orientation took {} ns", t_edges);

    let active: Vec<usize> = (0..k).filter(|&u| is_non_blank[u]).collect();

    if active.is_empty() {
        log::info!("UTIG: no non-blank txs in this sub-dag, nothing to propose");
        return;
    }

    // 5) Tarjan SCC only on non-blank nodes
    let mut index_counter: i32 = 0;
    let mut stack: Vec<usize> = Vec::new();
    let mut on_stack: Vec<bool> = vec![false; k];
    let mut dfn: Vec<i32> = vec![0; k];
    let mut low: Vec<i32> = vec![0; k];
    let mut scc_id: Vec<i32> = vec![-1; k];
    let mut scc_count: i32 = 0;
    let mut sccs: Vec<Vec<usize>> = Vec::new();

    fn strongconnect(
        u: usize,
        index_counter: &mut i32,
        stack: &mut Vec<usize>,
        on_stack: &mut [bool],
        dfn: &mut [i32],
        low: &mut [i32],
        edges: &Vec<Vec<usize>>,
        scc_id: &mut [i32],
        scc_count: &mut i32,
        sccs: &mut Vec<Vec<usize>>,
    ) {
        *index_counter += 1;
        dfn[u] = *index_counter;
        low[u] = *index_counter;
        stack.push(u);
        on_stack[u] = true;

        for &v in &edges[u] {
            if dfn[v] == 0 {
                strongconnect(v, index_counter, stack, on_stack, dfn, low, edges, scc_id, scc_count, sccs);
                low[u] = std::cmp::min(low[u], low[v]);
            } else if on_stack[v] {
                low[u] = std::cmp::min(low[u], dfn[v]);
            }
        }

        if low[u] == dfn[u] {
            let mut comp = Vec::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack[w] = false;
                scc_id[w] = *scc_count;
                comp.push(w);
                if w == u {
                    break;
                }
            }
            sccs.push(comp);
            *scc_count += 1;
        }
    }

    // **only start DFS from non-blank nodes**
    for &u in &active {
        if dfn[u] == 0 {
            strongconnect(
                u,
                &mut index_counter,
                &mut stack,
                &mut on_stack,
                &mut dfn,
                &mut low,
                &edges,
                &mut scc_id,
                &mut scc_count,
                &mut sccs,
            );
        }
    }

    // 6) Condensation graph + topo sort, again only over non-blank nodes
    let scc_n = sccs.len();
    let mut gc: Vec<Vec<usize>> = vec![Vec::new(); scc_n];
    let mut indegree: Vec<usize> = vec![0; scc_n];

    for &u in &active {
        let su = scc_id[u];
        if su < 0 { continue; }
        let su = su as usize;
        for &v in &edges[u] {
            if !is_non_blank[v] { continue; } // redundant but safe
            let sv = scc_id[v];
            if sv < 0 { continue; }
            let sv = sv as usize;
            if su == sv { continue; }
            gc[su].push(sv);
            indegree[sv] += 1;
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

    // 7) Anchor: last SCC in topo with at least one SOLID tx.
    let mut anchor_idx: Option<usize> = None;
    for (idx, &scc_index) in topo.iter().enumerate() {
        let comp = &sccs[scc_index];
        if comp.iter().any(|&v| is_solid[v]) {
            anchor_idx = Some(idx);
        }
    }

    if let Some(anchor) = anchor_idx {
        let mut final_local: Vec<usize> = Vec::new();

        for topo_pos in 0..=anchor {
            let scc_index = topo[topo_pos];
            let comp = &sccs[scc_index];

            if comp.len() == 1 {
                final_local.push(comp[0]);
            } else {
                let mut sorted = comp.clone();
                sorted.sort_unstable();
                final_local.extend(sorted);
            }
        }

        let solid_nodes = is_solid.iter().filter(|&&solid| solid).count();
        log::info!(
            "solid_nodes : {} {}", solid_nodes, final_local.len()
        );

        let mut unordered_pairs_global: Vec<(usize, usize)> = Vec::new();
        let p = final_local.len();

        for a in 0..p {
            let i = final_local[a];
            if is_solid[i] {
                continue;
            }
            for b in (a + 1)..p {
                let j = final_local[b];
                if is_solid[j] {
                    continue;
                }

                let w_ij = weight[w_idx(i, j, k)];
                let w_ji = weight[w_idx(j, i, k)];

                if w_ij < non_blank_threshold && w_ji < non_blank_threshold {
                    let gi = local_to_global[i];
                    let gj = local_to_global[j];
                    unordered_pairs_global.push((gi, gj));
                }
            }
        }

        log::info!(
            "unordered len : {}", unordered_pairs_global.len()
        );

        // Map local indices back to GLOBAL tx indices.
        let final_global: Vec<usize> = final_local
            .into_iter()
            .map(|li| local_to_global[li])
            .collect();

        let total = start_total.elapsed().as_nanos();
        log::info!(
            "UTIG: finalized prefix length = {}, anchor_scc_idx = {}, total ns = {}",
            final_global.len(),
            anchor,
            total
        );

        let _ = tx_utig_results.blocking_send(final_global);
    } else {
        let total = start_total.elapsed().as_nanos();
        log::info!(
            "UTIG: no solid anchor in this sub_dag, total ns = {}",
            total
        );
    }
}
