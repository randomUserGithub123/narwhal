use futures::channel::mpsc::Sender;
use primary::Round;
use store::Store;
// Copyright(C) Facebook, Inc. and its affiliates.
use tokio::sync::mpsc::Receiver;
use tokio::task;
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use crypto::{Digest, Hash, PublicKey};
use nohash::{IntMap, IntSet};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    TxDigest(Digest),
    Batch(PublicKey, Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

use crate::batch_maker::Batch;

const MAX_TX: usize = 20_000;
const MATRIX_POOL_SIZE: usize = 6; // M

pub struct UTIGMatrix {
    pub weight: Vec<u8>,          // N×N
    pub support: Vec<u8>,         // N
    pub is_non_blank: Vec<bool>,   // N
    pub is_solid: Vec<bool>,       // N
    pub edges: Vec<Vec<u16>>,    // adjacency
}

impl UTIGMatrix {
    pub fn new() -> Self {
        UTIGMatrix {
            weight: vec![0; MAX_TX * MAX_TX],
            support: vec![0; MAX_TX],
            is_non_blank: vec![false; MAX_TX],
            is_solid: vec![false; MAX_TX],
            edges: (0..MAX_TX).map(|_| Vec::with_capacity(64)).collect(),
        }
    }

    #[inline]
    pub fn reset(&mut self, k: usize) {
        // fast clear: only reset the slice actually used
        self.weight[..k * k].fill(0);
        self.support[..k].fill(0);
        self.is_non_blank[..k].fill(false);
        self.is_solid[..k].fill(false);
        for e in &mut self.edges[..k] { e.clear(); }
    }
}

pub struct UTIGMatrixPool {
    pub pool: [UTIGMatrix; MATRIX_POOL_SIZE],
    pub used: [bool; MATRIX_POOL_SIZE],
    pub next: usize,
}

impl UTIGMatrixPool {
    pub fn new() -> Self {
        UTIGMatrixPool {
            pool: [
                UTIGMatrix::new(),
                UTIGMatrix::new(),
                UTIGMatrix::new(),
                UTIGMatrix::new(),
                UTIGMatrix::new(),
                UTIGMatrix::new(),
            ],
            used: [false; MATRIX_POOL_SIZE],
            next: 0,
        }
    }

    pub fn acquire_slot(&mut self) -> Option<usize> {
        for i in 0..MATRIX_POOL_SIZE {
            let idx = (self.next + i) % MATRIX_POOL_SIZE;
            if !self.used[idx] {
                self.used[idx] = true;
                self.next = (idx + 1) % MATRIX_POOL_SIZE;
                return Some(idx);
            }
        }
        None
    }

    pub fn release_slot(&mut self, idx: usize) {
        debug_assert!(idx < MATRIX_POOL_SIZE);
        debug_assert!(self.used[idx]);
        self.used[idx] = false;
    }
}


static UTIG_POOL: Lazy<Mutex<UTIGMatrixPool>> =
    Lazy::new(|| Mutex::new(UTIGMatrixPool::new()));

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
                    let mut digest_to_local: HashMap<Vec<u8>, usize> = HashMap::new();
                    let mut next_idx: usize = 0;

                    for (_author, lo_digests) in &author_to_lo_digests_subdag {
                        for lo_digest in lo_digests {
                            
                            let read_res = self.store.read(lo_digest.to_vec()).await;
                            let serialized = match read_res {
                                Ok(Some(v)) => v,
                                Ok(None) => {
                                    log::warn!(
                                        "LocalOrder {:?} missing in RocksDB store",
                                        lo_digest
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    log::error!(
                                        "Error reading LocalOrder {:?} from store: {}",
                                        lo_digest, e
                                    );
                                    continue;
                                }
                            };

                            let local_order: Vec<Vec<u8>> = match bincode::deserialize(&serialized) {
                                Ok(WorkerMessage::Batch(_author, batch)) => batch,
                                Ok(WorkerMessage::TxDigest(..)) => {
                                    log::error!(
                                        "Got TxDigest?"
                                    );
                                    continue;
                                },
                                Ok(WorkerMessage::BatchRequest(..)) => {
                                    log::error!(
                                        "Got BatchRequest?"
                                    );
                                    continue;
                                },
                                Err(e) => {
                                    log::error!(
                                        "Failed to deserialize LocalOrder {:?} from store: {}",
                                        lo_digest, e
                                    );
                                    continue;
                                }
                            };

                            let mut indices: Vec<usize> = Vec::with_capacity(local_order.len());

                            for tx_digest in local_order {
                                let idx = *digest_to_local.entry(tx_digest.clone()).or_insert_with(|| {
                                    let curr = next_idx;
                                    next_idx += 1;
                                    curr
                                });
                                indices.push(idx);
                            }

                            indices_sets.push(indices);

                        }
                    }

                    let k = next_idx;
                    let t2 = start_time.elapsed().as_nanos() - t1;
                    log::info!(
                        "t2 : {}\nunique txs (k): {}\nLocalOrders processed: {}",
                        t2,
                        k,
                        indices_sets.len()
                    );

                    let non_blank = self.non_blank_threshold;
                    let solid = self.solid_threshold;
                    let tx_utig_results = self.tx_utig_results.clone();

                    // tokio::task::spawn_blocking(move || {
                    //     run_utig(indices_sets, k, non_blank as u8, solid as u8, tx_utig_results);
                    // });
                    let _handler = tokio_rayon::spawn(move || {
                        run_utig(indices_sets, k, non_blank as u8, solid as u8, tx_utig_results);
                    });

                    log::info!(
                        "\nspawning UTIG: {}", start_time.elapsed().as_nanos() - t2
                    );

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

                Some((author, lo_digest, local_order)) = self.rx_local_orders.recv() => {

                    log::info!("rx_local_orders: {} {}", author, lo_digest);

                    self.author_to_lo_digests
                        .entry(author)
                        .or_default()
                        .push(lo_digest.clone());
                    
                    // self.digest_to_local_order
                    //     .insert(lo_digest, local_order);

                },
                Some(final_order) = self.rx_utig_results.recv() => {
                    // TODO:
                }

            }
        }
    }
}


pub fn run_utig(
    indices_sets: Vec<Vec<usize>>,
    k: usize,
    non_blank_threshold: u8,
    solid_threshold: u8,
    tx_utig_results: tokio::sync::mpsc::Sender<Vec<usize>>,
) {

    let start_total = Instant::now();
    let mut last = start_total;

    if k == 0 || indices_sets.is_empty() {
        log::info!("UTIG: empty sub-dag (k=0 or no local orders), nothing to do");
        return;
    }

    let (slot_idx, matrix_ptr) = {
        let mut pool = UTIG_POOL
            .lock()
            .expect("UTIG_POOL mutex poisoned");

        let idx = pool
            .acquire_slot()
            .expect("UTIGMatrixPool exhausted: no free matrices");

        let matrix_ptr: *mut UTIGMatrix = &mut pool.pool[idx];

        (idx, matrix_ptr)
    };

    let matrix: &mut UTIGMatrix = unsafe { &mut *matrix_ptr };

    // Aliases into the preallocated matrix.
    let weight = &mut matrix.weight;
    let support = &mut matrix.support;
    let is_non_blank = &mut matrix.is_non_blank;
    let is_solid = &mut matrix.is_solid;
    let edges = &mut matrix.edges;

    #[inline]
    fn w_idx(i: usize, j: usize, k: usize) -> usize {
        i * k + j
    }

    let now = Instant::now();
    let t1 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        "UTIG t1: {}", t1
    );

    // ============================================================
    // (3) For each non-blank tx, add a vertex tx to V
    //     -> compute tx_count (support), non-blank set, solid set.
    // ============================================================
    for order in &indices_sets {
        for &tx in order {
            // tx in [0..k)
            let new_sup = support[tx].saturating_add(1);
            support[tx] = new_sup;

            if new_sup >= non_blank_threshold {
                is_non_blank[tx] = true;
            }
            if new_sup >= solid_threshold {
                is_solid[tx] = true;
            }
        }
    }

    let active: Vec<usize> = (0..k).filter(|&u| is_non_blank[u]).collect();
    if active.is_empty() {
        log::info!("UTIG: no non-blank txs in this sub-dag, nothing to propose");
        matrix.reset(k);
        {
            let mut pool = UTIG_POOL
                .lock()
                .expect("UTIG_POOL mutex poisoned");
            pool.release_slot(slot_idx);
        }
        return;
    }

    let now = Instant::now();
    let t3 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        "UTIG t3: {}", t3
    );

    // ============================================================
    // (4) Add edges to E
    //
    // Here weight[u,v] acts like edge_count[u][v] in the C++ code:
    //   edge_count[from][to]++ for every “from -> to” in local orders.
    // Then we orient edges based on counts and thresholds.
    // ============================================================

    // First: fill edge_count via local orders.
    for order in &indices_sets {
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

                let idx = w_idx(from, to, k);
                weight[idx] = weight[idx].saturating_add(1);
            }
        }
    }

    // Now, build the directed graph on non-blank txs.
    //
    // We use a predicate similar in spirit to Themis:
    //   - keep edges where count >= non_blank_threshold
    //   - do not add edges between blank vertices
    //
    // Additionally, we avoid adding duplicate edges (cheap check).
    for u in 0..k {
        if !is_non_blank[u] {
            continue;
        }

        for v in 0..k {
            if u == v || !is_non_blank[v] {
                continue;
            }

            let cnt = weight[w_idx(u, v, k)];
            if cnt < non_blank_threshold {
                continue;
            }

            // avoid duplicates
            let v16 = v as u16;
            if !edges[u].contains(&v16) {
                edges[u].push(v16);
            }
        }
    }

    let now = Instant::now();
    let t4 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        "UTIG t4: {}", t4
    );

    // ============================================================
    // (5) Compute condensation graph G* (SCCs + topo sort)
    //     -> Tarjan SCC on the non-blank subgraph
    // ============================================================

    let mut index_counter: i32 = 0;
    let mut stack: Vec<usize> = Vec::new();
    let mut on_stack: Vec<bool> = vec![false; k];
    let mut dfn: Vec<i32> = vec![0; k];
    let mut low: Vec<i32> = vec![0; k];
    let mut scc_id: Vec<i32> = vec![-1; k];
    let mut sccs: Vec<Vec<usize>> = Vec::new();

    fn strongconnect(
        u: usize,
        index_counter: &mut i32,
        stack: &mut Vec<usize>,
        on_stack: &mut [bool],
        dfn: &mut [i32],
        low: &mut [i32],
        edges: &Vec<Vec<u16>>,
        scc_id: &mut [i32],
        sccs: &mut Vec<Vec<usize>>,
    ) {
        *index_counter += 1;
        dfn[u] = *index_counter;
        low[u] = *index_counter;
        stack.push(u);
        on_stack[u] = true;

        for &v16 in &edges[u] {
            let v = v16 as usize;
            if dfn[v] == 0 {
                strongconnect(
                    v,
                    index_counter,
                    stack,
                    on_stack,
                    dfn,
                    low,
                    edges,
                    scc_id,
                    sccs,
                );
                if low[v] < low[u] {
                    low[u] = low[v];
                }
            } else if on_stack[v] {
                if dfn[v] < low[u] {
                    low[u] = dfn[v];
                }
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

    // Run Tarjan only on non-blank nodes
    for &u in &active {
        if dfn[u] == 0 {
            strongconnect(
                u,
                &mut index_counter,
                &mut stack,
                &mut on_stack,
                &mut dfn,
                &mut low,
                edges,
                &mut scc_id,
                &mut sccs,
            );
        }
    }

    let scc_n = sccs.len();
    if scc_n == 0 {
        log::info!("UTIG: SCC decomposition empty, nothing to propose");
        matrix.reset(k);
        {
            let mut pool = UTIG_POOL
                .lock()
                .expect("UTIG_POOL mutex poisoned");
            pool.release_slot(slot_idx);
        }
        return;
    }

    // Build condensation graph G* (over SCCs) and topo sort it.
    let mut gc: Vec<Vec<usize>> = vec![Vec::new(); scc_n];
    let mut indegree: Vec<usize> = vec![0; scc_n];

    for &u in &active {
        let su = scc_id[u];
        if su < 0 {
            continue;
        }
        let su = su as usize;

        for &v16 in &edges[u] {
            let v = v16 as usize;
            if !is_non_blank[v] {
                continue;
            }
            let sv = scc_id[v];
            if sv < 0 {
                continue;
            }
            let sv = sv as usize;
            if su == sv {
                continue;
            }
            gc[su].push(sv);
        }
    }

    // Deduplicate edges and compute indegrees
    for u in 0..scc_n {
        gc[u].sort_unstable();
        gc[u].dedup();
        for &v in &gc[u] {
            indegree[v] = indegree[v].saturating_add(1);
        }
    }

    // Topological sort over the SCC DAG.
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

    let now = Instant::now();
    let t5 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        "UTIG t5: {}", t5
    );

    // ============================================================
    // (6) Find last vertex `V` in S that has a solid transaction
    // ============================================================

    let mut anchor_idx: Option<usize> = None;
    for (idx, &scc_index) in topo.iter().enumerate() {
        let comp = &sccs[scc_index];
        if comp.iter().any(|&tx| is_solid[tx]) {
            anchor_idx = Some(idx);
        }
    }

    if anchor_idx.is_none() {
        matrix.reset(k);
        {
            let mut pool = UTIG_POOL
                .lock()
                .expect("UTIG_POOL mutex poisoned");
            pool.release_slot(slot_idx);
        }
        let total = start_total.elapsed().as_nanos();
        log::info!(
            "UTIG: no solid anchor in this sub-dag, total ns = {}",
            total
        );
        return;
    }

    let anchor = anchor_idx.unwrap();

    let now = Instant::now();
    let t6 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        "UTIG t6: {}", t6
    );

    // ============================================================
    // (7) Remove txs that are part of SCCs after V in S
    //     (in our case: build the final ordered prefix of tx indices)
    // ============================================================

    let mut final_local: Vec<usize> = Vec::new();

    // Keep SCCs topo[0..=anchor], discard the rest.
    for topo_pos in 0..=anchor {
        let scc_index = topo[topo_pos];
        let comp = &sccs[scc_index];

        if comp.len() == 1 {
            final_local.push(comp[0]);
        } else {
            // Deterministic order inside SCC (e.g. by index).
            let mut sorted = comp.clone();
            sorted.sort_unstable();
            final_local.extend(sorted);
        }
    }

    let solid_nodes = is_solid
        .iter()
        .take(k)
        .filter(|&&solid| solid)
        .count();

    let now = Instant::now();
    let t7 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        "UTIG t7: {}", t7
    );

    log::info!(
        "UTIG: finalized prefix length = {}, solid_nodes = {}, anchor_scc_idx = {}, total ns = {}",
        final_local.len(),
        solid_nodes,
        anchor,
        start_total.elapsed().as_nanos()
    );

    // ============================================================
    // (8) Output result: local tx indices to finalize
    // ============================================================

    let _ = tx_utig_results.blocking_send(final_local);

    matrix.reset(k);

    {
        let mut pool = UTIG_POOL
            .lock()
            .expect("UTIG_POOL mutex poisoned");
        pool.release_slot(slot_idx);
    }

    let now = Instant::now();
    let t8 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        "UTIG t8: {}", t8
    );

}