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
    store: Store,
    
    rx_local_orders: Receiver<(PublicKey, Digest, Batch)>,
    rx_header_update: Receiver<(PublicKey, Round, Vec<Digest>)>,
    rx_consensus_update: Receiver<Vec<(Round, Vec<PublicKey>)>>,

    n: u64,
    f: u64,
    gamma: f64,
    non_blank_threshold: u16,
    solid_threshold: u16,

    author_to_lo_digests: HashMap<PublicKey, Vec<Option<Digest>>>,
    digest_to_seq: HashMap<PublicKey, HashMap<Digest, usize>>,
    author_round_boundaries: HashMap<PublicKey, Vec<(Round, usize, usize)>>,
    pending_headers: HashMap<PublicKey, Vec<(Round, Vec<Digest>)>>,
    
    pending_subdags: VecDeque<Vec<(Round, Vec<PublicKey>)>>,

    tx_utig_results: tokio::sync::mpsc::Sender<(Vec<usize>, bool)>,
    rx_utig_results: tokio::sync::mpsc::Receiver<(Vec<usize>, bool)>,
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
            digest_to_seq: HashMap::new(),
            author_round_boundaries: HashMap::new(),
            pending_headers: HashMap::new(),
            pending_subdags: VecDeque::new(),
            rx_utig_results,
            tx_utig_results,
        }
    }

    pub fn start(self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    #[inline]
    fn parse_seq_le(local_order: &Batch) -> Option<usize> {
        let first = local_order.get(0)?;
        if first.len() != 8 {
            panic!("seq prefix wrong length: expected 8, got {}", first.len());
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&first[..8]);
        let seq_u64 = u64::from_le_bytes(arr);

        if (seq_u64 as usize) as u64 != seq_u64 {
            panic!("seq {} does not fit into usize", seq_u64);
        }
        Some(seq_u64 as usize)
    }

    fn has_full_range(&self, author: &PublicKey, start: usize, end: usize) -> bool {
        let Some(v) = self.author_to_lo_digests.get(author) else { return false; };
        if end >= v.len() { return false; }
        v[start..=end].iter().all(|x| x.is_some())
    }

    fn can_process_subdag(&self, sub_dag: &[(Round, Vec<PublicKey>)]) -> bool {
        for (round, authors) in sub_dag {
            for author in authors {
                let Some(bounds) = self.author_round_boundaries.get(author) else { return false; };
                let Some((_, start, end)) = bounds.iter().find(|(r,_,_)| r == round) else { return false; };
                if !self.has_full_range(author, *start, *end) { return false; }
            }
        }
        true
    }

    async fn process_subdag(&mut self, sub_dag: Vec<(Round, Vec<PublicKey>)>) {
        let start_time = Instant::now();
        let mut author_to_lo_digests_subdag: HashMap<PublicKey, Vec<Digest>> = HashMap::new();

        for (round, authors) in sub_dag.iter() {
            for author in authors {
                let round_boundaries = self.author_round_boundaries.get(author).unwrap();
                let Some((_r, start_idx, end_idx)) = round_boundaries.iter().find(|(r, _, _)| r == round) else {
                    panic!("Missing boundary for author {:?}, round {} (should have been checked)", author, round);
                };

                let Some(author_local_orders) = self.author_to_lo_digests.get(author) else {
                    panic!("Author {:?} not found in author_to_lo_digests", author);
                };

                if *end_idx >= author_local_orders.len() {
                    panic!(
                        "Invalid boundary ({},{}) for author {:?} - only {} local orders",
                        start_idx, end_idx, author, author_local_orders.len()
                    );
                }

                let lo_slice = &author_local_orders[*start_idx..=*end_idx];
                
                for maybe_digest in lo_slice {
                    if let Some(digest) = maybe_digest {
                        author_to_lo_digests_subdag
                            .entry(*author)
                            .or_default()
                            .push(digest.clone());
                    } else {
                        panic!("None digest in boundary for author {:?}, round {}", author, round);
                    }
                }
            }
        }

        let t1 = start_time.elapsed().as_nanos();
        log::info!("t1 (boundary extraction): {}", t1);

        let mut indices_sets: Vec<Vec<usize>> = Vec::new();
        let mut digest_to_local: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut next_idx: usize = 0;

        let mut authors: Vec<PublicKey> = author_to_lo_digests_subdag.keys().cloned().collect();
        authors.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        for author in authors {
            let lo_digests = &author_to_lo_digests_subdag[&author];
            for lo_digest in lo_digests {
                let read_res = self.store.notify_read(lo_digest.to_vec()).await;
                let serialized = match read_res {
                    Ok(v) => v,
                    Err(e) => {
                        panic!("Error reading LocalOrder {:?} from store: {}", lo_digest, e);
                    }
                };

                let local_order: Vec<Vec<u8>> = match bincode::deserialize(&serialized) {
                    Ok(WorkerMessage::Batch(_author, batch)) => batch,
                    Ok(_) => {
                        panic!("Unexpected WorkerMessage type for {:?}", lo_digest);
                    }
                    Err(e) => {
                        panic!("Failed to deserialize LocalOrder {:?} from store: {}", lo_digest, e);
                    }
                };

                // Skip the first element (sequence number)
                let mut indices: Vec<usize> = Vec::with_capacity(local_order.len() - 1);
                for tx_digest in local_order.iter().skip(1) {
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
            "t2 (store reads + indexing): {}\nunique txs (k): {}\nLocalOrders processed: {}",
            t2, k, indices_sets.len()
        );

        let non_blank = self.non_blank_threshold;
        let solid = self.solid_threshold;
        let tx_utig_results = self.tx_utig_results.clone();

        let _handler = tokio_rayon::spawn(move || {
            run_utig(indices_sets, k, non_blank as u8, solid as u8, tx_utig_results);
        });

        log::info!("spawning UTIG: {}", start_time.elapsed().as_nanos() - t2);
    }

    async fn try_process_pending_subdags(&mut self) {
        let mut i = 0;
        while i < self.pending_subdags.len() {
            if self.can_process_subdag(&self.pending_subdags[i]) {
                let sub_dag = self.pending_subdags.remove(i).unwrap();
                log::info!("Processing previously pending sub-dag with {} rounds", sub_dag.len());
                self.process_subdag(sub_dag).await;
            } else {
                i += 1;
            }
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(sub_dag) = self.rx_consensus_update.recv() => {
                    log::info!("Received sub-dag : {:?}", sub_dag);

                    if self.can_process_subdag(&sub_dag) {
                        log::info!("Sub-dag ready for immediate processing");
                        self.process_subdag(sub_dag).await;
                    } else {
                        log::warn!("Sub-dag missing data, adding to pending queue (queue size: {})", 
                                  self.pending_subdags.len() + 1);
                        self.pending_subdags.push_back(sub_dag);
                    }
                },

                Some((author, round, lo_digests)) = self.rx_header_update.recv() => {
                    log::info!("rx_header_update: author {:?}, round {}, {} digests",
                              author, round, lo_digests.len());

                    let maybe_seq_map = self.digest_to_seq.get(&author);

                    let Some(seq_map) = maybe_seq_map else {
                        log::warn!(
                            "rx_header_update: deferring header (author={:?}, round={}): no digest_to_seq yet; {} lo_digests",
                            author, round, lo_digests.len()
                        );
                        self.pending_headers.entry(author).or_default().push((round, lo_digests));
                        continue;
                    };

                    if lo_digests.iter().any(|d| !seq_map.contains_key(d)) {
                        log::warn!(
                            "rx_header_update: deferring header (author={:?}, round={}): missing lo_digests in digest_to_seq",
                            author, round
                        );
                        self.pending_headers.entry(author).or_default().push((round, lo_digests));
                        continue;
                    }

                    let mut start = usize::MAX;
                    let mut end = 0usize;
                    let mut uniq = HashSet::with_capacity(lo_digests.len());

                    for d in &lo_digests {
                        uniq.insert(d.clone());
                        let s = seq_map[d];
                        start = start.min(s);
                        end = end.max(s);
                    }

                    if uniq.len() != lo_digests.len() {
                        panic!("rx_header_update: duplicate LO digests in header for {:?}, round {}", author, round);
                    }

                    if end + 1 - start != lo_digests.len() {
                        panic!(
                            "rx_header_update: non-contiguous seq window for {:?}, round {} (start={}, end={}, count={})",
                            author, round, start, end, lo_digests.len()
                        );
                    }

                    self.author_round_boundaries
                        .entry(author)
                        .or_default()
                        .push((round, start, end));
                    
                    // Check if any pending sub-dags can now be processed
                    self.try_process_pending_subdags().await;
                },

                Some((author, lo_digest, local_order)) = self.rx_local_orders.recv() => {
                    log::info!("rx_local_orders: author {:?}, digest {:?}", author, lo_digest);

                    let Some(seq) = Self::parse_seq_le(&local_order) else {
                        panic!("rx_local_orders: failed to parse seq for digest {:?}", lo_digest);
                    };

                    {
                        let v = self.author_to_lo_digests.entry(author).or_default();
                        if v.len() <= seq {
                            v.resize_with(seq + 1, || None);
                        }
                        v[seq] = Some(lo_digest.clone());
                    }

                    {
                        let m = self.digest_to_seq.entry(author).or_default();
                        m.insert(lo_digest, seq);
                    }

                    let mut pending = self.pending_headers.remove(&author).unwrap_or_default();
                    if !pending.is_empty() {
                        let seq_map = match self.digest_to_seq.get(&author) {
                            Some(m) => m,
                            None => {
                                self.pending_headers.insert(author, pending);
                                continue;
                            }
                        };

                        let mut unresolved: Vec<(Round, Vec<Digest>)> = Vec::new();
                        let mut newly_resolved: Vec<(Round, usize, usize)> = Vec::new();

                        for (r, ds) in pending.drain(..) {
                            if ds.iter().any(|d| !seq_map.contains_key(d)) {
                                unresolved.push((r, ds));
                                continue;
                            }

                            let mut start = usize::MAX;
                            let mut end = 0usize;
                            for d in &ds {
                                let s = seq_map[d];
                                start = start.min(s);
                                end = end.max(s);
                            }

                            if end + 1 - start != ds.len() {
                                panic!(
                                    "pending header non-contiguous seq window for {:?}, round {} (start={}, end={}, count={})",
                                    author, r, start, end, ds.len()
                                );
                            }

                            newly_resolved.push((r, start, end));
                        }

                        if !unresolved.is_empty() {
                            self.pending_headers.insert(author, unresolved);
                        }

                        if !newly_resolved.is_empty() {
                            self.author_round_boundaries
                                .entry(author)
                                .or_default()
                                .extend(newly_resolved);
                            
                            // Check if any pending sub-dags can now be processed
                            self.try_process_pending_subdags().await;
                        }
                    }
                },
                
                Some((final_order, is_complete)) = self.rx_utig_results.recv() => {
                    if is_complete{

                    }else{
                        // TODO:
                    }
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
    tx_utig_results: tokio::sync::mpsc::Sender<(Vec<usize>, bool)>,
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

    // Build the directed graph on non-blank txs.
    for &u in &active {
        for &v in &active {
            if u >= v { continue; }
            let kuv = weight[w_idx(u,v,k)];
            let kvu = weight[w_idx(v,u,k)];

            if kuv < non_blank_threshold && kvu < non_blank_threshold {
                continue;
            }

            let dir_uv =
                if kuv > kvu { true }
                else if kvu > kuv { false }
                else {
                    u < v
                };

            if dir_uv { edges[u].push(v as u16); }
            else      { edges[v].push(u as u16); }
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

    #[inline]
    fn pair_key(u: usize, v: usize) -> u64 {
        let (a, b) = if u < v { (u as u32, v as u32) } else { (v as u32, u as u32) };
        ((a as u64) << 32) | (b as u64)
    }

    let kept_shaded: Vec<usize> = final_local
        .iter()
        .copied()
        .filter(|&u| !is_solid[u])
        .collect();

    let mut missing_edges: Vec<u64> = Vec::new();
    for i in 0..kept_shaded.len() {
        let u = kept_shaded[i];
        for j in (i + 1)..kept_shaded.len() {
            let v = kept_shaded[j];

            let kuv = weight[w_idx(u, v, k)];
            let kvu = weight[w_idx(v, u, k)];

            if kuv < non_blank_threshold && kvu < non_blank_threshold {
                missing_edges.push(pair_key(u, v));
            }
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
        "UTIG: finalized prefix length = {}, solid_nodes = {}, shaded_nodes = {}, missing_edges = {}, anchor_scc_idx = {}, total ns = {}",
        final_local.len(),
        solid_nodes,
        kept_shaded.len(),
        missing_edges.len(),
        anchor,
        start_total.elapsed().as_nanos()
    );

    // ============================================================
    // (8) Output result: local tx indices to finalize
    // ============================================================

    let _ = tx_utig_results.blocking_send((final_local, missing_edges.len() == 0));

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