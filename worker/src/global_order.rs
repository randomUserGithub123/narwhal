use primary::Round;
// Copyright(C) Facebook, Inc. and its affiliates.
use tokio::sync::mpsc::Receiver;
use tokio::task;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use crypto::{Digest, Hash, PublicKey};

use crate::batch_maker::Batch;

const MAX_TX: usize = 10_000;

struct SubDagBuffers {
    weight_matrix: Vec<Vec<u16>>,
    needed_lo_digests: Vec<Digest>,
    dirty_nodes: Vec<usize>,
    dirty_pairs: Vec<(usize, usize)>,
    support: Vec<u16>,
    is_non_blank: Vec<bool>,
    is_solid: Vec<bool>,
    edges: Vec<Vec<usize>>,

    index_counter: i32,
    stack: Vec<usize>,
    on_stack: Vec<bool>,
    dfn: Vec<i32>,
    low: Vec<i32>,
    scc_id: Vec<i32>,
    scc_count: i32,
    sccs: Vec<Vec<usize>>,

    // Condensation graph + topo.
    gc: Vec<Vec<usize>>,
    indegree: Vec<usize>,
    topo: Vec<usize>,
    q: VecDeque<usize>,

    final_order: Vec<usize>,

}

impl SubDagBuffers {

    fn new() -> Self {
        SubDagBuffers {
            weight_matrix: vec![vec![0u16; MAX_TX]; MAX_TX],
            needed_lo_digests: Vec::new(),
            dirty_nodes: Vec::with_capacity(MAX_TX),
            dirty_pairs: Vec::with_capacity(MAX_TX),
            support: vec![0u16; MAX_TX],
            is_non_blank: vec![false; MAX_TX],
            is_solid: vec![false; MAX_TX],
            edges: vec![Vec::new(); MAX_TX],

            index_counter: 0,
            stack: Vec::new(),
            on_stack: vec![false; MAX_TX],
            dfn: vec![0i32; MAX_TX],
            low: vec![0i32; MAX_TX],
            scc_id: vec![-1i32; MAX_TX],
            scc_count: 0,
            sccs: Vec::new(),

            gc: vec![Vec::new(); MAX_TX],
            indegree: vec![0; MAX_TX],
            topo: Vec::new(),
            q: VecDeque::new(),

            final_order: Vec::new(),
        }
    }

}

struct GlobalOrderState {

    tx_digest_to_index: HashMap<Vec<u8>, usize>,
    index_to_tx_digest: Vec<Vec<u8>>,
    free_indices: Vec<usize>,

    // lo_contributions: HashMap<Digest, Vec<usize>>,
}

impl GlobalOrderState {

    fn new() -> Self {

        let tx_digest_to_index = HashMap::with_capacity(MAX_TX);
        let index_to_tx_digest = Vec::with_capacity(MAX_TX);

        GlobalOrderState {
            tx_digest_to_index,
            index_to_tx_digest,
            free_indices: Vec::new(),
            // lo_contributions: HashMap::new(),
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

            if idx >= MAX_TX {
                panic!("GlobalOrder: exceeded MAX_TX ({}) distinct transactions", MAX_TX);
            }

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

pub struct GlobalOrder {
    
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

    subdag_buffers: Vec<Arc<Mutex<SubDagBuffers>>>,
    subdag_counter: usize,

    tx_utig_results: tokio::sync::mpsc::Sender<Vec<usize>>,
    rx_utig_results: tokio::sync::mpsc::Receiver<Vec<usize>>,

}

impl GlobalOrder {

    pub fn new(
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

        const NUM_BUFFERS: usize = 3;
        let mut subdag_buffers = Vec::with_capacity(NUM_BUFFERS);
        for _ in 0..NUM_BUFFERS {
            subdag_buffers.push(Arc::new(Mutex::new(SubDagBuffers::new())));
        }

        let (tx_utig_results, rx_utig_results) = tokio::sync::mpsc::channel(1024);

        GlobalOrder {
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
            subdag_buffers,
            subdag_counter: 0,
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

                    // // ---- pick buffer & spawn heavy UTIG job on blocking thread ----
                    let buf_index = self.subdag_counter % self.subdag_buffers.len();
                    self.subdag_counter = self.subdag_counter.wrapping_add(1);
                    let buf_arc = self.subdag_buffers[buf_index].clone();

                    let non_blank_threshold = self.non_blank_threshold;
                    let solid_threshold = self.solid_threshold;

                    let tx_utig_results = self.tx_utig_results.clone();
                    task::spawn_blocking(move || {
                        let start = Instant::now();

                        let mut buf = buf_arc.lock().expect("lock SubDagBuffers");

                        let SubDagBuffers {
                            weight_matrix,
                            needed_lo_digests: _,
                            dirty_nodes,
                            dirty_pairs,
                            support,
                            is_non_blank,
                            is_solid,
                            edges,
                            index_counter,
                            stack,
                            on_stack,
                            dfn,
                            low,
                            scc_id,
                            scc_count,
                            sccs,
                            gc,
                            indegree,
                            topo,
                            q,
                            final_order,
                        } = &mut *buf;

                        // -------- Phase 2: support, weights, dirty_nodes/pairs --------
                        for indices in &indices_sets {

                            log::info!(
                                "LEN {}", indices.len()
                            );

                            for (pos, &i) in indices.iter().enumerate() {
                                if i >= MAX_TX {
                                    log::error!("rx_consensus_update: tx index {} >= MAX_TX", i);
                                    continue;
                                }

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
                                    if j >= MAX_TX {
                                        log::error!("rx_consensus_update: tx index {} >= MAX_TX", j);
                                        continue;
                                    }

                                    let old_w = weight_matrix[i][j];
                                    let new_w = old_w.saturating_add(1);
                                    weight_matrix[i][j] = new_w;

                                    if old_w < non_blank_threshold && new_w >= non_blank_threshold {
                                        let (a, b) = if i < j { (i, j) } else { (j, i) };
                                        dirty_pairs.push((a, b));
                                    }
                                }
                            }
                        }

                        let t_weights = start.elapsed().as_nanos();
                        log::info!("t(weights + support): {}", t_weights);

                        // -------- Phase 3: orientation predicate and edges --------
                        let predicate_p = |u: usize, v: usize, w_uv: u16, w_vu: u16, t_edge: u16| -> bool {
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

                        for &(u_raw, v_raw) in dirty_pairs.iter() {
                            let u = u_raw;
                            let v = v_raw;

                            if !is_non_blank[u] || !is_non_blank[v] {
                                continue;
                            }

                            let w_uv = weight_matrix[u][v];
                            let w_vu = weight_matrix[v][u];

                            if w_uv == 0 && w_vu == 0 {
                                continue;
                            }

                            let u_to_v = predicate_p(u, v, w_uv, w_vu, non_blank_threshold);
                            let v_to_u = predicate_p(v, u, w_vu, w_uv, non_blank_threshold);

                            if u_to_v {
                                edges[u].push(v);
                            } else if v_to_u {
                                edges[v].push(u);
                            }
                        }

                        let t_edges = start.elapsed().as_nanos() - t_weights;
                        log::info!("t(edges): {}", t_edges);

                        // -------- Phase 4: Tarjan on affected nodes --------

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

                        for &u in dirty_nodes.iter() {
                            if dfn[u] == 0 {
                                strongconnect(
                                    u,
                                    index_counter,
                                    stack,
                                    on_stack,
                                    dfn,
                                    low,
                                    edges,
                                    scc_id,
                                    scc_count,
                                    sccs,
                                );
                            }
                        }

                        let t_tarjan = start.elapsed().as_nanos() - t_weights - t_edges;
                        log::info!("t(Tarjan): {}", t_tarjan);

                        // -------- Phase 5: condensation + topo + finalize --------
                        let scc_n = sccs.len();
                        gc.clear();
                        gc.resize(scc_n, Vec::new());
                        indegree.clear();
                        indegree.resize(scc_n, 0);

                        for u in 0..MAX_TX {
                            let su = scc_id[u];
                            if su < 0 {
                                continue;
                            }
                            let su = su as usize;

                            for &v in &edges[u] {
                                let sv = scc_id[v];
                                if sv < 0 {
                                    continue;
                                }
                                let sv = sv as usize;
                                if su == sv {
                                    continue;
                                }
                                gc[su].push(sv);
                                indegree[sv] += 1;
                            }
                        }

                        topo.clear();
                        q.clear();
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

                        // anchor: last SCC with solid tx
                        let mut anchor_idx: Option<usize> = None;
                        for (idx, &scc_index) in topo.iter().enumerate() {
                            let comp = &sccs[scc_index];
                            if comp.iter().any(|&v| is_solid[v]) {
                                anchor_idx = Some(idx);
                            }
                        }

                        if let Some(anchor) = anchor_idx {
                            final_order.clear();
                            for topo_pos in 0..=anchor {
                                let scc_index = topo[topo_pos];
                                let comp = &sccs[scc_index];
                                if comp.len() == 1 {
                                    final_order.push(comp[0]);
                                } else {
                                    let mut sorted = comp.clone();
                                    sorted.sort_unstable();
                                    final_order.extend(sorted);
                                }
                            }

                            let total = start.elapsed().as_nanos();
                            log::info!(
                                "GlobalOrder: finalized prefix length = {}, anchor_scc_idx = {}, total ns = {}",
                                final_order.len(),
                                anchor,
                                total,
                            );

                            if let Err(_e) = tx_utig_results.blocking_send(final_order.clone()) {
                                // GlobalOrder might have been dropped; handle if needed
                            }

                        } else {
                            let total = start.elapsed().as_nanos();
                            log::info!(
                                "GlobalOrder: no solid anchor in this sub_dag, total ns = {}",
                                total
                            );
                        }

                    });

                    log::info!(
                        "spawning took time : {}", start_time.elapsed().as_nanos() - t2
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
