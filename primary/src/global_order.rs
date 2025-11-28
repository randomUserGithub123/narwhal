// Copyright (C) Facebook, Inc. and its affiliates.
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::mpsc::Receiver;

use petgraph::graph::{Graph, NodeIndex};
use petgraph::algo::{condensation, toposort};
use nohash_hasher::IntMap;

use config::Committee;
use crypto::{Digest, PublicKey};
use store::Store;
use crate::messages::Certificate;

type TxId = u64;

struct AnchorState {
    /// Graph whose vertices are tx ids, edges are "tx must come before tx'".
    graph: Graph<TxId, u32>,
    /// Mapping from tx id to node index in the graph.
    index_by_tx: HashMap<TxId, NodeIndex>,
    /// Nodes (primaries) that have contributed a FIFO for this anchor.
    seen_authors: HashSet<PublicKey>,
    /// Number of local FIFOs in which each tx appears (for solid / shaded / blank classification).
    vertex_weights: HashMap<TxId, u32>,
}

/// Receives (anchor, certificate) pairs from consensus and builds a fair global order
/// using local FIFO orderings stored in `store` keyed by header.fifo.0.
pub struct GlobalOrder {
    /// Committee info.
    committee: Committee,
    /// Persistent store for FIFO queues.
    store: Store,
    /// Receives (anchor, certificate) pairs from Consensus.
    ///
    /// - `anchor`: the leader certificate that this certificate is anchored to.
    /// - `certificate`: a certificate whose FIFO list we should include in the anchor's graph.
    rx_certificates: Receiver<(Certificate, Certificate)>,

    /// Per-anchor dependency graphs and counters.
    anchors: HashMap<Digest, AnchorState>,

    /// Number of nodes and max faulty nodes.
    n: u32,
    f: u32,
    /// Fairness parameter γ.
    gamma: f64,

    /// Certificates waiting for their FIFO bytes to appear in storage.
    pending: VecDeque<(Certificate, Certificate)>,

    /// Transactions that have already been committed (globally) across anchors.
    committed_txs: HashSet<TxId>,
}

impl GlobalOrder {
    /// Spawn the global order task.
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_certificates: Receiver<(Certificate, Certificate)>,
    ) {
        let n = committee.size() as u32;
        // Standard BFT assumption; adjust if you want different fault model.
        let f = (n - 1) / 4;
        let gamma = 1.0;

        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                store,
                rx_certificates,
                anchors: HashMap::new(),
                n,
                f,
                gamma,
                pending: VecDeque::new(),
                committed_txs: HashSet::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((anchor, certificate)) = self.rx_certificates.recv().await {
            
            let mut to_process: Vec<(Certificate, Certificate)> = Vec::new();
            to_process.extend(self.pending.drain(..));
            to_process.push((anchor, certificate));

            for (anchor, cert) in to_process {
                let fifo_digest = cert.header.fifo.0.clone();
                let author: PublicKey = cert.header.author;

                match self.store.read(fifo_digest.to_vec()).await {
                    Ok(Some(fifo_bytes)) => {
                        
                        let fifo_vec: Vec<TxId> = match bincode::deserialize(&fifo_bytes) {
                            Ok(v) => v,
                            Err(e) => {
                                log::error!(
                                    "Failed to deserialize FIFO bytes for {}: {}",
                                    fifo_digest,
                                    e
                                );
                                continue;
                            }
                        };

                        // if anchor.header.id == cert.header.id{
                        //     self.process_certificate(&anchor, author, fifo_vec);
                        //     if let Some(fresh) = self.fair_propose(&anchor) {
                        //         if !fresh.is_empty() {
                        //             log::info!(
                        //                 "Anchor {} finalized fair order of {} new txs",
                        //                 anchor.header,
                        //                 fresh.len()
                        //             );
                        //         }
                        //     }
                        // }else{
                        //     self.process_certificate(&anchor, author, fifo_vec);
                        // }

                    }
                    Ok(None) => {
                        self.pending.push_back((anchor, cert));
                    }
                    Err(e) => {
                        log::error!("FIFO store.read failed for {}: {}", fifo_digest, e);
                        self.pending.push_back((anchor, cert));
                    }
                }
            }
        }
    }

    fn anchor_state(&mut self, anchor: &Certificate) -> &mut AnchorState {
        let anchor_id = anchor.header.id.clone();
        self.anchors
            .entry(anchor_id)
            .or_insert_with(|| AnchorState {
                graph: Graph::new(),
                index_by_tx: HashMap::new(),
                seen_authors: HashSet::new(),
                vertex_weights: HashMap::new(),
            })
    }

    
    fn process_certificate(&mut self, anchor: &Certificate, author: PublicKey, fifo: Vec<TxId>) {

        let mut tx_map: IntMap<TxId, usize> = IntMap::default();
        let mut matrix: Vec<Vec<u32>> = vec![vec![0u32; fifo.len()]; fifo.len()];

        let start_time = std::time::Instant::now();

        for i in 0..fifo.len() {
            if !tx_map.contains_key(&fifo[i]){
                tx_map.insert(fifo[i], tx_map.len());
            }
        }

        let t1 = start_time.elapsed().as_nanos();

        log::info!(
            "t1 : {}\nFIFO LENGTH IS : {}", t1, fifo.len()
        );

        for i in 0..fifo.len() {
            let a = *tx_map.get(&fifo[i]).unwrap();
            for j in (i + 1)..fifo.len() {
                let b = *tx_map.get(&fifo[j]).unwrap();
                matrix[a][b] += 1;
            }
        }

        log::info!(
            "t2 : {}\n", start_time.elapsed().as_nanos() - t1
        );

    }

    fn get_or_insert_node(rs: &mut AnchorState, tx: TxId) -> NodeIndex {
        if let Some(&idx) = rs.index_by_tx.get(&tx) {
            idx
        } else {
            let idx = rs.graph.add_node(tx);
            rs.index_by_tx.insert(tx, idx);
            idx
        }
    }

    fn add_edge(rs: &mut AnchorState, from: TxId, to: TxId) {
        let from_idx = Self::get_or_insert_node(rs, from);
        let to_idx = Self::get_or_insert_node(rs, to);
        if let Some(eid) = rs.graph.find_edge(from_idx, to_idx) {
            if let Some(w) = rs.graph.edge_weight_mut(eid) {
                *w += 1;
            }
        } else {
            rs.graph.add_edge(from_idx, to_idx, 1u32);
        }
    }

    fn fair_propose(&mut self, anchor: &Certificate) -> Option<Vec<TxId>> {
        let anchor_id = anchor.header.id.clone();
        let rs = self.anchors.get(&anchor_id)?;

        let n = self.n;
        let f = self.f;

        let solid_threshold = n - 2 * f;

        let mut solids = HashSet::new();

        for (&tx, &count) in &rs.vertex_weights {
            if count >= solid_threshold {
                solids.insert(tx);
            } 
            // else: shaded (implicitly)
        }

        let condensed = condensation(rs.graph.clone(), true);
        let topo = toposort(&condensed, None).ok()?;

        let mut last_solid_pos: Option<usize> = None;
        for (pos, comp_idx) in topo.iter().enumerate() {
            let has_solid = condensed[*comp_idx]
                .iter()
                .any(|tx| solids.contains(tx));
            if has_solid {
                last_solid_pos = Some(pos);
            }
        }

        let cutoff = match last_solid_pos {
            Some(i) => i,
            None => {
                // No solid transactions yet → nothing safely commit-able.
                return Some(vec![]);
            }
        };

        let mut order = Vec::new();
        for comp_idx in topo.into_iter().take(cutoff + 1) {
            let mut txs = condensed[comp_idx].clone();
            txs.sort();
            order.extend(txs);
        }

        let mut fresh = Vec::new();
        for tx in order {
            if self.committed_txs.insert(tx) {
                fresh.push(tx);
            }
        }

        Some(fresh)
    }
}
