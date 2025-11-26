// Copyright(C) Facebook, Inc. and its affiliates.
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::Receiver;

use petgraph::stable_graph::StableDiGraph;
use petgraph::graph::{Graph, NodeIndex};
use petgraph::algo::{condensation, toposort};

use crypto::Digest;
use store::Store;
use crate::messages::{Certificate, Header, Vote};

type TxId = u64;

struct RoundState {
    graph: Graph<TxId, ()>,
    index_by_tx: HashMap<TxId, NodeIndex>,
    pair_counts: HashMap<(TxId, TxId), u32>,
    seen_authors: HashSet<crypto::PublicKey>,
}

/// Receives certificates from primary. Then proceeds to create a global order graph from local FIFO orderings.
pub struct GlobalOrder {
    /// The persistent storage.
    store: Store,
    /// Receives certificates from Core.
    rx_certificates: Receiver<Certificate>,

    rounds: HashMap<u64, RoundState>,
    n: u32,
    f: u32,
    gamma: f64,
}

impl GlobalOrder {
    pub fn spawn(store: Store, rx_certificates: Receiver<Certificate>) {
        let n = 10;
        let f = 2;
        let gamma = 1.0;
        tokio::spawn(async move {
            Self { 
                store, 
                rx_certificates,
                rounds: HashMap::new(),
                n,
                f,
                gamma
            }.run().await;
        });
    }

    async fn run(&mut self) {
        while let Some(certificate) = self.rx_certificates.recv().await {
            let fifo_digest: Digest = certificate.header.fifo.0;
            let round: u64 = certificate.header.round;
            let author: crypto::PublicKey = certificate.header.author;

            match self.store.read(fifo_digest.to_vec()).await {
                Ok(Some(fifo_bytes)) => {

                    log::info!(
                        "FIFO bytes digest {}",
                        fifo_digest
                    );

                    let fifo_vec: Vec<u64> = bincode::deserialize(&fifo_bytes)
                        .expect("Failed to deserialize FIFO bytes");

                    // self.process_certificate(round, author, fifo_vec);
                }
                Ok(None) => {
                    log::info!(
                        "FIFO bytes not yet available for digest {} – will skip this certificate for now",
                        fifo_digest
                    );
                    continue;
                }
                Err(e) => {
                    log::error!("FIFO store.read failed for {}: {}", fifo_digest, e);
                    continue;
                }
            }
    
        }
    }

    fn round_state(&mut self, round: u64) -> &mut RoundState {
        self.rounds.entry(round).or_insert_with(|| RoundState {
            graph: Graph::new(),
            index_by_tx: HashMap::new(),
            pair_counts: HashMap::new(),
            seen_authors: HashSet::new(),
        })
    }

    fn process_certificate(&mut self, round: u64, author: crypto::PublicKey, fifo: Vec<TxId>) {

        let threshold = (self.n as f64 * (1.0 - self.gamma) + self.f as f64 + 1.0).ceil() as u32;
        let quorum = (self.n - self.f) as usize;
        
        // Track whether we just reached quorum in this call
        let mut reached_quorum = false;

        {
            // 2) Borrow RoundState in its own block
            let rs = self.round_state(round);

            rs.seen_authors.insert(author);

            // ensure nodes exist
            for &tx in &fifo {
                Self::get_or_insert_node(rs, tx);
            }

            // For each ordered pair tx_i < tx_j
            for i in 0..fifo.len() {
                for j in (i + 1)..fifo.len() {
                    let a = fifo[i];
                    let b = fifo[j];

                    *rs.pair_counts.entry((a, b)).or_insert(0) += 1;

                    let c_ab = *rs.pair_counts.get(&(a, b)).unwrap_or(&0);
                    let c_ba = *rs.pair_counts.get(&(b, a)).unwrap_or(&0);

                    if c_ab >= threshold && c_ba >= threshold {
                        if c_ab >= c_ba {
                            Self::add_edge(rs, a, b);
                        } else {
                            Self::add_edge(rs, b, a);
                        }
                    } else if c_ab >= threshold {
                        Self::add_edge(rs, a, b);
                    } else if c_ba >= threshold {
                        Self::add_edge(rs, b, a);
                    } else {
                        // not enough info yet
                    }
                }
            }

            if rs.seen_authors.len() >= quorum {
                reached_quorum = true;
            }
        } // <-- rs (&mut RoundState) is dropped here

        // 3) Now it is safe to immutably borrow self again
        if reached_quorum {
            // let _ = self.compute_order_for_round(round);
        }

    }

    fn get_or_insert_node(rs: &mut RoundState, tx: TxId) -> NodeIndex {
        if let Some(&idx) = rs.index_by_tx.get(&tx) {
            idx
        } else {
            let idx = rs.graph.add_node(tx);
            rs.index_by_tx.insert(tx, idx);
            idx
        }
    }

    fn add_edge(rs: &mut RoundState, from: TxId, to: TxId) {
        let from_idx = Self::get_or_insert_node(rs, from);
        let to_idx = Self::get_or_insert_node(rs, to);
        if !rs.graph.contains_edge(from_idx, to_idx) {
            rs.graph.add_edge(from_idx, to_idx, ());
        }
    }

    fn compute_order_for_round(&self, round: u64) -> Option<Vec<TxId>> {
        let rs = self.rounds.get(&round)?;

        // Clone the graph because `condensation` takes ownership.
        let g = rs.graph.clone();

        // Node weights become Vec<TxId>.
        let condensed = condensation(g, true);

        let topo = toposort(&condensed, None).ok()?;

        let mut result = Vec::new();
        for comp_idx in topo {
            let txs_in_comp: &Vec<TxId> = &condensed[comp_idx];
            let mut txs = txs_in_comp.clone();
            txs.sort();
            result.extend(txs);
        }

        Some(result)
    }

}
