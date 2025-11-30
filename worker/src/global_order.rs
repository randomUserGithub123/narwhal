// Copyright(C) Facebook, Inc. and its affiliates.
use tokio::sync::mpsc::Receiver;
use tokio::task;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use crypto::Digest;

use crate::batch_maker::Batch;

const MAX_TX: usize = 10_000;

struct GlobalOrderState {
    tx_digest_to_index: HashMap<Vec<u8>, usize>,
    index_to_tx_digest: Vec<Vec<u8>>,
    weight_matrix: Vec<Vec<u64>>,
    vertex_status: Vec<u64>,
    lo_contributions: HashMap<Digest, Vec<usize>>,
}

impl GlobalOrderState {
    fn new() -> Self {
        let tx_digest_to_index = HashMap::with_capacity(MAX_TX);
        let index_to_tx_digest = Vec::with_capacity(MAX_TX);

        let weight_matrix = vec![vec![0u64; MAX_TX]; MAX_TX];

        let vertex_status = vec![0u64; MAX_TX];

        GlobalOrderState {
            tx_digest_to_index,
            index_to_tx_digest,
            weight_matrix,
            vertex_status,
            lo_contributions: HashMap::new(),
        }
    }

    fn index_for_digest(&mut self, digest: Vec<u8>) -> usize {
        if let Some(&idx) = self.tx_digest_to_index.get(&digest) {
            return idx;
        }

        let idx = self.index_to_tx_digest.len();

        if idx >= MAX_TX {
            panic!("GlobalOrder: exceeded MAX_TX ({}) distinct transactions", MAX_TX);
        }

        self.tx_digest_to_index.insert(digest.clone(), idx);
        self.index_to_tx_digest.push(digest);

        idx
    }

    fn apply_local_order(&mut self, lo_digest: Digest, local_order: Batch) {
        
        let indices: Vec<usize> = local_order
            .into_iter()
            .map(|digest| self.index_for_digest(digest))
            .collect();

        for (pos, &i) in indices.iter().enumerate() {
            self.vertex_status[i] += 1;
            for &j in &indices[pos + 1..] {
                self.weight_matrix[i][j] += 1;
            }
        }

        self.lo_contributions
            .insert(lo_digest, indices);

    }

    fn remove_local_order(&mut self, lo_digest: &Digest) {
        if let Some(indices) = self.lo_contributions.remove(lo_digest) {
            for (pos, &i) in indices.iter().enumerate() {
                if self.vertex_status[i] > 0 {
                    self.vertex_status[i] -= 1;
                }
                for &j in &indices[pos + 1..] {
                    if self.weight_matrix[i][j] > 0 {
                        self.weight_matrix[i][j] -= 1;
                    }
                }
            }
        }
    }

}

pub struct GlobalOrder {
    
    rx_local_orders: Receiver<(crypto::PublicKey, Digest, Batch)>,
    state: Arc<Mutex<GlobalOrderState>>,
}

impl GlobalOrder {
    pub fn spawn(rx_local_orders: Receiver<(crypto::PublicKey, Digest, Batch)>) {
        tokio::spawn(async move {
            let state = Arc::new(Mutex::new(GlobalOrderState::new()));

            GlobalOrder {
                rx_local_orders,
                state,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((author, lo_digest, local_order)) = self.rx_local_orders.recv().await {
            let state = Arc::clone(&self.state);

            task::spawn_blocking(move || {
                let start_time = Instant::now();

                // if let Ok(mut guard) = state.lock() {
                //     guard.apply_local_order(local_order);
                // } else {
                //     log::error!("GlobalOrder: failed to lock state mutex");
                //     return;
                // }

                log::info!(
                    "GlobalOrder: processing one local_order took {} ns",
                    start_time.elapsed().as_nanos()
                );
            });

        }
    }
}
