// Copyright(C) Facebook, Inc. and its affiliates.
use tokio::sync::mpsc::Receiver;
use std::collections::{HashMap};

use crate::batch_maker::Batch;

const MAX_TX: usize = 10_000;

// Construct GlobalOrder graph.
pub struct GlobalOrder {
    /// Input channel to receive the LocalOrders.
    rx_local_orders: Receiver<Batch>,

    tx_digest_to_index: HashMap<Vec<u8>, usize>,
    index_to_tx_digest: Vec<Vec<u8>>,
    weight_matrix: Vec<Vec<u64>>,
    vertex_status: Vec<u64>
}

impl GlobalOrder {
    pub fn spawn(rx_local_orders: Receiver<Batch>) {
        tokio::spawn(async move {

            let tx_digest_to_index = HashMap::with_capacity(MAX_TX);
            let index_to_tx_digest = Vec::with_capacity(MAX_TX);

            let weight_matrix = vec![vec![0u64; MAX_TX]; MAX_TX];

            let vertex_status = vec![064; MAX_TX];

            Self {
                rx_local_orders,
                tx_digest_to_index,
                index_to_tx_digest,
                weight_matrix,
                vertex_status
            }
            .run()
            .await;
        });
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

    async fn run(&mut self) {
        while let Some(local_order) = self.rx_local_orders.recv().await {
            
            let start_time = std::time::Instant::now();

            // let indices: Vec<usize> = local_order
            //     .into_iter()
            //     .map(|digest| self.index_for_digest(digest))
            //     .collect();

            // for (pos, &i) in indices.iter().enumerate() {
            //     self.vertex_status[i] += 1;
            //     for &j in &indices[pos + 1..] {
            //         self.weight_matrix[i][j] += 1;
            //     }
            // }

            log::info!(
                "TIME IT TOOK : {}", start_time.elapsed().as_nanos()
            );

        }
    }
}
