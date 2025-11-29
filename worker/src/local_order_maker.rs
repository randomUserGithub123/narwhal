use crate::batch_maker::Batch;
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use network::ReliableSender;
use std::net::SocketAddr;
use std::collections::{VecDeque, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

pub type LocalOrder = VecDeque<Vec<u8>>;

/// Assemble clients tx_digests into LocalOrder.
pub struct LocalOrderMaker {
    /// The preferred LocalOrder size (in bytes).
    lo_size: usize,
    /// The maximum delay after which to seal the LocalOrder (in ms).
    max_lo_delay: u64,
    /// Channel to receive transactions from the network.
    rx_tx_digests: Receiver<Digest>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,

    tx_local_orders: Sender<Batch>,

    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current LocalOrder.
    current_local_order: LocalOrder,

    seen_tx_digests: HashSet<Digest>,

    /// Holds the size of the current LocalOrder (in bytes).
    current_lo_size: usize,
    /// A network sender to broadcast the LocalOrders to the other workers.
    network: ReliableSender,
}

impl LocalOrderMaker {
    pub fn spawn(
        lo_size: usize,
        max_lo_delay: u64,
        rx_tx_digests: Receiver<Digest>,
        tx_message: Sender<QuorumWaiterMessage>,
        tx_local_orders: Sender<Batch>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                lo_size,
                max_lo_delay,
                rx_tx_digests,
                tx_message,
                tx_local_orders,
                workers_addresses,
                current_local_order: LocalOrder::with_capacity(lo_size * 2),
                seen_tx_digests: HashSet::new(),
                current_lo_size: 0,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_lo_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble tx_digests into LocalOrders of preset size.
                Some(tx_digest) = self.rx_tx_digests.recv() => {
                    if self.seen_tx_digests.insert(tx_digest.clone()) {
                        self.current_lo_size += 1;
                        self.current_local_order.push_back(tx_digest.to_vec());
                        if self.current_lo_size >= self.lo_size {
                            self.seal().await;
                            timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_lo_delay));
                        }
                    }
                },

                // If the timer triggers, seal the LocalOrder even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_local_order.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_lo_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current LocalOrder.
    async fn seal(&mut self) {

        // Serialize the local order.
        self.current_lo_size = 0;
        let local_order: Vec<_> = self.current_local_order.drain(..).collect();

        // Send to global_order.rs
        self.tx_local_orders
            .send(local_order.clone())
            .await
            .expect("Failed to send LocalOrder");

        let message = WorkerMessage::Batch(local_order);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        // Broadcast the LocalOrder through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");

    }
}
