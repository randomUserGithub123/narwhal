// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL: receives committed subdags from consensus and
// broadcasts them to workers for fair ordering.
use crate::messages::Certificate;
use crate::primary::{PrimaryWorkerMessage, Round};
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use log::info;
use network::SimpleSender;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate>,
    /// FairDAG-RL: receives entire committed subdags from consensus.
    rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
    /// The network addresses of our workers.
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    network: SimpleSender,
}

impl GarbageCollector {
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate>,
        rx_committed_subdags: Receiver<(Round, Vec<Certificate>)>,
    ) {
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();

        tokio::spawn(async move {
            Self {
                consensus_round,
                rx_consensus,
                rx_committed_subdags,
                addresses,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;

        loop {
            tokio::select! {
                // Handle individual committed certificates (cleanup path).
                Some(certificate) = self.rx_consensus.recv() => {
                    let round = certificate.round();
                    if round > last_committed_round {
                        last_committed_round = round;

                        // Trigger cleanup on the primary.
                        self.consensus_round.store(round, Ordering::Relaxed);

                        // Trigger cleanup on the workers.
                        let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                            .expect("Failed to serialize cleanup message");
                        self.network
                            .broadcast(self.addresses.clone(), Bytes::from(bytes))
                            .await;
                    }
                },

                // FairDAG-RL: Handle entire committed subdags.
                // Forward to workers for fair ordering.
                Some((leader_round, certificates)) = self.rx_committed_subdags.recv() => {
                    info!(
                        "GC: broadcasting ExecuteSubdag for leader round {} with {} certs to {} workers",
                        leader_round, certificates.len(), self.addresses.len()
                    );

                    let bytes = bincode::serialize(
                        &PrimaryWorkerMessage::ExecuteSubdag(leader_round, certificates)
                    )
                    .expect("Failed to serialize ExecuteSubdag message");

                    self.network
                        .broadcast(self.addresses.clone(), Bytes::from(bytes))
                        .await;
                },
            }
        }
    }
}