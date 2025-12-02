// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::PrimaryWorkerMessage;
use crate::Round;

use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use network::SimpleSender;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<(Certificate, bool)>,
    /// The network addresses of our workers.
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    network: SimpleSender,
    /// Send back rounds that we successfully committed, firtst value is when it was committed, second round is the certificate round of proposal
    tx_committed_own_headers: Sender<Round>,
    /// Ourselves
    us: PublicKey,

    /// A network sender to send to OF_worker.
    simple_network: SimpleSender,
    header_to_local_orders: HashMap<Digest, (Round, Vec<Digest>)>,
    rx_header_arrival: Receiver<(Round, PublicKey, Digest, Vec<Digest>)>,
    committed_headers_without_local_orders: HashSet<Digest>,
    of_worker_address: SocketAddr,

    current_subdag: HashMap<Round, HashSet<PublicKey>>,

}

impl GarbageCollector {
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<(Certificate, bool)>,
        tx_committed_own_headers: Sender<Round>,
        rx_header_arrival: Receiver<(Round, PublicKey, Digest, Vec<Digest>)>,
    ) {
        let addresses = committee
            .our_workers(&name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();

        let of_worker_address = committee
            .worker(&name, &0)
            .expect("OF_worker address exists")
            .primary_to_worker;

        tokio::spawn(async move {
            Self {
                consensus_round,
                rx_consensus,
                addresses,
                network: SimpleSender::new(),
                tx_committed_own_headers,
                us: name,
                simple_network: SimpleSender::new(),
                header_to_local_orders: HashMap::new(),
                rx_header_arrival,
                committed_headers_without_local_orders: HashSet::new(),
                of_worker_address,
                current_subdag: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;

        loop {
            tokio::select! {

                Some((certificate, is_leader)) = self.rx_consensus.recv() => {

                    let round = certificate.header.round;
                    let author = certificate.header.author;

                    if let Some((_hdr_round, _local_orders)) =
                        self.header_to_local_orders.remove(&certificate.header.id)
                    {
                        
                    }else {
                        self.committed_headers_without_local_orders.insert(certificate.header.id.clone());
                    }

                    self.current_subdag
                        .entry(round)
                        .or_default()
                        .insert(author);

                    if is_leader {

                        let mut entries: Vec<(Round, HashSet<PublicKey>)> =
                            self.current_subdag.drain().collect();
                        entries.sort_by_key(|(round, _)| *round);

                        let sub_dag: Vec<(Round, Vec<PublicKey>)> = entries
                            .into_iter()
                            .map(|(round, authors_set)| {
                                let authors: Vec<PublicKey> = authors_set.into_iter().collect();
                                (round, authors)
                            })
                            .collect();

                        let message = PrimaryWorkerMessage::CommittedSubDag(
                            sub_dag
                        );
                        let serialized = bincode::serialize(&message)
                            .expect("Failed to serialize our own CommittedSubDag message");
                        self.simple_network.send(self.of_worker_address, Bytes::from(serialized)).await;

                        if self.committed_headers_without_local_orders.len() > 0 {
                            log::info!(
                                "committed_headers_without_local_orders : {}", self.committed_headers_without_local_orders.len()
                            );
                        }
                        
                    }

                    let round = certificate.round();
                    if round > last_committed_round {
                        last_committed_round = round;

                        // Trigger cleanup on the primary.
                        self.consensus_round.store(round, Ordering::Relaxed);

                        // Trigger cleanup on the workers..
                        let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                            .expect("Failed to serialize our own message");
                        self.network
                            .broadcast(self.addresses.clone(), Bytes::from(bytes))
                            .await;
                    }

                    // Report rounds in which we have our block committed
                    if certificate.header.author == self.us {
                        self.tx_committed_own_headers
                            .send(round)
                            .await
                            .expect("Could not send own committed round back");
                    }
                },
                Some((round, author, header_digest, local_orders)) = self.rx_header_arrival.recv() => {
                    self.header_to_local_orders
                        .insert(header_digest.clone(), (round, local_orders)); 

                    if self.committed_headers_without_local_orders.remove(&header_digest){

                    }
                }

            }
        }
    }
}
