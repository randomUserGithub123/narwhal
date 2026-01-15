use crate::batch_maker::Batch;
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use ed25519_dalek::{Digest as _, Sha512};
use std::convert::TryInto as _;
use network::ReliableSender;
use std::net::SocketAddr;
use std::collections::{VecDeque, HashSet, HashMap};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use lz4_flex::compress_prepend_size;

pub type LocalOrder = VecDeque<Vec<u8>>;

fn pack_and_compress_edges(directed_edges: &[u32]) -> Vec<u8> {
    if directed_edges.is_empty() {
        return vec![];
    }
    
    let mut sorted: Vec<u32> = directed_edges.to_vec();
    sorted.sort_unstable();
    
    let mut deltas: Vec<u8> = Vec::with_capacity(sorted.len() * 3);
    let mut prev = 0u32;
    for &e in &sorted {
        let delta = e.wrapping_sub(prev);
        prev = e;
        let mut v = delta;
        while v >= 0x80 {
            deltas.push((v as u8) | 0x80);
            v >>= 7;
        }
        deltas.push(v as u8);
    }
    
    compress_prepend_size(&deltas)
}

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

    tx_local_orders: Sender<(PublicKey, Digest, Batch)>,

    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current LocalOrder.
    current_local_order: LocalOrder,

    /// Maps tx_digest -> global sequence number when first seen (for ordering)
    tx_digest_to_order: HashMap<Digest, u64>,
    
    /// Global counter for ordering transactions
    global_tx_counter: u64,

    /// Holds the size of the current LocalOrder (in bytes).
    current_lo_size: usize,

    sequence_number: u64,

    /// A network sender to broadcast the LocalOrders to the other workers.
    network: ReliableSender,

    our_public_key: PublicKey,

    rx_fair_propose: Receiver<(u64, Vec<u16>, Vec<Digest>, Vec<u32>)>,

    // Track pending fair proposals: subdag_id -> (pending_edges, digest_to_vertex_map, missing_digests_set)
    pending_fair_proposals: HashMap<u64, (Vec<u32>, HashMap<Digest, u16>, HashSet<Digest>)>,
    
    // Queue of ready directed edge votes to include in seals: Vec<(sub_dag_id, directed_edges)>
    // Each edge is encoded as (from << 32 | to) where from -> to is the voted direction
    // These persist until cleanup signal is received from global_order
    ready_fair_proposals: Vec<(u64, Vec<u32>)>,
}

impl LocalOrderMaker {
    pub fn spawn(
        lo_size: usize,
        max_lo_delay: u64,
        rx_tx_digests: Receiver<Digest>,
        tx_message: Sender<QuorumWaiterMessage>,
        tx_local_orders: Sender<(PublicKey, Digest, Batch)>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        our_public_key: PublicKey,
        rx_fair_propose: Receiver<(u64, Vec<u16>, Vec<Digest>, Vec<u32>)>,
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
                tx_digest_to_order: HashMap::new(),
                global_tx_counter: 0,
                current_lo_size: 0,
                sequence_number: 0,
                network: ReliableSender::new(),
                our_public_key,
                rx_fair_propose,
                pending_fair_proposals: HashMap::new(),
                ready_fair_proposals: Vec::new(),
            }
            .run()
            .await;
        });
    }

    /// Compute directed edge vote based on local ordering.
    /// Returns (from, to) where from came before to in our local view.
    /// If we don't know the order, use vertex index as tiebreaker.
    fn vote_edge_direction(
        &self,
        u_vertex: u16,
        v_vertex: u16,
        u_digest: &Digest,
        v_digest: &Digest,
    ) -> (u16, u16) {
        let u_order = self.tx_digest_to_order.get(u_digest);
        let v_order = self.tx_digest_to_order.get(v_digest);
        
        match (u_order, v_order) {
            (Some(&u_ord), Some(&v_ord)) => {
                if u_ord < v_ord {
                    (u_vertex, v_vertex)
                } else if v_ord < u_ord {
                    (v_vertex, u_vertex)
                } else {
                    // Same order (shouldn't happen), use vertex index as tiebreaker
                    if u_vertex < v_vertex {
                        (u_vertex, v_vertex)
                    } else {
                        (v_vertex, u_vertex)
                    }
                }
            }
            (Some(_), None) => (u_vertex, v_vertex),
            (None, Some(_)) => (v_vertex, u_vertex),
            (None, None) => {
                if u_vertex < v_vertex {
                    (u_vertex, v_vertex)
                } else {
                    (v_vertex, u_vertex)
                }
            }
        }
    }

    #[inline]
    fn pack_directed_edge(from: u16, to: u16) -> u32 {
        ((from as u32) << 16) | (to as u32)
    }

    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_lo_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {

                Some(tx_digest) = self.rx_tx_digests.recv() => {
                    if !self.tx_digest_to_order.contains_key(&tx_digest) {

                        log::info!(
                            "Receival of tx_digest {} at position {}", 
                            tx_digest, 
                            self.global_tx_counter
                        );

                        self.tx_digest_to_order.insert(tx_digest.clone(), self.global_tx_counter);
                        self.global_tx_counter += 1;
                        
                        self.current_lo_size += 1;
                        self.current_local_order.push_back(tx_digest.to_vec());
                        
                        self.check_pending_proposals(&tx_digest);
                        
                        if self.current_lo_size >= self.lo_size {
                            self.seal().await;
                            timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_lo_delay));
                        }
                    }
                },

                Some((sub_dag_id, missing_edge_vertices, missing_tx_digests, missing_edges)) = self.rx_fair_propose.recv() => {
                    
                    // CLEANUP SIGNAL from global_order after FairFinalize
                    if missing_edge_vertices.is_empty() && missing_tx_digests.is_empty() && missing_edges.is_empty() {
                        log::info!(
                            "rx_fair_propose: CLEANUP signal for sub_dag_id={}",
                            sub_dag_id
                        );
                        
                        // Only clean pending_fair_proposals - ready_fair_proposals already drained on seal
                        self.pending_fair_proposals.remove(&sub_dag_id);
                        
                        log::info!(
                            "Cleaned up sub_dag_id={}: pending_proposals={}",
                            sub_dag_id, 
                            self.pending_fair_proposals.len()
                        );
                        continue;
                    }
                    
                    log::info!(
                        "rx_fair_propose: sub_dag_id={}, vertices={}, tx_digests={}, edges={}",
                        sub_dag_id, missing_edge_vertices.len(), missing_tx_digests.len(), missing_edges.len()
                    );
                    
                    if missing_edge_vertices.len() != missing_tx_digests.len() {
                        log::error!(
                            "sub_dag_id={}: mismatch between vertices ({}) and digests ({})",
                            sub_dag_id, missing_edge_vertices.len(), missing_tx_digests.len()
                        );
                        continue;
                    }
                    
                    let mut digest_to_vertex: HashMap<Digest, u16> = HashMap::new();
                    let mut vertex_to_digest: HashMap<u16, Digest> = HashMap::new();
                    let mut missing_digests_set: HashSet<Digest> = HashSet::new();
                    
                    for i in 0..missing_edge_vertices.len() {
                        let vertex_idx = missing_edge_vertices[i];
                        let digest = missing_tx_digests[i].clone();
                        
                        digest_to_vertex.insert(digest.clone(), vertex_idx);
                        vertex_to_digest.insert(vertex_idx, digest.clone());
                        
                        if !self.tx_digest_to_order.contains_key(&digest) {
                            missing_digests_set.insert(digest);
                        }
                    }
                    
                    let mut directed_edge_votes: Vec<u32> = Vec::new();
                    let mut pending_edges: Vec<u32> = Vec::new();
                    
                    for &edge in &missing_edges {
                        let u = (edge >> 16) as u16;
                        let v = (edge & 0xFFFF) as u16;
                        
                        let u_digest = vertex_to_digest.get(&u);
                        let v_digest = vertex_to_digest.get(&v);
                        
                        match (u_digest, v_digest) {
                            (Some(u_dig), Some(v_dig)) => {
                                let u_known = self.tx_digest_to_order.contains_key(u_dig);
                                let v_known = self.tx_digest_to_order.contains_key(v_dig);
                                
                                if u_known && v_known {
                                    let (from, to) = self.vote_edge_direction(u, v, u_dig, v_dig);
                                    directed_edge_votes.push(Self::pack_directed_edge(from, to));
                                } else {
                                    pending_edges.push(edge);
                                }
                            },
                            _ => {
                                log::error!(
                                    "sub_dag_id={}: edge ({},{}) references unknown vertex",
                                    sub_dag_id, u, v
                                );
                                pending_edges.push(edge);
                            }
                        }
                    }
                    
                    log::info!(
                        "sub_dag_id={}: directed_votes={}, pending_edges={}, missing_digests={}",
                        sub_dag_id, directed_edge_votes.len(), pending_edges.len(), missing_digests_set.len()
                    );
                    
                    if !directed_edge_votes.is_empty() {
                        self.ready_fair_proposals.push((sub_dag_id, directed_edge_votes));
                    }
                    
                    if !pending_edges.is_empty() {
                        self.pending_fair_proposals.insert(
                            sub_dag_id,
                            (pending_edges, digest_to_vertex, missing_digests_set)
                        );
                    }
                },

                () = &mut timer => {
                    if !self.current_local_order.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_lo_delay));
                }
            }

            tokio::task::yield_now().await;
        }
    }

    fn check_pending_proposals(&mut self, tx_digest: &Digest) {
        let mut new_ready: Vec<(u64, Vec<u32>)> = Vec::new();
        
        // Extract reference before mutable borrow of pending_fair_proposals
        let tx_digest_to_order = &self.tx_digest_to_order;
        
        for (sub_dag_id, (pending_edges, digest_to_vertex, missing_digests_set)) in self.pending_fair_proposals.iter_mut() {
            if !missing_digests_set.remove(tx_digest) {
                continue;
            }
            
            let vertex_idx = match digest_to_vertex.get(tx_digest) {
                Some(&idx) => idx,
                None => continue,
            };
            
            log::info!(
                "sub_dag_id={}: resolved vertex {} (remaining missing: {})",
                sub_dag_id, vertex_idx, missing_digests_set.len()
            );
            
            let vertex_to_digest: HashMap<u16, Digest> = digest_to_vertex
                .iter()
                .map(|(d, &v)| (v, d.clone()))
                .collect();
            
            let mut newly_voted: Vec<u32> = Vec::new();
            pending_edges.retain(|&edge| {
                let u = (edge >> 16) as u16;
                let v = (edge & 0xFFFF) as u16;
                
                let (u_digest, v_digest) = match (vertex_to_digest.get(&u), vertex_to_digest.get(&v)) {
                    (Some(ud), Some(vd)) => (ud, vd),
                    _ => return true,
                };
                
                let u_resolved = !missing_digests_set.contains(u_digest) 
                    && tx_digest_to_order.contains_key(u_digest);
                let v_resolved = !missing_digests_set.contains(v_digest)
                    && tx_digest_to_order.contains_key(v_digest);
                
                if u_resolved && v_resolved {
                    // Inline vote_edge_direction logic
                    let u_order = tx_digest_to_order.get(u_digest);
                    let v_order = tx_digest_to_order.get(v_digest);
                    
                    let (from, to) = match (u_order, v_order) {
                        (Some(&u_ord), Some(&v_ord)) => {
                            if u_ord < v_ord {
                                (u, v)
                            } else if v_ord < u_ord {
                                (v, u)
                            } else {
                                if u < v { (u, v) } else { (v, u) }
                            }
                        }
                        (Some(_), None) => (u, v),
                        (None, Some(_)) => (v, u),
                        (None, None) => {
                            if u < v { (u, v) } else { (v, u) }
                        }
                    };
                    
                    newly_voted.push(Self::pack_directed_edge(from, to));
                    false
                } else {
                    true
                }
            });
            
            if !newly_voted.is_empty() {
                log::info!("sub_dag_id={}: {} newly voted edges", sub_dag_id, newly_voted.len());
                new_ready.push((*sub_dag_id, newly_voted));
            }
        }
        
        self.ready_fair_proposals.extend(new_ready);
        self.pending_fair_proposals.retain(|_, (pending_edges, _, _)| !pending_edges.is_empty());
    }

    async fn seal(&mut self) {
        self.current_lo_size = 0;
        let mut local_order: Vec<_> = self.current_local_order.drain(..).collect();

        let seq_bytes = self.sequence_number.to_le_bytes().to_vec();
        local_order.insert(0, seq_bytes);
        self.sequence_number += 1;

        if !self.ready_fair_proposals.is_empty() {
            log::info!(
                "seal: including {} fair proposal batches in this LocalOrder",
                self.ready_fair_proposals.len()
            );
            
            for (sub_dag_id, directed_edges) in self.ready_fair_proposals.drain(..) {
                // Sentinel marker (32 bytes of 0xFF)
                local_order.push(vec![0xFF; 32]);
                // Sub-dag ID (8 bytes)
                local_order.push(sub_dag_id.to_le_bytes().to_vec());
                // Edge count (8 bytes) - needed for decoding
                local_order.push((directed_edges.len() as u64).to_le_bytes().to_vec());
                // Compressed edge blob (single entry!)
                let compressed = pack_and_compress_edges(&directed_edges);
                
                log::info!(
                    "seal: appended {} edge votes for sub_dag_id={}, compressed {} -> {} bytes",
                    directed_edges.len(),
                    sub_dag_id,
                    directed_edges.len() * 4,
                    compressed.len()
                );
                
                local_order.push(compressed);
            }
            // ready_fair_proposals is now empty after drain
        }

        let message = WorkerMessage::Batch(self.our_public_key, local_order.clone());
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        let digest = Digest(
            Sha512::digest(&serialized)[..32]
                .try_into()
                .unwrap(),
        );

        self.tx_local_orders
            .send((self.our_public_key, digest.clone(), local_order))
            .await
            .expect("Failed to send LocalOrder");

        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        self.tx_message
            .send(QuorumWaiterMessage {
                digest,
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }

}