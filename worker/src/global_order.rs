use futures::channel::mpsc::Sender;
use primary::Round;
use store::Store;
// Copyright(C) Facebook, Inc. and its affiliates.
use tokio::sync::mpsc::Receiver;
use tokio::task;
use std::convert::TryInto as _;
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use crypto::{Digest, Hash, PublicKey};
use nohash::{IntMap, IntSet};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use lz4_flex::decompress_size_prepended;

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    TxDigest(Digest),
    Batch(PublicKey, Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

use crate::batch_maker::Batch;

const MAX_TX: usize = 60_000;
const MAX_ORDERS: usize = 500;  // Max LocalOrders per subdag
const MATRIX_POOL_SIZE: usize = 10; // M

pub struct UTIGMatrix {
    // === UTIG fields ===
    pub weight: Vec<u8>,           // MAX_TX × MAX_TX / 2 (direct access)
    pub support: Vec<u8>,          // MAX_TX (direct access)
    pub is_non_blank: Vec<bool>,
    pub is_solid: Vec<bool>,
    pub edges: Vec<Vec<u16>>,

    // === AUNCEL fields ===
    pub positions: Vec<u16>,
    pub weight_sum: Vec<f64>,
}

impl UTIGMatrix {

    pub fn new() -> Self {
        UTIGMatrix {
            // UTIG - direct u8 arrays
            weight: vec![0u8; (MAX_TX * MAX_TX + 1) / 2], // Nibble-packed:
            support: vec![0u8; MAX_TX],
            is_non_blank: vec![false; MAX_TX],
            is_solid: vec![false; MAX_TX],
            edges: (0..MAX_TX).map(|_| Vec::with_capacity(64)).collect(),

            // AUNCEL
            positions: vec![u16::MAX; MAX_TX * MAX_ORDERS],
            weight_sum: vec![0.0; MAX_TX],
        }
    }

    /// Reset for UTIG use
    #[inline]
    pub fn reset_utig(&mut self, k: usize) {
        let packed_size = (k * k + 1) / 2;
        self.weight[..packed_size].fill(0);
        self.support[..k].fill(0);
        self.is_non_blank[..k].fill(false);
        self.is_solid[..k].fill(false);
        for e in &mut self.edges[..k] { e.clear(); }
    }

    /// Reset for FairUpdate (only weight needed)
    #[inline]
    pub fn reset_fair_update(&mut self, k: usize) {
        let packed_size = (k * k + 1) / 2;
        self.weight[..packed_size].fill(0);
    }

    /// Reset for AUNCEL use
    #[inline]
    pub fn reset_auncel(&mut self, k: usize, num_orders: usize) {
        self.support[..k].fill(0);
        self.weight_sum[..k].fill(0.0);
        self.positions[..k * num_orders].fill(u16::MAX);
    }

    // === AUNCEL position helpers ===

    #[inline(always)]
    pub fn get_position(&self, tx: usize, order_idx: usize, num_orders: usize) -> Option<usize> {
        let val = self.positions[tx * num_orders + order_idx];
        if val == u16::MAX { None } else { Some(val as usize) }
    }

    #[inline(always)]
    pub fn set_position(&mut self, tx: usize, order_idx: usize, num_orders: usize, pos: usize) {
        self.positions[tx * num_orders + order_idx] = pos as u16;
    }
}

#[inline(always)]
fn get_weight(weight_packed: &[u8], idx: usize) -> u8 {
    let byte_idx = idx >> 1;
    let byte = weight_packed[byte_idx];
    if idx & 1 == 0 {
        byte & 0x0F
    } else {
        byte >> 4
    }
}

#[inline(always)]
fn set_weight(weight_packed: &mut [u8], idx: usize, value: u8) {
    let byte_idx = idx >> 1;
    if idx & 1 == 0 {
        weight_packed[byte_idx] = (weight_packed[byte_idx] & 0xF0) | (value & 0x0F);
    } else {
        weight_packed[byte_idx] = (weight_packed[byte_idx] & 0x0F) | (value << 4);
    }
}

#[inline(always)]
fn inc_weight(weight_packed: &mut [u8], idx: usize) {
    let byte_idx = idx >> 1;
    if idx & 1 == 0 {
        let low = weight_packed[byte_idx] & 0x0F;
        if low < 15 {
            weight_packed[byte_idx] += 1;
        }
    } else {
        let high = weight_packed[byte_idx] >> 4;
        if high < 15 {
            weight_packed[byte_idx] += 0x10;
        }
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

fn unpack_and_decompress_edges(compressed: &[u8], expected_count: usize) -> Vec<u32> {
    if compressed.is_empty() || expected_count == 0 {
        return vec![];
    }
    
    // LZ4 decompress
    let deltas = match decompress_size_prepended(compressed) {
        Ok(d) => d,
        Err(e) => {
            log::error!("Failed to decompress edges: {}", e);
            return vec![];
        }
    };
    
    // Varint + delta decode
    let mut edges: Vec<u32> = Vec::with_capacity(expected_count);
    let mut prev = 0u32;
    let mut i = 0;
    
    while i < deltas.len() && edges.len() < expected_count {
        // Read varint
        let mut delta = 0u32;
        let mut shift = 0;
        loop {
            if i >= deltas.len() {
                break;
            }
            let b = deltas[i];
            i += 1;
            delta |= ((b & 0x7F) as u32) << shift;
            if b < 0x80 {
                break;
            }
            shift += 7;
            if shift > 28 {
                // Overflow protection
                log::error!("Varint overflow during edge decoding");
                return edges;
            }
        }
        
        prev = prev.wrapping_add(delta);
        edges.push(prev);
    }
    
    edges
}

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

    tx_fair_propose: tokio::sync::mpsc::Sender<(u64, Vec<u16>, Vec<Digest>, Vec<u32>)>,
    sub_dag_count: u64,

    author_to_lo_digests: HashMap<PublicKey, Vec<Option<Digest>>>,
    digest_to_seq: HashMap<PublicKey, HashMap<Digest, usize>>,
    author_round_boundaries: HashMap<PublicKey, Vec<(Round, usize, usize)>>,
    pending_headers: HashMap<PublicKey, Vec<(Round, Vec<Digest>)>>,
    
    pending_subdags: VecDeque<Vec<(Round, Vec<PublicKey>)>>,

    tx_utig_results: tokio::sync::mpsc::Sender<(u64, Vec<Vec<usize>>, Vec<u16>, Vec<(u16,u16)>, Vec<u16>, Vec<u32>)>,
    rx_utig_results: tokio::sync::mpsc::Receiver<(u64, Vec<Vec<usize>>, Vec<u16>, Vec<(u16,u16)>, Vec<u16>, Vec<u32>)>,

    finalized_subdags: HashSet<u64>,
    pending_subdags_fair: HashSet<u64>,

    pending_fair_updates: HashMap<u64, HashMap<PublicKey, Digest>>,

    next_to_finalize: u64,
    already_finalized: HashSet<Digest>,

    use_auncel: bool,
    auncel_weight_k: f64,
    auncel_use_final_phase: bool,

    tx_auncel_results: tokio::sync::mpsc::Sender<(u64, Vec<Vec<usize>>)>,
    rx_auncel_results: tokio::sync::mpsc::Receiver<(u64, Vec<Vec<usize>>)>,
}

impl GlobalOrder {

    fn region_data_key(sub_dag_id: u64) -> Vec<u8> {
        let mut key = vec![0xFF; 8];
        key.extend_from_slice(&sub_dag_id.to_le_bytes());
        key
    }

    fn index_to_digest_key(sub_dag_id: u64) -> Vec<u8> {
        let mut key = vec![0xFE; 8];
        key.extend_from_slice(&sub_dag_id.to_le_bytes());
        key
    }
    
    fn subdag_state_key(sub_dag_id: u64) -> Vec<u8> {
        let mut key = vec![0xFD; 8];
        key.extend_from_slice(&sub_dag_id.to_le_bytes());
        key
    }
    
    fn finalized_indices_key(sub_dag_id: u64) -> Vec<u8> {
        let mut key = vec![0xFC; 8];
        key.extend_from_slice(&sub_dag_id.to_le_bytes());
        key
    }

    pub fn new(
        store: Store,
        rx_local_orders: Receiver<(PublicKey, Digest, Batch)>,
        rx_header_update: Receiver<(PublicKey, Round, Vec<Digest>)>,
        rx_consensus_update: Receiver<Vec<(Round, Vec<PublicKey>)>>,
        n: u64,
        f: u64,
        gamma: f64,
        tx_fair_propose: tokio::sync::mpsc::Sender<(u64, Vec<u16>, Vec<Digest>, Vec<u32>)>,
    ) -> Self {
        let non_blank_threshold =
            ((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0).floor() as u16;
        let solid_threshold = (n - 2 * f) as u16;

        let (tx_utig_results, rx_utig_results) = tokio::sync::mpsc::channel(1024);
        let (tx_auncel_results, rx_auncel_results) = tokio::sync::mpsc::channel(1024);

        GlobalOrder {
            store,
            rx_local_orders,
            rx_header_update,
            rx_consensus_update,
            n,
            f,
            gamma,
            tx_fair_propose,
            sub_dag_count: 0,
            non_blank_threshold,
            solid_threshold,
            author_to_lo_digests: HashMap::new(),
            digest_to_seq: HashMap::new(),
            author_round_boundaries: HashMap::new(),
            pending_headers: HashMap::new(),
            pending_subdags: VecDeque::new(),
            rx_utig_results,
            tx_utig_results,
            finalized_subdags: HashSet::new(),
            pending_subdags_fair: HashSet::new(),
            pending_fair_updates: HashMap::new(),
            next_to_finalize: 0,
            already_finalized: HashSet::new(),
            use_auncel: false,
            auncel_weight_k: 0.5,
            auncel_use_final_phase: false,
            tx_auncel_results,
            rx_auncel_results,
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

        let rounds: Vec<Round> = sub_dag.iter().map(|(r, _)| *r).collect();
        let min_round = rounds.iter().min().copied().unwrap_or(0);
        let max_round = rounds.iter().max().copied().unwrap_or(0);
        let total_author_entries: usize = sub_dag.iter().map(|(_, authors)| authors.len()).sum();
        
        log::warn!(
            "SUBDAG STRUCTURE: sub_dag_id={}, rounds={}, span={}..{} ({} rounds), author_entries={}",
            self.sub_dag_count,
            sub_dag.len(),
            min_round,
            max_round,
            max_round - min_round + 1,
            total_author_entries
        );
        for (round, authors) in &sub_dag {
            log::info!("  round {}: {} authors", round, authors.len());
        }

        let mut author_to_lo_digests_subdag: HashMap<PublicKey, Vec<Digest>> = HashMap::new();

        // Find the final (maximum) round
        let final_round = sub_dag.iter().map(|(r, _)| *r).max().unwrap_or(0);
        log::info!("process_subdag: final_round={}", final_round);

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
        let mut index_to_digest: Vec<Vec<u8>> = Vec::new();
        let mut next_idx: usize = 0;

        // Track FairUpdate vote locations (don't extract edges yet)
        let mut found_fair_updates: Vec<(u64, PublicKey, Digest)> = Vec::new();

        let mut authors: Vec<PublicKey> = author_to_lo_digests_subdag.keys().cloned().collect();
        authors.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        for author in authors {
            let lo_digests = &author_to_lo_digests_subdag[&author];
            
            // Determine which LocalOrders to extract fair proposals from
            // Option 1: Only from final round
            // Get the author's round boundaries to find which LOs belong to final round
            let round_boundaries = self.author_round_boundaries.get(&author).unwrap();
            let final_round_boundary = round_boundaries.iter().find(|(r, _, _)| *r == final_round);
            
            let (final_round_start, final_round_end) = match final_round_boundary {
                Some((_r, start, end)) => (*start, *end),
                None => (usize::MAX, 0), // Author not in final round
            };

            let mut indices: Vec<usize> = Vec::new();
            
            for (lo_idx, lo_digest) in lo_digests.iter().enumerate() {
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

                // Determine the sequence number of this LocalOrder
                let seq_num = match Self::parse_seq_le(&local_order) {
                    Some(seq) => seq,
                    None => {
                        log::warn!("Could not parse seq for LocalOrder {:?}", lo_digest);
                        0 // Fallback, but this shouldn't happen
                    }
                };

                {
                    let tx_digests_in_order: Vec<String> = local_order.iter()
                        .enumerate()
                        .filter_map(|(idx, entry)| {
                            if idx == 0 { return None; } // Skip sequence number
                            if entry.len() == 32 && entry.iter().all(|&b| b == 0xFF) { return None; } // Skip sentinel
                            if entry.len() == 8 { return None; } // Skip sub_dag_id/edge_count
                            if entry.len() == 32 {
                                Some(format!("{:?}", Digest(entry.clone().try_into().unwrap())))
                            } else {
                                None // Skip compressed blobs
                            }
                        })
                        .collect();
                    
                    log::info!(
                        "sub_dag_id={}: Author {:?}, LocalOrder seq={}, lo_digest={:?}, tx_digests=[{}]",
                        self.sub_dag_count,
                        author,
                        seq_num,
                        lo_digest,
                        tx_digests_in_order.join(", ")
                    );
                }
                
                let mut tx_idx = 0;
                while tx_idx < local_order.len() {
                    if tx_idx == 0 {
                        // Skip sequence number
                        tx_idx += 1;
                        continue;
                    }
                    
                    let tx_digest = &local_order[tx_idx];
                    
                    // Check if this is a sentinel marker (32 bytes of 0xFF)
                    if tx_digest.len() == 32 && tx_digest.iter().all(|&b| b == 0xFF) {
                        
                        // Extract sub_dag_id to know which sub-dag this vote is for
                        if tx_idx + 1 < local_order.len() {
                            let sub_dag_id_bytes = &local_order[tx_idx + 1];
                            if sub_dag_id_bytes.len() == 8 {
                                let mut arr = [0u8; 8];
                                arr.copy_from_slice(sub_dag_id_bytes);
                                let vote_sub_dag_id = u64::from_le_bytes(arr);
                                
                                // Record vote location (lazy - don't extract edges yet)
                                found_fair_updates.push((vote_sub_dag_id, author, lo_digest.clone()));
                                
                                log::info!(
                                    "Found FairUpdate vote: sub_dag_id={}, author={:?}, lo_digest={:?}",
                                    vote_sub_dag_id, author, lo_digest
                                );
                            }
                        }
                        
                        // NEW FORMAT: sentinel + sub_dag_id + edge_count + compressed_blob = 4 entries
                        // Skip past all 4 entries
                        tx_idx += 4;
                        continue;
                    }
                    
                    // Regular tx_digest - add to indices (ALWAYS, regardless of round)
                    let idx = *digest_to_local.entry(tx_digest.clone()).or_insert_with(|| {
                        let curr = next_idx;
                        index_to_digest.push(tx_digest.clone());
                        next_idx += 1;
                        curr
                    });
                    indices.push(idx);
                    tx_idx += 1;
                }

            }

            if !indices.is_empty() {
                indices_sets.push(indices);
            }

        }

        let k = next_idx;
        let t2 = start_time.elapsed().as_nanos() - t1;
        log::info!(
            "t2 (store reads + indexing): {}\nunique txs (k): {}\nLocalOrders processed: {}\nfound_fair_updates found for {} subdags",
            t2, k, indices_sets.len(), found_fair_updates.len()
        );
        
        // USED WHEN SORTING INSIDE SCC ---> Aequitas approach
        let mut sorted_digests = index_to_digest.clone();
        sorted_digests.sort_unstable();

        let mut sorted_rank: Vec<usize> = vec![0usize; k];
        for (new_idx, digest) in sorted_digests.iter().enumerate() {
            let old_idx = *digest_to_local
                .get(digest)
                .expect("canonicalization: digest missing from digest_to_local");
            sorted_rank[old_idx] = new_idx;
        }

        index_to_digest = sorted_digests;

        for order in indices_sets.iter_mut() {
            for idx in order.iter_mut() {
                *idx = sorted_rank[*idx];
            }
        }
        
        for (vote_sub_dag_id, vote_author, vote_lo_digest) in found_fair_updates {
            self.pending_fair_updates
                .entry(vote_sub_dag_id)
                .or_default()
                .insert(vote_author, vote_lo_digest);
        }

        // Check if any pending sub_dags now have quorum
        let quorum_threshold = (self.n - self.f) as usize;
        let ready_subdags: Vec<u64> = self.pending_fair_updates
            .iter()
            .filter(|(_, authors)| authors.len() >= quorum_threshold)
            .map(|(&id, _)| id)
            .collect();

        for ready_sub_dag_id in ready_subdags {
            self.process_fair_update_quorum(ready_sub_dag_id).await;
        }

        let sub_dag_id = self.sub_dag_count;
        self.sub_dag_count += 1;
        
        let index_ser = bincode::serialize(&index_to_digest).expect("Failed to serialize index_to_digest");
        self.store.write(Self::index_to_digest_key(sub_dag_id), index_ser).await;
        self.store.write(Self::subdag_state_key(sub_dag_id), vec![0]).await;
        
        if self.use_auncel {
            
            let auncel_weight_k = self.auncel_weight_k;
            let auncel_use_final_phase = self.auncel_use_final_phase;
            let non_blank_threshold = self.non_blank_threshold as usize;
            let tx_auncel_results = self.tx_auncel_results.clone();
            
            let _handler = tokio_rayon::spawn(move || {
                run_auncel_order(
                    sub_dag_id,
                    indices_sets,
                    k,
                    non_blank_threshold,
                    auncel_weight_k,
                    auncel_use_final_phase,
                    tx_auncel_results,
                );
            });
            
            log::info!(
                "process_subdag: spawned AUNCEL ordering (k={}, weight_k={}, final_phase={}) in {}ns",
                k, auncel_weight_k, auncel_use_final_phase, start_time.elapsed().as_nanos()
            );
        } else {
            
            let non_blank = self.non_blank_threshold;
            let solid = self.solid_threshold;
            let tx_utig_results = self.tx_utig_results.clone();
            
            let _handler = tokio_rayon::spawn(move || {
                run_utig(sub_dag_id, indices_sets, k, non_blank as u8, solid as u8, tx_utig_results);
            });

            log::info!(
                "process_subdag: spawned UTIG ordering in {}ns",
                start_time.elapsed().as_nanos()
            );
        }

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

    async fn process_fair_update_quorum(&mut self, sub_dag_id: u64) {
        let votes = match self.pending_fair_updates.remove(&sub_dag_id) {
            Some(v) => v,
            None => return,
        };
        
        log::info!(
            "process_fair_update_quorum: sub_dag_id={}, {} votes",
            sub_dag_id, votes.len()
        );
        
        // Send cleanup signal to LocalOrderMaker
        let _ = self.tx_fair_propose
            .send((sub_dag_id, vec![], vec![], vec![]))
            .await;
        
        self.pending_subdags_fair.remove(&sub_dag_id);
        
        // Fetch edges from each LocalOrder
        let mut author_edges_map: HashMap<PublicKey, Vec<u32>> = HashMap::new();
        
        for (author, lo_digest) in votes {
            let edges = match self.extract_fair_update_edges(&lo_digest, sub_dag_id).await {
                Some(e) => e,
                None => {
                    log::warn!(
                        "Failed to extract edges for sub_dag_id={} from author={:?}",
                        sub_dag_id, author
                    );
                    continue;
                }
            };
            
            if !edges.is_empty() {
                author_edges_map.insert(author, edges);
            }
        }
        
        let quorum_threshold = (self.n - self.f) as usize;
        if author_edges_map.len() < quorum_threshold {
            log::warn!(
                "sub_dag_id={}: only {} valid votes after extraction (need {})",
                sub_dag_id, author_edges_map.len(), quorum_threshold
            );
            return;
        }
        
        // Load region_b data and spawn FairUpdate
        let combined_data = match self.store.read(Self::region_data_key(sub_dag_id)).await {
            Ok(Some(data)) => data,
            _ => {
                log::error!("sub_dag_id={}: region_data not found", sub_dag_id);
                return;
            }
        };
        
        let tx_utig_results = self.tx_utig_results.clone();
        let n = self.n;
        let f = self.f;
        let gamma = self.gamma;
        
        log::info!(
            "sub_dag_id={}: spawning FairUpdate with {} author votes",
            sub_dag_id, author_edges_map.len()
        );
        
        let _handler = tokio_rayon::spawn(move || {
            apply_fair_update_and_finalize(
                combined_data,
                sub_dag_id,
                author_edges_map,
                n,
                f,
                gamma,
                tx_utig_results,
            );
        });
    }

    /// Extract FairUpdate edges for a specific sub_dag_id from a LocalOrder
    async fn extract_fair_update_edges(&mut self, lo_digest: &Digest, target_sub_dag_id: u64) -> Option<Vec<u32>> {
        let serialized = self.store.read(lo_digest.to_vec()).await.ok()??;
        
        let local_order: Vec<Vec<u8>> = match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(_, batch)) => batch,
            _ => return None,
        };
        
        let mut tx_idx = 1; // Skip sequence number
        while tx_idx < local_order.len() {
            let tx_digest = &local_order[tx_idx];
            
            // Check for sentinel
            if tx_digest.len() == 32 && tx_digest.iter().all(|&b| b == 0xFF) {
                // Need at least: sentinel + sub_dag_id + edge_count + compressed_blob
                if tx_idx + 3 >= local_order.len() {
                    break;
                }
                
                // Read sub_dag_id
                let sub_dag_id_bytes = &local_order[tx_idx + 1];
                if sub_dag_id_bytes.len() != 8 {
                    tx_idx += 4;
                    continue;
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(sub_dag_id_bytes);
                let sub_dag_id = u64::from_le_bytes(arr);
                
                // Read edge count
                let edge_count_bytes = &local_order[tx_idx + 2];
                if edge_count_bytes.len() != 8 {
                    tx_idx += 4;
                    continue;
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(edge_count_bytes);
                let edge_count = u64::from_le_bytes(arr) as usize;
                
                // If this is the target sub_dag_id, decompress and extract edges
                if sub_dag_id == target_sub_dag_id {
                    let compressed_blob = &local_order[tx_idx + 3];
                    
                    log::info!(
                        "extract_fair_update_edges: sub_dag_id={}, edge_count={}, compressed_size={}",
                        sub_dag_id, edge_count, compressed_blob.len()
                    );
                    
                    let edges = unpack_and_decompress_edges(compressed_blob, edge_count);
                    
                    if edges.len() != edge_count {
                        log::warn!(
                            "Edge count mismatch: expected {}, got {}",
                            edge_count, edges.len()
                        );
                    }
                    
                    return Some(edges);
                }
                
                // Skip past this sentinel (4 entries total)
                tx_idx += 4;
            } else {
                tx_idx += 1;
            }
        }
        
        None
    }

    async fn try_finalize_sequential(&mut self) {
        loop {
            let sub_dag_id = self.next_to_finalize;
            
            // Check if this sub-dag exists and is Ready
            let state_data = match self.store.read(Self::subdag_state_key(sub_dag_id)).await {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => {
                    log::debug!(
                        "try_finalize_sequential: sub_dag_id={} not found, stopping",
                        sub_dag_id
                    );
                    break;
                }
            };
            
            // State: 0=Processing, 1=WaitingForUpdates, 2=Ready, 3=Finalized
            let state = state_data[0];
            
            if state != 2 {
                log::debug!(
                    "try_finalize_sequential: sub_dag_id={} not ready (state={}), stopping",
                    sub_dag_id, state
                );
                break;
            }
            
            log::info!(
                "try_finalize_sequential: finalizing sub_dag_id={} (next_to_finalize={})",
                sub_dag_id, self.next_to_finalize
            );
            
            // Load index_to_digest from disk
            let index_to_digest: Vec<Vec<u8>> = match self.store.read(Self::index_to_digest_key(sub_dag_id)).await {
                Ok(Some(data)) => bincode::deserialize(&data).expect("deserialize failed"),
                _ => {
                    log::error!("sub_dag_id={}: index_to_digest not found!", sub_dag_id);
                    break;
                }
            };
            // Load finalized SCC batches from disk.
            // Each inner Vec is the (sorted) transactions of one SCC, in execution order.
            let finalized_sccs: Vec<Vec<usize>> = match self.store.read(Self::finalized_indices_key(sub_dag_id)).await {
                Ok(Some(data)) => bincode::deserialize(&data).expect("deserialize failed"),
                _ => {
                    log::error!("sub_dag_id={}: finalized_indices not found!", sub_dag_id);
                    break;
                }
            };

            // Map indices to digests, filtering already_finalized. While doing so, log SCC.
            let mut executed: Vec<(u64, Digest)> = Vec::new();
            let mut skipped_count = 0;

            for (scc_pos, group) in finalized_sccs.iter().enumerate() {
                let scc_label: u64 = if self.use_auncel { sub_dag_id } else { scc_pos as u64 };

                for idx in group {
                    if *idx >= index_to_digest.len() {
                        log::warn!(
                            "sub_dag_id={}: index {} out of bounds (len={})",
                            sub_dag_id, idx, index_to_digest.len()
                        );
                        continue;
                    }

                    let digest_bytes = &index_to_digest[*idx];
                    if digest_bytes.len() != 32 {
                        log::warn!(
                            "sub_dag_id={}: invalid digest length {} at index {}",
                            sub_dag_id, digest_bytes.len(), idx
                        );
                        continue;
                    }

                    let arr: [u8; 32] = digest_bytes.clone().try_into().unwrap();
                    let digest = Digest(arr);

                    if self.already_finalized.contains(&digest) {
                        skipped_count += 1;
                    } else {
                        self.already_finalized.insert(digest.clone());
                        executed.push((scc_label, digest));
                    }
                }
            }

            log::info!(
                "sub_dag_id={}: FINALIZED! {} transactions ({} duplicates skipped)",
                sub_dag_id,
                executed.len(),
                skipped_count
            );

            for (scc_label, digest) in executed.iter() {
                log::info!("sub_dag_id={}: scc={}: Executed {:?}", sub_dag_id, scc_label, digest);
            }
            
            // Mark as Finalized (3)
            self.store.write(Self::subdag_state_key(sub_dag_id), vec![3]).await;
            self.finalized_subdags.insert(sub_dag_id);
            
            // Send cleanup signal to LocalOrderMaker
            let _ = self.tx_fair_propose
                .send((sub_dag_id, vec![], vec![], vec![]))
                .await;
            
            // Move to next sub-dag
            self.next_to_finalize += 1;
            
            log::info!(
                "sub_dag_id={}: marked Finalized, next_to_finalize={}",
                sub_dag_id, self.next_to_finalize
            );
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
                
                Some((sub_dag_id, finalized_now, region_b_v, region_b_e, missing_edge_vertices, missing_edges)) = self.rx_utig_results.recv() => {
                    let finalized_tx_count: usize = finalized_now.iter().map(|g| g.len()).sum();

                    log::info!(
                        "rx_utig_results: sub_dag_id={}, finalized_txs={}, finalized_sccs={}, region_b_v={}, region_b_e={}, missing_edges={}",
                        sub_dag_id, finalized_tx_count, finalized_now.len(), region_b_v.len(), region_b_e.len(), missing_edges.len()
                    );

                    if !finalized_now.is_empty() && region_b_v.is_empty() && region_b_e.is_empty() && missing_edges.is_empty() {
                        log::info!(
                            "sub_dag_id={}: FairUpdate+Finalize completed, {} txs ({} sccs)",
                            sub_dag_id, finalized_tx_count, finalized_now.len()
                        );
                        
                        let state = self.store.read(Self::subdag_state_key(sub_dag_id)).await
                            .ok().flatten().and_then(|v| v.get(0).copied()).unwrap_or(0);

                        if state == 1 {
                            let mut full: Vec<Vec<usize>> = Vec::new();

                            if let Ok(Some(bytes)) = self.store.read(Self::finalized_indices_key(sub_dag_id)).await {
                                let prefix: Vec<Vec<usize>> = bincode::deserialize(&bytes).expect("deserialize prefix failed");
                                full.extend(prefix);
                            } else {
                                log::error!("sub_dag_id={}: state=1 but missing prefix on disk", sub_dag_id);
                            }

                            full.extend(finalized_now);

                            self.store.write(Self::finalized_indices_key(sub_dag_id), bincode::serialize(&full).unwrap()).await;
                        } else {
                            self.store.write(Self::finalized_indices_key(sub_dag_id), bincode::serialize(&finalized_now).unwrap()).await;
                        }

                        self.store.write(Self::subdag_state_key(sub_dag_id), vec![2]).await;
                        
                        self.pending_subdags_fair.remove(&sub_dag_id);
                        
                        self.try_finalize_sequential().await;
                        continue;
                    }

                    // This is from run_utig - has region_b data
                    
                    // Load index_to_digest from DISK (not RAM)
                    let index_to_digest: Vec<Vec<u8>> = bincode::deserialize(
                        &self.store.read(Self::index_to_digest_key(sub_dag_id))
                            .await
                            .expect("Store read failed")
                            .expect("index_to_digest not found")
                    ).expect("deserialize failed");

                    // Store finalized solid prefix (can be finalized when turn comes)
                    let solid_ser = bincode::serialize(&finalized_now).expect("serialize failed");
                    self.store.write(Self::finalized_indices_key(sub_dag_id), solid_ser).await;

                    // Store region_b data to disk (for FairUpdate later if needed)
                    let mut combined_data: Vec<u8> = Vec::new();
                    let region_b_v_ser = bincode::serialize(&region_b_v).expect("serialize");
                    combined_data.extend_from_slice(&(region_b_v_ser.len() as u64).to_le_bytes());
                    combined_data.extend_from_slice(&region_b_v_ser);
                    combined_data.extend_from_slice(&[0xFF; 8]);
                    let region_b_e_ser = bincode::serialize(&region_b_e).expect("serialize");
                    combined_data.extend_from_slice(&(region_b_e_ser.len() as u64).to_le_bytes());
                    combined_data.extend_from_slice(&region_b_e_ser);
                    combined_data.extend_from_slice(&[0xFF; 8]);
                    // Also store index_to_digest in combined_data for FairUpdate
                    let index_ser = bincode::serialize(&index_to_digest).expect("serialize");
                    combined_data.extend_from_slice(&(index_ser.len() as u64).to_le_bytes());
                    combined_data.extend_from_slice(&index_ser);
                    self.store.write(Self::region_data_key(sub_dag_id), combined_data).await;

                    log::info!(
                        "sub_dag_id={}: stored to disk, finalized_solid_txs={}, finalized_solid_sccs={}, region_b_v={}, missing_edges={}",
                        sub_dag_id, finalized_tx_count, finalized_now.len(), region_b_v.len(), missing_edges.len()
                    );

                    // Has missing edges - need FairUpdate votes
                    self.store.write(Self::subdag_state_key(sub_dag_id), vec![1]).await;
                    
                    // Build missing_tx_digests for LocalOrderMaker
                    let mut missing_tx_digests: Vec<Digest> = Vec::with_capacity(missing_edge_vertices.len());
                    for &vid in &missing_edge_vertices {
                        let idx = vid as usize;
                        if idx >= index_to_digest.len() {
                            panic!("missing edge vertex idx {} out of bounds", idx);
                        }
                        let arr: [u8; 32] = index_to_digest[idx].clone().try_into().unwrap();
                        missing_tx_digests.push(Digest(arr));
                    }

                    log::info!(
                        "sub_dag_id={}: {} missing edges, sent to LocalOrderMaker for voting",
                        sub_dag_id, missing_edges.len()
                    );
                    
                    self.pending_subdags_fair.insert(sub_dag_id);
                    let _ = self.tx_fair_propose
                        .send((sub_dag_id, missing_edge_vertices, missing_tx_digests, missing_edges))
                        .await;
                    
                },
                Some((sub_dag_id, finalized_sccs)) = self.rx_auncel_results.recv() => {
                    let finalized_tx_count: usize = finalized_sccs.iter().map(|g| g.len()).sum();

                    log::info!(
                        "rx_auncel_results: sub_dag_id={}, finalized={} txs (single-round)",
                        sub_dag_id, finalized_tx_count
                    );

                    let finalized_ser = bincode::serialize(&finalized_sccs).expect("serialize failed");
                    self.store.write(Self::finalized_indices_key(sub_dag_id), finalized_ser).await;
                    
                    self.store.write(Self::subdag_state_key(sub_dag_id), vec![2]).await;
                    
                    self.try_finalize_sequential().await;
                },

            }
        }
    }
}

fn apply_fair_update_and_finalize(
    combined_data: Vec<u8>,
    sub_dag_id: u64,
    author_edges_map: HashMap<PublicKey, Vec<u32>>,
    n: u64,
    f: u64,
    gamma: f64,
    tx_utig_results: tokio::sync::mpsc::Sender<(u64, Vec<Vec<usize>>, Vec<u16>, Vec<(u16,u16)>, Vec<u16>, Vec<u32>)>,
) {
    let start_time = Instant::now();
    
    // Deserialize region_b data
    let mut offset = 0;
    let mut len_bytes = [0u8; 8];
    
    len_bytes.copy_from_slice(&combined_data[offset..offset + 8]);
    offset += 8;
    let region_b_v_len = u64::from_le_bytes(len_bytes) as usize;
    let region_b_v: Vec<u16> = bincode::deserialize(&combined_data[offset..offset + region_b_v_len]).unwrap();
    offset += region_b_v_len + 8;
    
    len_bytes.copy_from_slice(&combined_data[offset..offset + 8]);
    offset += 8;
    let region_b_e_len = u64::from_le_bytes(len_bytes) as usize;
    let mut region_b_e: Vec<(u16, u16)> = bincode::deserialize(&combined_data[offset..offset + region_b_e_len]).unwrap();
    
    log::info!(
        "apply_fair_update: sub_dag_id={}, region_b_v={}, region_b_e={}, authors={}",
        sub_dag_id, region_b_v.len(), region_b_e.len(), author_edges_map.len()
    );
    
    if region_b_v.is_empty() {
        log::info!("apply_fair_update: sub_dag_id={}, empty region_b", sub_dag_id);
        return;
    }
    
    // Thresholds per paper
    let non_blank_threshold = ((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0).floor() as u16;
    let solid_threshold = (n - 2 * f) as u16;  // n-2f for "tx ∈_{n-2f} L_updates"
    
    let k = region_b_v.iter().map(|&v| v as usize + 1).max().unwrap_or(0);
    
    let mut existing_edges: HashSet<(u16, u16)> = region_b_e.iter().cloned().collect();
    
    // weight[from][to] = number of votes for directed edge from -> to
    let mut weight: Vec<Vec<u16>> = vec![vec![0; k]; k];
    
    // Track how many authors voted on edges involving each tx
    let mut tx_author_count: Vec<HashSet<usize>> = vec![HashSet::new(); k];
    
    for (author_idx, (_author, edges_vec)) in author_edges_map.iter().enumerate() {
        for &directed_edge in edges_vec {
            let from = (directed_edge >> 16) as u16;
            let to = (directed_edge & 0xFFFF) as u16;
            
            if (from as usize) < k && (to as usize) < k {
                weight[from as usize][to as usize] += 1;
                tx_author_count[from as usize].insert(author_idx);
                tx_author_count[to as usize].insert(author_idx);
            }
        }
    }
    
    let mut new_edges_count = 0;
    
    for &u in &region_b_v {
        for &v in &region_b_v {
            if u >= v { continue; }
            
            // Skip if edge already exists
            if existing_edges.contains(&(u, v)) || existing_edges.contains(&(v, u)) {
                continue;
            }
            
            let kuv = weight[u as usize][v as usize];
            let kvu = weight[v as usize][u as usize];
            
            // Per FairUpdate: determine direction based on weight, then check source condition
            // "If tx ∈_{n-2f} L_updates, k ≥ k′ and k ≥ n(1−γ)+f+1 then add edge (tx, tx')"
            
            let u_in_enough = tx_author_count[u as usize].len() as u16 >= solid_threshold;
            let v_in_enough = tx_author_count[v as usize].len() as u16 >= solid_threshold;
            
            // Determine which direction has higher weight (k >= k' condition)
            // Then check if SOURCE of that direction is in enough updates
            if kuv >= kvu {
                // Direction would be u → v
                if u_in_enough && kuv >= non_blank_threshold {
                    region_b_e.push((u, v));
                    existing_edges.insert((u, v));
                    new_edges_count += 1;
                }
            } else {
                // Direction would be v → u
                if v_in_enough && kvu >= non_blank_threshold {
                    region_b_e.push((v, u));
                    existing_edges.insert((v, u));
                    new_edges_count += 1;
                }
            } 
        }
    }
    
    log::info!(
        "apply_fair_update: sub_dag_id={}, added {} new edges, total={}, elapsed={}ns",
        sub_dag_id, new_edges_count, region_b_e.len(), start_time.elapsed().as_nanos()
    );
    
    // FairFinalize: SCC + topo sort (rest unchanged)
    let mut edges: Vec<Vec<u16>> = vec![Vec::new(); k];
    for &(u, v) in &region_b_e {
        edges[u as usize].push(v);
    }
    
    let mut index_counter: i32 = 0;
    let mut stack: Vec<usize> = Vec::new();
    let mut on_stack: Vec<bool> = vec![false; k];
    let mut dfn: Vec<i32> = vec![0; k];
    let mut low: Vec<i32> = vec![0; k];
    let mut scc_id: Vec<i32> = vec![-1; k];
    let mut sccs: Vec<Vec<usize>> = Vec::new();
    
    fn strongconnect(
        u: usize, index_counter: &mut i32, stack: &mut Vec<usize>, on_stack: &mut [bool],
        dfn: &mut [i32], low: &mut [i32], edges: &[Vec<u16>], scc_id: &mut [i32], sccs: &mut Vec<Vec<usize>>,
    ) {
        *index_counter += 1;
        dfn[u] = *index_counter;
        low[u] = *index_counter;
        stack.push(u);
        on_stack[u] = true;
        for &v16 in &edges[u] {
            let v = v16 as usize;
            if dfn[v] == 0 {
                strongconnect(v, index_counter, stack, on_stack, dfn, low, edges, scc_id, sccs);
                if low[v] < low[u] { low[u] = low[v]; }
            } else if on_stack[v] && dfn[v] < low[u] {
                low[u] = dfn[v];
            }
        }
        if low[u] == dfn[u] {
            let mut comp = Vec::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack[w] = false;
                scc_id[w] = sccs.len() as i32;
                comp.push(w);
                if w == u { break; }
            }
            sccs.push(comp);
        }
    }
    
    for &v in &region_b_v {
        let u = v as usize;
        if dfn[u] == 0 {
            strongconnect(u, &mut index_counter, &mut stack, &mut on_stack, &mut dfn, &mut low, &edges, &mut scc_id, &mut sccs);
        }
    }
    
    let scc_n = sccs.len();
    let mut gc: Vec<Vec<usize>> = vec![Vec::new(); scc_n];
    let mut indegree: Vec<usize> = vec![0; scc_n];
    
    for &v in &region_b_v {
        let u = v as usize;
        let su = scc_id[u];
        if su < 0 { continue; }
        let su = su as usize;
        for &v16 in &edges[u] {
            let v = v16 as usize;
            let sv = scc_id[v];
            if sv < 0 || su == sv as usize { continue; }
            gc[su].push(sv as usize);
        }
    }
    
    for u in 0..scc_n {
        gc[u].sort_unstable();
        gc[u].dedup();
        for &v in &gc[u] {
            indegree[v] += 1;
        }
    }
    
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
            indegree[v] -= 1;
            if indegree[v] == 0 {
                q.push_back(v);
            }
        }
    }

    let mut finalized_now: Vec<Vec<usize>> = Vec::new();
    let mut finalized_tx_count: usize = 0;
    for &scc_idx in &topo {
        let mut group = sccs[scc_idx].clone();
        group.sort_unstable(); // TODO: Implement the Hamiltonian approach from paper
        finalized_tx_count += group.len();
        finalized_now.push(group);
    }

    log::info!(
        "apply_fair_finalize: sub_dag_id={}, finalized {} txs ({} sccs) in {}ns",
        sub_dag_id,
        finalized_tx_count,
        finalized_now.len(),
        start_time.elapsed().as_nanos()
    );

    let _ = tx_utig_results.blocking_send((sub_dag_id, finalized_now, vec![], vec![], vec![], vec![]));

}

pub fn run_utig(
    sub_dag_id: u64,
    indices_sets: Vec<Vec<usize>>,
    k: usize,
    non_blank_threshold: u8,
    solid_threshold: u8,
    tx_utig_results: tokio::sync::mpsc::Sender<(u64, Vec<Vec<usize>>, Vec<u16>, Vec<(u16,u16)>, Vec<u16>, Vec<u32>)>,
) {

    let start_total = Instant::now();
    let mut last = start_total;

    if k == 0 || indices_sets.is_empty() {
        log::info!(": empty sub-dag (k=0 or no local orders), nothing to do");
        return;
    }

    let (slot_idx, matrix_ptr) = {
        let mut pool = UTIG_POOL
            .lock()
            .expect("_POOL mutex poisoned");

        let idx = pool
            .acquire_slot()
            .expect("MatrixPool exhausted: no free matrices");

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
        " t1: {}", t1
    );

    // ============================================================
    // (3) For each non-blank tx, add a vertex tx to V
    //     -> compute tx_count (support), non-blank set, solid set.
    // ============================================================
    for order in &indices_sets {
        for &tx in order {
            
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
        log::info!(": no non-blank txs in this sub-dag, nothing to propose");
        matrix.reset_utig(k);
        {
            let mut pool = UTIG_POOL
                .lock()
                .expect("_POOL mutex poisoned");
            pool.release_slot(slot_idx);
        }
        return;
    }

    let now = Instant::now();
    let t3 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        " t3: {}", t3
    );

    // ============================================================
    // (4) Add edges to E
    // ============================================================

    // Fill edge_count via local orders.
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
                inc_weight(weight, idx);
            }
        }
    }

    // Build the directed graph on non-blank txs.
    for &u in &active {
        for &v in &active {
            if u >= v { continue; }
            
            let kuv = get_weight(weight, w_idx(u, v, k));  // NIBBLE get
            let kvu = get_weight(weight, w_idx(v, u, k));  // NIBBLE get

            if kuv < non_blank_threshold && kvu < non_blank_threshold {
                continue;
            }

            let dir_uv =
                if kuv >= kvu { true }
                else {
                    false
                };

            if dir_uv { edges[u].push(v as u16); }
            else      { edges[v].push(u as u16); }
        }
    }

    let now = Instant::now();
    let t4 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        " t4: {}", t4
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
        log::info!(": SCC decomposition empty, nothing to propose");
        matrix.reset_utig(k);
        {
            let mut pool = UTIG_POOL
                .lock()
                .expect("_POOL mutex poisoned");
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
        " t5: {}", t5
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
        matrix.reset_utig(k);
        {
            let mut pool = UTIG_POOL
                .lock()
                .expect("_POOL mutex poisoned");
            pool.release_slot(slot_idx);
        }
        let total = start_total.elapsed().as_nanos();
        log::info!(
            ": no solid anchor in this sub-dag, total ns = {}",
            total
        );
        return;
    }

    let anchor = anchor_idx.unwrap();

    let now = Instant::now();
    let t6 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        " t6: {}", t6
    );

    // ============================================================
    // OPTIMIZATION: "As an optimization, transactions whose SCC as well as all
    //   SCCs that precede it contains only solid transactions can also
    //   be finalized immediately"~\cite{Themis}
    // ============================================================
    let mut start_b: usize = anchor + 1;
    for topo_pos in 0..=anchor {
        let comp = &sccs[topo[topo_pos]];
        if comp.iter().any(|&tx| !is_solid[tx]) {
            start_b = topo_pos;
            break;
        }
    }

    let mut finalized_now: Vec<Vec<usize>> = Vec::new();
    for topo_pos in 0..start_b {
        let mut group = sccs[topo[topo_pos]].clone();
        group.sort_unstable(); // TODO: Implement the Hamiltonian approach from paper
        finalized_now.push(group);
    }

    // ============================================================
    // (7) Remove txs that are part of SCCs after V in S
    //     (in our case: build the final ordered prefix of tx indices)
    // ============================================================

    let mut region_b_local: Vec<usize> = Vec::new();
    let mut region_b_sccs: Vec<Vec<usize>> = Vec::new();
    if start_b <= anchor {
        for topo_pos in start_b..=anchor {
            let mut group = sccs[topo[topo_pos]].clone();
            group.sort_unstable(); // TODO: Implement the Hamiltonian approach from paper
            region_b_local.extend(group.iter().copied());
            region_b_sccs.push(group);
        }
    }

    let mut in_b: Vec<bool> = vec![false; k];
    for &u in &region_b_local {
        in_b[u] = true;
    }

    #[inline]
    fn pair_key(u: usize, v: usize) -> u32 {
        let (a, b) = if u < v { (u as u16, v as u16) } else { (v as u16, u as u16) };
        ((a as u32) << 16) | (b as u32)
    }

    // Only shaded vertices in Region B can be missing edges (paper property).
    let kept_shaded: Vec<usize> = region_b_local
        .iter()
        .copied()
        .filter(|&u| !is_solid[u])
        .collect();

    let mut missing_edges: Vec<u32> = Vec::new();
    for i in 0..kept_shaded.len() {
        let u = kept_shaded[i];
        for j in (i + 1)..kept_shaded.len() {
            let v = kept_shaded[j];
            
            let kuv = get_weight(weight, w_idx(u, v, k));  // NIBBLE get
            let kvu = get_weight(weight, w_idx(v, u, k));  // NIBBLE get

            if kuv < non_blank_threshold && kvu < non_blank_threshold {
                missing_edges.push(pair_key(u, v));
            }
        }
    }

    let mut missing_edge_vertices: Vec<u16> = Vec::with_capacity(missing_edges.len() * 2);

    for &key in &missing_edges {
        let u = (key >> 16) as u16;
        let v = (key & 0xFFFF) as u16;
        missing_edge_vertices.push(u);
        missing_edge_vertices.push(v);
    }

    missing_edge_vertices.sort_unstable();
    missing_edge_vertices.dedup();

    let now = Instant::now();
    let t7 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        "t7: {}", t7
    );

    let region_b_len = region_b_local.len();
    let shaded_len = kept_shaded.len();
    if missing_edges.is_empty() && !region_b_local.is_empty() {
        finalized_now.extend(region_b_sccs);
        region_b_local.clear();
    }
    let finalized_tx_count: usize = finalized_now.iter().map(|g| g.len()).sum();
    let region_b_v: Vec<u16> = region_b_local.iter().map(|&u| u as u16).collect();
    let mut region_b_e: Vec<(u16, u16)> = Vec::new();
    for &u in &region_b_local {
        let u16 = u as u16;
        for &v16 in &edges[u] {
            let v = v16 as usize;
            if v < k && in_b[v] {
                region_b_e.push((u16, v16));
            }
        }
    }

    log::info!(
        "finalized prefix txs = {}, finalized_sccs = {}, solid_nodes = {}, shaded_nodes = {}, missing_edges = {}, anchor_scc_idx = {}, total ns = {}",
        finalized_tx_count,
        finalized_now.len(),
        region_b_len - shaded_len,
        shaded_len,
        missing_edges.len(),
        anchor,
        start_total.elapsed().as_nanos()
    );

    // ============================================================
    // (8) Output result: local tx indices to finalize
    // ============================================================

    let _ = tx_utig_results.blocking_send((sub_dag_id, finalized_now, region_b_v, region_b_e, missing_edge_vertices, missing_edges));

    matrix.reset_utig(k);

    {
        let mut pool = UTIG_POOL
            .lock()
            .expect("_POOL mutex poisoned");
        pool.release_slot(slot_idx);
    }

    let now = Instant::now();
    let t8 = now.duration_since(last).as_nanos();
    last = now;
    log::info!(
        " t8: {}", t8
    );

}

pub fn run_auncel_order(
    sub_dag_id: u64,
    indices_sets: Vec<Vec<usize>>,
    k: usize,
    non_blank_threshold: usize,
    weight_k: f64,
    use_final_order_phase: bool,
    tx_results: tokio::sync::mpsc::Sender<(u64, Vec<Vec<usize>>)>,
) {
    let start_time = Instant::now();

    if k == 0 || indices_sets.is_empty() {
        log::info!("run_auncel_order: sub_dag_id={}, empty input", sub_dag_id);
        let _ = tx_results.blocking_send((sub_dag_id, vec![]));
        return;
    }

    let num_orders = indices_sets.len();

    // Validate limits
    if k > MAX_TX {
        log::error!("run_auncel_order: k={} exceeds MAX_TX={}", k, MAX_TX);
        let _ = tx_results.blocking_send((sub_dag_id, vec![]));
        return;
    }
    if num_orders > MAX_ORDERS {
        log::error!("run_auncel_order: num_orders={} exceeds MAX_ORDERS={}", num_orders, MAX_ORDERS);
        let _ = tx_results.blocking_send((sub_dag_id, vec![]));
        return;
    }

    // ===== Acquire matrix from pool =====
    let (slot_idx, matrix_ptr) = {
        let mut pool = UTIG_POOL.lock().expect("UTIG_POOL mutex poisoned");
        let idx = pool.acquire_slot().expect("MatrixPool exhausted");
        let matrix_ptr: *mut UTIGMatrix = &mut pool.pool[idx];
        (idx, matrix_ptr)
    };

    let matrix: &mut UTIGMatrix = unsafe { &mut *matrix_ptr };

    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 1: Weight Order Phase
    // ═══════════════════════════════════════════════════════════════════════

    for (order_idx, order) in indices_sets.iter().enumerate() {
        for (pos, &tx_idx) in order.iter().enumerate() {
            // Record position
            matrix.set_position(tx_idx, order_idx, num_orders, pos);

            // Compute weight: W = 1 - k^d where d is 1-indexed position
            let d = (pos + 1) as f64;
            let w = 1.0 - weight_k.powf(d);
            matrix.weight_sum[tx_idx] += w;

            matrix.support[tx_idx] = matrix.support[tx_idx].saturating_add(1);
        }
    }

    let t1 = start_time.elapsed().as_nanos();

    // Filter to non-blank transactions
    let mut non_blank: Vec<usize> = (0..k)
        .filter(|&i| matrix.support[i] as usize >= non_blank_threshold)
        .collect();

    if non_blank.is_empty() {
        log::info!("run_auncel_order: sub_dag_id={}, no non-blank txs", sub_dag_id);

        let _ = tx_results.blocking_send((sub_dag_id, vec![]));

        matrix.reset_auncel(k, num_orders);

        {
            let mut pool = UTIG_POOL.lock().expect("UTIG_POOL mutex poisoned");
            pool.release_slot(slot_idx);
        }

        return;
    }

    // Sort by weight sum ASCENDING (lower weight = appeared earlier = comes first)
    non_blank.sort_by(|&a, &b| {
        matrix.weight_sum[a]
            .partial_cmp(&matrix.weight_sum[b])
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.cmp(&b))
    });

    let t2 = start_time.elapsed().as_nanos();
    log::info!(
        "run_auncel_order: sub_dag_id={}, Phase 1 done: {} non-blank txs (of {}), time={}ns",
        sub_dag_id, non_blank.len(), k, t2
    );

    // ═══════════════════════════════════════════════════════════════════════
    // PHASE 2: Final Order Phase (Recursive) - Optional
    // ═══════════════════════════════════════════════════════════════════════

    let final_order = if use_final_order_phase {
        auncel_final_order_phase_pooled(&non_blank, matrix, num_orders)
    } else {
        non_blank
    };

    let t3 = start_time.elapsed().as_nanos();
    log::info!(
        "run_auncel_order: sub_dag_id={}, complete: {} txs, total={}ns",
        sub_dag_id, final_order.len(), t3
    );


    let _ = tx_results.blocking_send((sub_dag_id, vec![final_order]));

    matrix.reset_auncel(k, num_orders);
    // ===== Release slot =====
    {
        let mut pool = UTIG_POOL.lock().expect("UTIG_POOL mutex poisoned");
        pool.release_slot(slot_idx);
    }
}

/// Phase 2 using pooled matrix for position lookups
fn auncel_final_order_phase_pooled(
    txs: &[usize],
    matrix: &UTIGMatrix,
    num_orders: usize,
) -> Vec<usize> {
    if txs.is_empty() {
        return vec![];
    }
    if txs.len() == 1 {
        return txs.to_vec();
    }

    let mid_idx = txs.len() / 2;
    let medium_tx = txs[mid_idx];

    let mut pre_sequence: Vec<usize> = Vec::new();
    let mut post_sequence: Vec<usize> = Vec::new();

    for (i, &tx) in txs.iter().enumerate() {
        if i == mid_idx {
            continue;
        }

        let (before_count, after_count) = auncel_compare_pair_pooled(
            tx,
            medium_tx,
            matrix,
            num_orders,
        );

        if before_count > after_count {
            pre_sequence.push(tx);
        } else if after_count > before_count {
            post_sequence.push(tx);
        } else {
            if tx < medium_tx {
                pre_sequence.push(tx);
            } else {
                post_sequence.push(tx);
            }
        }
    }

    let mut result = auncel_final_order_phase_pooled(&pre_sequence, matrix, num_orders);
    result.push(medium_tx);
    result.extend(auncel_final_order_phase_pooled(&post_sequence, matrix, num_orders));

    result
}

#[inline]
fn auncel_compare_pair_pooled(
    tx_a: usize,
    tx_b: usize,
    matrix: &UTIGMatrix,
    num_orders: usize,
) -> (usize, usize) {
    let mut a_before_b = 0usize;
    let mut b_before_a = 0usize;

    let base_a = tx_a * num_orders;
    let base_b = tx_b * num_orders;

    for order_idx in 0..num_orders {
        let pos_a = matrix.positions[base_a + order_idx];
        let pos_b = matrix.positions[base_b + order_idx];

        // u16::MAX means "not present"
        if pos_a != u16::MAX && pos_b != u16::MAX {
            if pos_a < pos_b {
                a_before_b += 1;
            } else if pos_b < pos_a {
                b_before_a += 1;
            }
        }
    }

    (a_before_b, b_before_a)
}