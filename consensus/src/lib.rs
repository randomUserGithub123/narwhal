// Copyright(C) Facebook, Inc. and its affiliates.
// Modified for FairDAG-RL (v2 — simplified design).
//
// KEY INSIGHT: Ordering indicators travel inside the batch data through the
// normal Narwhal pipeline. After Tusk commits a leader, we:
//   1. Collect all certificates in the committed subdag
//   2. For each certificate, look up its batch digests (header.payload)
//   3. Read each batch from the store
//   4. Deserialize the batch to extract (tx_bytes, ordering_indicator) pairs
//   5. Feed the reconstructed local orderings to the FairnessLayer
//
// This means NO changes to Header, Proposer, PrimaryWorkerMessage, or primary.rs.
// The only changes outside this file are:
//   - BatchMaker: assigns OI and stores (tx, OI) in batch
//   - WorkerMessage::Batch type: Vec<(Transaction, u64)> instead of Vec<Transaction>

use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use fairdag_fairness::{
    CommittedSubdag, CommittedVertex, FairnessLayer, TxDigest,
};
use log::{debug, error, info, log_enabled, warn};
use primary::{Certificate, Round};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use std::convert::TryInto;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// Batch entry type — must match worker::batch_maker::BatchEntry.
type Transaction = Vec<u8>;
type BatchEntry = (Transaction, u64); // (raw_tx, ordering_indicator)
type Batch = Vec<BatchEntry>;

/// WorkerMessage — must match worker::worker::WorkerMessage for deserialization.
/// We only need the Batch variant for reading from store.
#[derive(Debug, Serialize, Deserialize)]
enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, PublicKey),
}

/// The state that needs to be persisted for crash-recovery.
struct State {
    last_committed_round: Round,
    last_committed: HashMap<PublicKey, Round>,
    dag: Dag,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receives new certificates from the primary.
    rx_primary: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup).
    tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// NEW: Outputs fair-ordered transaction digests to the application layer.
    tx_fair_output: Sender<Vec<TxDigest>>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,

    /// Block height.
    block_height: u64,

    /// NEW: The FairDAG-RL fairness layer.
    fairness_layer: FairnessLayer,

    /// NEW: Sorted committee keys for replica indexing.
    sorted_keys: Vec<PublicKey>,

    /// NEW: Store access — needed to read batch contents after commit.
    store: Store,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        store: Store,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
        tx_fair_output: Sender<Vec<TxDigest>>,
    ) {
        let n = committee.size();
        let f = (n - 1) / 3;

        let mut sorted_keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        sorted_keys.sort();

        let fairness_layer = FairnessLayer::new(sorted_keys.clone(), f);

        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                rx_primary,
                tx_primary,
                tx_output,
                tx_fair_output,
                genesis: Certificate::genesis(&committee),
                block_height: 0,
                fairness_layer,
                sorted_keys,
                store,
            }
            .run()
            .await;
        });
    }

    /// Extract the subdag A_r from committed certificates by reading their
    /// batches from the store and extracting (tx_digest, ordering_indicator) pairs.
    async fn extract_subdag(
        &self,
        leader_round: Round,
        committed_sequence: &[Certificate],
    ) -> CommittedSubdag {
        let mut vertices: Vec<CommittedVertex> = Vec::new();

        for cert in committed_sequence {
            let author = cert.origin();
            let replica_index = self
                .sorted_keys
                .iter()
                .position(|k| *k == author)
                .expect("Certificate author not in committee");

            // Collect ordering entries from ALL batches referenced by this certificate.
            let mut ordering_entries: Vec<(TxDigest, u64)> = Vec::new();

            for batch_digest in cert.header.payload.keys() {
                // Read the serialized batch from the store.
                match self.store.clone().read(batch_digest.to_vec()).await {
                    Ok(Some(serialized_batch)) => {
                        // Deserialize: this is a WorkerMessage::Batch(Vec<(tx_bytes, oi)>)
                        match bincode::deserialize::<WorkerMessage>(&serialized_batch) {
                            Ok(WorkerMessage::Batch(batch_entries)) => {
                                for (tx_bytes, oi) in batch_entries {
                                    // Extract tx digest: first 8 bytes as u64
                                    let tx_id = Self::extract_tx_digest(&tx_bytes);
                                    ordering_entries.push((tx_id, oi));
                                }
                            }
                            Ok(_) => {
                                warn!(
                                    "Unexpected message type in store for batch {:?}",
                                    batch_digest
                                );
                            }
                            Err(e) => {
                                error!(
                                    "Failed to deserialize batch {:?}: {}",
                                    batch_digest, e
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        warn!(
                            "Batch {:?} not found in store (may have been GC'd)",
                            batch_digest
                        );
                    }
                    Err(e) => {
                        error!("Store read error for batch {:?}: {}", batch_digest, e);
                    }
                }
            }

            if !ordering_entries.is_empty() {
                debug!(
                    "FairDAG: cert round={} author={:?} has {} ordering entries",
                    cert.round(),
                    author,
                    ordering_entries.len()
                );
            }

            vertices.push(CommittedVertex {
                replica: author,
                replica_index,
                round: cert.round(),
                ordering_entries,
            });
        }

        // Sort vertices by round (ascending)
        vertices.sort_by_key(|v| v.round);

        CommittedSubdag {
            leader_round,
            vertices,
        }
    }

    /// Extract a u64 transaction digest from raw transaction bytes.
    /// Convention: first 8 bytes are the tx unique identifier.
    fn extract_tx_digest(tx: &[u8]) -> TxDigest {
        if tx.len() >= 8 {
            u64::from_be_bytes(tx[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            let mut hash: u64 = 0;
            for (i, &byte) in tx.iter().enumerate() {
                hash ^= (byte as u64) << ((i % 8) * 8);
            }
            hash
        }
    }

    async fn run(&mut self) {
        let mut state = State::new(self.genesis.clone());

        while let Some(certificate) = self.rx_primary.recv().await {
            debug!("Processing {:?}", certificate);
            let round = certificate.round();

            state
                .dag
                .entry(round)
                .or_insert_with(HashMap::new)
                .insert(certificate.origin(), (certificate.digest(), certificate));

            let r = round - 1;

            if r % 2 != 0 || r < 4 {
                continue;
            }

            let leader_round = r - 2;
            if leader_round <= state.last_committed_round {
                continue;
            }
            let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            let stake: Stake = state
                .dag
                .get(&(r - 1))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(_, x)| x.header.parents.contains(&leader_digest))
                .map(|(_, x)| self.committee.stake(&x.origin()))
                .sum();

            if stake < self.committee.validity_threshold() {
                debug!("Leader {:?} does not have enough support", leader);
                continue;
            }

            debug!("Leader {:?} has enough support", leader);
            let mut sequence = Vec::new();
            for leader in self.order_leaders(leader, &state).iter().rev() {
                for x in self.order_dag(leader, &state) {
                    state.update(&x, self.gc_depth);
                    sequence.push(x);
                }
            }

            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // =================================================================
            // FairDAG-RL: Extract subdag and process through fairness layer
            // =================================================================
            if !sequence.is_empty() {
                let subdag = self.extract_subdag(leader_round, &sequence).await;
                let fair_ordered = self.fairness_layer.process_subdag(&subdag);

                if !fair_ordered.is_empty() {
                    info!(
                        "FairDAG: outputting {} fair-ordered transactions from leader round {}",
                        fair_ordered.len(),
                        leader_round
                    );
                    if let Err(e) = self.tx_fair_output.send(fair_ordered).await {
                        warn!("Failed to output fair-ordered transactions: {}", e);
                    }
                }
            }

            // Output the sequence in the right order (original Tusk output).
            for certificate in sequence {
                self.block_height += 1;

                #[cfg(not(feature = "benchmark"))]
                {
                    info!("Committed {}", certificate.header);
                    info!(
                        "FairDag Committed {} in height {}",
                        certificate.header.id, self.block_height
                    );
                }

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    info!("Committed {} -> {:?}", certificate.header, digest);
                    info!(
                        "FairDag Committed {} in height {}",
                        certificate.header.id, self.block_height
                    );
                }

                self.tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(certificate).await {
                    warn!("Failed to output certificate: {}", e);
                }
            }
        }
    }

    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for r in (state.last_committed_round + 2..=leader.round() - 2)
            .rev()
            .step_by(2)
        {
            let (_, prev_leader) = match self.leader(r, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            if self.linked(leader, prev_leader, &state.dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }
        to_commit
    }

    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = dag
                .get(&(r))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue,
                };

                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}