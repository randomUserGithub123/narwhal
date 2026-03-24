// Copyright(C) FairDAG-RL Implementation.
//
// LocalOrderTracker: Tracks the local arrival order of transactions at this replica.
//
// In FairDAG-RL, each replica's local ordering reflects the order in which it
// FIRST observed each transaction — regardless of whether it arrived directly
// from a client or indirectly via another worker's batch.
//
// This struct is shared (via Arc) between:
//   - BatchMaker: records client transactions, retrieves OI for sealing batches
//   - WorkerReceiverHandler: records transactions arriving in other workers' batches
//   - MissingEdgeTracker: queries OIs for explicit edge contributions
//
// Uses std::sync::Mutex (not tokio::sync::Mutex) because the critical section
// is just a HashMap lookup + counter increment — no async work inside the lock.

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};

pub type TxDigest = u64;

/// Tracks the local arrival order of transactions at this replica.
#[derive(Clone)]
pub struct LocalOrderTracker {
    inner: Arc<Mutex<TrackerInner>>,
}

struct TrackerInner {
    /// Monotonically increasing counter — the next OI to assign.
    counter: u64,
    /// Map from tx_digest → ordering_indicator (assigned on first arrival).
    seen: HashMap<TxDigest, u64>,
}

impl LocalOrderTracker {
    pub fn new() -> Self {
        LocalOrderTracker {
            inner: Arc::new(Mutex::new(TrackerInner {
                counter: 0,
                seen: HashMap::new(),
            })),
        }
    }

    /// Record that a transaction was observed. If this is the first time we see
    /// this tx, assign the next ordering indicator. Returns the OI (whether new
    /// or previously assigned).
    pub fn record(&self, tx_digest: TxDigest) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        if let Some(&oi) = inner.seen.get(&tx_digest) {
            oi
        } else {
            inner.counter += 1;
            let oi = inner.counter;
            inner.seen.insert(tx_digest, oi);
            oi
        }
    }

    /// Check if a transaction has been seen and return its OI if so.
    pub fn get_oi(&self, tx_digest: TxDigest) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner.seen.get(&tx_digest).copied()
    }

    /// Current counter value (for debugging/logging).
    pub fn current_counter(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.counter
    }

    // =========================================================================
    // Explicit-edge-update helpers
    // =========================================================================

    /// Check if ALL of the given tx digests have been seen locally.
    /// Returns true only if every digest has an assigned OI.
    pub fn has_all(&self, digests: &[TxDigest]) -> bool {
        let inner = self.inner.lock().unwrap();
        digests.iter().all(|d| inner.seen.contains_key(d))
    }

    /// Collect OIs for a set of tx digests. Returns None for any unseen digest.
    /// Use after `has_all()` returns true to bulk-export the OIs.
    pub fn get_ois_bulk(&self, digests: &[TxDigest]) -> Vec<(TxDigest, Option<u64>)> {
        let inner = self.inner.lock().unwrap();
        digests
            .iter()
            .map(|&d| (d, inner.seen.get(&d).copied()))
            .collect()
    }

    /// Collect OIs for a set of tx digests, returning only those that are present.
    /// Panics if any digest is missing — call only after `has_all()` confirms.
    pub fn get_ois_bulk_unwrap(&self, digests: &[TxDigest]) -> Vec<(TxDigest, u64)> {
        let inner = self.inner.lock().unwrap();
        digests
            .iter()
            .map(|&d| {
                let oi = inner
                    .seen
                    .get(&d)
                    .copied()
                    .expect("get_ois_bulk_unwrap called with unseen digest");
                (d, oi)
            })
            .collect()
    }
}

/// Extract a u64 transaction digest from raw transaction bytes.
pub fn extract_tx_digest(tx: &[u8]) -> TxDigest {
    if tx.len() > 8 {
        u64::from_be_bytes(tx[1..9].try_into().unwrap_or([0u8; 8]))
    } else {
        let mut hash: u64 = 0;
        for (i, &byte) in tx.iter().enumerate() {
            hash ^= (byte as u64) << ((i % 8) * 8);
        }
        hash
    }
}