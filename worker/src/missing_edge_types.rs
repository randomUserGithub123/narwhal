// Copyright(C) FairDAG-RL Implementation.
//
// missing_edge_types.rs
//
// Shared types for the explicit missing edge update protocol.
//
// Flow:
//   1. FairnessLayer constructs graph G_r → finds missing edges (tx pairs
//      where neither weight(d1,d2) nor weight(d2,d1) reaches threshold).
//   2. FairnessLayer sends MissingEdgeRequest { graph_id, missing_tx_digests }
//      to BatchMaker via channel.
//   3. BatchMaker waits until it has local OIs for ALL txs referenced in a
//      request, then constructs a MissingEdgeUpdate containing its pairwise
//      orderings for those pairs.
//   4. The MissingEdgeUpdate is serialized, lz4-compressed, and attached to
//      the next batch (WorkerMessage::Batch now carries an optional compressed
//      update payload).
//   5. Upon consensus commit, FairDagProcessor extracts MissingEdgeUpdates
//      from batches. Once a graph accumulates n-f replica updates, it can
//      resolve all missing edges and become a tournament.
//   6. FairnessLayer notifies BatchMaker (via GraphResolved) so it stops
//      including updates for already-resolved graphs.

use serde::{Deserialize, Serialize};

pub type TxDigest = u64;
pub type GraphId = u64; // = leader_round that created the graph

/// Direction of a pairwise edge as seen by one replica.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EdgeDirection {
    /// d1 was received before d2 at this replica
    Forward,  // edge d1 → d2
    /// d2 was received before d1 at this replica
    Reverse,  // edge d2 → d1
    /// This replica has not seen both transactions (should not happen in practice
    /// if we wait for all txs, but included for robustness)
    Unknown,
}

/// A single pairwise ordering vote from one replica for a specific pair.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairwiseVote {
    pub d1: TxDigest,
    pub d2: TxDigest,      // canonical ordering: d1 < d2
    pub direction: EdgeDirection,
}

/// A replica's edge update for a specific graph.
/// This is what gets lz4-compressed and embedded in batches.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MissingEdgeUpdate {
    pub graph_id: GraphId,
    pub replica_index: usize,
    pub votes: Vec<PairwiseVote>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Messages: FairnessLayer → BatchMaker
// ─────────────────────────────────────────────────────────────────────────────

/// Sent from FairnessLayer to BatchMaker when a new graph has missing edges.
#[derive(Clone, Debug)]
pub struct MissingEdgeRequest {
    pub graph_id: GraphId,
    /// The set of transaction digests involved in missing edges.
    /// BatchMaker must wait until it has local OIs for ALL of these.
    pub missing_tx_digests: Vec<TxDigest>,
    /// The actual pairs that need resolution (canonical: d1 < d2).
    pub missing_pairs: Vec<(TxDigest, TxDigest)>,
}

/// Sent from FairnessLayer to BatchMaker when a graph becomes a tournament
/// (or is finalized). BatchMaker should stop producing updates for this graph.
#[derive(Clone, Debug)]
pub struct GraphResolved {
    pub graph_id: GraphId,
}

/// Union of messages from FairnessLayer to BatchMaker.
#[derive(Clone, Debug)]
pub enum FairnessToWorkerMessage {
    MissingEdgeRequest(MissingEdgeRequest),
    GraphResolved(GraphResolved),
}