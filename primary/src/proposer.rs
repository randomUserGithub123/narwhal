// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info};
use std::collections::{BTreeMap, VecDeque};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    /// Holds the batches' digests waiting to be included in the next header.
    batch_digests: VecDeque<(Digest, WorkerId)>,

    /// Holds the LocalOrder' digests waiting to be included in the next header.
    local_order_digests: VecDeque<(Digest, WorkerId)>,

    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
    /// Keeps track of the size (in bytes) of LocalOrders' digests that we received so far.
    lo_size: usize,

    /// Holds the map of proposed previous round headers and their digest messages, to ensure that
    /// all batches' digest included will eventually be re-sent.
    proposed_headers: BTreeMap<Round, (Header, VecDeque<(Digest, WorkerId)>, VecDeque<(Digest, WorkerId)>)>,
    /// Committed headers channel on which we get updates on which of
    /// our own headers have been committed.
    rx_committed_own_headers: Receiver<Round>,
    /// The round of the maximum own header we committed
    max_committed_header: Round,

    at_least_one_local_order: bool,

}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        tx_core: Sender<Header>,
        rx_committed_own_headers: Receiver<Round>,
    ) {
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                last_parents: genesis,
                batch_digests: VecDeque::with_capacity(2 * header_size),
                local_order_digests: VecDeque::with_capacity(2 * header_size),
                payload_size: 0,
                lo_size: 0,
                proposed_headers: BTreeMap::new(),
                rx_committed_own_headers,
                max_committed_header: 0,
                at_least_one_local_order: false,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {

        let header_batch_digests: VecDeque<_> = self.batch_digests.drain(..).collect();
        let header_local_order_digests: VecDeque<_> = self.local_order_digests.drain(..).collect();

        // Make a new header.
        let header = Header::new(
            self.name,
            self.round,
            header_batch_digests.iter().cloned().collect(),
            header_local_order_digests.iter().cloned().collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        self.proposed_headers
            .insert(self.round, (header.clone(), header_batch_digests, header_local_order_digests));

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = (self.payload_size >= self.header_size) && (self.lo_size >= self.header_size);
            let timer_expired = timer.is_elapsed();
            if (timer_expired || enough_digests) && enough_parents && self.at_least_one_local_order {
                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;
                self.lo_size = 0;
                self.at_least_one_local_order = false;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    if round < self.round {
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;
                    debug!("Dag moved to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;
                }
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    if worker_id == 0 {
                        self.lo_size += digest.size();
                        self.at_least_one_local_order = true;
                        self.local_order_digests.push_back((digest, worker_id));
                    }else{
                        self.payload_size += digest.size();
                        self.batch_digests.push_back((digest, worker_id));
                    }
                }
                Some(round) = self.rx_committed_own_headers.recv() => {
                    debug!("Committed own header, round={}, header_round={}", self.round, round);

                    // Remove committed headers from the list of pending
                    self.max_committed_header = self.max_committed_header.max(round);
                    let Some(_) = self.proposed_headers.remove(&round) else {
                        info!("Own committed header not found at round {round}, probably because of restarts.");
                        // There can still be later committed headers in proposed_headers.
                        continue;
                    };


                    // Now for any round below the current commit round we re-insert
                    // the batches into the digests we need to send, effectively re-sending
                    // them in FIFO order.
                    // Oldest to newest payloads.
                    let mut batch_digests_to_resend = VecDeque::new();
                    let mut local_order_digests_to_resend = VecDeque::new();
                    // Oldest to newest rounds.
                    let mut retransmit_rounds = Vec::new();

                    // Iterate in order of rounds of our own headers.
                    for (header_round, (_, batch_included_digests, local_order_included_digests)) in &mut self.proposed_headers {
                        // Stop once we have processed headers at and below last committed round.
                        if *header_round > self.max_committed_header  {
                            break;
                        }
                        // Add payloads from oldest to newest.
                        batch_digests_to_resend.append(batch_included_digests);
                        local_order_digests_to_resend.append(local_order_included_digests);
                        retransmit_rounds.push(*header_round);
                    }

                    if !retransmit_rounds.is_empty() {
                        let num_to_resend = batch_digests_to_resend.len();
                        // Since all of digests_to_resend are roughly newer than self.digests,
                        // prepend digests_to_resend to the digests for the next header.
                        batch_digests_to_resend.append(&mut self.batch_digests);
                        self.batch_digests = batch_digests_to_resend;

                        local_order_digests_to_resend.append(&mut self.local_order_digests);
                        self.local_order_digests = local_order_digests_to_resend;

                        // Now delete the headers with batches we re-transmit
                        for round in &retransmit_rounds {
                            self.proposed_headers.remove(round);
                        }

                        debug!(
                            "Retransmit {} batches in undelivered headers {:?} at commit round {:?}, remaining headers {}",
                            num_to_resend,
                            retransmit_rounds,
                            self.round,
                            self.proposed_headers.len()
                        );

                    }
                },
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
