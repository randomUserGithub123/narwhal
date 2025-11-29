// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{SerializedBatchDigestMessage, WorkerMessage};
use bytes::Bytes;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use network::SimpleSender;
use std::convert::TryInto;
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;
pub type Transaction = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
        // Receive other workers txs to send to OF_worker
        mut rx_transaction: Receiver<Transaction>,
        // Our OF_Worker address
        of_worker_address: SocketAddr
    ) {
        tokio::spawn(async move {

            let mut simple_network = SimpleSender::new();

            loop {
                tokio::select! {

                    Some(batch) = rx_batch.recv() => {
                        // Hash the batch.
                        let digest = Digest(Sha512::digest(&batch)[..32].try_into().unwrap());

                        // Store the batch.
                        store.write(digest.to_vec(), batch).await;

                        // Deliver the batch's digest.
                        let message = match own_digest {
                            true => WorkerPrimaryMessage::OurBatch(digest, id),
                            false => WorkerPrimaryMessage::OthersBatch(digest, id),
                        };
                        let message = bincode::serialize(&message)
                            .expect("Failed to serialize our own worker-primary message");
                        tx_digest
                            .send(message)
                            .await
                            .expect("Failed to send digest");
                    },

                    Some(transaction) = rx_transaction.recv() => {

                        // Send to OF_worker to store in LocalOrder queue
                        let tx_digest = Digest(
                            Sha512::digest(&transaction)[..32]
                                .try_into()
                                .expect("Sha512 output must be at least 32 bytes"),
                        );
                        let message = WorkerMessage::TxDigest(tx_digest);
                        let serialized = bincode::serialize(&message)
                            .expect("Failed to serialize our own tx digest");
                        simple_network.send(of_worker_address, Bytes::from(serialized)).await;

                    }

                }
            }
            
        });
    }
}
