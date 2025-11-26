// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

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
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {

                if batch.starts_with(b"FIFO"){

                    let fifo_vec_bytes = &batch[4 ..];
                    let digest = Digest(
                        Sha512::digest(fifo_vec_bytes)[..32]
                            .try_into()
                            .unwrap(),
                    );

                    let message = match own_digest {
                        true => WorkerPrimaryMessage::OurBatch(digest.clone(), id, true),
                        false => WorkerPrimaryMessage::OthersBatch(digest.clone(), id, true, fifo_vec_bytes.to_vec()),
                    };
                    let message = bincode::serialize(&message)
                        .expect("Failed to serialize our own worker-primary message");

                    tx_digest
                        .send(message)
                        .await
                        .expect("Failed to send digest");

                    store.write(digest.to_vec(), batch).await;

                }else{

                    // Hash the batch.
                    let digest = Digest(Sha512::digest(&batch)[..32].try_into().unwrap());
                    let digest_to_vec = digest.to_vec();

                    // Deliver the batch's digest.
                    let message = match own_digest {
                        true => WorkerPrimaryMessage::OurBatch(digest, id, false),
                        false => WorkerPrimaryMessage::OthersBatch(digest, id, false, Vec::default()),
                    };
                    let message = bincode::serialize(&message)
                        .expect("Failed to serialize our own worker-primary message");
                    tx_digest
                        .send(message)
                        .await
                        .expect("Failed to send digest");

                    // Store the batch.
                    store.write(digest_to_vec, batch).await;

                }
            }
        });
    }
}
