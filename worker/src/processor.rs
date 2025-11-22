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

                    assert!(
                        batch.len() >= 4 + 32 + 8
                    );

                    let payload = &batch[4..];

                    let pk_bytes: [u8; 32] = payload[0..32]
                        .try_into()
                        .expect("pk slice must be 32 bytes");

                    let round_bytes: [u8; 8] = payload[32..40]
                        .try_into()
                        .expect("round slice must be 8 bytes");
                    let round = u64::from_le_bytes(round_bytes);

                    let fifo_vec_bytes = &payload[40..];
                    let digest = Digest(
                        Sha512::digest(fifo_vec_bytes)[..32]
                            .try_into()
                            .unwrap(),
                    );

                    let mut key = b"FIFO".to_vec();
                    key.extend_from_slice(&pk_bytes);
                    key.extend_from_slice(&round.to_le_bytes());
                    key.extend_from_slice(&digest.to_vec());

                    let message = match own_digest {
                        true => WorkerPrimaryMessage::OurBatch(digest, id, true),
                        false => WorkerPrimaryMessage::OthersBatch(digest, id, true),
                    };
                    let message = bincode::serialize(&message)
                        .expect("Failed to serialize our own worker-primary message");

                    tx_digest
                        .send(message)
                        .await
                        .expect("Failed to send digest");

                    store.write(key, payload.to_vec()).await;

                }else{

                    // Hash the batch.
                    let digest = Digest(Sha512::digest(&batch)[..32].try_into().unwrap());
                    let digest_to_vec = digest.to_vec();

                    // Deliver the batch's digest.
                    let message = match own_digest {
                        true => WorkerPrimaryMessage::OurBatch(digest, id, false),
                        false => WorkerPrimaryMessage::OthersBatch(digest, id, false),
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
