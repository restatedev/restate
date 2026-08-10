// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::never::Never;

use restate_bifrost::{
    Bifrost, EnqueueError, EnqueueWithNotificationResult, ErrorRecoveryStrategy, InputRecord,
};
use restate_storage_api::deduplication_table::EpochSequenceNumber;
use restate_types::{
    config::Configuration,
    logs::{BodyWithKeys, Keys, LogId},
    net::ingest::IngestRecord,
    time::NanosSinceEpoch,
};
use restate_wal_protocol::v2::{Command, CommandWithKeys, Dedup, Envelope, ErasedCommand, Raw};

use crate::partition::leadership::Error;

// A constant since it's very unlikely that we can derive a meaningful configuration
// that the user can reason about. The queue's memory budget, on the other hand, is
// configurable via `worker.self-proposal-queue-memory-limit`.
const MAX_BIFROST_APPEND_BATCH: usize = 5000;

static BIFROST_APPENDER_TASK: &str = "bifrost-appender";

pub struct SelfProposer {
    epoch_sequence_number: EpochSequenceNumber,
    bifrost_appender: restate_bifrost::AppenderHandle<Envelope<Raw>>,
}

impl SelfProposer {
    pub fn new(
        log_id: LogId,
        epoch_sequence_number: EpochSequenceNumber,
        bifrost: &Bifrost,
    ) -> Result<Self, Error> {
        let memory_limit = Configuration::pinned()
            .worker
            .self_proposal_queue_memory_limit;
        let bifrost_appender = bifrost
            .create_background_appender(
                log_id,
                ErrorRecoveryStrategy::ExtendChainPreferred,
                Some(memory_limit),
                MAX_BIFROST_APPEND_BATCH,
            )?
            .start("self-appender")?;

        Ok(Self {
            epoch_sequence_number,
            bifrost_appender,
        })
    }

    pub fn mark_as_leader(&mut self) {
        // we wouldn't fail if this didn't work out, subsequent operations will fail anyway.
        self.bifrost_appender.sender().mark_as_preferred();
    }

    pub fn mark_as_non_leader(&mut self) {
        // we wouldn't fail if this didn't work out, subsequent operations will fail anyway.
        self.bifrost_appender.sender().forget_preference();
    }

    /// Self-propose many commands to Bifrost, attaching ESN-based dedup information.
    /// Returns the number of bytes proposed (the serialized size of all the commands).
    pub fn self_propose_many<I, T, C>(&mut self, cmds: I) -> Result<usize, Error>
    where
        I: ExactSizeIterator<Item = T>,
        T: CommandWithKeys<C>,
        C: Command,
    {
        // allocate a sequence number range for the batch
        let leader_epoch = self.epoch_sequence_number.leader_epoch;

        let start_seq = self.epoch_sequence_number.sequence_number;
        let end_seq = start_seq + cmds.len() as u64;

        let envelopes = cmds.enumerate().map(|(idx, command)| {
            let keys = command.keys();
            let envelope = Envelope::new(
                Dedup::SelfProposal {
                    leader_epoch,
                    seq: start_seq + idx as u64,
                },
                command.inner(),
            )
            .into_raw();
            BodyWithKeys::new(envelope, keys)
        });

        let bytes_written = self
            .bifrost_appender
            .sender()
            .enqueue_many(envelopes)
            .map_err(|e| Error::SelfProposer(e.to_string()))?;

        // update the sequence number range for the next batch
        self.epoch_sequence_number = EpochSequenceNumber {
            leader_epoch,
            sequence_number: end_seq,
        };

        Ok(bytes_written)
    }

    /// Self-propose a single command to Bifrost, attaching ESN-based dedup information.
    /// Returns the number of bytes proposed (the serialized size of the command).
    pub fn self_propose<C: Command>(
        &mut self,
        command: impl CommandWithKeys<C>,
    ) -> Result<usize, Error> {
        let esn = self.next_esn();
        let dedup = Dedup::SelfProposal {
            leader_epoch: esn.leader_epoch,
            seq: esn.sequence_number,
        };

        let keys = command.keys();
        let envelope = Envelope::new(dedup, command.inner());

        self.bifrost_appender
            .sender()
            .enqueue(BodyWithKeys::new(envelope.into_raw(), keys))
            .map_err(|e| Error::SelfProposer(e.to_string()))
    }

    /// Self-propose a single erased-command to Bifrost, attaching ESN-based dedup information.
    /// Returns the number of bytes proposed (the serialized size of the command).
    pub fn self_propose_erased(
        &mut self,
        keys: Keys,
        command: ErasedCommand,
    ) -> Result<usize, Error> {
        let esn = self.next_esn();
        let dedup = Dedup::SelfProposal {
            leader_epoch: esn.leader_epoch,
            seq: esn.sequence_number,
        };

        let envelope = Envelope::from_erased_command(dedup, command);

        self.bifrost_appender
            .sender()
            .enqueue(BodyWithKeys::new(envelope, keys))
            .map_err(|e| Error::SelfProposer(e.to_string()))
    }

    /// Self-propose a single command to Bifrost, attaching ESN-based dedup information.
    /// Compared to [`SelfProposer::self_propose`], this method bypasses all memory accounting
    /// on the appender.
    /// This should only be used for commands that can't afford a backpressue.
    /// Returns the number of bytes proposed (the serialized size of the command).
    pub fn self_propose_unaccounted<C: Command>(
        &mut self,
        command: impl CommandWithKeys<C>,
    ) -> Result<usize, Error> {
        let esn = self.next_esn();
        let dedup = Dedup::SelfProposal {
            leader_epoch: esn.leader_epoch,
            seq: esn.sequence_number,
        };

        let keys = command.keys();
        let envelope = Envelope::new(dedup, command.inner());

        self.bifrost_appender
            .sender()
            .enqueue_unaccounted(BodyWithKeys::new(envelope.into_raw(), keys))
            .map_err(|e| Error::SelfProposer(e.to_string()))
    }

    /// Append a command to Bifrost **without** dedup information, returning the number of bytes
    /// written and a [`CommitToken`](restate_bifrost::CommitToken).
    ///
    /// Unlike [`Self::self_propose`], this does not attach an epoch sequence number. Records
    /// appended this way are never filtered by the dedup mechanism during leadership transitions,
    /// which makes them safe for fire-and-forget ingress commands (signals, invocation responses).
    pub fn append_with_notification(
        &mut self,
        keys: Keys,
        command: ErasedCommand,
    ) -> Result<EnqueueWithNotificationResult, Error> {
        let envelope = Envelope::from_erased_command(Dedup::None, command);

        self.bifrost_appender
            .sender()
            .enqueue_with_notification(BodyWithKeys::new(envelope, keys))
            .map_err(|e| Error::SelfProposer(e.to_string()))
    }

    /// Forward externally-created records to Bifrost, returning the number of bytes written and a
    /// [`CommitToken`](restate_bifrost::CommitToken).
    ///
    /// The records already carry their own dedup information in their headers; no ESN is attached.
    /// Internally this uses `enqueue_many_unchecked` which does not check record sizes. Hence
    /// the only limit here is the networking max message size.
    pub fn forward_many_with_notification(
        &mut self,
        records: impl ExactSizeIterator<Item = IngestRecord>,
    ) -> Result<EnqueueWithNotificationResult, EnqueueError<()>> {
        let sender = self.bifrost_appender.sender();

        let inputs = records.map(|record| {
            // Skip decoding the envelope; build the InputRecord directly from the raw bytes.
            // The ingestion client should only handle payloads of type Envelope.
            unsafe {
                InputRecord::from_bytes_unchecked(
                    NanosSinceEpoch::now(),
                    record.keys,
                    record.record,
                )
            }
        });

        let bytes_written = sender.enqueue_many_unchecked(inputs)?;

        Ok(EnqueueWithNotificationResult {
            commit_token: sender.notify_committed()?,
            bytes_written,
        })
    }

    /// Waits for self proposer to fail. This method will only complete with an error if the self
    /// proposer has failed. There is no guarantee up to which point the self proposer has finished
    /// processing the proposed commands.
    pub async fn join_on_err(&mut self) -> Result<Never, Error> {
        let result = self.bifrost_appender.join().await;

        Err(match result {
            Ok(()) => Error::task_terminated_unexpectedly(BIFROST_APPENDER_TASK),
            Err(err) => Error::task_failed(BIFROST_APPENDER_TASK, err),
        })
    }

    pub fn has_capacity(&self) -> bool {
        self.bifrost_appender.sender_ref().has_capacity()
    }

    pub fn wait_for_capacity(&self) -> impl std::future::Future<Output = ()> + 'static {
        self.bifrost_appender.sender_ref().wait_for_capacity()
    }

    fn next_esn(&mut self) -> EpochSequenceNumber {
        let esn = self.epoch_sequence_number;
        self.epoch_sequence_number = self.epoch_sequence_number.next();
        esn
    }
}
