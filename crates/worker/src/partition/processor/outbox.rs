// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use core::ops::RangeInclusive;
use std::collections::VecDeque;

use tracing::trace;

use restate_storage_api::StorageError;
use restate_storage_api::blob_table::WriteBlobTable;
use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
use restate_storage_api::outbox_table::{OutboxMessage, ReadOutboxTable, WriteOutboxTable};
use restate_types::blob::BlobId;
use restate_types::message::MessageIndex;
use restate_worker_api::processor::{OutboxAccess, OutboxMut};

pub struct Outbox {
    /// First outbox message index that needs to be sent out.
    outbox_head_seq: MessageIndex,
    /// Sequence number of the next outbox message to be appended.
    outbox_tail_seq: MessageIndex,
    /// Blobs referenced by the outbox messages, in ascending message index order. One reference
    /// is released when the message is truncated. Whether a message references a blob is a local
    /// decision of this replica, hence this is derived from the local outbox rather than the log.
    blobs: VecDeque<(MessageIndex, BlobId)>,
}

impl Outbox {
    #[cfg(test)]
    pub fn new_empty() -> Self {
        Self {
            outbox_tail_seq: 0,
            outbox_head_seq: 0,
            blobs: VecDeque::new(),
        }
    }

    pub async fn create<S>(storage: &mut S) -> Result<Self, StorageError>
    where
        S: ReadOutboxTable + ReadFsmTable,
    {
        let outbox_seq_number = storage.get_outbox_seq_number().await?;
        let outbox_head_seq_number = storage.get_outbox_head_seq_number().await?;
        let outbox_head_seq = outbox_head_seq_number.unwrap_or(outbox_seq_number);

        // One-time scan of the pending outbox messages to rebuild the referenced blobs. The
        // outbox only holds messages that weren't shuffled yet, so it's normally short.
        let mut blobs = VecDeque::new();
        let mut next = outbox_head_seq;
        while let Some((index, message)) = storage.get_next_outbox_message(next).await? {
            if let Some(blob) = related_blob(&message) {
                blobs.push_back((index, blob));
            }
            next = index + 1;
        }

        Ok(Self {
            outbox_tail_seq: outbox_seq_number,
            outbox_head_seq,
            blobs,
        })
    }
}

/// The blob referenced by an outbox message, if any. Only opaque messages reference blobs.
fn related_blob(message: &OutboxMessage) -> Option<BlobId> {
    match message {
        OutboxMessage::Opaque(opaque) => opaque.related_blob,
        _ => None,
    }
}

impl OutboxAccess for Outbox {
    fn outbox_tail(&self) -> MessageIndex {
        self.outbox_tail_seq
    }
}

impl OutboxMut for Outbox {
    fn truncate_outbox_to<S>(&mut self, txn: &mut S, to: MessageIndex) -> Result<(), StorageError>
    where
        S: WriteOutboxTable + WriteBlobTable,
    {
        // todo: Add validation or clamping to avoid truncating >= the outbox tail
        if self.outbox_head_seq > to {
            // We've already truncated those messages before
            return Ok(());
        }
        let range = RangeInclusive::new(self.outbox_head_seq, to);
        trace!(
            restate.outbox.seq_from = range.start(),
            restate.outbox.seq_to = range.end(),
            "Effect: Truncate outbox"
        );

        txn.truncate_outbox(range)?;

        while let Some((_, blob)) = self.blobs.pop_front_if(|(index, _)| *index <= to) {
            trace!(%blob, "Effect: Release outbox blob");
            txn.decrement_ref_count(blob, 1);
        }

        self.outbox_head_seq = to + 1;
        Ok(())
    }

    /// Enqueue a message to the outbox of this processor.
    fn enqueue<S>(&mut self, txn: &mut S, message: &OutboxMessage) -> Result<(), StorageError>
    where
        S: WriteOutboxTable + WriteFsmTable,
    {
        txn.put_outbox_message(self.outbox_tail_seq, message)?;
        if let Some(blob) = related_blob(message) {
            self.blobs.push_back((self.outbox_tail_seq, blob));
        }
        // need to store the next outbox sequence number
        self.outbox_tail_seq += 1;
        txn.put_outbox_seq_number(self.outbox_tail_seq)?;
        Ok(())
    }
}
