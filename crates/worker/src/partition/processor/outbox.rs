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

use tracing::trace;

use restate_storage_api::StorageError;
use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
use restate_storage_api::outbox_table::{OutboxMessage, ReadOutboxTable, WriteOutboxTable};
use restate_types::message::MessageIndex;
use restate_worker_api::processor::{OutboxAccess, OutboxMut};

pub struct Outbox {
    /// First outbox message index that needs to be sent out.
    outbox_head_seq: Option<MessageIndex>,
    /// Sequence number of the next outbox message to be appended.
    outbox_tail_seq: MessageIndex,
}

impl Outbox {
    #[cfg(test)]
    pub fn new_empty() -> Self {
        Self {
            outbox_tail_seq: 0,
            outbox_head_seq: None,
        }
    }

    /// Builds an outbox with pre-set head/tail sequence numbers, without touching
    /// storage. Lets partition-command tests simulate a partially-truncated outbox.
    #[cfg(test)]
    pub fn seed(tail: MessageIndex, head: Option<MessageIndex>) -> Self {
        Self {
            outbox_tail_seq: tail,
            outbox_head_seq: head,
        }
    }

    pub async fn create<S>(storage: &mut S) -> Result<Self, StorageError>
    where
        S: ReadOutboxTable + ReadFsmTable,
    {
        let outbox_seq_number = storage.get_outbox_seq_number().await?;
        let outbox_head_seq_number = storage.get_outbox_head_seq_number().await?;
        Ok(Self {
            outbox_tail_seq: outbox_seq_number,
            outbox_head_seq: outbox_head_seq_number,
        })
    }
}

impl OutboxAccess for Outbox {
    fn outbox_tail(&self) -> MessageIndex {
        self.outbox_tail_seq
    }

    fn outbox_head(&self) -> Option<MessageIndex> {
        self.outbox_head_seq
    }
}

impl OutboxMut for Outbox {
    fn truncate_outbox_to(
        &mut self,
        txn: &mut impl WriteOutboxTable,
        to: MessageIndex,
    ) -> Result<(), StorageError> {
        // todo: Add validation or clamping to avoid truncating >= the outbox tail
        let range = RangeInclusive::new(self.outbox_head_seq.unwrap_or(to), to);
        trace!(
            restate.outbox.seq_from = range.start(),
            restate.outbox.seq_to = range.end(),
            "Effect: Truncate outbox"
        );

        txn.truncate_outbox(range)?;

        self.outbox_head_seq = Some(to + 1);
        Ok(())
    }

    /// Enqueue a message to the outbox of this processor.
    fn enqueue<S>(&mut self, txn: &mut S, message: &OutboxMessage) -> Result<(), StorageError>
    where
        S: WriteOutboxTable + WriteFsmTable,
    {
        txn.put_outbox_message(self.outbox_tail_seq, message)?;
        // need to store the next outbox sequence number
        self.outbox_tail_seq += 1;
        txn.put_outbox_seq_number(self.outbox_tail_seq)?;
        Ok(())
    }
}
