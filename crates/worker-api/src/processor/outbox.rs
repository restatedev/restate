// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::StorageError;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};
use restate_types::message::MessageIndex;

/// Read access to the partition outbox — the queue of messages destined for other partitions
/// (i.e. operations crossing the partition-key boundary).
pub trait HasOutbox {
    /// Returns a read-only view of the outbox.
    fn outbox(&self) -> impl OutboxAccess;
}

/// Mutating access to the outbox. Enqueues and truncations are staged into the supplied storage
/// transaction and only take effect when it commits.
pub trait HasOutboxMut: HasOutbox {
    /// Returns a mutable view of the outbox.
    fn outbox_mut(&mut self) -> impl OutboxMut;
}

/// Read-only view of the outbox head/tail cursors.
pub trait OutboxAccess {
    /// Sequence number of the next outbox message to be appended.
    fn outbox_tail(&self) -> MessageIndex;
    /// First outbox message index that needs to be sent out.
    fn outbox_head(&self) -> Option<MessageIndex>;
}

/// Mutable view of the outbox: append new messages and truncate delivered ones.
pub trait OutboxMut: OutboxAccess {
    /// Truncate the outbox to the given sequence number (inclusive).
    fn truncate_outbox_to(
        &mut self,
        txn: &mut impl WriteOutboxTable,
        to: MessageIndex,
    ) -> Result<(), StorageError>;

    /// Enqueue a message to the outbox of this processor.
    fn enqueue<S>(&mut self, txn: &mut S, message: &OutboxMessage) -> Result<(), StorageError>
    where
        S: WriteOutboxTable + WriteFsmTable;
}

// -- Boilerplate --

impl<P: HasOutbox> HasOutbox for &P {
    #[inline]
    fn outbox(&self) -> impl OutboxAccess {
        (**self).outbox()
    }
}

impl<P: HasOutbox> HasOutbox for &mut P {
    #[inline]
    fn outbox(&self) -> impl OutboxAccess {
        (**self).outbox()
    }
}

impl<P: HasOutboxMut> HasOutboxMut for &mut P {
    #[inline]
    fn outbox_mut(&mut self) -> impl OutboxMut {
        (**self).outbox_mut()
    }
}

impl<T: OutboxAccess> OutboxAccess for &T {
    #[inline]
    fn outbox_tail(&self) -> MessageIndex {
        (**self).outbox_tail()
    }
    #[inline]
    fn outbox_head(&self) -> Option<MessageIndex> {
        (**self).outbox_head()
    }
}

impl<T: OutboxAccess> OutboxAccess for &mut T {
    #[inline]
    fn outbox_tail(&self) -> MessageIndex {
        (**self).outbox_tail()
    }
    #[inline]
    fn outbox_head(&self) -> Option<MessageIndex> {
        (**self).outbox_head()
    }
}

impl<T: OutboxMut> OutboxMut for &mut T {
    #[inline]
    fn truncate_outbox_to(
        &mut self,
        txn: &mut impl WriteOutboxTable,
        to: MessageIndex,
    ) -> Result<(), StorageError> {
        (**self).truncate_outbox_to(txn, to)
    }

    #[inline]
    fn enqueue<S>(&mut self, txn: &mut S, message: &OutboxMessage) -> Result<(), StorageError>
    where
        S: WriteOutboxTable + WriteFsmTable,
    {
        (**self).enqueue(txn, message)
    }
}
