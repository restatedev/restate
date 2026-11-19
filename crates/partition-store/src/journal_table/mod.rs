// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use futures::{Stream, StreamExt};
use futures_util::stream;

use restate_core::io::IoExecutor;
use restate_memory::{ClaimedMemoryPool, LocalMemoryLease, LocalMemoryPool};
use restate_rocksdb::{Priority, RocksDbReadPerfGuard};
use restate_storage_api::journal_table::{
    JournalEntry, ReadJournalTable, ScanJournalTable, ScanJournalTableRange, WriteJournalTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{BudgetedReadError, Result, StorageError};
use restate_types::identifiers::{
    EntryIndex, InvocationId, InvocationUuid, JournalEntryId, PartitionKey, WithPartitionKey,
};

use crate::TableKind::Journal;
use crate::budgeted_reads::{MAX_INLINE_ROWS, budgeted_get};
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, define_table_key};
use crate::scan::PhysicalScan;
use crate::{
    PartitionStore, PartitionStoreSnapshot, PartitionStoreTransaction, StorageAccess,
    StorageIterator, StorageRead, TableScan, TableScanIterationDecision, break_on_err,
};

define_table_key!(
    Journal,
    KeyKind::Journal,
    JournalKey(partition_key: PartitionKey, invocation_uuid: InvocationUuid, journal_index: u32)
);

/// Lazy iterator over journal entries. Exposes [`peek_item`](Self::peek_item)
/// for zero-copy access to raw key/value slices and [`advance`](Self::advance)
/// to move forward. Also implements [`Iterator`] for convenience.
pub(crate) struct JournalEntryIter<I> {
    iter: I,
    remaining: u32,
}

impl<I: StorageIterator> JournalEntryIter<I> {
    fn new(iter: I, journal_length: EntryIndex) -> Self {
        Self {
            iter,
            remaining: journal_length,
        }
    }

    /// Returns the raw `(key, value)` byte slices at the current iterator
    /// position without decoding or advancing. Returns `None` when exhausted.
    pub fn peek_item(&self) -> Option<Result<(&[u8], &[u8])>> {
        if self.remaining == 0 {
            return None;
        }
        self.iter.item().transpose()
    }

    /// Advances the iterator to the next entry.
    pub fn advance(&mut self) {
        self.iter.next();
        self.remaining -= 1;
    }
}

/// Decodes a journal key/value pair from raw byte slices.
fn decode_journal_entry(k: &[u8], v: &[u8]) -> Result<(EntryIndex, JournalEntry)> {
    let mut k = k;
    let mut v = v;
    let index = JournalKey::deserialize_from(&mut k)?.journal_index;
    let entry = JournalEntry::decode(&mut v)?;
    Ok((index, entry))
}

impl<I: StorageIterator> Iterator for JournalEntryIter<I> {
    type Item = Result<(EntryIndex, JournalEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = match self.peek_item()? {
            Ok(item) => item,
            Err(e) => return Some(Err(e)),
        };
        let result = decode_journal_entry(k, v);
        self.advance();
        Some(result)
    }
}

fn write_journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> JournalKey {
    JournalKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
        journal_index,
    }
}

fn put_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
    journal_entry: &JournalEntry,
) -> Result<()> {
    let key = write_journal_entry_key(invocation_id, journal_index);

    storage.put_kv_proto(key, journal_entry)
}

fn get_journal_entry<S: StorageRead>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
) -> Result<Option<JournalEntry>> {
    let key = write_journal_entry_key(invocation_id, journal_index);

    storage.get_value_proto(key)
}

fn get_journal<'a, S: StorageRead>(
    storage: &'a S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<JournalEntryIter<S::Iterator<'a>>> {
    let _x = RocksDbReadPerfGuard::new("get-journal-iter-setup");
    let key = JournalKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let iter = storage.iterator_from(TableScan::Prefix(key))?;

    Ok(JournalEntryIter::new(iter, journal_length))
}

fn journal_physical_scan(invocation_id: &InvocationId) -> PhysicalScan<Bytes> {
    let key = JournalKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    TableScan::Prefix(key).into()
}

fn delete_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<()> {
    let mut key = write_journal_entry_key(invocation_id, 0);
    let k = &mut key;
    for journal_index in 0..journal_length {
        k.journal_index = journal_index;
        storage.delete_key(k)?;
    }
    Ok(())
}

impl ReadJournalTable for PartitionStore {
    async fn get_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
    ) -> Result<Option<JournalEntry>> {
        self.assert_partition_key(invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("get-journal-entry");
        get_journal_entry(self, invocation_id, journal_index)
    }

    fn get_journal<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send + 'a> {
        self.assert_partition_key(invocation_id)?;
        Ok(stream::iter(get_journal(
            self,
            invocation_id,
            journal_length,
        )?))
    }
}

impl ScanJournalTable for PartitionStore {
    fn for_each_journal<
        F: FnMut((JournalEntryId, JournalEntry)) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: ScanJournalTableRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        let scan = match range {
            ScanJournalTableRange::PartitionKey(partition_key) => {
                TableScan::ScanPartitionKeyRange::<JournalKeyBuilder>(partition_key)
            }
            ScanJournalTableRange::InvocationId(invocation_id) => {
                let start_partition_key = invocation_id.start().partition_key();
                let end_partition_key = invocation_id.end().partition_key();
                let start = JournalKey::builder()
                    .partition_key(start_partition_key)
                    .invocation_uuid(invocation_id.start().invocation_uuid());

                let end = JournalKey::builder()
                    .partition_key(end_partition_key)
                    .invocation_uuid(invocation_id.end().invocation_uuid());

                TableScan::RangeInclusive(start, end)
            }
        };

        self.iterator_for_each(
            "df-v1-journal",
            Priority::Low,
            scan,
            move |(mut key, mut value)| {
                let journal_key = break_on_err(JournalKey::deserialize_from(&mut key))?;
                let journal_entry = break_on_err(JournalEntry::decode(&mut value))?;

                let (partition_key, invocation_uuid, entry_index) = journal_key.split();

                let journal_entry_id = JournalEntryId::from_parts(
                    InvocationId::from_parts(partition_key, invocation_uuid),
                    entry_index,
                );

                f((journal_entry_id, journal_entry)).map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadJournalTable for PartitionStoreTransaction<'_> {
    async fn get_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
    ) -> Result<Option<JournalEntry>> {
        self.assert_partition_key(invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("get-journal-entry");
        get_journal_entry(self, invocation_id, journal_index)
    }

    fn get_journal<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send + 'a> {
        self.assert_partition_key(invocation_id)?;
        Ok(stream::iter(get_journal(
            self,
            invocation_id,
            journal_length,
        )?))
    }
}

impl ReadJournalTable for PartitionStoreSnapshot<'_> {
    async fn get_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
    ) -> Result<Option<JournalEntry>> {
        self.assert_partition_key(invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("get-journal-entry");
        get_journal_entry(self, invocation_id, journal_index)
    }

    fn get_journal<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send + 'a> {
        self.assert_partition_key(invocation_id)?;
        Ok(stream::iter(get_journal(
            self,
            invocation_id,
            journal_length,
        )?))
    }
}

impl PartitionStore {
    /// Budget-gated point read of a single journal entry.
    ///
    /// Fetches raw bytes from the store, peeks the serialized size, acquires a
    /// [`LocalMemoryLease`] from `budget`, and only then decodes the entry.
    pub async fn get_journal_v1_entry_budgeted(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
        budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(JournalEntry, LocalMemoryLease)>, BudgetedReadError> {
        self.assert_partition_key(invocation_id)?;
        let key = write_journal_entry_key(invocation_id, journal_index);
        let key = {
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };
        budgeted_get(
            self,
            "get-journal-entry-budgeted",
            KeyKind::Journal,
            &key,
            budget,
            |value| value.len(),
            |mut value, lease| Ok((JournalEntry::decode(&mut value)?, lease)),
        )
        .await
    }
}

impl PartitionStoreSnapshot<'_> {
    /// Budget-gated journal stream.
    ///
    /// Each entry's raw byte size is peeked **before** deserialization. A
    /// [`LocalMemoryLease`] is acquired from `budget` for that size, and only
    /// then is the entry decoded. This prevents memory allocation when the
    /// budget is exhausted.
    ///
    /// `budget` may be lent to a worker of `executor` until the stream is done
    /// or dropped: the stream keeps it borrowed, and claiming it again
    /// afterwards waits for it to come back.
    pub fn get_journal_v1_budgeted<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
        budget: ClaimedMemoryPool<'a>,
        executor: &(impl IoExecutor + Sync),
    ) -> Result<
        impl Stream<
            Item = std::result::Result<
                (EntryIndex, JournalEntry, LocalMemoryLease),
                BudgetedReadError,
            >,
        > + Send
        + 'a,
    > {
        self.assert_partition_key(invocation_id)?;
        if journal_length == 0 {
            return Ok(stream::empty().left_stream());
        }
        let mut remaining = journal_length;
        let scan = self.prepare_budgeted_scan(
            "get-journal-budgeted",
            journal_physical_scan(invocation_id),
            budget,
            |_key, value| value.len(),
            move |key, value, lease| {
                let row =
                    decode_journal_entry(key, value).map(|(index, entry)| (index, entry, lease));
                remaining -= 1;
                // Stop at the last entry rather than stepping past it.
                if remaining == 0 {
                    TableScanIterationDecision::BreakWith(row)
                } else {
                    TableScanIterationDecision::Emit(row)
                }
            },
        );
        let rows = scan.run_cached_then_offload(MAX_INLINE_ROWS, executor)?;
        Ok(rows.right_stream())
    }
}

impl WriteJournalTable for PartitionStoreTransaction<'_> {
    fn put_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
        journal_entry: &JournalEntry,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        put_journal_entry(self, invocation_id, journal_index, journal_entry)
    }

    fn delete_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("delete-journal");
        delete_journal(self, invocation_id, journal_length)
    }
}

#[cfg(test)]
mod tests {
    use crate::journal_table::write_journal_entry_key;
    use crate::keys::EncodeTableKeyPrefix;
    use bytes::Bytes;
    use restate_types::identifiers::{InvocationId, InvocationUuid};

    fn journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> Bytes {
        write_journal_entry_key(invocation_id, journal_index)
            .serialize()
            .freeze()
    }

    #[test]
    fn journal_keys_sort_lex() {
        //
        // across invocations
        //
        assert!(
            journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
                0
            ) < journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(2)),
                0
            )
        );
        //
        // within the same service and key
        //
        let mut previous_key = journal_entry_key(
            &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
            0,
        );
        for i in 1..300 {
            let current_key = journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
                i,
            );
            assert!(previous_key < current_key);
            previous_key = current_key;
        }
    }
}
