// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use futures::Stream;
use futures_util::stream;
use rocksdb::{DBAccess, DBRawIteratorWithThreadMode};

use restate_memory::{LocalMemoryLease, LocalMemoryPool};
use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::journal_table::{
    JournalEntry, ReadJournalTable, ScanJournalTable, WriteJournalTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{BudgetedReadError, Result, StorageError};
use restate_types::identifiers::{
    EntryIndex, InvocationId, InvocationUuid, JournalEntryId, PartitionKey, WithPartitionKey,
};

use crate::TableKind::Journal;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan, break_on_err};

define_table_key!(
    Journal,
    KeyKind::Journal,
    JournalKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        journal_index: u32
    )
);

/// Lazy iterator over journal entries. Supports two-phase iteration:
/// [`peek_value_size`](Self::peek_value_size) to inspect the raw byte size before
/// decoding, then [`decode_and_advance`](Self::decode_and_advance) to decode and
/// move forward. Also implements [`Iterator`] for convenience.
pub struct JournalEntryIter<'a, DB: DBAccess> {
    iter: DBRawIteratorWithThreadMode<'a, DB>,
    remaining: u32,
}

impl<'a, DB: DBAccess> JournalEntryIter<'a, DB> {
    fn new(iter: DBRawIteratorWithThreadMode<'a, DB>, journal_length: EntryIndex) -> Self {
        Self {
            iter,
            remaining: journal_length,
        }
    }

    /// Returns the raw serialized byte size of the value at the current iterator
    /// position without decoding or advancing. Returns `None` when exhausted.
    pub fn peek_value_size(&self) -> Option<Result<usize>> {
        if self.remaining == 0 {
            return None;
        }
        match self.iter.item() {
            Some((_k, v)) => Some(Ok(v.len())),
            None => self
                .iter
                .status()
                .err()
                .map(|err| Err(StorageError::Generic(err.into()))),
        }
    }

    /// Decodes the current entry and advances the iterator.
    /// Must only be called after [`peek_value_size`](Self::peek_value_size) returned
    /// `Some(Ok(_))`.
    pub fn decode_and_advance(&mut self) -> Option<Result<(EntryIndex, JournalEntry)>> {
        if self.remaining == 0 {
            return None;
        }

        let Some((mut k, mut v)) = self.iter.item() else {
            return self
                .iter
                .status()
                .err()
                .map(|err| Err(StorageError::Generic(err.into())));
        };

        let key_result =
            JournalKey::deserialize_from(&mut k).map(|journal_key| journal_key.journal_index);
        let entry_result = JournalEntry::decode(&mut v);

        self.iter.next();
        self.remaining -= 1;

        Some(key_result.and_then(|key| entry_result.map(|entry| (key, entry))))
    }
}

impl<DB: DBAccess> Iterator for JournalEntryIter<'_, DB> {
    type Item = Result<(EntryIndex, JournalEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.decode_and_advance()
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

fn get_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
) -> Result<Option<JournalEntry>> {
    let key = write_journal_entry_key(invocation_id, journal_index);

    storage.get_value_proto(key)
}

fn get_journal<'a, S: StorageAccess>(
    storage: &'a S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<JournalEntryIter<'a, S::DBAccess<'a>>> {
    let _x = RocksDbPerfGuard::new("get-journal-iter-setup");
    let key = JournalKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let iter = storage.iterator_from(TableScan::SinglePartitionKeyPrefix(
        invocation_id.partition_key(),
        key,
    ))?;

    Ok(JournalEntryIter::new(iter, journal_length))
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
        let _x = RocksDbPerfGuard::new("get-journal-entry");
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

    fn get_journal_budgeted<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
        budget: &'a mut LocalMemoryPool,
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
        let iter = get_journal(self, invocation_id, journal_length)?;
        Ok(budgeted_journal_stream(iter, budget))
    }
}

impl ScanJournalTable for PartitionStore {
    fn for_each_journal<
        F: FnMut((JournalEntryId, JournalEntry)) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        self.iterator_for_each(
            "df-v1-journal",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<JournalKey>(range),
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
        let _x = RocksDbPerfGuard::new("get-journal-entry");
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

    fn get_journal_budgeted<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
        budget: &'a mut LocalMemoryPool,
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
        let iter = get_journal(self, invocation_id, journal_length)?;
        Ok(budgeted_journal_stream(iter, budget))
    }
}

/// Wraps a [`JournalEntryIter`] into an async [`Stream`] that acquires a memory
/// lease from `budget` **before** decoding each entry (peek → reserve → decode).
fn budgeted_journal_stream<'a, DB: DBAccess + Send>(
    iter: JournalEntryIter<'a, DB>,
    budget: &'a mut LocalMemoryPool,
) -> impl Stream<
    Item = std::result::Result<(EntryIndex, JournalEntry, LocalMemoryLease), BudgetedReadError>,
> + Send
+ 'a {
    futures::stream::unfold((iter, budget), |(mut iter, budget)| async move {
        // Phase 1: peek raw byte size without decoding
        let raw_size = match iter.peek_value_size() {
            Some(Ok(size)) => size,
            Some(Err(e)) => return Some((Err(e.into()), (iter, budget))),
            None => return None,
        };

        // Phase 2: acquire lease before allocating any memory for the decoded entry
        let lease = match budget.reserve(raw_size).await {
            Ok(lease) => lease,
            Err(e) => return Some((Err(e.into()), (iter, budget))),
        };

        // Phase 3: decode (budget already secured)
        match iter.decode_and_advance() {
            Some(Ok((idx, entry))) => Some((Ok((idx, entry, lease)), (iter, budget))),
            Some(Err(e)) => Some((Err(e.into()), (iter, budget))),
            None => {
                // peek_value_size returned Some but decode returned None — should not happen.
                // Defensive: treat as stream exhaustion and drop the lease.
                None
            }
        }
    })
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
        let _x = RocksDbPerfGuard::new("delete-journal");
        delete_journal(self, invocation_id, journal_length)
    }
}

#[cfg(test)]
mod tests {
    use crate::journal_table::write_journal_entry_key;
    use crate::keys::TableKeyPrefix;
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
