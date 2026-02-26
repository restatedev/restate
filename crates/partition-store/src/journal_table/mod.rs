// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::Stream;
use futures_util::stream;
use rocksdb::{DBAccess, DBRawIteratorWithThreadMode};

use restate_memory::{LocalMemoryLease, LocalMemoryPool};
use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::journal_table::{
    JournalEntry, ReadJournalTable, ScanJournalTable, ScanJournalTableRange, WriteJournalTable,
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

/// Lazy iterator over journal entries. Exposes [`peek_item`](Self::peek_item)
/// for zero-copy access to raw key/value slices and [`advance`](Self::advance)
/// to move forward. Also implements [`Iterator`] for convenience.
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

    /// Returns the raw `(key, value)` byte slices at the current iterator
    /// position without decoding or advancing. Returns `None` when exhausted.
    pub fn peek_item(&self) -> Option<Result<(&[u8], &[u8])>> {
        if self.remaining == 0 {
            return None;
        }
        match self.iter.item() {
            Some((k, v)) => Some(Ok((k, v))),
            None => self
                .iter
                .status()
                .err()
                .map(|err| Err(StorageError::Generic(err.into()))),
        }
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

impl<DB: DBAccess> Iterator for JournalEntryIter<'_, DB> {
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

fn get_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
) -> Result<Option<JournalEntry>> {
    let key = write_journal_entry_key(invocation_id, journal_index);

    storage.get_value_proto(key)
}

/// Budget-gated point read with a unified reserve-read-adjust loop.
///
/// Each iteration reads the raw value from RocksDB, compares the actual byte
/// size against the lease held so far, and either:
/// - **decodes immediately** when the lease already covers the value,
/// - **tops up synchronously** via [`LocalMemoryPool::try_reserve`] when the
///   delta is small enough,
/// - **drops the pinned slice** (to avoid pinning RocksDB memtable memory
///   across `.await`), awaits the deficit via [`LocalMemoryPool::reserve`],
///   then retries.
///
/// On the very first iteration the lease is empty, so the first `try_reserve`
/// acts as the fast path (single RocksDB read, no `.await` when budget is
/// immediately available).
async fn get_journal_entry_budgeted<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
    budget: &mut LocalMemoryPool,
) -> std::result::Result<Option<(JournalEntry, LocalMemoryLease)>, BudgetedReadError> {
    let key = write_journal_entry_key(invocation_id, journal_index);

    // Serialize key once — reused for all reads.
    let buf = {
        let key_buf = storage.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buf);
        key_buf.split()
    };

    let mut lease = budget.empty_lease();

    loop {
        // Read raw value from RocksDB.
        // RocksDbPerfGuard is !Send and must not live across .await.
        let deficit = {
            let _x = RocksDbPerfGuard::new("get-journal-entry-budgeted");
            let Some(pinned) = storage.get(Journal, &buf)? else {
                return Ok(None);
            };

            let raw_size = pinned.as_ref().len();
            if raw_size <= lease.size() {
                // Lease already covers (or exceeds) the value — shrink and decode.
                lease.shrink(lease.size() - raw_size);
                let mut slice = pinned.as_ref();
                let entry = JournalEntry::decode(&mut slice)?;
                return Ok(Some((entry, lease)));
            }

            // Need more budget. Try synchronous top-up first.
            let deficit = raw_size - lease.size();
            if let Some(extra) = budget.try_reserve(deficit) {
                lease.merge(extra);
                let mut slice = pinned.as_ref();
                let entry = JournalEntry::decode(&mut slice)?;
                return Ok(Some((entry, lease)));
            }

            deficit
        };

        // Pinned slice dropped — safe to .await now.
        let extra = budget.reserve(deficit).await?;
        lease.merge(extra);
        // Loop back: re-read to check whether the value changed while we waited.
    }
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

    async fn get_journal_entry_budgeted(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
        budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(JournalEntry, LocalMemoryLease)>, BudgetedReadError> {
        self.assert_partition_key(invocation_id)?;
        get_journal_entry_budgeted(self, invocation_id, journal_index, budget).await
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
        range: ScanJournalTableRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        let scan = match range {
            ScanJournalTableRange::PartitionKey(partition_key) => {
                TableScan::FullScanPartitionKeyRange::<JournalKeyBuilder>(partition_key)
            }
            ScanJournalTableRange::InvocationId(invocation_id) => {
                let start = JournalKey::builder()
                    .partition_key(invocation_id.start().partition_key())
                    .invocation_uuid(invocation_id.start().invocation_uuid());

                let end = JournalKey::builder()
                    .partition_key(invocation_id.end().partition_key())
                    .invocation_uuid(invocation_id.end().invocation_uuid());

                TableScan::KeyRangeInclusiveInSinglePartition(self.partition_id(), start, end)
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
        let _x = RocksDbPerfGuard::new("get-journal-entry");
        get_journal_entry(self, invocation_id, journal_index)
    }

    async fn get_journal_entry_budgeted(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
        budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(JournalEntry, LocalMemoryLease)>, BudgetedReadError> {
        self.assert_partition_key(invocation_id)?;
        get_journal_entry_budgeted(self, invocation_id, journal_index, budget).await
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
/// lease from `budget` **before** decoding each entry.
///
/// Each entry is produced via a unified reserve-read-adjust loop: peek the raw
/// byte size, attempt synchronous [`LocalMemoryPool::try_reserve`], and decode
/// from the same iterator slices on success (fast path — no `.await`). When
/// `try_reserve` fails the borrow is dropped and the deficit is awaited via
/// [`LocalMemoryPool::reserve`], then the loop re-peeks the (unchanged)
/// iterator position.
fn budgeted_journal_stream<'a, DB: DBAccess + Send>(
    iter: JournalEntryIter<'a, DB>,
    budget: &'a mut LocalMemoryPool,
) -> impl Stream<
    Item = std::result::Result<(EntryIndex, JournalEntry, LocalMemoryLease), BudgetedReadError>,
> + Send
+ 'a {
    futures::stream::unfold((iter, budget), |(mut iter, budget)| async move {
        let mut lease = budget.empty_lease();
        loop {
            let deficit = {
                let (k, v) = match iter.peek_item() {
                    Some(Ok(item)) => item,
                    Some(Err(e)) => return Some((Err(e.into()), (iter, budget))),
                    None => return None,
                };

                let raw_size = v.len();
                if raw_size <= lease.size() {
                    // Lease already covers the value — shrink excess and decode.
                    lease.shrink(lease.size() - raw_size);
                    match decode_journal_entry(k, v) {
                        Ok((idx, entry)) => {
                            iter.advance();
                            return Some((Ok((idx, entry, lease)), (iter, budget)));
                        }
                        Err(e) => return Some((Err(e.into()), (iter, budget))),
                    }
                }

                // Need more budget. Try synchronous top-up first.
                let deficit = raw_size - lease.size();
                if let Some(extra) = budget.try_reserve(deficit) {
                    lease.merge(extra);
                    match decode_journal_entry(k, v) {
                        Ok((idx, entry)) => {
                            iter.advance();
                            return Some((Ok((idx, entry, lease)), (iter, budget)));
                        }
                        Err(e) => return Some((Err(e.into()), (iter, budget))),
                    }
                }

                deficit
            };

            // Slow path: borrow dropped — safe to .await now.
            let extra = match budget.reserve(deficit).await {
                Ok(l) => l,
                Err(e) => return Some((Err(e.into()), (iter, budget))),
            };
            lease.merge(extra);
            // Loop back: iterator hasn't moved, peek_item returns the same data.
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
