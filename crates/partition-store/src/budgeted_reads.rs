// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Budgeted reads, which reserve a row's budget before decoding the row:
//! [`budgeted_get`] for one key and [`BudgetedScan`] for the rows of a scan.

use restate_memory::{LentMemoryPool, LocalMemoryLease, ReserveMemory};
use restate_rocksdb::RocksDbPerfExt;
use restate_storage_api::{BudgetedReadError, StorageError};

use crate::TableScanIterationDecision;
use crate::keys::KeyKind;
use crate::partition_store::{StorageIterator, StorageRead};

/// A row of a budgeted read, or why the read is incomplete.
pub(crate) type Row<T> = Result<T, BudgetedReadError>;

/// Reads the value of `key`. Its `cost` is reserved from `budget` before
/// `decode` gets the value with its lease. Returns `None` if the key has no
/// value.
///
/// Waits for the budget if needed, without holding on to the value: it's read
/// again afterwards, as it may have changed unless `storage` is a snapshot.
pub(crate) async fn budgeted_get<S, B, T>(
    storage: &S,
    name: &'static str,
    key_kind: KeyKind,
    key: &[u8],
    budget: &mut B,
    cost: impl Fn(&[u8]) -> usize,
    decode: impl FnOnce(&[u8], LocalMemoryLease) -> Result<T, StorageError>,
) -> Row<Option<T>>
where
    S: StorageRead,
    B: ReserveMemory,
{
    async move {
        let mut reserved: Option<LocalMemoryLease> = None;
        loop {
            let cost = {
                let Some(value) = storage.get(key_kind, key)? else {
                    return Ok(None);
                };

                let cost = cost(&value);
                match &mut reserved {
                    // Reserved while waiting, and the value hasn't grown since.
                    Some(lease) if lease.size() >= cost => lease.shrink(lease.size() - cost),
                    _ => {
                        drop(reserved.take());
                        reserved = budget.try_reserve(cost);
                    }
                }
                if let Some(lease) = reserved.take() {
                    return Ok(Some(decode(&value, lease)?));
                }
                cost
            };
            // The value is dropped: it isn't kept in memory while waiting.
            reserved = Some(budget.reserve(cost).await?);
        }
    }
    .with_read_perf(name)
    .await
}

/// Reads the rows of a budgeted scan one at a time, reserving each row's budget
/// from a lent pool before decoding the row.
///
/// Dropping it gives the lent pool back.
pub(crate) struct BudgetedScan<I, C, D> {
    iterator: I,
    budget: LentMemoryPool,
    cost: C,
    decode: D,
    done: bool,
}

impl<T, I, C, D> BudgetedScan<I, C, D>
where
    I: StorageIterator,
    C: Fn(&[u8], &[u8]) -> usize,
    D: FnMut(&[u8], &[u8], LocalMemoryLease) -> TableScanIterationDecision<T>,
{
    /// Reads the rows that `iterator` is positioned at and the ones after it.
    pub(crate) fn new(iterator: I, budget: LentMemoryPool, cost: C, decode: D) -> Self {
        Self {
            iterator,
            budget,
            cost,
            decode,
            done: false,
        }
    }

    /// The next row that `decode` builds, waiting for its budget if needed.
    /// `None` once the scan is over.
    pub(crate) async fn next(&mut self) -> Result<Option<T>, BudgetedReadError> {
        while !self.done {
            let Some((key, value)) = self.iterator.item()? else {
                break;
            };
            let cost = (self.cost)(key, value);
            let lease = self.budget.reserve(cost).await?;

            let Some((key, value)) = self.iterator.item()? else {
                break;
            };
            match (self.decode)(key, value, lease) {
                TableScanIterationDecision::Emit(row) => {
                    self.iterator.next();
                    return Ok(Some(row?));
                }
                TableScanIterationDecision::BreakWith(row) => {
                    self.done = true;
                    return Ok(Some(row?));
                }
                TableScanIterationDecision::Continue => self.iterator.next(),
                TableScanIterationDecision::Break => self.done = true,
            }
        }
        Ok(None)
    }
}
