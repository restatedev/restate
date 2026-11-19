// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Whether the invocations a partition stores and its invocation entries
//! correspond.
//!
//! Once a partition has fully migrated to vqueues, every invocation it stores
//! has a vqueue entry under the same ID, and every invocation entry has its
//! invocation: the lifecycle creates and removes both in the same command. Reads
//! that find invocations through the entries, such as through an entry index,
//! depend on that. [`PartitionStore::invocation_entry_mismatches`] checks it for
//! one store by reading both tables in full, for tests and diagnostics.

use std::collections::BTreeSet;

use rocksdb::ReadOptions;

use restate_storage_api::StorageError;
use restate_types::identifiers::{BaseEntryId, InvocationId};
use restate_types::vqueues::EntryKind;

use crate::invocation_status_table::InvocationStatusKey;
use crate::keys::{DecodeTableKey, KeyKind};
use crate::vqueue_table::EntryStatusKey;
use crate::{PartitionStore, Result};

/// The invocations without an entry, and the invocation entries without an
/// invocation, of one partition store.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct InvocationEntryMismatches {
    /// Displayed like the invocation ID.
    pub invocations_without_entry: Vec<BaseEntryId>,
    pub entries_without_invocation: Vec<BaseEntryId>,
}

impl InvocationEntryMismatches {
    pub fn is_empty(&self) -> bool {
        self.invocations_without_entry.is_empty() && self.entries_without_invocation.is_empty()
    }
}

impl PartitionStore {
    /// Compares the stored invocations with the stored invocation entries. Reads
    /// both tables in full, from the latest committed state.
    pub fn invocation_entry_mismatches(&self) -> Result<InvocationEntryMismatches> {
        let invocations: BTreeSet<BaseEntryId> =
            self.raw_keys(KeyKind::InvocationStatus, |mut key| {
                let key = InvocationStatusKey::deserialize_from(&mut key)?;
                Ok(Some(BaseEntryId::from(InvocationId::from_parts(
                    key.partition_key,
                    key.invocation_uuid,
                ))))
            })?;
        let entries: BTreeSet<BaseEntryId> =
            self.raw_keys(KeyKind::VQueueEntryStatus, |mut key| {
                let key = EntryStatusKey::deserialize_from(&mut key)?;
                Ok((key.id.kind() == EntryKind::Invocation).then_some(key.id))
            })?;
        Ok(InvocationEntryMismatches {
            invocations_without_entry: invocations.difference(&entries).copied().collect(),
            entries_without_invocation: entries.difference(&invocations).copied().collect(),
        })
    }

    /// The keys of `kind` in this store, mapped by `decode`.
    fn raw_keys<T: Ord>(
        &self,
        kind: KeyKind,
        mut decode: impl FnMut(&[u8]) -> Result<Option<T>>,
    ) -> Result<BTreeSet<T>> {
        let db = self.partition_db();
        let mut options = ReadOptions::default();
        options.set_total_order_seek(true);
        options.set_iterate_range(rocksdb::PrefixRange(kind.as_bytes()));
        let mut iterator = db
            .rocksdb()
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(db.cf_handle(), options);
        iterator.seek_to_first();
        let mut values = BTreeSet::new();
        while let Some(key) = iterator.key() {
            if let Some(value) = decode(key)? {
                values.insert(value);
            }
            iterator.next();
        }
        iterator
            .status()
            .map_err(|err| StorageError::Generic(err.into()))?;
        Ok(values)
    }
}
