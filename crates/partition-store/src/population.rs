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
//!
//! An invocation's public status is the one its entry's effective status tells,
//! and the status index finds it through the lanes of that status
//! ([`EntryByStatusCreatedKey::lanes`]). The lifecycle keeps the stored status
//! in step, and it shows where an entry tells none:
//! [`PartitionStore::invocation_lane_mismatches`] checks that each entry is in
//! a lane its invocation's stored status and completion result select.

use std::collections::BTreeSet;

use rocksdb::ReadOptions;

use restate_storage_api::StorageError;
use restate_storage_api::protobuf_types::v1::invocation_status_v2::Status as InvocationState;
use restate_storage_api::protobuf_types::v1::response_result::ResponseResult;
use restate_storage_api::vqueue_table::{RawStatusHeaderRef, Stage, Status};
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::EntryByStatusCreatedClause;
use restate_types::identifiers::{BaseEntryId, InvocationId};
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::invocation_status_table::{InvocationStatusKey, decode_invocation_status_lazy};
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind};
use crate::vqueue_table::{EntryByStatusCreatedKey, EntryStatusKey};
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

/// An invocation whose entry isn't in any lane its stored status and
/// completion result select: the status its entry tells differs from the
/// stored one.
#[derive(Debug, PartialEq, Eq)]
pub struct LaneMismatch {
    /// Displayed like the invocation ID.
    pub id: BaseEntryId,
    pub status: &'static str,
    pub completion_result: Option<&'static str>,
    /// The entry's stored stage and status.
    pub entry: (Stage, Status),
}

impl PartitionStore {
    /// The stored invocations whose entries aren't in a lane their stored
    /// status and completion result select ([`EntryByStatusCreatedKey::lanes`]),
    /// from the latest committed state. Invocations without an entry are
    /// [`Self::invocation_entry_mismatches`]' concern.
    pub fn invocation_lane_mismatches(&self) -> Result<Vec<LaneMismatch>> {
        let db = self.partition_db();
        let raw = db.rocksdb().inner().as_raw_db();
        let mut options = ReadOptions::default();
        options.set_total_order_seek(true);
        options.set_iterate_range(rocksdb::PrefixRange(KeyKind::InvocationStatus.as_bytes()));
        let mut iterator = raw.raw_iterator_cf_opt(db.cf_handle(), options);
        iterator.seek_to_first();
        let mut mismatches = Vec::new();
        let mut entry_key = Vec::new();
        while let (Some(mut key), Some(value)) = (iterator.key(), iterator.value()) {
            let key = InvocationStatusKey::deserialize_from(&mut key)?;
            let id = BaseEntryId::from(InvocationId::from_parts(
                key.partition_key,
                key.invocation_uuid,
            ));
            let invocation = decode_invocation_status_lazy(value)?;
            let status = match invocation.inner.status() {
                InvocationState::Scheduled => "scheduled",
                InvocationState::Inboxed => "inboxed",
                InvocationState::Invoked => "invoked",
                InvocationState::Suspended => "suspended",
                InvocationState::Paused => "paused",
                InvocationState::Completed => "completed",
                InvocationState::UnknownStatus => return Err(StorageError::DataIntegrityError),
            };
            let completion_result = match status {
                "completed" => Some(match invocation.response_result()? {
                    ResponseResult::ResponseSuccess(_) => "success",
                    ResponseResult::ResponseFailure(_) => "failure",
                }),
                _ => None,
            };
            entry_key.clear();
            EntryStatusKey::from(id).serialize_to(&mut entry_key);
            if let Some(mut stored) = raw
                .get_pinned_cf(db.cf_handle(), &entry_key)
                .map_err(|err| StorageError::Generic(err.into()))?
                .as_deref()
            {
                let header = RawStatusHeaderRef::decode_stored(&mut stored)?;
                let entry = (header.stage, header.status);
                let selection = Selection::All
                    .and(EntryByStatusCreatedClause::Status(ValuePredicate::Equal(
                        ReString::from_static(status),
                    )))
                    .and(EntryByStatusCreatedClause::CompletionResult(
                        ValuePredicate::Equal(completion_result.map(ReString::from_static)),
                    ));
                if !EntryByStatusCreatedKey::lanes(&selection).any(|lane| lane == entry) {
                    mismatches.push(LaneMismatch {
                        id,
                        status,
                        completion_result,
                        entry,
                    });
                }
            }
            iterator.next();
        }
        iterator
            .status()
            .map_err(|err| StorageError::Generic(err.into()))?;
        Ok(mismatches)
    }

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
