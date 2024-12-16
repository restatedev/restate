// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{protobuf_storage_encode_decode, Result};
use futures_util::Stream;
use restate_types::identifiers::{EntryIndex, InvocationId, JournalEntryId, PartitionKey};
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::NotificationId;
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;

// TODO this is annoying but we need this here because of package organization...
#[derive(Debug, Clone)]
pub struct StoredEntry(pub RawEntry);
protobuf_storage_encode_decode!(StoredEntry, crate::storage::v1::Entry);

#[derive(Debug, Clone, Default)]
pub struct NotificationsIndex(pub HashMap<NotificationId, EntryIndex>);
protobuf_storage_encode_decode!(NotificationsIndex, crate::storage::v1::NotificationsIndex);

pub trait ReadOnlyJournalTable {
    fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
    ) -> impl Future<Output = Result<Option<RawEntry>>> + Send;

    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, RawEntry)>> + Send;

    fn all_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send;

    fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<HashMap<NotificationId, EntryIndex>>> + Send;
}

pub trait JournalTable: ReadOnlyJournalTable {
    fn put_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
        journal_entry: &RawEntry,
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_journal(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> impl Future<Output = ()> + Send;
}

#[cfg(test)]
mod tests {
    use prost::Message;

    #[test]
    fn merging_notifications_map() {
        use crate::storage::v1::NotificationsIndex;

        let mut total = vec![];
        total.append(
            &mut NotificationsIndex {
                completions_index: [(1, 1)].into(),
                signals_index: [("a".to_string(), 2)].into(),
            }
            .encode_to_vec(),
        );
        total.append(
            &mut NotificationsIndex {
                completions_index: [(3, 3)].into(),
                signals_index: [("b".to_string(), 4)].into(),
            }
            .encode_to_vec(),
        );

        assert_eq!(
            NotificationsIndex::decode(total.as_slice()).unwrap(),
            NotificationsIndex {
                completions_index: [(1, 1), (3, 3)].into(),
                signals_index: [("a".to_string(), 2), ("b".to_string(), 4)].into()
            }
        );
    }
}
