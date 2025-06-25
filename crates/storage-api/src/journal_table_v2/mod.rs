// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::ops::RangeInclusive;

use futures::Stream;

use restate_types::identifiers::{EntryIndex, InvocationId, PartitionKey};
use restate_types::journal_v2::raw::{RawCommand, RawEntry};
use restate_types::journal_v2::{CompletionId, NotificationId};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

pub trait ReadOnlyJournalTable {
    fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
    ) -> impl Future<Output = Result<Option<RawEntry>>> + Send;

    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, RawEntry)>> + Send>;

    fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<HashMap<NotificationId, EntryIndex>>> + Send;

    fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> impl Future<Output = Result<Option<RawCommand>>> + Send;
}

pub trait ScanJournalTable {
    fn scan_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<
        impl Stream<Item = Result<(restate_types::identifiers::JournalEntryId, RawEntry)>> + Send,
    >;
}

pub trait JournalTable: ReadOnlyJournalTable {
    /// Related completion ids to this RawEntry, used to build the internal index
    fn put_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
        entry: &RawEntry,
        related_completion_ids: &[CompletionId],
    ) -> impl Future<Output = Result<()>> + Send;

    /// When length is available, it is suggested to provide it as it makes the delete more efficient.
    fn delete_journal(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug, Clone)]
pub struct StoredEntry(pub RawEntry);

impl PartitionStoreProtobufValue for StoredEntry {
    type ProtobufType = crate::protobuf_types::v1::Entry;
}

#[derive(Debug, Clone, Copy, derive_more::From, derive_more::Into)]
pub struct JournalEntryIndex(pub u32);
impl PartitionStoreProtobufValue for JournalEntryIndex {
    type ProtobufType = crate::protobuf_types::v1::JournalEntryIndex;
}
