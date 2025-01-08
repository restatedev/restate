// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Result;
use futures_util::Stream;
use restate_types::identifiers::{CommandIndex, InvocationId, JournalEntryId, PartitionKey};
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::NotificationId;
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;

pub trait ReadOnlyJournalTable {
    fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
    ) -> impl Future<Output = Result<Option<RawEntry>>> + Send;

    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        journal_length: CommandIndex,
    ) -> impl Stream<Item = Result<(CommandIndex, RawEntry)>> + Send;

    fn all_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send;

    fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<HashMap<NotificationId, CommandIndex>>> + Send;
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
        journal_length: CommandIndex,
    ) -> impl Future<Output = ()> + Send;
}
