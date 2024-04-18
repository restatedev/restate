// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use restate_types::identifiers::{FullInvocationId, PartitionKey, ServiceId, WithPartitionKey};
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;
use std::future::Future;
use std::ops::RangeInclusive;

#[derive(Debug, Clone, PartialEq)]
pub enum InboxEntry {
    Invocation(FullInvocationId),
    StateMutation(ExternalStateMutation),
}

impl InboxEntry {
    pub fn service_id(&self) -> &ServiceId {
        match self {
            InboxEntry::Invocation(invocation) => &invocation.service_id,
            InboxEntry::StateMutation(state_mutation) => &state_mutation.component_id,
        }
    }
}

protobuf_storage_encode_decode!(InboxEntry);

/// Entry of the inbox
#[derive(Debug, Clone, PartialEq)]
pub struct SequenceNumberInboxEntry {
    pub inbox_sequence_number: MessageIndex,
    pub inbox_entry: InboxEntry,
}

impl WithPartitionKey for SequenceNumberInboxEntry {
    fn partition_key(&self) -> PartitionKey {
        self.inbox_entry.service_id().partition_key()
    }
}

impl SequenceNumberInboxEntry {
    pub fn new(inbox_sequence_number: MessageIndex, inbox_entry: InboxEntry) -> Self {
        Self {
            inbox_sequence_number,
            inbox_entry,
        }
    }
    pub fn from_invocation(
        inbox_sequence_number: MessageIndex,
        full_invocation_id: FullInvocationId,
    ) -> Self {
        Self {
            inbox_sequence_number,
            inbox_entry: InboxEntry::Invocation(full_invocation_id),
        }
    }

    pub fn from_state_mutation(
        inbox_sequence_number: MessageIndex,
        state_mutation: ExternalStateMutation,
    ) -> Self {
        Self {
            inbox_sequence_number,
            inbox_entry: InboxEntry::StateMutation(state_mutation),
        }
    }

    pub fn service_id(&self) -> &ServiceId {
        self.inbox_entry.service_id()
    }
}

pub trait InboxTable {
    fn put_inbox_entry(
        &mut self,
        service_id: &ServiceId,
        inbox_entry: SequenceNumberInboxEntry,
    ) -> impl Future<Output = ()> + Send;

    fn delete_inbox_entry(
        &mut self,
        service_id: &ServiceId,
        sequence_number: u64,
    ) -> impl Future<Output = ()> + Send;

    fn peek_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<Option<SequenceNumberInboxEntry>>> + Send;

    fn pop_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<Option<SequenceNumberInboxEntry>>> + Send;

    fn inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send;

    fn all_inboxes(
        &mut self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send;
}
