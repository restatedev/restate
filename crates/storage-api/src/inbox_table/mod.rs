// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_types::identifiers::{InvocationId, PartitionKey, ServiceId, WithPartitionKey};
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

#[derive(Debug, Clone, PartialEq)]
pub enum InboxEntry {
    Invocation(ServiceId, InvocationId),
    StateMutation(ExternalStateMutation),
}

impl InboxEntry {
    pub fn service_id(&self) -> &ServiceId {
        match self {
            InboxEntry::Invocation(service_id, _) => service_id,
            InboxEntry::StateMutation(state_mutation) => &state_mutation.service_id,
        }
    }
}

impl PartitionStoreProtobufValue for InboxEntry {
    type ProtobufType = crate::protobuf_types::v1::InboxEntry;
}

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
        service_id: ServiceId,
        invocation_id: InvocationId,
    ) -> Self {
        Self {
            inbox_sequence_number,
            inbox_entry: InboxEntry::Invocation(service_id, invocation_id),
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

pub trait ReadInboxTable {
    fn peek_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<Option<SequenceNumberInboxEntry>>> + Send;

    fn inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send>;
}

pub trait ScanInboxTable {
    fn for_each_inbox<
        F: FnMut(SequenceNumberInboxEntry) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait WriteInboxTable {
    fn put_inbox_entry(
        &mut self,
        sequence_number: MessageIndex,
        inbox_entry: &InboxEntry,
    ) -> Result<()>;

    fn delete_inbox_entry(&mut self, service_id: &ServiceId, sequence_number: u64) -> Result<()>;

    /// Pops the next inbox entry for the given service.
    ///
    /// This method returns a `Future` because it performs a blocking read operation.
    /// The implementation may need to interact with storage in a way that is not
    /// immediately non-blocking, so the async interface is required.
    fn pop_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<Option<SequenceNumberInboxEntry>>> + Send;
}
