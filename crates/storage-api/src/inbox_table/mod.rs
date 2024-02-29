// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use restate_types::identifiers::{PartitionKey, ServiceId};
use restate_types::invocation::{MaybeFullInvocationId, ServiceInvocation};
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;
use std::future::Future;
use std::ops::RangeInclusive;

#[derive(Debug, Clone, PartialEq)]
pub enum InboxEntry {
    Invocation(ServiceInvocation),
    StateMutation(ExternalStateMutation),
}

impl InboxEntry {
    pub fn service_id(&self) -> &ServiceId {
        match self {
            InboxEntry::Invocation(invocation) => &invocation.fid.service_id,
            InboxEntry::StateMutation(state_mutation) => &state_mutation.component_id,
        }
    }
}

/// Entry of the inbox
#[derive(Debug, Clone, PartialEq)]
pub struct SequenceNumberInboxEntry {
    pub inbox_sequence_number: MessageIndex,
    pub inbox_entry: InboxEntry,
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
        service_invocation: ServiceInvocation,
    ) -> Self {
        Self {
            inbox_sequence_number,
            inbox_entry: InboxEntry::Invocation(service_invocation),
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

#[derive(Debug, Clone, PartialEq)]
pub struct SequenceNumberInvocation {
    pub inbox_sequence_number: MessageIndex,
    pub invocation: ServiceInvocation,
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

    /// Gets an invocation for the given invocation id.
    ///
    /// Important: This method can be quite costly if it is invoked with an `InvocationId` because
    /// it needs to scan all inboxes for the given partition key to match the given invocation uuid.
    fn get_invocation(
        &mut self,
        maybe_fid: impl Into<MaybeFullInvocationId>,
    ) -> impl Future<Output = Result<Option<SequenceNumberInvocation>>> + Send;
}
