// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{GetFuture, GetStream, PutFuture};
use restate_types::identifiers::{FullInvocationId, PartitionKey, ServiceId};
use restate_types::invocation::{MaybeFullInvocationId, ServiceInvocation};
use restate_types::message::MessageIndex;
use std::ops::RangeInclusive;

/// Entry of the inbox
#[derive(Debug, Clone, PartialEq)]
pub struct InboxEntry {
    pub inbox_sequence_number: MessageIndex,
    pub service_invocation: ServiceInvocation,
}

impl InboxEntry {
    pub fn new(inbox_sequence_number: MessageIndex, service_invocation: ServiceInvocation) -> Self {
        Self {
            inbox_sequence_number,
            service_invocation,
        }
    }

    pub fn service_id(&self) -> &ServiceId {
        &self.service_invocation.fid.service_id
    }

    pub fn fid(&self) -> &FullInvocationId {
        &self.service_invocation.fid
    }
}

pub trait InboxTable {
    fn put_invocation(&mut self, service_id: &ServiceId, inbox_entry: InboxEntry) -> PutFuture;

    fn delete_invocation(&mut self, service_id: &ServiceId, sequence_number: u64) -> PutFuture;

    fn peek_inbox(&mut self, service_id: &ServiceId) -> GetFuture<Option<InboxEntry>>;

    fn inbox(&mut self, service_id: &ServiceId) -> GetStream<InboxEntry>;

    fn all_inboxes(&mut self, range: RangeInclusive<PartitionKey>) -> GetStream<InboxEntry>;

    /// Scans the inbox for an inbox entry with the given invocation id.
    ///
    /// Important: This method can be quite costly if it is invoked with an `InvocationId` because
    /// it needs to scan all inboxes for the given partition key to match the given invocation uuid.
    fn contains(
        &mut self,
        maybe_fid: impl Into<MaybeFullInvocationId>,
    ) -> GetFuture<Option<(ServiceId, u64)>>;
}
