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
use restate_types::identifiers::{PartitionKey, ServiceId};
use restate_types::invocation::ServiceInvocation;
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
}

pub trait InboxTable {
    fn put_invocation(&mut self, service_id: &ServiceId, inbox_entry: InboxEntry) -> PutFuture;

    fn delete_invocation(&mut self, service_id: &ServiceId, sequence_number: u64) -> PutFuture;

    fn peek_inbox(&mut self, service_id: &ServiceId) -> GetFuture<Option<InboxEntry>>;

    fn inbox(&mut self, service_id: &ServiceId) -> GetStream<InboxEntry>;

    fn all_inboxes(&mut self, range: RangeInclusive<PartitionKey>) -> GetStream<InboxEntry>;
}
