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
use restate_types::identifiers::{PartitionKey, WithPartitionKey};
use restate_types::invocation::{
    AttachInvocationRequest, InvocationResponse, InvocationTermination, NotifySignalRequest,
    ServiceInvocation,
};
use std::future::Future;
use std::ops::RangeInclusive;

/// Types of outbox messages.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OutboxMessage {
    /// Service invocation to send to another partition processor
    ServiceInvocation(ServiceInvocation),

    /// Service response to sent to another partition processor
    ServiceResponse(InvocationResponse),

    /// Terminate invocation to send to another partition processor
    InvocationTermination(InvocationTermination),

    /// Attach invocation
    AttachInvocation(AttachInvocationRequest),

    /// Notify signal request
    NotifySignal(NotifySignalRequest),
}

impl WithPartitionKey for OutboxMessage {
    fn partition_key(&self) -> PartitionKey {
        match self {
            OutboxMessage::ServiceInvocation(si) => si.invocation_id.partition_key(),
            OutboxMessage::ServiceResponse(sr) => sr.id.partition_key(),
            OutboxMessage::InvocationTermination(it) => it.invocation_id.partition_key(),
            OutboxMessage::AttachInvocation(ai) => ai.invocation_query.partition_key(),
            OutboxMessage::NotifySignal(sig) => sig.partition_key(),
        }
    }
}

pub trait ReadOnlyOutboxTable {
    fn get_outbox_head_seq_number(&mut self) -> impl Future<Output = Result<Option<u64>>> + Send;
}

pub trait OutboxTable: ReadOnlyOutboxTable {
    fn put_outbox_message(
        &mut self,
        message_index: u64,
        outbox_message: &OutboxMessage,
    ) -> impl Future<Output = Result<()>> + Send;

    fn get_next_outbox_message(
        &mut self,
        next_sequence_number: u64,
    ) -> impl Future<Output = Result<Option<(u64, OutboxMessage)>>> + Send;

    fn get_outbox_message(
        &mut self,
        sequence_number: u64,
    ) -> impl Future<Output = Result<Option<OutboxMessage>>> + Send;

    fn truncate_outbox(
        &mut self,
        range: RangeInclusive<u64>,
    ) -> impl Future<Output = Result<()>> + Send;
}
