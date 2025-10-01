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

use restate_types::identifiers::{PartitionKey, WithPartitionKey};
use restate_types::invocation::{
    AttachInvocationRequest, InvocationResponse, InvocationTermination, NotifySignalRequest,
    ServiceInvocation,
};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

/// Types of outbox messages.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OutboxMessage {
    /// Service invocation to send to another partition processor
    ServiceInvocation(Box<ServiceInvocation>),

    /// Service response to sent to another partition processor
    ServiceResponse(InvocationResponse),

    /// Terminate invocation to send to another partition processor
    InvocationTermination(InvocationTermination),

    /// Attach invocation
    AttachInvocation(AttachInvocationRequest),

    /// Notify signal request
    NotifySignal(NotifySignalRequest),
}

impl PartitionStoreProtobufValue for OutboxMessage {
    type ProtobufType = crate::protobuf_types::v1::OutboxMessage;
}

impl WithPartitionKey for OutboxMessage {
    fn partition_key(&self) -> PartitionKey {
        match self {
            OutboxMessage::ServiceInvocation(si) => si.partition_key(),
            OutboxMessage::ServiceResponse(sr) => sr.partition_key(),
            OutboxMessage::InvocationTermination(it) => it.invocation_id.partition_key(),
            OutboxMessage::AttachInvocation(ai) => ai.partition_key(),
            OutboxMessage::NotifySignal(sig) => sig.partition_key(),
        }
    }
}

pub trait ReadOutboxTable {
    fn get_outbox_head_seq_number(&mut self) -> impl Future<Output = Result<Option<u64>>> + Send;

    fn get_next_outbox_message(
        &mut self,
        next_sequence_number: u64,
    ) -> impl Future<Output = Result<Option<(u64, OutboxMessage)>>> + Send;

    fn get_outbox_message(
        &mut self,
        sequence_number: u64,
    ) -> impl Future<Output = Result<Option<OutboxMessage>>> + Send;
}

pub trait WriteOutboxTable {
    fn put_outbox_message(
        &mut self,
        message_index: u64,
        outbox_message: &OutboxMessage,
    ) -> Result<()>;

    fn truncate_outbox(&mut self, range: RangeInclusive<u64>) -> Result<()>;
}
