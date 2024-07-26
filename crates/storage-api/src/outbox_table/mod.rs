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
use restate_types::identifiers::{PartitionId, PartitionKey, WithPartitionKey};
use restate_types::invocation::{InvocationResponse, InvocationTermination, ServiceInvocation};
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
}

protobuf_storage_encode_decode!(OutboxMessage);

impl WithPartitionKey for OutboxMessage {
    fn partition_key(&self) -> PartitionKey {
        match self {
            OutboxMessage::ServiceInvocation(si) => si.invocation_id.partition_key(),
            OutboxMessage::ServiceResponse(sr) => sr.id.partition_key(),
            OutboxMessage::InvocationTermination(it) => it.invocation_id.partition_key(),
        }
    }
}

pub trait OutboxTable {
    fn add_message(
        &mut self,
        partition_id: PartitionId,
        message_index: u64,
        outbox_message: OutboxMessage,
    ) -> impl Future<Output = ()> + Send;

    fn get_next_outbox_message(
        &mut self,
        partition_id: PartitionId,
        next_sequence_number: u64,
    ) -> impl Future<Output = Result<Option<(u64, OutboxMessage)>>> + Send;

    fn get_outbox_message(
        &mut self,
        partition_id: PartitionId,
        sequence_number: u64,
    ) -> impl Future<Output = Result<Option<OutboxMessage>>> + Send;

    fn truncate_outbox(
        &mut self,
        partition_id: PartitionId,
        range: RangeInclusive<u64>,
    ) -> impl Future<Output = ()> + Send;
}
