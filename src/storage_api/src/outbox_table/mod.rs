// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{GetFuture, PutFuture};
use restate_types::identifiers::{FullInvocationId, IngressId, PartitionId};
use restate_types::invocation::{InvocationResponse, ResponseResult, ServiceInvocation};
use std::ops::Range;

/// Types of outbox messages.
#[derive(Debug, Clone, PartialEq)]
pub enum OutboxMessage {
    /// Service invocation to send to another partition processor
    ServiceInvocation(ServiceInvocation),

    /// Service response to sent to another partition processor
    ServiceResponse(InvocationResponse),

    /// Service response to send to an ingress as a response to an external client request
    IngressResponse {
        ingress_id: IngressId,
        full_invocation_id: FullInvocationId,
        response: ResponseResult,
    },
}

pub trait OutboxTable {
    fn add_message(
        &mut self,
        partition_id: PartitionId,
        message_index: u64,
        outbox_message: OutboxMessage,
    ) -> PutFuture;

    fn get_next_outbox_message(
        &mut self,
        partition_id: PartitionId,
        next_sequence_number: u64,
    ) -> GetFuture<Option<(u64, OutboxMessage)>>;

    fn truncate_outbox(
        &mut self,
        partition_id: PartitionId,
        seq_to_truncate: Range<u64>,
    ) -> PutFuture;
}
