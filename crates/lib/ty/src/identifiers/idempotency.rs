// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;

use super::InvocationId;
use crate::invocation::InvocationTarget;
use crate::partitions::{PartitionKey, WithPartitionKey, deterministic_partition_key};

#[derive(Eq, Hash, PartialEq, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IdempotencyId {
    /// Identifies the invoked service
    pub service_name: ByteString,
    /// Service key, if any
    pub service_key: Option<ByteString>,
    /// Identifies the invoked service handler
    pub service_handler: ByteString,
    /// The user supplied idempotency_key
    pub idempotency_key: ByteString,

    pub(super) partition_key: PartitionKey,
}

impl IdempotencyId {
    pub fn new(
        service_name: ByteString,
        service_key: Option<ByteString>,
        service_handler: ByteString,
        idempotency_key: ByteString,
    ) -> Self {
        // The ownership model for idempotent invocations is the following:
        //
        // * For services without key, the partition key is the hash(idempotency key).
        //   This makes sure that for a given idempotency key and its scope, we always land in the same partition.
        // * For services with key, the partition key is the hash(service key), this due to the virtual object locking requirement.
        let partition_key = deterministic_partition_key(
            service_key.as_ref().map(|bs| bs.as_ref()),
            Some(&idempotency_key),
        )
        .expect("A deterministic partition key can always be generated for idempotency id");

        Self {
            service_name,
            service_key,
            service_handler,
            idempotency_key,
            partition_key,
        }
    }

    pub fn combine(
        invocation_id: InvocationId,
        invocation_target: &InvocationTarget,
        idempotency_key: ByteString,
    ) -> Self {
        IdempotencyId {
            service_name: invocation_target.service_name().clone(),
            service_key: invocation_target.key().cloned(),
            service_handler: invocation_target.handler_name().clone(),
            idempotency_key,
            partition_key: invocation_id.partition_key(),
        }
    }
}

impl WithPartitionKey for IdempotencyId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}
