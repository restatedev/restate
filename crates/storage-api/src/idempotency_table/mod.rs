// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::ops::RangeInclusive;

use restate_types::identifiers::{IdempotencyId, InvocationId, PartitionKey};

use super::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

#[derive(Debug, Clone, PartialEq)]
pub struct IdempotencyMetadata {
    pub invocation_id: InvocationId,
}

impl PartitionStoreProtobufValue for IdempotencyMetadata {
    type ProtobufType = crate::protobuf_types::v1::IdempotencyMetadata;
}

pub trait ReadOnlyIdempotencyTable {
    fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> impl Future<Output = Result<Option<IdempotencyMetadata>>> + Send;
}

pub trait ScanIdempotencyTable {
    fn for_each_idempotency_metadata<
        F: FnMut((IdempotencyId, IdempotencyMetadata)) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait IdempotencyTable: ReadOnlyIdempotencyTable {
    fn put_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
        metadata: &IdempotencyMetadata,
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> impl Future<Output = Result<()>> + Send;
}
