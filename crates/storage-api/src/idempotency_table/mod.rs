// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Result;

use futures_util::Stream;
use restate_types::identifiers::{IdempotencyId, InvocationId, PartitionKey};
use std::future::Future;
use std::ops::RangeInclusive;

#[derive(Debug, Clone, PartialEq)]
pub struct IdempotencyMetadata {
    pub invocation_id: InvocationId,
}

pub trait ReadOnlyIdempotencyTable {
    fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> impl Future<Output = Result<Option<IdempotencyMetadata>>> + Send;

    fn all_idempotency_metadata(
        &mut self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(IdempotencyId, IdempotencyMetadata)>> + Send;
}

pub trait IdempotencyTable: ReadOnlyIdempotencyTable {
    fn put_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
        metadata: IdempotencyMetadata,
    ) -> impl Future<Output = ()> + Send;

    fn delete_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> impl Future<Output = ()> + Send;
}
