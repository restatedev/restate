// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use restate_types::identifiers::InvocationId;
use restate_types::invocation::ResponseResult;
use restate_types::sharding::KeyRange;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

pub trait ReadOutputTable {
    fn get_output(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<Option<ResponseResult>>> + Send;
}

impl PartitionStoreProtobufValue for ResponseResult {
    type ProtobufType = crate::protobuf_types::v1::ResponseResult;
}

#[derive(Debug, Clone)]
pub enum ScanOutputTableRange {
    PartitionKey(KeyRange),
    InvocationId(RangeInclusive<InvocationId>),
}

pub trait ScanOutputTable {
    fn for_each_output<
        F: FnMut((InvocationId, ResponseResult)) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: ScanOutputTableRange,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait WriteOutputTable {
    fn put_output(&mut self, invocation_id: &InvocationId, result: &ResponseResult) -> Result<()>;

    fn delete_output(&mut self, invocation_id: &InvocationId) -> Result<()>;
}
