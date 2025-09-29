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

use restate_types::identifiers::{InvocationId, PartitionKey, ServiceId};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

#[derive(Debug, Default, Clone, PartialEq)]
pub enum VirtualObjectStatus {
    Locked(InvocationId),
    #[default]
    Unlocked,
}

impl PartitionStoreProtobufValue for VirtualObjectStatus {
    type ProtobufType = crate::protobuf_types::v1::VirtualObjectStatus;
}

pub trait ReadVirtualObjectStatusTable {
    fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<VirtualObjectStatus>> + Send;
}

pub trait ScanVirtualObjectStatusTable {
    fn for_each_virtual_object_status<
        F: FnMut((ServiceId, VirtualObjectStatus)) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait WriteVirtualObjectStatusTable {
    fn put_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
        status: &VirtualObjectStatus,
    ) -> Result<()>;

    fn delete_virtual_object_status(&mut self, service_id: &ServiceId) -> Result<()>;
}
