// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Result;
use restate_types::identifiers::{InvocationId, ServiceId};
use std::future::Future;

#[derive(Debug, Default, Clone, PartialEq)]
pub enum VirtualObjectStatus {
    Locked(InvocationId),
    #[default]
    Unlocked,
}

pub trait ReadOnlyVirtualObjectStatusTable {
    fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<VirtualObjectStatus>> + Send;
}

pub trait VirtualObjectStatusTable: ReadOnlyVirtualObjectStatusTable {
    fn put_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
        status: VirtualObjectStatus,
    ) -> impl Future<Output = ()> + Send;

    fn delete_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = ()> + Send;
}
