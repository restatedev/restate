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
use restate_types::identifiers::{InvocationUuid, ServiceId};
use std::future::Future;

#[derive(Debug, Default, Clone, PartialEq)]
pub enum ServiceStatus {
    Locked(InvocationUuid),
    #[default]
    Unlocked,
}

pub trait ReadOnlyServiceStatusTable {
    fn get_service_status(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<ServiceStatus>> + Send;
}

pub trait ServiceStatusTable: ReadOnlyServiceStatusTable {
    fn put_service_status(
        &mut self,
        service_id: &ServiceId,
        status: ServiceStatus,
    ) -> impl Future<Output = ()> + Send;

    fn delete_service_status(&mut self, service_id: &ServiceId) -> impl Future<Output = ()> + Send;
}
