// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::CodedError;

use restate_core::TaskCenter;
use restate_log_server::LogServerService;
use restate_types::config::UpdateableConfiguration;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum LogServerRoleBuildError {
    #[error("unknown")]
    #[code(unknown)]
    Unknown,
    #[error("failed building the admin service: {0}")]
    #[code(unknown)]
    AdminService(#[from] restate_admin::service::BuildError),
    #[error("failed building the service client: {0}")]
    #[code(unknown)]
    ServiceClient(#[from] restate_service_client::BuildError),
}

pub struct LogServerRole {
    updateable_config: UpdateableConfiguration,
    service: LogServerService,
}

impl LogServerRole {
    pub async fn create(
        task_center: TaskCenter,
        updateable_config: UpdateableConfiguration,
        metadata: Metadata,
        networking: Networking,
        metadata_writer: MetadataWriter,
        router_builder: &mut MessageRouterBuilder,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, AdminRoleBuildError> {
        let service = LogServerService
    }
}
