// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::{MessageRouterBuilder, Networking};
use restate_core::{Metadata, TaskCenter, TaskKind};
use restate_types::config::Configuration;
use restate_types::live::Live;

use crate::error::LogServerBuildError;
use crate::metric_definitions::describe_metrics;

pub struct LogServerService {
    _updateable_config: Live<Configuration>,
    task_center: TaskCenter,
    _metadata: Metadata,
}

impl LogServerService {
    pub async fn create(
        updateable_config: Live<Configuration>,
        task_center: TaskCenter,
        metadata: Metadata,
        _router_builder: &mut MessageRouterBuilder,
        _networking: Networking,
    ) -> Result<Self, LogServerBuildError> {
        describe_metrics();

        Ok(Self {
            _updateable_config: updateable_config,
            task_center,
            _metadata: metadata,
        })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let tc = self.task_center.clone();
        tc.spawn(TaskKind::SystemService, "log-server", None, async {
            self.run().await
        })?;

        Ok(())
    }

    async fn run(self) -> anyhow::Result<()> {
        Ok(())
    }
}
