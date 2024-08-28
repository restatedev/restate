// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: remove after scaffolding is complete
#![allow(unused)]

use restate_core::cancellation_watcher;
use restate_core::network::{MessageRouterBuilder, NetworkSender};
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::nodes_config::StorageState;

use crate::logstore::LogStore;

pub struct RequestPump {
    _configuration: Live<Configuration>,
}

impl RequestPump {
    pub fn new(
        mut configuration: Live<Configuration>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        Self {
            _configuration: configuration,
        }
    }
    /// Starts the main processing loop, exits on error or shutdown.
    pub async fn run<N: NetworkSender + 'static, S: LogStore>(
        mut self,
        _networking: N,
        _log_store: S,
        _storage_state: StorageState,
    ) -> anyhow::Result<()> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());
        // todo: remove after fleshing out
        #[allow(clippy::never_loop)]
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }
}
