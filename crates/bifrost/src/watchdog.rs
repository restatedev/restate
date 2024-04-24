// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use enum_map::Enum;
use restate_core::cancellation_watcher;
use restate_types::logs::metadata::ProviderKind;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::bifrost::BifrostInner;
use crate::loglet::LogletProvider;

pub type WatchdogSender = tokio::sync::mpsc::UnboundedSender<WatchdogCommand>;
type WatchdogReceiver = tokio::sync::mpsc::UnboundedReceiver<WatchdogCommand>;

/// The watchdog is a task manager for background jobs that needs to run on bifrost.
/// tasks managed by the watchdogs are cooperative and should not be terminated abruptly.
///
/// Tasks are expected to check for the cancellation token when appropriate and finalize their
/// work before termination.
pub struct Watchdog {
    inner: Arc<BifrostInner>,
    inbound: WatchdogReceiver,
    live_providers: Vec<Arc<dyn LogletProvider>>,
}

impl Watchdog {
    pub fn new(inner: Arc<BifrostInner>, inbound: WatchdogReceiver) -> Self {
        Self {
            inner,
            inbound,
            live_providers: Vec::with_capacity(ProviderKind::LENGTH),
        }
    }

    fn handle_command(&mut self, cmd: WatchdogCommand) {
        match cmd {
            WatchdogCommand::ScheduleMetadataSync => {
                // TODO: Convert to a managed background task
                tokio::spawn({
                    let bifrost = self.inner.clone();
                    async move {
                        let _ = bifrost.sync_metadata().await;
                    }
                });
            }

            WatchdogCommand::WatchProvider(provider) => {
                self.live_providers.push(provider.clone());
            }
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);
        debug!("Bifrost watchdog started");

        loop {
            tokio::select! {
            biased;
            _ = &mut shutdown => {
                self.shutdown().await;
                break;
            }
            Some(cmd) = self.inbound.recv() => {
                self.handle_command(cmd)
            }
            }
        }
        Ok(())
    }

    async fn shutdown(mut self) {
        let shutdown_timeout = Duration::from_secs(5);
        debug!("Bifrost watchdog shutdown started");
        // Stop accepting new commands
        self.inner.set_shutdown();
        self.inbound.close();
        debug!("Draining bifrost tasks");

        // Consume buffered commands
        let mut i = 0;
        while let Some(cmd) = self.inbound.recv().await {
            i += 1;
            self.handle_command(cmd)
        }
        debug!("Bifrost drained {i} commands due to an on-going shutdown");
        // Ask all tasks to shutdown
        // Stop all live providers.
        info!("Shutting down live bifrost providers");
        let mut providers = JoinSet::new();
        for provider in self.live_providers {
            providers.spawn(async move { provider.shutdown().await });
        }

        info!(
            "Waiting {:?} for bifrost providers to shutdown cleanly...",
            shutdown_timeout
        );
        if (tokio::time::timeout(shutdown_timeout, async {
            while let Some(res) = providers.join_next().await {
                if let Err(e) = res {
                    warn!("Bifrost provider failed on shutdown: {:?}", e);
                }
            }
        })
        .await)
            .is_err()
        {
            warn!(
                "Timed out shutting down {} bifrost providers!",
                providers.len()
            );
            providers.shutdown().await;
        }
        info!("Bifrost watchdog shutdown complete");
    }
}

pub enum WatchdogCommand {
    /// Request to sync metadata if the client believes that it's outdated.
    /// i.e. attempting to write to a sealed segment.
    #[allow(dead_code)]
    ScheduleMetadataSync,
    WatchProvider(Arc<dyn LogletProvider>),
}
