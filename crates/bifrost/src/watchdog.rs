// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use anyhow::Context;
use enum_map::Enum;
use restate_core::{cancellation_watcher, TaskCenter, TaskKind};
use restate_types::logs::metadata::ProviderKind;
use tokio::task::JoinSet;
use tracing::{debug, trace, warn};

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
    task_center: TaskCenter,
    inner: Arc<BifrostInner>,
    sender: WatchdogSender,
    inbound: WatchdogReceiver,
    live_providers: Vec<Arc<dyn LogletProvider>>,
}

impl Watchdog {
    pub fn new(
        task_center: TaskCenter,
        inner: Arc<BifrostInner>,
        sender: WatchdogSender,
        inbound: WatchdogReceiver,
    ) -> Self {
        Self {
            task_center,
            inner,
            sender,
            inbound,
            live_providers: Vec::with_capacity(ProviderKind::LENGTH),
        }
    }

    fn handle_command(&mut self, cmd: WatchdogCommand) {
        match cmd {
            WatchdogCommand::ScheduleMetadataSync => {
                let bifrost = self.inner.clone();
                let _ = self.task_center.spawn(
                    TaskKind::MetadataBackgroundSync,
                    "bifrost-metadata-sync",
                    None,
                    async move {
                        bifrost
                            .sync_metadata()
                            .await
                            .context("Bifrost background metadata sync")
                    },
                );
            }

            WatchdogCommand::WatchProvider(provider) => {
                self.live_providers.push(provider.clone());
                // TODO: Convert to a managed background task
                let _ = self.task_center.spawn(
                    TaskKind::BifrostBackgroundHighPriority,
                    "bifrost-provider-on-start",
                    None,
                    async move {
                        provider.post_start().await;
                        Ok(())
                    },
                );
            }
        }
    }

    pub fn sender(&self) -> WatchdogSender {
        self.sender.clone()
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);
        trace!("Bifrost watchdog started");

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
        trace!("Bifrost watchdog shutdown started");
        // Stop accepting new commands
        self.inner.set_shutdown();
        self.inbound.close();
        trace!("Draining bifrost tasks");

        // Consume buffered commands
        let mut i = 0;
        while let Some(cmd) = self.inbound.recv().await {
            i += 1;
            self.handle_command(cmd)
        }
        trace!("Bifrost drained {i} commands due to an on-going shutdown");
        // Ask all tasks to shutdown
        // Stop all live providers.
        trace!("Shutting down live bifrost providers");
        let mut providers = JoinSet::new();
        for provider in self.live_providers {
            providers.spawn(async move { provider.shutdown().await });
        }

        debug!(
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
        debug!("Bifrost watchdog shutdown complete");
    }
}

pub enum WatchdogCommand {
    /// Request to sync metadata if the client believes that it's outdated.
    /// i.e. attempting to write to a sealed segment.
    #[allow(dead_code)]
    ScheduleMetadataSync,
    WatchProvider(Arc<dyn LogletProvider>),
}
