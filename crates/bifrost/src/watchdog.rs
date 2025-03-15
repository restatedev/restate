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
use tokio::task::JoinSet;
use tracing::{debug, trace, warn};

use restate_core::metadata_store::{ReadWriteError, retry_on_retryable_error};
use restate_core::{TaskCenter, TaskKind, cancellation_watcher};
use restate_metadata_server::ReadModifyWriteError;
use restate_types::config::Configuration;
use restate_types::logs::metadata::{Logs, ProviderKind};
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;

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
    sender: WatchdogSender,
    inbound: WatchdogReceiver,
    live_providers: Vec<Arc<dyn LogletProvider>>,
}

impl Watchdog {
    pub fn new(
        inner: Arc<BifrostInner>,
        sender: WatchdogSender,
        inbound: WatchdogReceiver,
    ) -> Self {
        Self {
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
                let _ = TaskCenter::spawn(
                    TaskKind::MetadataBackgroundSync,
                    "bifrost-metadata-sync",
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
                let _ = TaskCenter::spawn(
                    TaskKind::BifrostBackgroundHighPriority,
                    "bifrost-provider-on-start",
                    async move {
                        provider.post_start().await;
                        Ok(())
                    },
                );
            }
            WatchdogCommand::LogTrimmed {
                log_id,
                requested_trim_point,
            } => {
                self.on_log_trim(log_id, requested_trim_point);
            }
        }
    }

    fn on_log_trim(&self, log_id: LogId, requested_trim_point: Lsn) {
        let bifrost = self.inner.clone();
        let _ = TaskCenter::spawn_child(
            TaskKind::BifrostBackgroundLowPriority,
            format!("trim-chain-{log_id}"),
            async move {
                let trim_point = bifrost
                    .get_trim_point(log_id)
                    .await
                    .context("cannot determine tail")?;
                if trim_point == Lsn::INVALID {
                    return Ok(());
                }
                // todo(asoli): Notify providers about trimmed loglets for pruning.
                let opts = Configuration::pinned();
                retry_on_retryable_error(opts.common.network_error_retry_policy.clone(), || {
                    trim_chain_if_needed(&bifrost, log_id, requested_trim_point, trim_point)
                })
                .await
                .with_context(|| {
                    format!(
                        "failed trimming the log chain after trimming log {} to trim-point {}",
                        log_id, trim_point
                    )
                })?;
                Ok(())
            },
        );
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
            providers
                .build_task()
                .name("shutdown-loglet-provider")
                .spawn(async move { provider.shutdown().await })
                .expect("to spawn provider shutdown");
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

#[derive(Debug)]
struct AlreadyTrimmed;

async fn trim_chain_if_needed(
    bifrost: &BifrostInner,
    log_id: LogId,
    requested_trim_point: Lsn,
    actual_trim_point: Lsn,
) -> Result<(), ReadWriteError> {
    let new_logs = bifrost
        .metadata_writer
        .metadata_store_client()
        .read_modify_write(BIFROST_CONFIG_KEY.clone(), |current: Option<Logs>| {
            let logs = current.expect("logs should be initialized by BifrostService");
            let mut logs_builder = logs.into_builder();
            let mut chain_builder = logs_builder.chain(log_id).expect("log id exists");

            // trim_prefix's lsn is exclusive. Trim-point is inclusive of the last trimmed lsn,
            // therefore, we need to trim _including_ the trim point.
            chain_builder.trim_prefix(actual_trim_point.next());
            let Some(logs) = logs_builder.build_if_modified() else {
                // already trimmed, nothing to be done.
                return Err(AlreadyTrimmed);
            };
            Ok(logs)
        })
        .await;
    match new_logs {
        Ok(logs) => {
            bifrost.metadata_writer.submit(Arc::new(logs));
            debug!(
                "Log {} chain has been trimmed to trim-point {} after requesting trim to {}",
                log_id, actual_trim_point, requested_trim_point,
            );
        }
        Err(ReadModifyWriteError::FailedOperation(AlreadyTrimmed)) => {
            // nothing to do
        }
        Err(ReadModifyWriteError::ReadWrite(err)) => return Err(err),
    };
    Ok(())
}

pub enum WatchdogCommand {
    /// Request to sync metadata if the client believes that it's outdated.
    /// i.e. attempting to write to a sealed segment.
    #[allow(dead_code)]
    ScheduleMetadataSync,
    WatchProvider(Arc<dyn LogletProvider>),
    LogTrimmed {
        log_id: LogId,
        /// NOTE: This is **not** the actual trim point, this could easily be Lsn::MAX (legal)
        /// Only used for logging, never use this value as an authoritative trim-point.
        requested_trim_point: Lsn,
    },
}
