// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use enum_map::Enum;
use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, trace, warn};

use restate_core::{
    ShutdownError, TaskCenter, TaskCenterFutureExt, TaskHandle, TaskKind, cancellation_watcher,
};
use restate_metadata_store::{ReadModifyWriteError, ReadWriteError, retry_on_retryable_error};
use restate_types::config::Configuration;
use restate_types::logs::metadata::{Logs, ProviderKind};
use restate_types::logs::{LogId, Lsn, SequenceNumber};

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
    in_flight_trim: Option<restate_core::task_center::TaskHandle<()>>,
    pending_trims: TrimRequests,
}

type TrimRequests = HashMap<LogId, Lsn>;

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
            in_flight_trim: None,
            pending_trims: HashMap::with_capacity(128),
        }
    }

    fn handle_command(&mut self, cmd: WatchdogCommand) {
        match cmd {
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
                self.store_trim_request(log_id, requested_trim_point);
            }
        }
    }

    fn spawn_trim(&self, mut trim_requests: TrimRequests) -> Result<TaskHandle<()>, ShutdownError> {
        let bifrost = self.inner.clone();
        TaskCenter::spawn_unmanaged(
            TaskKind::BifrostBackgroundLowPriority,
            "trim-chains",
            async move {
                if trim_requests.is_empty() {
                    return;
                }

                // Concurrently look up the trim points of all the requests
                let trim_point_futures: FuturesUnordered<_> = trim_requests
                    .drain()
                    .map(async |(log_id, requested_trim_point)| {
                        match bifrost.get_trim_point(log_id).await {
                            // an invalid lsn means that the log has never been trimmed
                            Ok(actual_trim_point) if actual_trim_point == Lsn::INVALID => None,
                            Ok(actual_trim_point) => Some(TrimPoint {
                                log_id,
                                requested_trim_point,
                                actual_trim_point,
                            }),
                            Err(err) => {
                                warn!(
                                    "Bifrost watchdog failed to find a trim point for {log_id}; will \
                                    not be able to process the request to trim chain to {requested_trim_point}: {err}"
                                );
                                None
                            },
                        }
                    })
                    .collect();

                let trim_points: Vec<TrimPoint> = trim_point_futures
                    .filter_map(|trim_point| trim_point)
                    .collect()
                    .await;

                if trim_points.is_empty() {
                    return;
                }

                let retry_policy = Configuration::pinned()
                    .common
                    .network_error_retry_policy
                    .clone();

                // todo(asoli): Notify providers about trimmed loglets for pruning.
                if let Err(err) = retry_on_retryable_error(retry_policy, || {
                    trim_chains_if_needed(&bifrost, &trim_points)
                })
                .await
                {
                    warn!("Bifrost watchdog trim chains failed: {err}",);
                }
            },
        )
    }

    pub fn sender(&self) -> WatchdogSender {
        self.sender.clone()
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);
        trace!("Bifrost watchdog started");

        loop {
            if self.in_flight_trim.is_none() && !self.pending_trims.is_empty() {
                let trims = self.pending_trims.drain().collect();
                match self.spawn_trim(trims) {
                    Ok(task_handle) => self.in_flight_trim = Some(task_handle),
                    Err(ShutdownError) => {
                        self.shutdown().await;
                        break;
                    }
                }
            }

            tokio::select! {
            biased;
            _ = &mut shutdown => {
                self.shutdown().await;
                break;
            }
            Some(cmd) = self.inbound.recv() => {
                self.handle_command(cmd)
            }
            Some(_) = OptionFuture::from(self.in_flight_trim.as_mut()) => {
                self.in_flight_trim = None;
            }
            }
        }
        Ok(())
    }

    fn store_trim_request(&mut self, log_id: LogId, requested_trim_point: Lsn) {
        self.pending_trims
            .entry(log_id)
            .and_modify(|lsn| {
                if *lsn < requested_trim_point {
                    *lsn = requested_trim_point;
                }
            })
            .or_insert(requested_trim_point);
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
                .spawn(async move { provider.shutdown().await }.in_current_tc())
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

struct TrimPoint {
    log_id: LogId,
    requested_trim_point: Lsn,
    actual_trim_point: Lsn,
}

#[derive(Debug)]
struct AlreadyTrimmed;

async fn trim_chains_if_needed(
    bifrost: &BifrostInner,
    trim_points: &[TrimPoint],
) -> Result<(), ReadWriteError> {
    let new_logs = bifrost
        .metadata_writer
        .global_metadata()
        .read_modify_write(|current: Option<Arc<Logs>>| {
            let logs = current.expect("logs should be initialized by BifrostService");
            let mut logs_builder = logs.as_ref().clone().into_builder();

            for trim_point in trim_points {
                let mut chain_builder = logs_builder
                    .chain(trim_point.log_id)
                    .expect("log id exists");

                // trim_prefix's lsn is exclusive. Trim-point is inclusive of the last trimmed lsn,
                // therefore, we need to trim _including_ the trim point.
                chain_builder.trim_prefix(trim_point.actual_trim_point.next());
            }

            let Some(logs) = logs_builder.build_if_modified() else {
                // already trimmed, nothing to be done.
                return Err(AlreadyTrimmed);
            };

            Ok(logs)
        })
        .await;
    match new_logs {
        Ok(_) => {
            for trim_point in trim_points {
                debug!(
                    "Log {} chain has been trimmed to trim-point {} after requesting trim to {}",
                    trim_point.log_id,
                    trim_point.actual_trim_point,
                    trim_point.requested_trim_point,
                );
            }
        }
        Err(ReadModifyWriteError::FailedOperation(AlreadyTrimmed)) => {
            // nothing to do
        }
        Err(ReadModifyWriteError::ReadWrite(err)) => return Err(err),
    };
    Ok(())
}

pub enum WatchdogCommand {
    WatchProvider(Arc<dyn LogletProvider>),
    LogTrimmed {
        log_id: LogId,
        /// NOTE: This is **not** the actual trim point, this could easily be Lsn::MAX (legal)
        /// Only used for logging, never use this value as an authoritative trim-point.
        requested_trim_point: Lsn,
    },
}
