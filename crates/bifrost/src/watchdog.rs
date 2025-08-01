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

use ahash::{HashMap, HashMapExt};
use enum_map::Enum;
use futures::future::OptionFuture;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, trace, warn};

use restate_core::{
    Metadata, MetadataWriter, ShutdownError, TaskCenter, TaskCenterFutureExt, TaskHandle, TaskKind,
    cancellation_watcher,
};
use restate_types::config::Configuration;
use restate_types::logs::metadata::{Logs, ProviderKind, SegmentIndex};
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::retries::with_jitter;

use crate::BifrostAdmin;
use crate::bifrost::BifrostInner;
use crate::log_chain_writer::{LogChainCommand, LogChainWriter};
use crate::loglet::{Improvement, LogletProvider};

pub type WatchdogSender = mpsc::UnboundedSender<WatchdogCommand>;
type WatchdogReceiver = mpsc::UnboundedReceiver<WatchdogCommand>;

const IMPROVEMENT_ROUND_INTERVAL: Duration = Duration::from_secs(1);
// this duration is jitter-ed with +/- 50% to splay updates
const IMPROVEMENT_ACTION_AFTER: Duration = Duration::from_secs(5);

pub enum WatchdogCommand {
    WatchProvider(Arc<dyn LogletProvider>),
    /// A log appender running on this node has indicated that it's the preferred writer
    PreferenceAcquire(LogId),
    /// Indicating that a preference token has been dropped
    PreferenceRelease(LogId),
    ChainCommand(LogChainCommand),
    LogTrimmed {
        log_id: LogId,
        /// NOTE: This is **not** the actual trim point, this could easily be Lsn::MAX (legal)
        /// Only used for logging, never use this value as an authoritative trim-point.
        requested_trim_point: Lsn,
    },
}

/// The watchdog is a task manager for background jobs that needs to run on bifrost.
/// tasks managed by the watchdogs are cooperative and should not be terminated abruptly.
///
/// Tasks are expected to check for the cancellation token when appropriate and finalize their
/// work before termination.
pub struct Watchdog {
    inner: Arc<BifrostInner>,
    chain_writer_tx: mpsc::UnboundedSender<LogChainCommand>,
    chain_writer_task: TaskHandle<()>,
    inbound: WatchdogReceiver,
    live_providers: Vec<Arc<dyn LogletProvider>>,
    in_flight_trim: Option<restate_core::task_center::TaskHandle<()>>,
    my_preferred_logs: HashMap<LogId, PreferredLog>,
    pending_trims: TrimRequests,
}

struct PreferredLog {
    /// the number of appender tasks that have indicated that they are the preferred writer
    ref_cnt: usize,
    /// the last time we checked this log for improvement
    last_checked: Instant,
    in_flight: Option<(SegmentIndex, TaskHandle<()>)>,
}

impl Default for PreferredLog {
    fn default() -> Self {
        Self {
            ref_cnt: 1,
            last_checked: Instant::now(),
            in_flight: None,
        }
    }
}

type TrimRequests = HashMap<LogId, Lsn>;

impl Watchdog {
    pub fn start(
        inner: Arc<BifrostInner>,
        inbound: WatchdogReceiver,
        metadata_writer: MetadataWriter,
    ) -> anyhow::Result<()> {
        let (chain_writer_tx, chain_writer_task) = LogChainWriter::start(metadata_writer)?;

        let watchdog = Self {
            inner,
            chain_writer_tx,
            chain_writer_task,
            inbound,
            live_providers: Vec::with_capacity(ProviderKind::LENGTH),
            in_flight_trim: None,
            pending_trims: HashMap::with_capacity(128),
            my_preferred_logs: HashMap::default(),
        };

        TaskCenter::spawn(
            TaskKind::BifrostWatchdog,
            "bifrost-watchdog",
            watchdog.run(),
        )?;
        Ok(())
    }

    fn handle_command(&mut self, cmd: WatchdogCommand) {
        match cmd {
            WatchdogCommand::ChainCommand(cmd) => {
                if let Err(e) = self.chain_writer_tx.send(cmd) {
                    let cmd = e.0;
                    warn!(
                        ?cmd,
                        "Failed to send command to log-chain writer after it has stopped"
                    );
                }
            }
            WatchdogCommand::PreferenceAcquire(log_id) => {
                self.my_preferred_logs
                    .entry(log_id)
                    .and_modify(|l| l.ref_cnt += 1)
                    .or_default();
            }
            WatchdogCommand::PreferenceRelease(log_id) => {
                match self.my_preferred_logs.entry(log_id) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        let current = entry.get_mut();
                        current.ref_cnt = current.ref_cnt.saturating_sub(1);
                        if current.ref_cnt == 0 {
                            entry.remove();
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(_) => {}
                }
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
                self.store_trim_request(log_id, requested_trim_point);
            }
        }
    }

    fn spawn_trim(&self, mut trim_requests: TrimRequests) -> Result<TaskHandle<()>, ShutdownError> {
        let bifrost = self.inner.clone();
        let weak_writer_tx = self.chain_writer_tx.downgrade();
        TaskCenter::spawn_unmanaged_child(
            TaskKind::BifrostBackgroundLowPriority,
            "trim-chains",
            async move {
                if trim_requests.is_empty() {
                    return;
                }

                // Concurrently look up the trim points of all the requests
                let trim_point_futures: JoinSet<_> = trim_requests
                    .drain()
                    .map(|(log_id, requested_trim_point)| {
                        let bifrost = bifrost.clone();
                        let weak_writer_tx = weak_writer_tx.clone();
                        async move {
                            match bifrost.get_trim_point(log_id).await {
                                // an invalid lsn means that the log has never been trimmed
                                Ok(actual_trim_point) if actual_trim_point != Lsn::INVALID => {
                                    debug!(
                                        "Log {} chain has been trimmed to trim-point {} after requesting trim to {}",
                                        log_id,
                                        actual_trim_point,
                                        requested_trim_point,
                                    );
                                    if let Some(tx) =  weak_writer_tx.upgrade() {
                                        let _ = tx.send(LogChainCommand::trim_prefix(log_id, actual_trim_point));
                                    }
                                },
                                Ok(_) => {},
                                Err(err) => {
                                    warn!(
                                        "Bifrost watchdog failed to find a trim point for {log_id}; will \
                                        not be able to process the request to trim chain to {requested_trim_point}: {err}"
                                    );
                                },
                            }
                        }.in_current_tc()
                    })
                    .collect();

                trim_point_futures.join_all().await;
            },
        )
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);
        trace!("Bifrost watchdog started");

        let mut improvement_interval = tokio::time::interval(IMPROVEMENT_ROUND_INTERVAL);
        let mut logs = Metadata::with_current(|m| m.updateable_logs_metadata());
        let mut config = Configuration::live();

        loop {
            if self.in_flight_trim.is_none()
                && !self.pending_trims.is_empty()
                && !TaskCenter::is_shutdown_requested()
            {
                let trims = self.pending_trims.drain().collect();
                match self.spawn_trim(trims) {
                    Ok(task_handle) => self.in_flight_trim = Some(task_handle),
                    Err(ShutdownError) => {
                        // ignore in-flight trims if task-center is shutting down.
                        // we don't want to prematurely shutdown the watchdog as we
                        // might need it to continue running "during" shutdown.
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
                _tick = improvement_interval.tick() => {
                    if !config.live_load().bifrost.disable_auto_improvement
                        && TaskCenter::is_my_node_alive()
                        && !TaskCenter::is_shutdown_requested() {
                            // check if we have logs to improve
                            let logs = logs.live_load();
                            self.improve_logs(logs);
                    }
                }
                Some(_) = OptionFuture::from(self.in_flight_trim.as_mut()) => {
                    self.in_flight_trim = None;
                }
            }
        }
        Ok(())
    }

    /// Checks if we have logs with preferred writers running being local and reconfigures them for
    /// better performance/placement if needed.
    ///
    /// This will only take action over 1/3 of the preferred logs at a time to reduce
    /// the chances of stomping.
    fn improve_logs(&mut self, logs: &Logs) {
        let allowed_to_action = self.my_preferred_logs.len().div_ceil(3);
        let mut actioned: Vec<LogId> = Vec::default();
        for (log_id, preferred) in self.my_preferred_logs.iter_mut() {
            debug_assert!(preferred.ref_cnt > 0);
            if preferred.last_checked.elapsed() < with_jitter(IMPROVEMENT_ACTION_AFTER, 0.5) {
                continue;
            }
            let Some(chain) = logs.chain(log_id) else {
                // check again later
                preferred.last_checked = Instant::now();
                continue;
            };

            let provider_config = &logs.configuration().default_provider;
            let tail_segment = chain.tail();
            let segment_index = tail_segment.index();
            let current_provider = tail_segment.config.kind;

            // Abort the task if it has not finished and the tail segment has moved beyond what the
            // task is about.
            //
            // if there is a task in-flight, we let it continue and we touch the checked instant
            // since we don't need to do anything here.
            if let Some((in_flight_segment_index, in_flight_task)) = &preferred.in_flight {
                if *in_flight_segment_index < segment_index {
                    // the task is outdated, we can cancel it
                    in_flight_task.abort();
                    preferred.in_flight = None;
                } else if !in_flight_task.is_finished() {
                    // check again later
                    preferred.last_checked = Instant::now();
                    continue;
                } else if in_flight_task.is_finished() {
                    preferred.in_flight = None;
                    // reset the check time to slow down reaction time
                    preferred.last_checked = Instant::now();
                    continue;
                }
            }

            let may_improve = if current_provider.is_seal_marker() {
                // Do not improve logs with sealed chains. Those will be reconfigured on new
                // appends.
                Improvement::None
            } else if current_provider != provider_config.kind() {
                Improvement::Possible {
                    reason: format!(
                        "provider change from {current_provider} to {}",
                        provider_config.kind(),
                    ),
                }
            } else {
                // same provider and current is not a seal marker.
                let current_params = &tail_segment.config.params;
                let Ok(provider) = self.inner.provider_for(provider_config.kind()) else {
                    // check again later
                    preferred.last_checked = Instant::now();
                    continue;
                };
                match provider.may_improve_params(*log_id, current_params, provider_config) {
                    Ok(improvement) => improvement,
                    Err(err) => {
                        debug!(
                            log_id = %log_id,
                            %segment_index,
                            %err,
                            "[Auto Improvement] Bifrost watchdog failed to check if the log can be improved",
                        );
                        Improvement::None
                    }
                }
            };

            preferred.last_checked = Instant::now();
            if let Improvement::Possible { reason } = may_improve {
                actioned.push(*log_id);
                debug!(
                    log_id = %log_id,
                    %segment_index,
                    "[Auto Improvement] Bifrost will reconfigure the log because {reason}"
                );
                let Ok(task) =
                    TaskCenter::spawn_unmanaged(TaskKind::Disposable, "seal-for-improvement", {
                        let log_id = *log_id;
                        let bifrost = Arc::clone(&self.inner);
                        async move {
                            let _ = BifrostAdmin::new(&bifrost)
                                .seal_and_auto_extend_chain(log_id, Some(segment_index))
                                .await;
                        }
                    })
                else {
                    // we are shutting down, there is no point in continuing
                    break;
                };

                preferred.in_flight = Some((segment_index, task));
            }
            // only action on 1/3 of the logs in every round
            if actioned.len() >= allowed_to_action {
                break;
            }
        }

        if !actioned.is_empty() {
            info!(
                "[Auto Improvement] Bifrost will reconfigure logs to improve placement. logs={:?}",
                actioned
            );
        }
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
        drop(self.chain_writer_tx);

        debug!("Waiting for log-chain writer to shutdown");
        let _ = self.chain_writer_task.await;
        debug!("Bifrost watchdog shutdown complete");
    }
}
