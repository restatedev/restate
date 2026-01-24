// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use ulid::Ulid;

use restate_core::network::{ServiceMessage, Verdict};
use restate_invoker_impl::ChannelStatusReader;
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, ReplayStatus, RunMode};
use restate_types::identifiers::{LeaderEpoch, PartitionKey};
use restate_types::net::partition_processor::PartitionLeaderService;

use crate::partition::{LeadershipInfo, TargetLeaderState};

pub type LeaderEpochToken = Ulid;

#[derive(Clone, Debug, PartialEq)]
pub enum LeaderState {
    Leader(LeaderEpoch),
    AwaitingLeaderEpoch(LeaderEpochToken),
    Follower,
}

#[derive(Debug)]
pub enum ProcessorState {
    Starting {
        target_run_mode: RunMode,
        start_time: Instant,
        delay: Option<Duration>,
    },
    Started {
        processor: Option<StartedProcessor>,
        leader_state: LeaderState,
        start_time: Instant,
        delay: Option<Duration>,
    },
    Stopping {
        processor: Option<StartedProcessor>,
    },
}

impl ProcessorState {
    pub fn starting(target_run_mode: RunMode, delay: Option<Duration>) -> Self {
        Self::Starting {
            target_run_mode,
            start_time: Instant::now(),
            delay,
        }
    }

    pub fn stopping(processor: StartedProcessor) -> Self {
        Self::Stopping {
            processor: Some(processor),
        }
    }

    pub fn started(
        processor: StartedProcessor,
        start_time: Instant,
        delay: Option<Duration>,
    ) -> Self {
        Self::Started {
            processor: Some(processor),
            leader_state: LeaderState::Follower,
            start_time,
            delay,
        }
    }

    pub fn stop(&mut self) {
        match self {
            ProcessorState::Starting { .. } => {
                // let's see whether we can stop a starting PP eagerly
                *self = ProcessorState::Stopping { processor: None }
            }
            ProcessorState::Started { processor, .. } => {
                let processor = processor.take().expect("must be some");
                processor.cancel();
                *self = ProcessorState::Stopping {
                    processor: Some(processor),
                };
            }
            ProcessorState::Stopping { .. } => {
                // already stopping
            }
        };
    }

    pub fn run_as_follower(&mut self) {
        match self {
            ProcessorState::Starting {
                target_run_mode, ..
            } => {
                *target_run_mode = RunMode::Follower;
            }
            ProcessorState::Started {
                processor,
                leader_state,
                ..
            } => {
                match leader_state {
                    LeaderState::Leader(_) => {
                        processor.as_ref().expect("must be some").step_down();
                        *leader_state = LeaderState::Follower;
                    }
                    LeaderState::AwaitingLeaderEpoch(_) => {
                        *leader_state = LeaderState::Follower;
                    }
                    LeaderState::Follower => {
                        // nothing to do
                    }
                }
            }
            ProcessorState::Stopping { .. } => {
                // we first need to stop before we check whether we should run again
            }
        }
    }

    /// Returns a new [`LeaderEpochToken`] if a new leader epoch should be obtained.
    pub fn run_as_leader(&mut self) -> Option<LeaderEpochToken> {
        match self {
            ProcessorState::Starting {
                target_run_mode, ..
            } => {
                debug!("Starting partition processor as leader.");
                *target_run_mode = RunMode::Leader;
                None
            }
            ProcessorState::Started {
                processor,
                leader_state,
                ..
            } => {
                match leader_state {
                    LeaderState::Leader(leader_epoch) => {
                        if *leader_epoch
                            < processor
                                .as_ref()
                                .expect("must be some")
                                .last_observed_leader_epoch()
                                .unwrap_or(LeaderEpoch::INVALID)
                        {
                            // our processor, which is supposed to be the leader, has observed a newer leader epoch --> try to obtain a higher one
                            debug!(old_leader_epoch = %leader_epoch, "Need a higher leader epoch to retake leadership.");
                            let leader_epoch_token = LeaderEpochToken::new();
                            *leader_state = LeaderState::AwaitingLeaderEpoch(leader_epoch_token);
                            Some(leader_epoch_token)
                        } else {
                            debug!(%leader_epoch, "Trying to become leader with the current leader epoch since I haven't seen a higher one.");
                            None
                        }
                    }
                    LeaderState::AwaitingLeaderEpoch(_) => {
                        debug!("Awaiting new leader epoch.");
                        // still waiting for pending leader epoch
                        None
                    }
                    LeaderState::Follower => {
                        debug!("Need a leader epoch to become a leader.");
                        let leader_epoch_token = LeaderEpochToken::new();
                        *leader_state = LeaderState::AwaitingLeaderEpoch(leader_epoch_token);
                        Some(leader_epoch_token)
                    }
                }
            }
            ProcessorState::Stopping { .. } => {
                // we first need to stop before we check whether we should run again
                None
            }
        }
    }

    pub fn on_leader_epoch_obtained(
        &mut self,
        leadership_info: Box<LeadershipInfo>,
        leader_epoch_token: LeaderEpochToken,
    ) {
        let leader_epoch = leadership_info.leader_epoch;
        match self {
            ProcessorState::Starting { .. } => {
                debug!(
                    "Received leader epoch while starting partition processor. Probably originated from a previous attempt."
                );
            }
            ProcessorState::Started {
                processor,
                leader_state,
                ..
            } => match leader_state {
                LeaderState::Leader(_) => {
                    debug!("Received leader epoch while already being leader. Ignoring.");
                }
                LeaderState::AwaitingLeaderEpoch(token) => {
                    if *token == leader_epoch_token {
                        if leader_epoch
                            > processor
                                .as_ref()
                                .expect("must be some")
                                .last_observed_leader_epoch()
                                .unwrap_or(LeaderEpoch::INVALID)
                        {
                            processor
                                .as_ref()
                                .expect("must be some")
                                .run_for_leader(leadership_info);
                            debug!(%leader_epoch, "Instruct partition processor to run as leader.");
                            *leader_state = LeaderState::Leader(leader_epoch);
                        } else {
                            debug!(%leader_epoch, "Ignoring leader epoch since there is already a newer known leader epoch.");
                            *leader_state = LeaderState::Follower;
                        }
                    } else {
                        debug!(
                            "Received leader epoch token does not match the expected token. Ignoring."
                        );
                    }
                }
                LeaderState::Follower => {
                    debug!("Received leader epoch while being in follower state. Ignoring.");
                }
            },
            ProcessorState::Stopping { .. } => {
                debug!("Received leader epoch while stopping partition processor. Ignoring.");
            }
        }
    }

    pub fn is_valid_leader_epoch_token(&self, leader_epoch_token: LeaderEpochToken) -> bool {
        match self {
            ProcessorState::Starting { .. } => false,
            ProcessorState::Started { leader_state, .. } => {
                matches!(leader_state, LeaderState::AwaitingLeaderEpoch(token) if *token == leader_epoch_token)
            }
            ProcessorState::Stopping { .. } => false,
        }
    }

    pub fn partition_processor_status(&self) -> Option<PartitionProcessorStatus> {
        match self {
            ProcessorState::Starting {
                target_run_mode, ..
            } => {
                let status = PartitionProcessorStatus {
                    planned_mode: *target_run_mode,
                    ..Default::default()
                };

                Some(status)
            }
            ProcessorState::Started {
                processor,
                leader_state,
                ..
            } => {
                let mut status = processor
                    .as_ref()
                    .expect("must be some")
                    .watch_rx
                    .borrow()
                    .clone();

                // The reason why we override the planned mode here is that we want to report it as
                // soon as we start getting a leader epoch for a candidate. Once the leader epoch
                // is obtained, we want to keep reporting the planned mode as leader until the pp
                // tells us differently by reporting a higher observed leader epoch.
                status.planned_mode = match leader_state {
                    LeaderState::Leader(leader_epoch) => {
                        if *leader_epoch
                            >= status
                                .last_observed_leader_epoch
                                .unwrap_or(LeaderEpoch::INVALID)
                        {
                            // we still have a chance to become leader
                            RunMode::Leader
                        } else {
                            // with our current leader epoch, we can't become leader anymore
                            RunMode::Follower
                        }
                    }
                    // we don't know yet which leader epoch we will get from the metadata store -->
                    // assume it will be higher than the current one
                    LeaderState::AwaitingLeaderEpoch(_) => RunMode::Leader,
                    LeaderState::Follower => RunMode::Follower,
                };

                Some(status)
            }
            ProcessorState::Stopping { .. } => {
                // todo report stopping status back to the cluster controller
                None
            }
        }
    }

    pub fn try_send_rpc(&self, msg: ServiceMessage<PartitionLeaderService>) {
        match self {
            ProcessorState::Starting { .. } => msg.fail(Verdict::LoadShedding),
            ProcessorState::Started { processor, .. } => {
                if let Err(err) = processor.as_ref().expect("must be some").try_send_rpc(msg) {
                    match err {
                        TrySendError::Full(msg) => msg.fail(Verdict::LoadShedding),
                        TrySendError::Closed(msg) => msg.fail(Verdict::SortCodeNotFound),
                    }
                }
            }
            ProcessorState::Stopping { .. } => msg.fail(Verdict::SortCodeNotFound),
        }
    }

    /// The Partition Processor is in a state in which it is acceptable to create and publish
    /// snapshots. Since we generally don't want newer snapshots to move backwards in applied LSN,
    /// the current implementation checks whether the processor is fully caught up with the log.
    pub fn should_publish_snapshots(&self) -> bool {
        match self {
            ProcessorState::Started {
                processor: Some(started_processor),
                ..
            } if started_processor.watch_rx.borrow().replay_status == ReplayStatus::Active => {
                // At this point we don't care about leadership status, only that the processor is up to date
                true
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct StartedProcessor {
    cancellation_token: CancellationToken,
    key_range: RangeInclusive<PartitionKey>,
    control_tx: watch::Sender<TargetLeaderState>,
    status_reader: ChannelStatusReader,
    network_svc_tx: mpsc::Sender<ServiceMessage<PartitionLeaderService>>,
    watch_rx: watch::Receiver<PartitionProcessorStatus>,
}

impl StartedProcessor {
    pub fn new(
        cancellation_token: CancellationToken,
        key_range: RangeInclusive<PartitionKey>,
        control_tx: watch::Sender<TargetLeaderState>,
        status_reader: ChannelStatusReader,
        network_svc_tx: mpsc::Sender<ServiceMessage<PartitionLeaderService>>,
        watch_rx: watch::Receiver<PartitionProcessorStatus>,
    ) -> Self {
        Self {
            cancellation_token,
            key_range,
            control_tx,
            status_reader,
            network_svc_tx,
            watch_rx,
        }
    }

    fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    fn last_observed_leader_epoch(&self) -> Option<LeaderEpoch> {
        self.watch_rx.borrow().last_observed_leader_epoch
    }

    pub fn step_down(&self) {
        self.control_tx.send_if_modified(|target_state| {
            if !matches!(*target_state, TargetLeaderState::Follower) {
                *target_state = TargetLeaderState::Follower;
                true
            } else {
                false
            }
        });
    }

    pub fn run_for_leader(&self, leadership_info: Box<LeadershipInfo>) {
        self.control_tx.send_if_modified(|target_state| {
            // Compare by leader_epoch only for equality check
            let should_update = match target_state {
                TargetLeaderState::Leader(info) => {
                    info.leader_epoch != leadership_info.leader_epoch
                }
                TargetLeaderState::Follower => true,
            };
            if should_update {
                *target_state = TargetLeaderState::Leader(leadership_info);
                true
            } else {
                false
            }
        });
    }

    #[inline]
    pub fn key_range(&self) -> &RangeInclusive<PartitionKey> {
        &self.key_range
    }

    #[inline]
    pub fn invoker_status_reader(&self) -> &ChannelStatusReader {
        &self.status_reader
    }

    #[allow(clippy::result_large_err)]
    pub fn try_send_rpc(
        &self,
        msg: ServiceMessage<PartitionLeaderService>,
    ) -> Result<(), TrySendError<ServiceMessage<PartitionLeaderService>>> {
        self.network_svc_tx.try_send(msg)
    }
}
