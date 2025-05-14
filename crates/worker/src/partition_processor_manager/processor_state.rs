// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

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
use restate_types::time::MillisSinceEpoch;

use crate::partition::PartitionProcessorControlCommand;

pub type LeaderEpochToken = Ulid;

#[derive(Debug, thiserror::Error)]
pub enum ProcessorStateError {
    #[error("partition processor is busy")]
    Busy,
    #[error("partition processor is shutting down")]
    ShuttingDown,
}

impl<T> From<TrySendError<T>> for ProcessorStateError {
    fn from(value: TrySendError<T>) -> Self {
        match value {
            TrySendError::Full(_) => ProcessorStateError::Busy,
            TrySendError::Closed(_) => ProcessorStateError::ShuttingDown,
        }
    }
}

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
    },
    Started {
        processor: Option<StartedProcessor>,
        leader_state: LeaderState,
    },
    Stopping {
        processor: Option<StartedProcessor>,
        restart_as: Option<RunMode>,
    },
}

impl ProcessorState {
    pub fn starting(target_run_mode: RunMode) -> Self {
        Self::Starting { target_run_mode }
    }

    pub fn stopping(processor: StartedProcessor) -> Self {
        Self::Stopping {
            processor: Some(processor),
            restart_as: None,
        }
    }

    pub fn started(processor: StartedProcessor) -> Self {
        Self::Started {
            processor: Some(processor),
            leader_state: LeaderState::Follower,
        }
    }

    pub fn stop(&mut self) {
        match self {
            ProcessorState::Starting { .. } => {
                // let's see whether we can stop a starting PP eagerly
                *self = ProcessorState::Stopping {
                    restart_as: None,
                    processor: None,
                }
            }
            ProcessorState::Started { processor, .. } => {
                let processor = processor.take().expect("must be some");
                processor.cancel();
                *self = ProcessorState::Stopping {
                    restart_as: None,
                    processor: Some(processor),
                };
            }
            ProcessorState::Stopping { restart_as, .. } => {
                *restart_as = None;
            }
        };
    }

    pub fn run_as_follower(&mut self) -> Result<(), ProcessorStateError> {
        match self {
            ProcessorState::Starting {
                target_run_mode, ..
            } => {
                *target_run_mode = RunMode::Follower;
            }
            ProcessorState::Started {
                processor,
                leader_state,
            } => {
                match leader_state {
                    LeaderState::Leader(_) => {
                        processor.as_ref().expect("must be some").step_down()?;
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
            ProcessorState::Stopping { restart_as, .. } => {
                *restart_as = Some(RunMode::Follower);
            }
        }

        Ok(())
    }

    /// Returns a new [`LeaderEpochToken`] if a new leader epoch should be obtained.
    pub fn run_as_leader(&mut self) -> Option<LeaderEpochToken> {
        match self {
            ProcessorState::Starting { target_run_mode } => {
                debug!("Starting partition processor as leader.");
                *target_run_mode = RunMode::Leader;
                None
            }
            ProcessorState::Started {
                processor,
                leader_state,
            } => {
                match leader_state {
                    LeaderState::Leader(leader_epoch) => {
                        // our processor that is supposed to be the leader has observed a newer leader epoch
                        if *leader_epoch
                            < processor
                                .as_ref()
                                .expect("must be some")
                                .last_observed_leader_epoch()
                                .unwrap_or(LeaderEpoch::INITIAL)
                        {
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
            ProcessorState::Stopping { restart_as, .. } => {
                debug!("Restarting partition processor as leader.");
                *restart_as = Some(RunMode::Leader);
                None
            }
        }
    }

    pub fn on_leader_epoch_obtained(
        &mut self,
        leader_epoch: LeaderEpoch,
        leader_epoch_token: LeaderEpochToken,
    ) -> Result<(), ProcessorStateError> {
        match self {
            ProcessorState::Starting { .. } => {
                debug!(
                    "Received leader epoch while starting partition processor. Probably originated from a previous attempt."
                );
            }
            ProcessorState::Started {
                processor,
                leader_state,
            } => match leader_state {
                LeaderState::Leader(_) => {
                    debug!("Received leader epoch while already being leader. Ignoring.");
                }
                LeaderState::AwaitingLeaderEpoch(token) => {
                    if *token == leader_epoch_token {
                        processor
                            .as_ref()
                            .expect("must be some")
                            .run_for_leader(leader_epoch)?;
                        debug!(%leader_epoch, "Instruct partition processor to run as leader.");
                        *leader_state = LeaderState::Leader(leader_epoch);
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

        Ok(())
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
            ProcessorState::Starting { target_run_mode } => {
                let status = PartitionProcessorStatus {
                    planned_mode: *target_run_mode,
                    ..Default::default()
                };

                Some(status)
            }
            ProcessorState::Started {
                processor,
                leader_state,
            } => {
                let mut status = processor
                    .as_ref()
                    .expect("must be some")
                    .watch_rx
                    .borrow()
                    .clone();

                // update the planned mode based on the current leader state
                status.planned_mode = match leader_state {
                    LeaderState::Leader(_) => RunMode::Leader,
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
    _created_at: MillisSinceEpoch,
    key_range: RangeInclusive<PartitionKey>,
    control_tx: mpsc::Sender<PartitionProcessorControlCommand>,
    status_reader: ChannelStatusReader,
    network_svc_tx: mpsc::Sender<ServiceMessage<PartitionLeaderService>>,
    watch_rx: watch::Receiver<PartitionProcessorStatus>,
}

impl StartedProcessor {
    pub fn new(
        cancellation_token: CancellationToken,
        key_range: RangeInclusive<PartitionKey>,
        control_tx: mpsc::Sender<PartitionProcessorControlCommand>,
        status_reader: ChannelStatusReader,
        network_svc_tx: mpsc::Sender<ServiceMessage<PartitionLeaderService>>,
        watch_rx: watch::Receiver<PartitionProcessorStatus>,
    ) -> Self {
        Self {
            cancellation_token,
            _created_at: MillisSinceEpoch::now(),
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

    pub fn step_down(&self) -> Result<(), TrySendError<PartitionProcessorControlCommand>> {
        self.control_tx
            .try_send(PartitionProcessorControlCommand::StepDown)
    }

    pub fn run_for_leader(
        &self,
        leader_epoch: LeaderEpoch,
    ) -> Result<(), TrySendError<PartitionProcessorControlCommand>> {
        self.control_tx
            .try_send(PartitionProcessorControlCommand::RunForLeader(leader_epoch))
    }

    #[inline]
    pub fn key_range(&self) -> &RangeInclusive<PartitionKey> {
        &self.key_range
    }

    #[inline]
    pub fn invoker_status_reader(&self) -> &ChannelStatusReader {
        &self.status_reader
    }

    pub fn try_send_rpc(
        &self,
        msg: ServiceMessage<PartitionLeaderService>,
    ) -> Result<(), TrySendError<ServiceMessage<PartitionLeaderService>>> {
        self.network_svc_tx.try_send(msg)
    }
}
