// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::PartitionProcessorControlCommand;
use crate::partition_processor_manager::Error;
use restate_core::metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_core::network::Incoming;
use restate_core::{ShutdownError, TaskCenter, TaskId};
use restate_invoker_impl::ChannelStatusReader;
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, SnapshotId};
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::partition_processor::PartitionProcessorRpcRequest;
use restate_types::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;
use std::ops::RangeInclusive;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

pub enum ProcessorState {
    Starting(JoinHandle<()>),
    Started(StartedProcessor),
}

impl ProcessorState {
    pub async fn stop(self, task_center: &TaskCenter) {
        match self {
            Self::Started(processor) => {
                let handle = task_center.cancel_task(processor.task_id);

                if let Some(handle) = handle {
                    debug!("Asked by cluster-controller to stop partition");
                    if let Err(err) = handle.await {
                        warn!("Partition processor crashed while shutting down: {err}");
                    }
                }
            }
            Self::Starting(handle) => handle.abort(),
        };
    }
}

pub struct StartedProcessor {
    partition_id: PartitionId,
    task_id: TaskId,
    _created_at: MillisSinceEpoch,
    key_range: RangeInclusive<PartitionKey>,
    planned_mode: RunMode,
    running_for_leadership_with_epoch: Option<LeaderEpoch>,
    handle: PartitionProcessorHandle,
    status_reader: ChannelStatusReader,
    rpc_tx: mpsc::Sender<Incoming<PartitionProcessorRpcRequest>>,
    watch_rx: watch::Receiver<PartitionProcessorStatus>,
}

impl StartedProcessor {
    pub fn new(
        partition_id: PartitionId,
        task_id: TaskId,
        key_range: RangeInclusive<PartitionKey>,
        handle: PartitionProcessorHandle,
        status_reader: ChannelStatusReader,
        rpc_tx: mpsc::Sender<Incoming<PartitionProcessorRpcRequest>>,
        watch_rx: watch::Receiver<PartitionProcessorStatus>,
    ) -> Self {
        Self {
            partition_id,
            task_id,
            _created_at: MillisSinceEpoch::now(),
            key_range,
            planned_mode: RunMode::Follower,
            running_for_leadership_with_epoch: None,
            handle,
            status_reader,
            rpc_tx,
            watch_rx,
        }
    }

    pub fn step_down(&mut self) -> Result<(), Error> {
        if self.planned_mode != RunMode::Follower {
            debug!("Asked by cluster-controller to demote partition to follower");
            self.handle.step_down()?;
        }

        self.running_for_leadership_with_epoch = None;
        self.planned_mode = RunMode::Follower;

        Ok(())
    }

    pub async fn run_for_leader(
        &mut self,
        metadata_store_client: MetadataStoreClient,
        node_id: GenerationalNodeId,
    ) -> Result<(), Error> {
        // run for leadership if there is no ongoing attempt or our current attempt is proven to be
        // unsuccessful because we have already seen a higher leader epoch.
        if self.running_for_leadership_with_epoch.is_none()
            || self
                .running_for_leadership_with_epoch
                .is_some_and(|my_leader_epoch| {
                    my_leader_epoch
                        < self
                            .watch_rx
                            .borrow()
                            .last_observed_leader_epoch
                            .unwrap_or(LeaderEpoch::INITIAL)
                })
        {
            // todo alternative could be to let the CC decide the leader epoch
            let leader_epoch =
                Self::obtain_next_epoch(metadata_store_client, self.partition_id, node_id).await?;
            debug!(%leader_epoch, "Asked by cluster-controller to promote partition to leader");
            self.running_for_leadership_with_epoch = Some(leader_epoch);
            self.handle.run_for_leader(leader_epoch)?;
        }

        self.planned_mode = RunMode::Leader;

        Ok(())
    }

    async fn obtain_next_epoch(
        metadata_store_client: MetadataStoreClient,
        partition_id: PartitionId,
        node_id: GenerationalNodeId,
    ) -> Result<LeaderEpoch, ReadModifyWriteError> {
        let epoch: EpochMetadata = metadata_store_client
            .read_modify_write(partition_processor_epoch_key(partition_id), |epoch| {
                let next_epoch = epoch
                    .map(|epoch: EpochMetadata| epoch.claim_leadership(node_id, partition_id))
                    .unwrap_or_else(|| EpochMetadata::new(node_id, partition_id));

                Ok(next_epoch)
            })
            .await?;
        Ok(epoch.epoch())
    }

    #[inline]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    #[inline]
    pub fn key_range(&self) -> &RangeInclusive<PartitionKey> {
        &self.key_range
    }

    #[inline]
    pub fn invoker_status_reader(&self) -> &ChannelStatusReader {
        &self.status_reader
    }

    #[inline]
    pub fn partition_processor_status(&self) -> PartitionProcessorStatus {
        self.watch_rx.borrow().clone()
    }

    #[inline]
    pub fn planned_mode(&self) -> RunMode {
        self.planned_mode
    }

    pub fn try_send_rpc(
        &self,
        rpc: Incoming<PartitionProcessorRpcRequest>,
    ) -> Result<(), TrySendError<Incoming<PartitionProcessorRpcRequest>>> {
        self.rpc_tx.try_send(rpc)
    }

    pub fn create_snapshot(
        &self,
        result_tx: oneshot::Sender<anyhow::Result<SnapshotId>>,
    ) -> Result<(), PartitionProcessorHandleError> {
        self.handle.create_snapshot(Some(result_tx))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PartitionProcessorHandleError {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("command could not be sent")]
    FailedSend,
}

impl<T> From<TrySendError<T>> for PartitionProcessorHandleError {
    fn from(value: TrySendError<T>) -> Self {
        match value {
            TrySendError::Full(_) => PartitionProcessorHandleError::FailedSend,
            TrySendError::Closed(_) => PartitionProcessorHandleError::Shutdown(ShutdownError),
        }
    }
}

pub struct PartitionProcessorHandle {
    control_tx: mpsc::Sender<PartitionProcessorControlCommand>,
}

impl PartitionProcessorHandle {
    pub fn new(control_tx: mpsc::Sender<PartitionProcessorControlCommand>) -> Self {
        Self { control_tx }
    }

    fn step_down(&self) -> Result<(), PartitionProcessorHandleError> {
        self.control_tx
            .try_send(PartitionProcessorControlCommand::StepDown)?;
        Ok(())
    }

    fn run_for_leader(
        &self,
        leader_epoch: LeaderEpoch,
    ) -> Result<(), PartitionProcessorHandleError> {
        self.control_tx
            .try_send(PartitionProcessorControlCommand::RunForLeader(leader_epoch))?;
        Ok(())
    }

    fn create_snapshot(
        &self,
        sender: Option<oneshot::Sender<anyhow::Result<SnapshotId>>>,
    ) -> Result<(), PartitionProcessorHandleError> {
        self.control_tx
            .try_send(PartitionProcessorControlCommand::CreateSnapshot(sender))?;
        Ok(())
    }
}
