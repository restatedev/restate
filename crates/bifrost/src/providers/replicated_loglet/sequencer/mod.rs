// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod append;
mod node;

use std::sync::Arc;

use futures::channel::oneshot;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use restate_core::{
    cancellation_watcher,
    network::{rpc_router::RpcRouter, MessageRouterBuilder, Networking, TransportConnect},
    task_center, Metadata, ShutdownError, TaskKind,
};
use restate_types::{
    config::Configuration,
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::Store,
    replicated_loglet::{NodeSet, ReplicatedLogletId},
    GenerationalNodeId,
};

use super::replication::spread_selector::SpreadSelector;
use crate::loglet::{util::TailOffsetWatch, LogletCommit};
use append::Appender;
use node::RemoteLogServerManager;

#[derive(thiserror::Error, Debug)]
pub enum SequencerError {
    #[error("loglet offset exhausted")]
    LogletOffsetExhausted,
    #[error("batch exceeds possible length")]
    InvalidBatchLength,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

/// Sequencer shared state
pub struct SequencerSharedState {
    node_id: GenerationalNodeId,
    loglet_id: ReplicatedLogletId,
    committed_tail: TailOffsetWatch,
    selector: SpreadSelector,
}

impl SequencerSharedState {
    pub fn node_id(&self) -> &GenerationalNodeId {
        &self.node_id
    }

    pub fn loglet_id(&self) -> &ReplicatedLogletId {
        &self.loglet_id
    }

    pub fn global_committed_tail(&self) -> &TailOffsetWatch {
        &self.committed_tail
    }
}

/// internal commands sent over the [`SequencerHandler`] to sequencer main loop
struct SequencerCommand<Input, Output> {
    input: Input,
    sender: oneshot::Sender<Output>,
}

impl<Input, Output> SequencerCommand<Input, Output> {
    fn new(input: Input) -> (oneshot::Receiver<Output>, Self) {
        let (sender, receiver) = oneshot::channel();
        (receiver, Self { input, sender })
    }
}

/// Available sequencer commands
enum SequencerCommands {
    /// executed commands
    EnqueueBatch(SequencerCommand<Arc<[Record]>, Result<LogletCommit, SequencerError>>),
}

struct SequencerCommandsWithPermit {
    permit: OwnedSemaphorePermit,
    command: SequencerCommands,
}

/// Available sequencer control and introspection commands
enum SequencerCtrlCommands {
    GetClusterState(SequencerCommand<(), ClusterState>),
}

/// Main interaction interface with the sequencer state machine
#[derive(Clone)]
pub struct SequencerHandle {
    /// internal commands channel.
    commands: mpsc::UnboundedSender<SequencerCommandsWithPermit>,
    ctrl: mpsc::Sender<SequencerCtrlCommands>,
    sequencer_shared_state: Arc<SequencerSharedState>,
    permits: Arc<Semaphore>,
}

pub(crate) struct SequencerHandleSink {
    commands: mpsc::UnboundedReceiver<SequencerCommandsWithPermit>,
    ctrl: mpsc::Receiver<SequencerCtrlCommands>,
}

impl SequencerHandle {
    pub(crate) fn new(
        sequencer_shared_state: Arc<SequencerSharedState>,
    ) -> (SequencerHandle, SequencerHandleSink) {
        let permits = Arc::new(Semaphore::new(
            Configuration::pinned()
                .bifrost
                .replicated_loglet
                .maximum_inflight_batches
                .into(),
        ));

        let (commands_sender, commands_receiver) = mpsc::unbounded_channel();
        let (ctrl_sender, ctrl_received) = mpsc::channel(64);
        (
            SequencerHandle {
                commands: commands_sender,
                ctrl: ctrl_sender,
                sequencer_shared_state,
                permits,
            },
            SequencerHandleSink {
                commands: commands_receiver,
                ctrl: ctrl_received,
            },
        )
    }

    pub fn sequencer_state(&self) -> &SequencerSharedState {
        &self.sequencer_shared_state
    }

    pub async fn get_cluster_state(&self) -> Result<ClusterState, ShutdownError> {
        let (receiver, command) = SequencerCommand::new(());
        self.ctrl
            .send(SequencerCtrlCommands::GetClusterState(command))
            .await
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)
    }

    pub async fn enqueue_batch(
        &self,
        payloads: Arc<[Record]>,
    ) -> Result<LogletCommit, SequencerError> {
        let permit = self.permits.clone().acquire_owned().await.unwrap();

        let (receiver, command) = SequencerCommand::new(payloads);
        self.commands
            .send(SequencerCommandsWithPermit {
                permit,
                command: SequencerCommands::EnqueueBatch(command),
            })
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)?
    }
}

#[derive(Clone, Debug)]
pub struct ClusterState {
    pub sequencer_id: GenerationalNodeId,
    pub global_committed_tail: TailState<LogletOffset>,
}

/// Sequencer
pub struct Sequencer<T> {
    sequencer_shared_state: Arc<SequencerSharedState>,
    log_server_manager: RemoteLogServerManager<T>,
    metadata: Metadata,
    next_write_offset: LogletOffset,
    rpc_router: RpcRouter<Store>,
    handle_sink: SequencerHandleSink,
}

impl<T: TransportConnect> Sequencer<T> {
    /// Create a new sequencer instance
    pub fn new(
        node_id: GenerationalNodeId,
        loglet_id: ReplicatedLogletId,
        node_set: NodeSet,
        selector: SpreadSelector,
        metadata: Metadata,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
    ) -> (SequencerHandle, Self) {
        // - register for all potential response streams from the log-server(s).

        // shared state with appenders
        let sequencer_shared_state = Arc::new(SequencerSharedState {
            node_id,
            loglet_id,
            selector,
            committed_tail: TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST)),
        });

        // create a command channel to be used by the sequencer handler. The handler then can be used
        // to call and execute commands on the sequencer directly
        let (handle, handle_sink) = SequencerHandle::new(Arc::clone(&sequencer_shared_state));

        let rpc_router = RpcRouter::new(router_builder);

        let log_server_manager = RemoteLogServerManager::new(loglet_id, networking, node_set);

        let sequencer = Sequencer {
            sequencer_shared_state,
            log_server_manager,
            metadata,
            next_write_offset: LogletOffset::OLDEST,
            rpc_router,
            handle_sink,
        };

        (handle, sequencer)
    }

    /// Start the sequencer main loop
    pub async fn start(mut self) {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        // enter main state machine loop
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    break;
                },
                Some(command) = self.handle_sink.commands.recv() => {
                    self.process_command(command).await;
                }
                Some(ctrl) = self.handle_sink.ctrl.recv() => {
                    self.process_ctrl(ctrl);
                }
            }
        }
    }

    fn process_ctrl(&mut self, ctrl: SequencerCtrlCommands) {
        match ctrl {
            SequencerCtrlCommands::GetClusterState(command) => {
                let SequencerCommand { sender, .. } = command;
                let _ = sender.send(self.get_cluster_state());
            }
        }
    }
    /// process calls from the SequencerHandler.
    async fn process_command(&mut self, command: SequencerCommandsWithPermit) {
        let SequencerCommandsWithPermit { permit, command } = command;

        match command {
            SequencerCommands::EnqueueBatch(command) => {
                let SequencerCommand {
                    input: request,
                    sender,
                } = command;

                let _ = sender.send(self.enqueue_batch(permit, request).await);
            }
        }
    }

    fn get_cluster_state(&self) -> ClusterState {
        ClusterState {
            global_committed_tail: self
                .sequencer_shared_state
                .global_committed_tail()
                .get()
                .to_owned(),
            sequencer_id: self.sequencer_shared_state.node_id,
        }
    }

    async fn enqueue_batch(
        &mut self,
        permit: OwnedSemaphorePermit,
        records: Arc<[Record]>,
    ) -> Result<LogletCommit, SequencerError> {
        if self
            .sequencer_shared_state
            .global_committed_tail()
            .is_sealed()
        {
            return Ok(LogletCommit::sealed());
        }

        let next_write_offset = records.last_offset(self.next_write_offset)?.next();

        let (loglet_commit, commit_resolver) = LogletCommit::deferred();

        let appender = Appender::new(
            Arc::clone(&self.sequencer_shared_state),
            self.log_server_manager.clone(),
            self.rpc_router.clone(),
            self.metadata.clone(),
            self.next_write_offset,
            records,
            permit,
            commit_resolver,
        );

        task_center().spawn(TaskKind::BifrostAppender, "appender", None, appender.run())?;
        self.next_write_offset = next_write_offset;

        Ok(loglet_commit)
    }
}

trait BatchExt {
    /// tail computes inflight tail after this batch is committed
    fn last_offset(&self, first_offset: LogletOffset) -> Result<LogletOffset, SequencerError>;
}

impl BatchExt for Arc<[Record]> {
    fn last_offset(&self, first_offset: LogletOffset) -> Result<LogletOffset, SequencerError> {
        let len = u32::try_from(self.len()).map_err(|_| SequencerError::InvalidBatchLength)?;

        first_offset
            .checked_add(len - 1)
            .map(LogletOffset::from)
            .ok_or(SequencerError::LogletOffsetExhausted)
    }
}
