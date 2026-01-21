// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_errors::NotRunningError;
use restate_futures_util::concurrency::Permit;
use restate_invoker_api::{Effect, InvocationStatusReport, InvokeInputJournal, StatusHandle};
use restate_types::identifiers::{InvocationId, PartitionKey, PartitionLeaderEpoch};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::Completion;
use restate_types::journal_v2::CommandIndex;
use restate_types::journal_v2::raw::RawNotification;
use restate_types::vqueue::VQueueId;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;
// -- Input messages

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct InvokeCommand {
    pub(super) partition: PartitionLeaderEpoch,
    pub(super) invocation_id: InvocationId,
    // removed in v1.6
    // pub(super) invocation_epoch: InvocationEpoch,
    pub(super) invocation_target: InvocationTarget,
    #[serde(skip)]
    pub(super) journal: InvokeInputJournal,
}

#[derive(derive_more::Debug)]
pub(crate) struct VQueueInvokeCommand {
    pub(super) qid: VQueueId,
    #[debug(skip)]
    pub(super) permit: Permit,
    pub(super) partition: PartitionLeaderEpoch,
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_target: InvocationTarget,
    #[debug(skip)]
    pub(super) journal: InvokeInputJournal,
}

#[derive(Debug)]
pub(crate) enum InputCommand<SR> {
    Invoke(Box<InvokeCommand>),
    VQInvoke(Box<VQueueInvokeCommand>),
    // TODO remove this when we remove journal v1
    // Journal V1 doesn't support epochs nor trim and restart
    Completion {
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        completion: Completion,
    },
    Notification {
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        notification: RawNotification,
    },
    StoredCommandAck {
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        command_index: CommandIndex,
    },

    /// Abort specific invocation id
    Abort {
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    },

    /// Retry now specific invocation id
    RetryNow {
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    },

    /// Pause specific invocation id
    Pause {
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    },

    /// Command used to clean up internal state when a partition leader is going away
    AbortAllPartition {
        partition: PartitionLeaderEpoch,
    },

    // needed for dynamic registration at Invoker
    RegisterPartition {
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        storage_reader: SR,
        sender: mpsc::Sender<Box<Effect>>,
    },
}

// -- Handles implementations. This is just glue code between the Input<Command> and the interfaces

#[derive(Debug, Clone)]
pub struct InvokerHandle<SR> {
    pub(super) input: mpsc::UnboundedSender<InputCommand<SR>>,
}

impl<SR: Send> restate_invoker_api::InvokerHandle<SR> for InvokerHandle<SR> {
    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        journal: InvokeInputJournal,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Invoke(Box::new(InvokeCommand {
                partition,
                invocation_id,
                invocation_target,
                journal,
            })))
            .map_err(|_| NotRunningError)
    }

    fn vqueue_invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        qid: VQueueId,
        permit: Permit,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        journal: InvokeInputJournal,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::VQInvoke(Box::new(VQueueInvokeCommand {
                qid,
                permit,
                partition,
                invocation_id,
                invocation_target,
                journal,
            })))
            .map_err(|_| NotRunningError)
    }

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        completion: Completion,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Completion {
                partition,
                invocation_id,
                completion,
            })
            .map_err(|_| NotRunningError)
    }

    fn notify_notification(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        notification: RawNotification,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Notification {
                partition,
                invocation_id,
                notification,
            })
            .map_err(|_| NotRunningError)
    }

    fn notify_stored_command_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        command_index: CommandIndex,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::StoredCommandAck {
                partition,
                invocation_id,
                command_index,
            })
            .map_err(|_| NotRunningError)
    }

    fn abort_all_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::AbortAllPartition { partition })
            .map_err(|_| NotRunningError)
    }

    fn abort_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Abort {
                partition,
                invocation_id,
            })
            .map_err(|_| NotRunningError)
    }

    fn retry_invocation_now(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::RetryNow {
                partition,
                invocation_id,
            })
            .map_err(|_| NotRunningError)
    }

    fn pause_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Pause {
                partition,
                invocation_id,
            })
            .map_err(|_| NotRunningError)
    }

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        storage_reader: SR,
        sender: mpsc::Sender<Box<Effect>>,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::RegisterPartition {
                partition,
                partition_key_range,
                sender,
                storage_reader,
            })
            .map_err(|_| NotRunningError)
    }
}

#[derive(Debug, Clone)]
pub struct ChannelStatusReader(
    pub(super)  mpsc::UnboundedSender<
        restate_futures_util::command::Command<
            RangeInclusive<PartitionKey>,
            Vec<InvocationStatusReport>,
        >,
    >,
);

impl StatusHandle for ChannelStatusReader {
    type Iterator = itertools::Either<
        std::iter::Empty<InvocationStatusReport>,
        std::vec::IntoIter<InvocationStatusReport>,
    >;

    async fn read_status(&self, keys: RangeInclusive<PartitionKey>) -> Self::Iterator {
        let (cmd, rx) = restate_futures_util::command::Command::prepare(keys);
        if self.0.send(cmd).is_err() {
            return itertools::Either::Left(std::iter::empty::<InvocationStatusReport>());
        }

        if let Ok(mut status_vec) = rx.await {
            status_vec.sort_by(|a, b| a.invocation_id().cmp(b.invocation_id()));
            itertools::Either::Right(status_vec.into_iter())
        } else {
            itertools::Either::Left(std::iter::empty::<InvocationStatusReport>())
        }
    }
}
