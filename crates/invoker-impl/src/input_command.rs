// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::mpsc;

use restate_errors::NotRunningError;
use restate_types::LimitKey;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::invocation::InvocationTarget;
use restate_types::journal_v2::{CommandIndex, NotificationId};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;
use restate_worker_api::invoker::{InvocationStatusReport, StatusHandle};
use restate_worker_api::resources::ReservedResources;
// -- Input messages

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct InvokeCommand {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_target: InvocationTarget,
}

#[derive(derive_more::Debug)]
pub(crate) struct VQueueInvokeCommand {
    pub(super) qid: VQueueId,
    #[debug(skip)]
    pub(super) permit: ReservedResources,
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_target: InvocationTarget,
    pub(super) limit_key: LimitKey<ReString>,
    pub(super) idempotency_key: Option<ReString>,
}

#[derive(Debug)]
pub(crate) enum InputCommand {
    Invoke(Box<InvokeCommand>),
    VQInvoke(Box<VQueueInvokeCommand>),
    // TODO remove this when we remove journal v1
    // Journal V1 doesn't support epochs nor trim and restart
    Completion {
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    },
    Notification {
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    },
    StoredCommandAck {
        invocation_id: InvocationId,
        command_index: CommandIndex,
    },

    /// Abort specific invocation id
    Abort {
        invocation_id: InvocationId,
    },

    /// Retry now specific invocation id
    RetryNow {
        invocation_id: InvocationId,
    },

    /// Pause specific invocation id
    Pause {
        invocation_id: InvocationId,
    },

    /// Command used to clean up internal state when a partition leader is going away
    AbortAll,
}

// -- Handles implementations. This is just glue code between the Input<Command> and the interfaces

#[derive(Debug, Clone)]
pub struct InvokerHandle {
    pub(super) input: mpsc::UnboundedSender<InputCommand>,
}

impl restate_worker_api::invoker::InvokerHandle for InvokerHandle {
    fn invoke(
        &mut self,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Invoke(Box::new(InvokeCommand {
                invocation_id,
                invocation_target,
            })))
            .map_err(|_| NotRunningError)
    }

    fn vqueue_invoke(
        &mut self,
        qid: VQueueId,
        permit: ReservedResources,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        limit_key: LimitKey<ReString>,
        idempotency_key: Option<ReString>,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::VQInvoke(Box::new(VQueueInvokeCommand {
                qid,
                permit,
                invocation_id,
                invocation_target,
                limit_key,
                idempotency_key,
            })))
            .map_err(|_| NotRunningError)
    }

    fn notify_completion(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Completion {
                invocation_id,
                entry_index,
            })
            .map_err(|_| NotRunningError)
    }

    fn notify_notification(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Notification {
                invocation_id,
                entry_index,
                notification_id,
            })
            .map_err(|_| NotRunningError)
    }

    fn notify_stored_command_ack(
        &mut self,
        invocation_id: InvocationId,
        command_index: CommandIndex,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::StoredCommandAck {
                invocation_id,
                command_index,
            })
            .map_err(|_| NotRunningError)
    }

    fn abort_all(&mut self) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::AbortAll)
            .map_err(|_| NotRunningError)
    }

    fn abort_invocation(&mut self, invocation_id: InvocationId) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Abort { invocation_id })
            .map_err(|_| NotRunningError)
    }

    fn retry_invocation_now(&mut self, invocation_id: InvocationId) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::RetryNow { invocation_id })
            .map_err(|_| NotRunningError)
    }

    fn pause_invocation(&mut self, invocation_id: InvocationId) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::Pause { invocation_id })
            .map_err(|_| NotRunningError)
    }
}

#[derive(Debug, Clone)]
pub struct ChannelStatusReader(
    pub(super)  mpsc::UnboundedSender<
        restate_futures_util::command::Command<KeyRange, Vec<InvocationStatusReport>>,
    >,
);

impl StatusHandle for ChannelStatusReader {
    type Iterator = itertools::Either<
        std::iter::Empty<InvocationStatusReport>,
        std::vec::IntoIter<InvocationStatusReport>,
    >;

    async fn read_status(&self, keys: KeyRange) -> Self::Iterator {
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
