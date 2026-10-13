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
use restate_types::invocation::{FencingToken, InvocationTarget};
use restate_types::journal_v2::{CommandIndex, NotificationId};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;
use restate_worker_api::invoker::InvocationStatusReport;
use restate_worker_api::resources::ReservedResources;
// -- Input messages

#[derive(derive_more::Debug)]
pub(crate) struct VQueueInvokeCommand {
    pub(super) qid: VQueueId,
    #[debug(skip)]
    pub(super) permit: ReservedResources,
    pub(super) invocation_id: InvocationId,
    pub(super) fencing_token: FencingToken,
    pub(super) invocation_target: InvocationTarget,
    pub(super) limit_key: LimitKey<ReString>,
    pub(super) idempotency_key: Option<ReString>,
}

#[derive(Debug)]
pub(crate) enum InputCommand {
    VQInvoke(Box<VQueueInvokeCommand>),
    Notification {
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    },
    StoredCommandAck {
        invocation_id: InvocationId,
        command_index: CommandIndex,
    },

    /// Abort specific invocation id (the current attempt, unconditionally).
    Abort {
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
    fn vqueue_invoke(
        &mut self,
        qid: VQueueId,
        permit: ReservedResources,
        invocation_id: InvocationId,
        fencing_token: FencingToken,
        invocation_target: InvocationTarget,
        limit_key: LimitKey<ReString>,
        idempotency_key: Option<ReString>,
    ) -> Result<(), NotRunningError> {
        self.input
            .send(InputCommand::VQInvoke(Box::new(VQueueInvokeCommand {
                qid,
                permit,
                invocation_id,
                fencing_token,
                invocation_target,
                limit_key,
                idempotency_key,
            })))
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

impl ChannelStatusReader {
    /// Reads live status without converting a stopped invoker into an empty result.
    pub async fn try_read_status(
        &self,
        keys: KeyRange,
    ) -> Result<Vec<InvocationStatusReport>, NotRunningError> {
        let (cmd, rx) = restate_futures_util::command::Command::prepare(keys);
        self.0.send(cmd).map_err(|_| NotRunningError)?;
        let mut statuses = rx.await.map_err(|_| NotRunningError)?;
        statuses.sort_by(|a, b| a.invocation_id().cmp(b.invocation_id()));
        Ok(statuses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn status_reader_distinguishes_empty_from_unavailable() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let reader = ChannelStatusReader(tx);
        let read = reader.try_read_status(KeyRange::FULL);
        let respond = async {
            rx.recv().await.unwrap().reply(Vec::new()).unwrap();
        };
        let (rows, ()) = tokio::join!(read, respond);
        assert!(rows.unwrap().is_empty());
        let read = reader.try_read_status(KeyRange::FULL);
        let abandon = async {
            drop(rx.recv().await.unwrap());
        };
        let (rows, ()) = tokio::join!(read, abandon);
        assert!(rows.is_err());
        drop(rx);
        assert!(reader.try_read_status(KeyRange::FULL).await.is_err());
    }
}
