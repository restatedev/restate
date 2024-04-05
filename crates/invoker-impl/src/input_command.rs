// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_errors::NotRunningError;
use restate_invoker_api::{
    Effect, InvocationStatusReport, InvokeInputJournal, ServiceHandle, StatusHandle,
};
use restate_types::identifiers::{
    EntryIndex, FullInvocationId, PartitionKey, PartitionLeaderEpoch,
};
use restate_types::journal::Completion;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;

// -- Input messages

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct InvokeCommand {
    pub(super) partition: PartitionLeaderEpoch,
    pub(super) full_invocation_id: FullInvocationId,
    #[serde(skip)]
    pub(super) journal: InvokeInputJournal,
}

#[derive(Debug)]
pub(crate) enum InputCommand {
    Invoke(InvokeCommand),
    Completion {
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        completion: Completion,
    },
    StoredEntryAck {
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        entry_index: EntryIndex,
    },

    /// Abort specific invocation id
    Abort {
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
    },

    /// Command used to clean up internal state when a partition leader is going away
    AbortAllPartition {
        partition: PartitionLeaderEpoch,
    },

    // needed for dynamic registration at Invoker
    RegisterPartition {
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        sender: mpsc::Sender<Effect>,
    },
}

// -- Handles implementations. This is just glue code between the Input<Command> and the interfaces

#[derive(Debug, Clone)]
pub struct ChannelServiceHandle {
    pub(super) input: mpsc::UnboundedSender<InputCommand>,
}

impl ServiceHandle for ChannelServiceHandle {
    type Future = futures::future::Ready<Result<(), NotRunningError>>;

    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Invoke(InvokeCommand {
                    partition,
                    full_invocation_id,
                    journal,
                }))
                .map_err(|_| NotRunningError),
        )
    }

    fn resume(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Invoke(InvokeCommand {
                    partition,
                    full_invocation_id,
                    journal,
                }))
                .map_err(|_| NotRunningError),
        )
    }

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        completion: Completion,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Completion {
                    partition,
                    full_invocation_id,
                    completion,
                })
                .map_err(|_| NotRunningError),
        )
    }

    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        entry_index: EntryIndex,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::StoredEntryAck {
                    partition,
                    full_invocation_id,
                    entry_index,
                })
                .map_err(|_| NotRunningError),
        )
    }

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::AbortAllPartition { partition })
                .map_err(|_| NotRunningError),
        )
    }

    fn abort_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Abort {
                    partition,
                    full_invocation_id,
                })
                .map_err(|_| NotRunningError),
        )
    }

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        sender: mpsc::Sender<Effect>,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::RegisterPartition {
                    partition,
                    partition_key_range,
                    sender,
                })
                .map_err(|_| NotRunningError),
        )
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

        if let Ok(status_vec) = rx.await {
            itertools::Either::Right(status_vec.into_iter())
        } else {
            itertools::Either::Left(std::iter::empty::<InvocationStatusReport>())
        }
    }
}
