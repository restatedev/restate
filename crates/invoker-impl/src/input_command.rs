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
    EntryIndex, InvocationId, PartitionKey, PartitionLeaderEpoch, ServiceId,
};
use restate_types::journal::Completion;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;

// -- Input messages

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct InvokeCommand {
    pub(super) partition: PartitionLeaderEpoch,
    pub(super) invocation_id: InvocationId,
    pub(super) service_id: ServiceId,
    #[serde(skip)]
    pub(super) journal: InvokeInputJournal,
}

#[derive(Debug)]
pub(crate) enum InputCommand {
    Invoke(InvokeCommand),
    Completion {
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        completion: Completion,
    },
    StoredEntryAck {
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    },

    /// Abort specific invocation id
    Abort {
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
        invocation_id: InvocationId,
        service_id: ServiceId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Invoke(InvokeCommand {
                    partition,
                    invocation_id,
                    service_id,
                    journal,
                }))
                .map_err(|_| NotRunningError),
        )
    }

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        completion: Completion,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Completion {
                    partition,
                    invocation_id,
                    completion,
                })
                .map_err(|_| NotRunningError),
        )
    }

    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::StoredEntryAck {
                    partition,
                    invocation_id,
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
        invocation_id: InvocationId,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Abort {
                    partition,
                    invocation_id,
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
