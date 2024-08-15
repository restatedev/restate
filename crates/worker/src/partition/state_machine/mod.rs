// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::metric_definitions::PARTITION_APPLY_COMMAND;
use crate::partition::storage::Transaction;
use metrics::{histogram, Histogram};
use restate_types::message::MessageIndex;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use std::time::Instant;

mod actions;
mod command_handler;
mod effect_interpreter;
mod tracing;

use ::tracing::Instrument;
pub use actions::Action;
pub use command_handler::StateReader;
pub use effect_interpreter::ActionCollector;
pub use effect_interpreter::StateStorage;
use restate_storage_api::invocation_status_table;
use restate_types::identifiers::PartitionKey;
use restate_types::journal::raw::{RawEntryCodec, RawEntryCodecError};
use restate_wal_protocol::Command;

pub struct StateMachine<Codec> {
    // initialized from persistent storage
    inbox_seq_number: MessageIndex,
    /// First outbox message index.
    outbox_head_seq_number: Option<MessageIndex>,
    /// Sequence number of the next outbox message to be appended.
    outbox_seq_number: MessageIndex,
    partition_key_range: RangeInclusive<PartitionKey>,
    latency: Histogram,

    /// This is used to establish whether for new invocations we should use the NeoInvocationStatus or not.
    /// The neo table is enabled via [restate_types::config::WorkerOptions::experimental_feature_new_invocation_status_table].
    default_invocation_status_source_table: invocation_status_table::SourceTable,

    _codec: PhantomData<Codec>,
}

impl<Codec> Debug for StateMachine<Codec> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachine")
            .field("inbox_seq_number", &self.inbox_seq_number)
            .field("outbox_head_seq_number", &self.outbox_head_seq_number)
            .field("outbox_seq_number", &self.outbox_seq_number)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to deserialize entry: {0}")]
    Codec(#[from] RawEntryCodecError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

impl<Codec> StateMachine<Codec> {
    pub fn new(
        inbox_seq_number: MessageIndex,
        outbox_seq_number: MessageIndex,
        outbox_head_seq_number: Option<MessageIndex>,
        partition_key_range: RangeInclusive<PartitionKey>,
        default_invocation_status_source_table: invocation_status_table::SourceTable,
    ) -> Self {
        let latency =
            histogram!(crate::metric_definitions::PARTITION_HANDLE_INVOKER_EFFECT_COMMAND);
        Self {
            inbox_seq_number,
            outbox_seq_number,
            outbox_head_seq_number,
            partition_key_range,
            latency,
            default_invocation_status_source_table,
            _codec: PhantomData,
        }
    }
}

pub(crate) struct StateMachineApplyContext<'a, S> {
    storage: &'a mut S,
    action_collector: &'a mut ActionCollector,
    is_leader: bool,
}

impl<Codec: RawEntryCodec> StateMachine<Codec> {
    pub async fn apply<TransactionType: restate_storage_api::Transaction + Send>(
        &mut self,
        command: Command,
        transaction: &mut Transaction<TransactionType>,
        action_collector: &mut ActionCollector,
        is_leader: bool,
    ) -> Result<(), Error> {
        let span = tracing::state_machine_apply_command_span(is_leader, &command);
        async {
            let start = Instant::now();
            // Apply the command
            let command_type = command.name();
            let res = self
                .on_apply(
                    StateMachineApplyContext {
                        storage: transaction,
                        action_collector,
                        is_leader,
                    },
                    command,
                )
                .await;
            histogram!(PARTITION_APPLY_COMMAND, "command" => command_type).record(start.elapsed());
            res
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests;
