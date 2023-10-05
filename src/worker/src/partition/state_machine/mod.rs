// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::storage::Transaction;
use command_interpreter::CommandInterpreter;
use dedup::DeduplicatingCommandInterpreter;
use restate_types::message::MessageIndex;

mod actions;
mod command_interpreter;
mod commands;
mod dedup;
mod effect_interpreter;
mod effects;

pub use actions::Action;
pub use command_interpreter::StateReader;
pub use commands::{
    AckCommand, AckMode, AckResponse, AckTarget, Command, DeduplicationSource, IngressAckResponse,
    ShuffleDeduplicationResponse,
};
pub use effect_interpreter::StateStorage;
pub use effect_interpreter::{ActionCollector, InterpretationResult};
pub use effects::Effects;
use restate_types::journal::raw::{RawEntryCodec, RawEntryCodecError};

pub struct StateMachine<Codec>(DeduplicatingCommandInterpreter<Codec>);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to deserialize entry: {0}")]
    Codec(#[from] RawEntryCodecError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

impl<Codec> StateMachine<Codec> {
    pub fn new(inbox_seq_number: MessageIndex, outbox_seq_number: MessageIndex) -> Self {
        Self(DeduplicatingCommandInterpreter::new(
            CommandInterpreter::new(inbox_seq_number, outbox_seq_number),
        ))
    }
}

impl<Codec: RawEntryCodec> StateMachine<Codec> {
    pub async fn tick<
        TransactionType: restate_storage_api::Transaction + Send,
        Collector: ActionCollector,
    >(
        &mut self,
        command: AckCommand,
        effects: &mut Effects,
        mut transaction: Transaction<TransactionType>,
        message_collector: Collector,
        is_leader: bool,
    ) -> Result<InterpretationResult<Transaction<TransactionType>, Collector>, Error> {
        // Handle the command, returns the span_relation to use to log effects
        let (fid, span_relation) = self.0.on_apply(command, effects, &mut transaction).await?;

        // Log the effects
        effects.log(is_leader, fid, span_relation);

        // Interpret effects
        effect_interpreter::EffectInterpreter::<Codec>::interpret_effects(
            effects,
            transaction,
            message_collector,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use bytestring::ByteString;
    use googletest::{all, assert_that, pat};
    use restate_invoker_api::InvokeInputJournal;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_storage_api::status_table::InvocationMetadata;
    use restate_storage_api::status_table::{InvocationStatus, StatusTable};
    use restate_storage_rocksdb::RocksDBStorage;
    use restate_test_util::matchers::*;
    use restate_test_util::test;
    use restate_types::identifiers::{FullInvocationId, PartitionKey};
    use restate_types::invocation::ServiceInvocation;
    use tempfile::tempdir;
    use tracing::info;

    type VecActionCollector = Vec<Action>;

    impl ActionCollector for VecActionCollector {
        fn collect(&mut self, message: Action) {
            self.push(message)
        }
    }

    // Test utility to test the StateMachine
    pub struct MockStateMachine {
        state_machine: StateMachine<ProtobufRawEntryCodec>,
        // TODO for the time being we use rocksdb storage because we have no mocks for storage interfaces.
        //  Perhaps we could make these tests faster by having those.
        rocksdb_storage: RocksDBStorage,
        effects_buffer: Effects,
    }

    impl Default for MockStateMachine {
        fn default() -> Self {
            MockStateMachine::new(0, 0)
        }
    }

    impl MockStateMachine {
        pub fn new(inbox_seq_number: MessageIndex, outbox_seq_number: MessageIndex) -> Self {
            let temp_dir = tempdir().unwrap();
            info!("Using RocksDB temp directory {}", temp_dir.path().display());
            Self {
                state_machine: StateMachine::new(inbox_seq_number, outbox_seq_number),
                rocksdb_storage: restate_storage_rocksdb::OptionsBuilder::default()
                    .path(temp_dir.into_path().to_str().unwrap().to_string())
                    .build()
                    .unwrap()
                    .build()
                    .unwrap(),
                effects_buffer: Default::default(),
            }
        }

        pub async fn tick(&mut self, command: AckCommand) -> Vec<Action> {
            let transaction = self.rocksdb_storage.transaction();
            self.state_machine
                .tick(
                    command,
                    &mut self.effects_buffer,
                    Transaction::new(0, 0..=PartitionKey::MAX, transaction),
                    VecActionCollector::default(),
                    true,
                )
                .await
                .unwrap()
                .commit()
                .await
                .unwrap()
        }

        pub fn storage(&self) -> &RocksDBStorage {
            &self.rocksdb_storage
        }
    }

    #[test(tokio::test)]
    async fn start_invocation() {
        let mut state_machine = MockStateMachine::default();

        let fid = FullInvocationId::generate("MySvc", Bytes::default());

        let actions = state_machine
            .tick(AckCommand::no_ack(Command::Invocation(ServiceInvocation {
                fid: fid.clone(),
                method_name: ByteString::from("MyMethod"),
                argument: Default::default(),
                response_sink: None,
                span_context: Default::default(),
            })))
            .await;

        assert_that!(
            actions,
            contains(pat!(Action::Invoke {
                full_invocation_id: eq(fid.clone()),
                invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
            }))
        );

        let invocation_status = state_machine
            .storage()
            .transaction()
            .get_invocation_status(&fid.service_id)
            .await
            .unwrap()
            .unwrap();
        assert_that!(
            invocation_status,
            pat!(InvocationStatus::Invoked(pat!(InvocationMetadata {
                invocation_uuid: eq(fid.invocation_uuid)
            })))
        )
    }
}
