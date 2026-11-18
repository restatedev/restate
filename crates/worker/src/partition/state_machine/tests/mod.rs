// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

mod delayed_send;
pub mod fixtures;
mod idempotency;
mod kill_cancel;
pub mod matchers;
mod workflow;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use bytestring::ByteString;
use futures::{StreamExt, TryStreamExt};
use googletest::{all, assert_that, pat};
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

use restate_core::TaskCenter;
use restate_partition_store::migrations::MigrationContext;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::Transaction;
use restate_storage_api::blob_table::{RawParts, ReadBlobTable};
use restate_storage_api::inbox_table::ReadInboxTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, CompletedResult, CompletionStatus, InFlightInvocationMetadata,
    InvocationStatus, InvocationStatusDiscriminants, ReadInvocationStatusTable, ResponseResultRef,
    WriteInvocationStatusTable,
};
use restate_storage_api::journal_table::{JournalEntry, ReadJournalTable};
use restate_storage_api::outbox_table::ReadOutboxTable;
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, VirtualObjectStatus, WriteVirtualObjectStatusTable,
};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::vqueue_table::scheduler::{
    RunAction, SchedulerAction, SchedulerDecisionsCommand,
};
use restate_storage_api::vqueue_table::stats::WaitStats;
use restate_test_util::matchers::*;
use restate_types::config::{Configuration, StorageOptions};
use restate_types::errors::{InvocationError, KILLED_INVOCATION_ERROR, codes};
use restate_types::identifiers::{
    AwakeableIdentifier, InvocationId, PartitionId, PartitionProcessorRpcRequestId, ServiceId,
};
use restate_types::invocation::client::{
    InvocationOutput, InvocationOutputResponse, SubmittedInvocationNotification,
};
use restate_types::invocation::{
    Header, InvocationTarget, InvocationTermination, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, Source, VirtualObjectHandlerType,
};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{CompleteAwakeableEntry, EntryResult, InvokeRequest};
use restate_types::journal::{Entry, EntryType};
use restate_types::journal_events::Event;
use restate_types::journal_v2::raw::TryFromEntry;
use restate_types::journal_v2::{EntryMetadata, OutputCommand, OutputResult};
use restate_types::logs::{Keys, SequenceNumber};
use restate_types::partitions::{Partition, PartitionFeatureChange, PersistedFeatures};
use restate_types::sharding::KeyRange;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::vqueues::Seq;
use restate_wal_protocol::v2::Command;
use restate_worker_api::invoker::{Effect, EffectKind};

use crate::ResolveResponseResult;
use crate::partition::processor::ProcessorRawContext;
use crate::partition::types::InvokerEffectKind;

pub struct TestEnv {
    // The state machine is now stateless; per-partition state lives in the
    // `Processor`. Tests drive it through an in-memory `ProcessorRawContext`.
    pub processor: ProcessorRawContext,
    // TODO for the time being we use rocksdb storage because we have no mocks for storage interfaces.
    //  Perhaps we could make these tests faster by having those.
    pub storage: PartitionStore,
    /// Whether the store is in a state the partition can reach: with vqueues
    /// enabled, everything it stores went through vqueues or their migration.
    /// Enabling vqueues on stored invocations without migrating them isn't.
    migrated: bool,
}

impl TestEnv {
    pub async fn shutdown(self) {
        TaskCenter::shutdown_node("test complete", 0).await;
    }

    /// Stores the content of every journal entry in the blob table however small, so every
    /// output too. Takes effect with the write blob table feature.
    pub fn always_write_output_blobs() {
        let mut config = Configuration::pinned().as_ref().clone();
        config.worker.inline_journal_entry_threshold = restate_memory::ByteCount::ZERO;
        restate_types::config::set_current_config(config);
    }

    pub async fn create() -> Self {
        Self::create_with_features(PersistedFeatures::default()).await
    }

    pub async fn create_with_features(features: PersistedFeatures) -> Self {
        Self::create_with_processor(ProcessorRawContext::new(
            Arc::new(Partition::new(PartitionId::MIN, KeyRange::FULL)),
            features,
        ))
        .await
    }

    pub async fn create_with_processor(processor: ProcessorRawContext) -> Self {
        // Try init logging, if not already initialized. This removes the need for the test_log macro
        let _ = tracing_subscriber::FmtSubscriber::builder().with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(  match std::env::var_os("RUST_LOG_SPAN_EVENTS") {
                Some(mut value) => {
                    value.make_ascii_lowercase();
                    let value = value.to_str().expect("test-log: RUST_LOG_SPAN_EVENTS must be valid UTF-8");
                    value.split(",").map(|filter| match filter.trim() {
                        "new" => FmtSpan::NEW,
                        "enter" => FmtSpan::ENTER,
                        "exit" => FmtSpan::EXIT,
                        "close" => FmtSpan::CLOSE,
                        "active" => FmtSpan::ACTIVE,
                        "full" => FmtSpan::FULL,
                        _ => panic!("test-log: RUST_LOG_SPAN_EVENTS must contain filters separated by `,`.\n\t\
                  For example: `active` or `new,close`\n\t\
                  Supported filters: new, enter, exit, close, active, full\n\t\
                  Got: {value}"),
                    }).fold(FmtSpan::NONE, |acc, filter| filter | acc)
                }
                None => FmtSpan::NONE,
            }).with_test_writer().try_init();

        RocksDbManager::init();
        let storage_options = StorageOptions::default();
        info!(
            "Using RocksDB temp directory {}",
            storage_options.data_dir("db").display()
        );
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut rocksdb_storage = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        // The experimental option enables both the partition feature and the storage feature
        if processor
            .fsm()
            .features()
            .is_invocation_status_output_blob_enabled()
        {
            let mut config = Configuration::pinned().as_ref().clone();
            config.common.experimental.set_write_blob_table(true);
            rocksdb_storage
                .verify_and_run_migrations(Default::default(), &config)
                .await
                .unwrap();
        }

        Self {
            processor,
            storage: rocksdb_storage,
            migrated: true,
        }
    }

    /// Applies the envelope with a fresh timestamp and the oldest LSN.
    pub async fn apply(&mut self, envelope: v2::Envelope<v2::Raw>) -> Vec<Action> {
        self.apply_at(envelope, MillisSinceEpoch::now(), Lsn::OLDEST)
            .await
    }

    /// Applies the envelope as if it was read from the log at `lsn` and created at `created_at`.
    pub async fn apply_at(
        &mut self,
        envelope: v2::Envelope<v2::Raw>,
        created_at: MillisSinceEpoch,
        lsn: Lsn,
    ) -> Vec<Action> {
        let mut transaction = self.storage.transaction();
        let mut action_collector = ActionCollector::default();
        let mut arena = BytesMut::new();
        StateMachine::apply(
            &mut self.processor,
            &mut transaction,
            DataRecord::new(created_at.into(), Keys::None, lsn, envelope),
            &mut action_collector,
            Some(LeaderContext::new(NonZeroUsize::new(64 * 1024))),
            &mut arena,
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();
        self.assert_invocations_match_entries();

        action_collector
    }

    /// Once the partition has fully migrated to vqueues, every invocation has its
    /// entry and every invocation entry its invocation, after every command:
    /// reads that find invocations through their entries depend on it. Each
    /// entry is also in a lane of the status index that its invocation's stored
    /// status and completion result select: the status the entry tells, which
    /// queries show, agrees with the stored one.
    fn assert_invocations_match_entries(&self) {
        if self.checks_invocation_entries() {
            let mismatches = self.storage.invocation_entry_mismatches().unwrap();
            std::assert!(
                mismatches.is_empty(),
                "invocations and their vqueue entries differ: {mismatches:?}"
            );
            let outside = self.storage.invocation_lane_mismatches().unwrap();
            std::assert!(
                outside.is_empty(),
                "entries outside their invocations' status lanes: {outside:?}"
            );
        }
    }

    pub async fn apply_multiple(
        &mut self,
        envelopes: impl IntoIterator<Item = v2::Envelope<v2::Raw>>,
    ) -> Vec<Action> {
        let mut actions = vec![];
        for envelop in envelopes {
            actions.append(&mut self.apply(envelop).await)
        }
        actions
    }

    pub fn storage(&mut self) -> &mut PartitionStore {
        &mut self.storage
    }

    pub async fn read_journal_to_vec(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Vec<journal_v2::Entry> {
        let entries = restate_storage_api::journal_table_v2::ReadJournalTable::get_journal(
            self.storage(),
            invocation_id,
            journal_length,
        )
        .expect("storage to be working")
        .try_collect::<Vec<_>>()
        .await
        .unwrap_or_else(|_| panic!("can read journal {invocation_id} of length {journal_length}"));

        let mut decoded = Vec::with_capacity(entries.len());
        for (i, e) in entries {
            let e = restate_storage_api::journal_table_v2::ReadJournalTable::hydrate_journal_entry(
                &self.storage,
                e,
            )
            .unwrap_or_else(|_| panic!("entry index {i} can be hydrated"));
            decoded.push(
                e.decode::<ServiceProtocolV4Codec, _>()
                    .unwrap_or_else(|_| panic!("entry index {i} can be decoded")),
            );
        }
        decoded
    }

    pub async fn read_journal_entry<E: TryFromEntry>(
        &mut self,
        invocation_id: InvocationId,
        idx: EntryIndex,
    ) -> E {
        let entry = restate_storage_api::journal_table_v2::ReadJournalTable::get_journal_entry(
            self.storage(),
            invocation_id,
            idx,
        )
        .await
        .expect("storage to be working")
        .expect("Entry to be present");
        restate_storage_api::journal_table_v2::ReadJournalTable::hydrate_journal_entry(
            &self.storage,
            entry,
        )
        .expect("Entry to be hydrated")
        .decode::<ServiceProtocolV4Codec, E>()
        .expect("Entry to be of the specified type")
    }

    /// Returns journal events ordered by timestamps
    pub async fn read_journal_events(&mut self, invocation_id: &InvocationId) -> Vec<Event> {
        let mut events =
            restate_storage_api::journal_events::ReadJournalEventsTable::get_journal_events(
                self.storage(),
                invocation_id,
            )
            .expect("storage to be working")
            .try_collect::<Vec<_>>()
            .await
            .expect("to be decodable");

        events.sort_by_key(|x| x.append_time);

        events
            .into_iter()
            .map(|e| e.event.into_event_or_unknown())
            .collect()
    }

    pub async fn modify_invocation_status(
        &mut self,
        invocation_id: InvocationId,
        f: impl FnOnce(&mut InvocationStatus),
    ) {
        let mut tx = self.storage().transaction();
        let mut status = tx.get_invocation_status(&invocation_id).await.unwrap();
        f(&mut status);
        tx.put_invocation_status(&invocation_id, &status).unwrap();
        tx.commit().await.unwrap();
    }

    pub async fn verify_journal_components(
        &mut self,
        invocation_id: InvocationId,
        entry_types: impl IntoIterator<Item = journal_v2::EntryType>,
    ) {
        let expected_entry_types = entry_types.into_iter().collect::<Vec<_>>();
        let expected_commands = expected_entry_types
            .iter()
            .filter(|e| e.is_command())
            .count();
        assert_that!(
            self.storage.get_invocation_status(&invocation_id).await,
            ok(all!(
                matchers::storage::has_journal_length(expected_entry_types.len() as u32),
                matchers::storage::has_commands(expected_commands as u32)
            ))
        );
        let actual_entry_types = self
            .read_journal_to_vec(invocation_id, expected_entry_types.len() as EntryIndex)
            .await
            .into_iter()
            .map(|e| e.ty())
            .collect::<Vec<_>>();
        assert_eq!(actual_entry_types, expected_entry_types);

        // Verify we don't go out of bounds
        assert_that!(
            self.storage
                .get_journal_entry(&invocation_id, expected_entry_types.len() as u32)
                .await,
            ok(none())
        );
    }

    /// Whether [`Self::apply_at`] checks that invocations and their vqueue entries
    /// correspond: once the partition has fully migrated, unless
    /// [`Self::set_enabled_features`] was used.
    pub fn checks_invocation_entries(&self) -> bool {
        self.migrated
            && self
                .processor
                .fsm()
                .features()
                .is_fully_migrated_to_vqueues()
    }

    /// Changes the features without migrating what is stored, unlike a partition,
    /// so invocations stored before enabling vqueues have no entries.
    pub fn set_enabled_features(&mut self, features: PersistedFeatures) {
        self.processor.set_enabled_features_in_memory(features);
        self.migrated = false;
    }

    /// Enables `change` from the next record on, as a version barrier does, for a change that
    /// needs no migration.
    pub fn enable_feature(&mut self, change: PartitionFeatureChange) {
        let mut features = *self.processor.enabled_features();
        change.apply_to(&mut features);
        self.processor.set_enabled_features_in_memory(features);
    }
}

type TestResult = Result<(), anyhow::Error>;

#[test(restate_core::test)]
async fn start_invocation() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let id = fixtures::mock_start_invocation(&mut test_env).await;

    let invocation_status = test_env.storage().get_invocation_status(&id).await.unwrap();
    assert_that!(invocation_status, pat!(InvocationStatus::Invoked(_)));
    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn shared_invocation_skips_inbox() -> TestResult {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::virtual_object(
        "MySvc",
        "MyKey",
        "MyHandler",
        VirtualObjectHandlerType::Shared,
    );

    // Let's lock the virtual object
    let mut tx = test_env.storage.transaction();
    tx.put_virtual_object_status(
        &invocation_target.as_keyed_service_id().unwrap(),
        &VirtualObjectStatus::Locked(InvocationId::mock_random()),
    )?;
    tx.commit().await.unwrap();
    drop(tx);

    // Start the invocation
    let invocation_id = fixtures::mock_start_invocation_with_invocation_target(
        &mut test_env,
        invocation_target.clone(),
    )
    .await;

    // Should be in invoked status
    let invocation_status = test_env
        .storage()
        .get_invocation_status(&invocation_id)
        .await
        .unwrap();
    assert_that!(
        invocation_status,
        pat!(InvocationStatus::Invoked(pat!(
            InFlightInvocationMetadata {
                invocation_target: eq(invocation_target.clone())
            }
        )))
    );
    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn complete_awakeable_with_success() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

    let callee_invocation_id = InvocationId::mock_random();
    let callee_entry_index = 10;
    let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
        CompleteAwakeableEntry {
            id: AwakeableIdentifier::new(callee_invocation_id, callee_entry_index)
                .to_string()
                .into(),
            result: EntryResult::Success(Bytes::default()),
        },
    ));

    let actions = test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry,
            },
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        target: pat!(JournalCompletionTarget {
                            caller_id: eq(callee_invocation_id),
                            caller_completion_id: eq(callee_entry_index),
                        }),
                        result: pat!(ResponseResult::Success { .. })
                    }
                ))
            )
        }))
    );
    test_env.shutdown().await;
}

#[test(restate_core::test)]
async fn complete_awakeable_with_failure() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

    let callee_invocation_id = InvocationId::mock_random();
    let callee_entry_index = 10;
    let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
        CompleteAwakeableEntry {
            id: AwakeableIdentifier::new(callee_invocation_id, callee_entry_index)
                .to_string()
                .into(),
            result: EntryResult::Failure(codes::BAD_REQUEST, "Some failure".into()),
        },
    ));

    let actions = test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry,
            },
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        target: pat!(JournalCompletionTarget {
                            caller_id: eq(callee_invocation_id),
                            caller_completion_id: eq(callee_entry_index),
                        }),
                        result: eq(ResponseResult::Failure(InvocationError::new(
                            codes::BAD_REQUEST,
                            "Some failure"
                        )))
                    }
                ))
            )
        }))
    );
    test_env.shutdown().await;
}

#[test(restate_core::test)]
async fn invoke_with_headers() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let service_id = ServiceId::mock_random();
    let invocation_id =
        fixtures::mock_start_invocation_with_service_id(&mut test_env, service_id.clone()).await;

    let actions = test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::invoke(
                    InvokeRequest {
                        service_name: service_id.service_name,
                        handler_name: "MyMethod".into(),
                        parameter: Bytes::default(),
                        headers: vec![Header::new("foo", "bar")],
                        key: service_id.key,
                        idempotency_key: None,
                    },
                    None,
                )),
            },
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(pat!(
                    restate_types::invocation::ServiceInvocation {
                        headers: eq(vec![Header::new("foo", "bar")])
                    }
                ))
            )
        }))
    );
    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn mutate_state() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let keyed_service_id = invocation_target.as_keyed_service_id().unwrap();
    let invocation_id = fixtures::mock_start_invocation_with_invocation_target(
        &mut test_env,
        invocation_target.clone(),
    )
    .await;

    let first_state_mutation: HashMap<Bytes, Bytes> = [
        (Bytes::from_static(b"foobar"), Bytes::from_static(b"foobar")),
        (Bytes::from_static(b"bar"), Bytes::from_static(b"bar")),
    ]
    .into_iter()
    .collect();

    let second_state_mutation: HashMap<Bytes, Bytes> =
        [(Bytes::from_static(b"bar"), Bytes::from_static(b"foo"))]
            .into_iter()
            .collect();

    // state should be empty
    assert_eq!(
        test_env
            .storage
            .get_all_user_states_for_service(&keyed_service_id)
            .unwrap()
            .count()
            .await,
        0
    );

    test_env
        .apply(commands::PatchStateCommand::test_envelope(
            ExternalStateMutation::new(keyed_service_id.clone(), None, first_state_mutation),
        ))
        .await;
    test_env
        .apply(commands::PatchStateCommand::test_envelope(
            ExternalStateMutation::new(
                keyed_service_id.clone(),
                None,
                second_state_mutation.clone(),
            ),
        ))
        .await;

    // terminating the ongoing invocation should trigger popping from the inbox until the
    // next invocation is found
    test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: InvokerEffectKind::End,
        }))
        .await;

    let all_states: HashMap<_, _> = test_env
        .storage
        .get_all_user_states_for_service(&keyed_service_id)
        .unwrap()
        .try_collect()
        .await?;

    assert_eq!(all_states, second_state_mutation);

    test_env.shutdown().await;
    Ok(())
}

fn state_mutation(service_id: &ServiceId) -> ExternalStateMutation {
    ExternalStateMutation::new(
        service_id.clone(),
        None,
        [(Bytes::from_static(b"key"), Bytes::from_static(b"value"))].into(),
    )
}

fn patch_state(mutation: ExternalStateMutation) -> v2::Envelope<v2::Raw> {
    commands::PatchStateCommand::test_envelope(mutation)
}

/// A scheduler decision to run the entry with `key` in `qid`.
fn run_decision(qid: &VQueueId, key: EntryKey) -> v2::Envelope<v2::Raw> {
    commands::SchedulerDecisionsCommand::test_envelope(SchedulerDecisionsCommand {
        qids: vec![(
            qid.clone(),
            vec![SchedulerAction::Run(RunAction {
                key,
                wait_stats: WaitStats::default(),
            })],
        )],
    })
}

/// Returns the stage, key and vqueue of the state mutation's entry.
async fn get_entry_status(
    test_env: &mut TestEnv,
    id: &StateMutationId,
) -> Option<(Stage, EntryKey, VQueueId)> {
    let transaction = test_env.storage.transaction();
    let header = transaction
        .get_vqueue_entry_status(&BaseEntryId::from(id))
        .await
        .unwrap()?;
    Some((
        header.stage(),
        *header.entry_key(),
        header.vqueue_id().clone(),
    ))
}

/// State mutations must be enqueued under the same id on all replicas (#5416).
#[test(restate_core::test)]
async fn state_mutation_ids_are_deterministic() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await;
    let service_id = ServiceId::new(None, "MySvc", "my-key");
    let partition_key = service_id.partition_key();
    let created_at = MillisSinceEpoch::now();

    // Without an id, the id is derived from the command's position in the log
    test_env
        .apply_at(
            patch_state(state_mutation(&service_id)),
            created_at,
            Lsn::new(10),
        )
        .await;
    let derived_id = StateMutationId::from_parts(partition_key, 0, 10);
    let (stage, key, _) = get_entry_status(&mut test_env, &derived_id).await.unwrap();
    assert_eq!(stage, Stage::Inbox);
    assert_eq!(key.seq(), Seq::from(Lsn::new(10)));

    // An id assigned by the admin api takes precedence
    let mutation = state_mutation(&service_id).with_generated_id();
    let assigned_id = mutation.id().unwrap();
    test_env
        .apply_at(patch_state(mutation.clone()), created_at, Lsn::new(11))
        .await;
    let (_, key, _) = get_entry_status(&mut test_env, &assigned_id).await.unwrap();
    assert_eq!(key.seq(), Seq::from(Lsn::new(11)));

    // A command with an already enqueued id is a duplicate and gets dropped
    test_env
        .apply_at(patch_state(mutation), created_at, Lsn::new(12))
        .await;
    let (_, key, qid) = get_entry_status(&mut test_env, &assigned_id).await.unwrap();
    assert_eq!(key.seq(), Seq::from(Lsn::new(11)));
    let duplicate_key = EntryKey::new(
        key.has_lock(),
        key.run_at(),
        Lsn::new(12),
        EntryId::from(StateMutationId::generate(partition_key)),
    );
    std::assert!(
        test_env
            .storage
            .transaction()
            .find_inbox_state_mutation_key(&qid, &duplicate_key)
            .await?
            .is_none()
    );

    test_env.shutdown().await;
    Ok(())
}

/// New invocations persist the sequence of the record that created them, which matches the
/// sequence of their vqueue entry.
#[test(restate_core::test)]
async fn invocation_creation_seq() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await;
    let invocation_id = InvocationId::mock_random();
    test_env
        .apply_at(
            commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                ..ServiceInvocation::mock()
            }),
            MillisSinceEpoch::now(),
            Lsn::new(10),
        )
        .await;

    let entry_seq = test_env
        .storage
        .transaction()
        .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
        .await?
        .unwrap()
        .seq();
    assert_eq!(entry_seq, Seq::from(Lsn::new(10)));
    let status = test_env
        .storage
        .get_invocation_status(&invocation_id)
        .await?;
    assert_eq!(status.get_creation_seq(), Some(entry_seq));

    test_env.shutdown().await;
    Ok(())
}

/// Invocations created before the creation seq was introduced get it backfilled from their
/// vqueue entry on transitions that write the invocation status anyway.
#[test(restate_core::test)]
async fn invocation_creation_seq_backfill() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await;
    let invocation_id = InvocationId::mock_random();
    test_env
        .apply_at(
            commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                ..ServiceInvocation::mock()
            }),
            MillisSinceEpoch::now(),
            Lsn::new(10),
        )
        .await;
    let entry_seq = Seq::from(Lsn::new(10));

    // Mimics an invocation created by an older version
    async fn clear_creation_seq(test_env: &mut TestEnv, invocation_id: InvocationId) {
        test_env
            .modify_invocation_status(invocation_id, |status| match status {
                InvocationStatus::Scheduled(scheduled) => scheduled.metadata.creation_seq = None,
                InvocationStatus::Inboxed(inboxed) => inboxed.metadata.creation_seq = None,
                status => status.get_invocation_metadata_mut().unwrap().creation_seq = None,
            })
            .await;
    }
    async fn get_creation_seq(test_env: &mut TestEnv, invocation_id: InvocationId) -> Option<Seq> {
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await
            .unwrap()
            .get_creation_seq()
    }

    // Run
    clear_creation_seq(&mut test_env, invocation_id).await;
    let header = test_env
        .storage
        .transaction()
        .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
        .await?
        .unwrap();
    test_env
        .apply(run_decision(header.vqueue_id(), *header.entry_key()))
        .await;
    assert_eq!(
        get_creation_seq(&mut test_env, invocation_id).await,
        Some(entry_seq)
    );

    // Pin deployment
    clear_creation_seq(&mut test_env, invocation_id).await;
    fixtures::mock_pinned_deployment_v7(&mut test_env, invocation_id).await;
    assert_eq!(
        get_creation_seq(&mut test_env, invocation_id).await,
        Some(entry_seq)
    );

    // Suspend
    clear_creation_seq(&mut test_env, invocation_id).await;
    test_env
        .apply(fixtures::invoker_suspended(
            invocation_id,
            NotificationId::for_completion(1),
        ))
        .await;
    let status = test_env
        .storage
        .get_invocation_status(&invocation_id)
        .await?;
    assert_that!(status, pat!(InvocationStatus::Suspended { .. }));
    assert_eq!(status.get_creation_seq(), Some(entry_seq));

    // Resume
    clear_creation_seq(&mut test_env, invocation_id).await;
    test_env
        .apply(commands::ResumeInvocationCommand::test_envelope(
            ResumeInvocationRequest {
                invocation_id,
                update_deployment_id: None,
                update_pinned_deployment_id: None,
                run_at: None,
                response_sink: None,
            },
        ))
        .await;
    let status = test_env
        .storage
        .get_invocation_status(&invocation_id)
        .await?;
    assert_that!(status, pat!(InvocationStatus::Invoked(_)));
    assert_eq!(status.get_creation_seq(), Some(entry_seq));

    test_env.shutdown().await;
    Ok(())
}

/// With vqueues enabled from the start, every invocation keeps its vqueue entry and every
/// invocation entry its invocation after every command, which `TestEnv::apply_at` checks:
/// through a scheduler run, completion with and without retention, purge, killing an
/// inboxed invocation and cancelling a scheduled one.
#[test(restate_core::test)]
async fn invocations_keep_their_vqueue_entries_through_their_lifecycle() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await;
    let invoke = |invocation_id, retention: Duration, execution_time| {
        commands::InvokeCommand::test_envelope(ServiceInvocation {
            invocation_id,
            invocation_target: InvocationTarget::mock_service(),
            completion_retention_duration: retention,
            journal_retention_duration: retention,
            execution_time,
            ..ServiceInvocation::mock()
        })
    };
    let output = |invocation_id| {
        fixtures::invoker_entry_effect(
            invocation_id,
            OutputCommand {
                result: OutputResult::Success(Default::default()),
                name: Default::default(),
            },
        )
    };
    let terminate = |invocation_id, flavor| {
        commands::TerminateInvocationCommand::test_envelope(InvocationTermination {
            invocation_id,
            flavor,
            response_sink: None,
        })
    };

    // Retained: run by the scheduler, completed, then purged.
    let retained = InvocationId::mock_random();
    test_env
        .apply(invoke(retained, Duration::from_secs(120), None))
        .await;
    let header = test_env
        .storage
        .transaction()
        .get_vqueue_entry_status(&BaseEntryId::from(retained))
        .await?
        .unwrap();
    test_env
        .apply(run_decision(header.vqueue_id(), *header.entry_key()))
        .await;
    assert_that!(
        test_env.storage.get_invocation_status(&retained).await?,
        pat!(InvocationStatus::Invoked(_))
    );
    test_env
        .apply_multiple([
            fixtures::pinned_deployment(retained, ServiceProtocolVersion::V6),
            output(retained),
            fixtures::invoker_end_effect(retained),
        ])
        .await;
    assert_that!(
        test_env.storage.get_invocation_status(&retained).await?,
        pat!(InvocationStatus::Completed(_))
    );
    test_env
        .apply(commands::PurgeInvocationCommand::test_envelope(
            PurgeInvocationRequest {
                invocation_id: retained,
                response_sink: None,
            },
        ))
        .await;
    assert_that!(
        test_env.storage.get_invocation_status(&retained).await?,
        pat!(InvocationStatus::Free)
    );

    // Not retained: completed and removed at once.
    let unretained = InvocationId::mock_random();
    test_env
        .apply(invoke(unretained, Duration::ZERO, None))
        .await;
    let header = test_env
        .storage
        .transaction()
        .get_vqueue_entry_status(&BaseEntryId::from(unretained))
        .await?
        .unwrap();
    test_env
        .apply_multiple([
            run_decision(header.vqueue_id(), *header.entry_key()),
            fixtures::pinned_deployment(unretained, ServiceProtocolVersion::V6),
            output(unretained),
            fixtures::invoker_end_effect(unretained),
        ])
        .await;
    assert_that!(
        test_env.storage.get_invocation_status(&unretained).await?,
        pat!(InvocationStatus::Free)
    );

    // Killed while inboxed, cancelled while scheduled.
    let inboxed = InvocationId::mock_random();
    let scheduled = InvocationId::mock_random();
    let later = MillisSinceEpoch::new(MillisSinceEpoch::now().as_u64() + 3_600_000);
    test_env
        .apply_multiple([
            invoke(inboxed, Duration::from_secs(120), None),
            invoke(scheduled, Duration::from_secs(120), Some(later)),
        ])
        .await;
    assert_that!(
        test_env.storage.get_invocation_status(&scheduled).await?,
        pat!(InvocationStatus::Scheduled(_))
    );
    test_env
        .apply_multiple([
            terminate(inboxed, TerminationFlavor::Kill),
            terminate(scheduled, TerminationFlavor::Cancel),
        ])
        .await;

    test_env.shutdown().await;
    Ok(())
}

/// The features of a partition with vqueues, before its vqueue lifecycle is refined.
fn vqueue_features() -> PersistedFeatures {
    PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
        PartitionFeatureChange::EnablePreflightInvocationTerminationRetention,
    ])
}

/// Records come from different nodes' clocks, so the record that completes an invocation can be
/// older than the one that created it. Ending, killing while inboxed and cancelling while
/// scheduled each complete the invocation no earlier than its creation. With a refined vqueue
/// lifecycle, its vqueue entry enters Finished at exactly that completion time; before, at the
/// completing record's time.
#[test(restate_core::test)]
async fn finished_entries_transition_at_the_invocation_completion_time() -> TestResult {
    let mut test_env = TestEnv::create_with_features(vqueue_features()).await;
    for lifecycle_refined in [false, true] {
        if lifecycle_refined {
            test_env.enable_feature(PartitionFeatureChange::EnableVqLifecycleRefined);
        }
        let created_at = MillisSinceEpoch::now();
        // The clock of the node writing the later records is five seconds behind.
        let behind = MillisSinceEpoch::new(created_at.as_u64() - 5_000);
        let later = MillisSinceEpoch::new(created_at.as_u64() + 3_600_000);
        let invoke = |invocation_id, execution_time| {
            commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                completion_retention_duration: Duration::from_secs(120),
                execution_time,
                ..ServiceInvocation::mock()
            })
        };
        let terminate = |invocation_id, flavor| {
            commands::TerminateInvocationCommand::test_envelope(InvocationTermination {
                invocation_id,
                flavor,
                response_sink: None,
            })
        };

        let [ended, inboxed, scheduled] = [(); 3].map(|_| InvocationId::mock_random());
        for (invocation_id, execution_time) in
            [(ended, None), (inboxed, None), (scheduled, Some(later))]
        {
            test_env
                .apply_at(
                    invoke(invocation_id, execution_time),
                    created_at,
                    Lsn::OLDEST,
                )
                .await;
        }
        let (_, qid, key) = invocation_entry(&mut test_env, &ended).await;
        for envelope in [
            run_decision(&qid, key),
            fixtures::pinned_deployment(ended, ServiceProtocolVersion::V6),
            fixtures::invoker_entry_effect(
                ended,
                OutputCommand {
                    result: OutputResult::Success(Default::default()),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(ended),
            terminate(inboxed, TerminationFlavor::Kill),
            terminate(scheduled, TerminationFlavor::Cancel),
        ] {
            test_env.apply_at(envelope, behind, Lsn::OLDEST).await;
        }

        let finished_at = if lifecycle_refined {
            created_at
        } else {
            behind
        };
        for invocation_id in [ended, inboxed, scheduled] {
            let InvocationStatus::Completed(completed) = test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await?
            else {
                panic!("{invocation_id} is completed and retained");
            };
            assert_eq!(
                completed.timestamps.completed_transition_time(),
                Some(created_at)
            );
            let entry = test_env
                .storage
                .transaction()
                .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
                .await?
                .unwrap();
            assert_eq!(entry.stage(), Stage::Finished);
            assert_eq!(
                entry.stats().transitioned_at,
                UniqueTimestamp::from_unix_millis_unchecked(finished_at),
                "{invocation_id}, refined: {lifecycle_refined}"
            );
        }
    }
    test_env.shutdown().await;
    Ok(())
}

/// Paused and suspended entries carry their status, and resuming or waking them up restores it.
/// An entry yielded by a leader change records `yielded` in the inbox. With a refined vqueue
/// lifecycle, its next run records `started`, so pausing or suspending it carries `started` and
/// it comes back ready to run. Before, a later run kept `yielded`, and so did pausing and
/// suspending.
#[test(restate_core::test)]
async fn parked_entries_carry_their_latest_run_status() -> TestResult {
    let mut test_env = TestEnv::create_with_features(vqueue_features()).await;
    for lifecycle_refined in [false, true] {
        if lifecycle_refined {
            test_env.enable_feature(PartitionFeatureChange::EnableVqLifecycleRefined);
        }
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::generate(&invocation_target, None);
        let entry = async |test_env: &mut TestEnv| {
            let header = test_env
                .storage
                .transaction()
                .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
                .await
                .unwrap()
                .unwrap();
            (header.stage(), header.status())
        };
        let run = async |test_env: &mut TestEnv| {
            let (_, qid, key) = invocation_entry(test_env, &invocation_id).await;
            test_env.apply(run_decision(&qid, key)).await;
        };
        let resume = || {
            commands::ResumeInvocationCommand::test_envelope(ResumeInvocationRequest {
                invocation_id,
                update_deployment_id: None,
                update_pinned_deployment_id: None,
                run_at: None,
                response_sink: None,
            })
        };

        test_env
            .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                invocation_target: invocation_target.clone(),
                completion_retention_duration: Duration::from_secs(120),
                journal_retention_duration: Duration::from_secs(120),
                ..ServiceInvocation::mock()
            }))
            .await;
        run(&mut test_env).await;
        fixtures::mock_pinned_deployment_v7(&mut test_env, invocation_id).await;
        test_env
            .apply(commands::InvokerEffectCommand::test_envelope(Effect {
                invocation_id,
                kind: InvokerEffectKind::Yield {
                    reason: YieldReason::PartitionLeaderChange,
                    error_event: None,
                    resume_at: None,
                },
            }))
            .await;
        assert_eq!(
            entry(&mut test_env).await,
            (Stage::Inbox, vqueue_table::Status::Yielded)
        );

        let ran = if lifecycle_refined {
            vqueue_table::Status::Started
        } else {
            vqueue_table::Status::Yielded
        };

        // Run again, pause, resume.
        run(&mut test_env).await;
        assert_eq!(entry(&mut test_env).await, (Stage::Running, ran));
        test_env
            .apply(commands::PauseInvocationCommand::test_envelope(
                commands::PauseInvocationCommand {
                    invocation_id,
                    request_id: Some(PartitionProcessorRpcRequestId::new()),
                },
            ))
            .await;
        assert_eq!(entry(&mut test_env).await, (Stage::Paused, ran));
        test_env.apply(resume()).await;
        assert_eq!(entry(&mut test_env).await, (Stage::Inbox, ran));

        // Run again, suspend, wake up.
        run(&mut test_env).await;
        test_env
            .apply(fixtures::invoker_suspended(
                invocation_id,
                NotificationId::for_completion(1),
            ))
            .await;
        assert_eq!(entry(&mut test_env).await, (Stage::Suspended, ran));
        test_env.apply(resume()).await;
        assert_eq!(entry(&mut test_env).await, (Stage::Inbox, ran));
    }
    test_env.shutdown().await;
    Ok(())
}

/// The stage of the invocation's vqueue entry, its queue and key. The entry must exist.
async fn invocation_entry(
    test_env: &mut TestEnv,
    invocation_id: &InvocationId,
) -> (Stage, VQueueId, EntryKey) {
    let header = test_env
        .storage
        .transaction()
        .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
        .await
        .unwrap()
        .expect("the invocation has its entry");
    (
        header.stage(),
        header.vqueue_id().clone(),
        *header.entry_key(),
    )
}

/// Like the test above, through the other paths an invocation takes: yielding back to the inbox
/// and running again, a manual pause and resume, and restarting a completed invocation as a new
/// one. `TestEnv::apply_at` checks after every command that invocations and their entries
/// correspond; the test makes sure the check is on.
#[test(restate_core::test)]
async fn invocations_keep_their_vqueue_entries_through_yield_pause_and_restart() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await;
    std::assert!(test_env.checks_invocation_entries());
    let status = async |test_env: &mut TestEnv, invocation_id| {
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await
            .unwrap()
            .discriminant()
    };
    let run = async |test_env: &mut TestEnv, invocation_id| {
        let (stage, qid, key) = invocation_entry(test_env, &invocation_id).await;
        assert_eq!(stage, Stage::Inbox);
        test_env.apply(run_decision(&qid, key)).await;
        assert_eq!(
            invocation_entry(test_env, &invocation_id).await.0,
            Stage::Running
        );
    };

    let invocation_target = InvocationTarget::mock_service();
    let invocation_id = InvocationId::generate(&invocation_target, None);
    test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            completion_retention_duration: Duration::from_secs(120),
            journal_retention_duration: Duration::from_secs(120),
            ..ServiceInvocation::mock()
        }))
        .await;
    run(&mut test_env, invocation_id).await;
    test_env
        .apply(fixtures::pinned_deployment(
            invocation_id,
            ServiceProtocolVersion::V6,
        ))
        .await;

    // Yield back to the inbox, then run again.
    test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: InvokerEffectKind::Yield {
                reason: YieldReason::PartitionLeaderChange,
                error_event: None,
                resume_at: None,
            },
        }))
        .await;
    run(&mut test_env, invocation_id).await;

    // Pause by hand, then resume and run again.
    test_env
        .apply(commands::PauseInvocationCommand::test_envelope(
            commands::PauseInvocationCommand {
                invocation_id,
                request_id: Some(PartitionProcessorRpcRequestId::new()),
            },
        ))
        .await;
    assert_eq!(
        status(&mut test_env, invocation_id).await,
        Some(InvocationStatusDiscriminants::Paused)
    );
    assert_eq!(
        invocation_entry(&mut test_env, &invocation_id).await.0,
        Stage::Paused
    );
    test_env
        .apply(commands::ResumeInvocationCommand::test_envelope(
            ResumeInvocationRequest {
                invocation_id,
                update_deployment_id: None,
                update_pinned_deployment_id: None,
                run_at: None,
                response_sink: None,
            },
        ))
        .await;
    run(&mut test_env, invocation_id).await;

    // Complete it, then restart it as a new invocation.
    test_env
        .apply_multiple([
            fixtures::invoker_entry_effect(
                invocation_id,
                OutputCommand {
                    result: OutputResult::Success(Default::default()),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(invocation_id),
        ])
        .await;
    assert_eq!(
        status(&mut test_env, invocation_id).await,
        Some(InvocationStatusDiscriminants::Completed)
    );
    assert_eq!(
        invocation_entry(&mut test_env, &invocation_id).await.0,
        Stage::Finished
    );
    let restarted = InvocationId::from_parts(
        invocation_id.partition_key(),
        InvocationUuid::generate(&invocation_target, None),
    );
    test_env
        .apply(commands::RestartAsNewInvocationCommand::test_envelope(
            RestartAsNewInvocationRequest {
                invocation_id,
                new_invocation_id: restarted,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: None,
                response_sink: None,
                span_context: None,
            },
        ))
        .await;
    assert_eq!(
        status(&mut test_env, restarted).await,
        Some(InvocationStatusDiscriminants::Inboxed)
    );
    run(&mut test_env, restarted).await;

    test_env.shutdown().await;
    Ok(())
}

/// Replicas might store state mutations that were enqueued before #5416 was fixed under different
/// ids. A decision to run them must still find the local entry.
#[test(restate_core::test)]
async fn run_state_mutation_with_different_entry_id() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await;
    let service_id = ServiceId::new(None, "MySvc", "my-key");
    let partition_key = service_id.partition_key();
    let created_at = MillisSinceEpoch::now();

    test_env
        .apply_at(
            patch_state(state_mutation(&service_id)),
            created_at,
            Lsn::new(10),
        )
        .await;
    let local_id = StateMutationId::from_parts(partition_key, 0, 10);
    let (_, local_key, qid) = get_entry_status(&mut test_env, &local_id).await.unwrap();

    // The leader stored the same state mutation under a different id
    let leader_key = |seq: u64| {
        EntryKey::new(
            local_key.has_lock(),
            local_key.run_at(),
            Lsn::new(seq),
            EntryId::from(StateMutationId::generate(partition_key)),
        )
    };
    // A decision for the position of the local entry runs it
    test_env.apply(run_decision(&qid, leader_key(10))).await;
    std::assert!(get_entry_status(&mut test_env, &local_id).await.is_none());
    let states: HashMap<_, _> = test_env
        .storage
        .get_all_user_states_for_service(&service_id)?
        .try_collect()
        .await?;
    assert_eq!(
        states,
        HashMap::from([(Bytes::from_static(b"key"), Bytes::from_static(b"value"))])
    );

    test_env.shutdown().await;
    Ok(())
}

/// Removing inconsistent state mutations removes all pending ones. Afterwards, decisions are only
/// matched by their exact id.
#[test(restate_core::test)]
async fn inconsistent_state_mutation_cleanup() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await;
    let service_id = ServiceId::new(None, "MySvc", "my-key");
    let partition_key = service_id.partition_key();
    let created_at = MillisSinceEpoch::now();

    test_env
        .apply_at(
            patch_state(state_mutation(&service_id)),
            created_at,
            Lsn::new(10),
        )
        .await;
    let assigned = state_mutation(&service_id).with_generated_id();
    let assigned_id = assigned.id().unwrap();
    test_env
        .apply_at(patch_state(assigned), created_at, Lsn::new(11))
        .await;
    let derived_id = StateMutationId::from_parts(partition_key, 0, 10);
    let (_, _, qid) = get_entry_status(&mut test_env, &derived_id).await.unwrap();

    let partition_db = test_env.storage.partition_db().clone();
    let removed = restate_vqueues::migrations::remove_pending_state_mutations(
        &MigrationContext::new(
            &Configuration::pinned(),
            &partition_db,
            KeyRange::FULL,
            CancellationToken::new(),
        ),
        test_env.processor.vqueues_mut(),
        UniqueTimestamp::from_unix_millis_unchecked(created_at),
    )
    .await?;

    assert_eq!(removed, 2);
    std::assert!(get_entry_status(&mut test_env, &derived_id).await.is_none());
    std::assert!(
        get_entry_status(&mut test_env, &assigned_id)
            .await
            .is_none()
    );
    let txn = test_env.storage.transaction();
    std::assert!(
        txn.get_vqueue(&qid)
            .await?
            .is_none_or(|meta| meta.is_inbox_empty())
    );
    drop(txn);

    // With the cleanup enabled, a decision with a different id no longer runs the local entry
    test_env.set_enabled_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
        PartitionFeatureChange::EnableInconsistentStateMutationCleanup,
    ]));
    test_env
        .apply_at(
            patch_state(state_mutation(&service_id)),
            created_at,
            Lsn::new(20),
        )
        .await;
    let local_id = StateMutationId::from_parts(partition_key, 0, 20);
    let (_, local_key, _) = get_entry_status(&mut test_env, &local_id).await.unwrap();
    test_env
        .apply(run_decision(
            &qid,
            EntryKey::new(
                local_key.has_lock(),
                local_key.run_at(),
                local_key.seq(),
                EntryId::from(StateMutationId::generate(partition_key)),
            ),
        ))
        .await;
    std::assert!(get_entry_status(&mut test_env, &local_id).await.is_some());
    assert_eq!(
        test_env
            .storage
            .get_all_user_states_for_service(&service_id)?
            .count()
            .await,
        0
    );

    test_env.shutdown().await;
    Ok(())
}

/// A terminal failure reported by the invoker must classify ABORTED as cancelled in the retained
/// vqueue entry, independently of the blob table feature.
#[test(restate_core::test)]
async fn invoker_failure_retained_vqueue_status() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await;

    for (code, expected_status) in [
        (codes::ABORTED, vqueue_table::Status::Cancelled),
        (codes::INTERNAL, vqueue_table::Status::Failed),
    ] {
        let invocation_id = InvocationId::mock_generate(&InvocationTarget::mock_service());
        test_env
            .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                completion_retention_duration: Duration::from_secs(120),
                ..ServiceInvocation::mock()
            }))
            .await;

        let entry_id = BaseEntryId::from(invocation_id);
        let header = test_env
            .storage
            .transaction()
            .get_vqueue_entry_status(&entry_id)
            .await?
            .expect("invocation must have a vqueue entry");
        test_env
            .apply_multiple([
                run_decision(header.vqueue_id(), *header.entry_key()),
                fixtures::pinned_deployment(invocation_id, ServiceProtocolVersion::V4),
                commands::InvokerEffectCommand::test_envelope(Effect {
                    invocation_id,
                    kind: InvokerEffectKind::Failed(InvocationError::new(code, "sdk error")),
                }),
            ])
            .await;

        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await?,
            pat!(InvocationStatus::Completed(_))
        );
        let header = test_env
            .storage
            .transaction()
            .get_vqueue_entry_status(&entry_id)
            .await?
            .expect("retained invocation must keep its vqueue entry");
        assert_eq!(header.stage(), Stage::Finished);
        assert_eq!(header.status(), expected_status);
    }

    test_env.shutdown().await;
    Ok(())
}

/// With the blob table, killed and failed invocations end their journal with an Output entry
/// whose result is read back from the output blob, like completed ones do.
#[test(restate_core::test)]
async fn output_entry_on_kill_and_failure() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableInvocationStatusOutputBlob,
    ]))
    .await;

    // Multi-byte characters, so truncating by bytes would split one. Over the inline threshold.
    let long_message = format!("{}{}", "é".repeat(150), "x".repeat(5000));
    // (kill, failure message), inlined unless the message is long
    for (kill, message) in [(true, ""), (false, "boom"), (false, long_message.as_str())] {
        let invocation_id = InvocationId::mock_generate(&InvocationTarget::mock_service());
        let (end, expected_error) = if kill {
            (
                commands::TerminateInvocationCommand::test_envelope(InvocationTermination {
                    invocation_id,
                    flavor: TerminationFlavor::Kill,
                    response_sink: None,
                }),
                KILLED_INVOCATION_ERROR,
            )
        } else {
            let error = InvocationError::new(codes::INTERNAL, message.to_owned());
            (
                commands::InvokerEffectCommand::test_envelope(Effect {
                    invocation_id,
                    kind: InvokerEffectKind::Failed(error.clone()),
                }),
                error,
            )
        };
        test_env
            .apply_multiple([
                commands::InvokeCommand::test_envelope(ServiceInvocation {
                    invocation_id,
                    invocation_target: InvocationTarget::mock_service(),
                    completion_retention_duration: Duration::from_secs(120),
                    journal_retention_duration: Duration::from_secs(120),
                    ..ServiceInvocation::mock()
                }),
                fixtures::pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
                end,
            ])
            .await;
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    journal_v2::CommandType::Input.into(),
                    journal_v2::CommandType::Output.into(),
                ],
            )
            .await;
        // Every journal ends with the Output, hydrated from the blob if needed
        assert_eq!(
            test_env.read_journal_to_vec(invocation_id, 2).await[1],
            journal_v2::Entry::from(OutputCommand {
                result: OutputResult::Failure(expected_error.clone().into()),
                name: Default::default(),
            })
        );

        let InvocationStatus::Completed(CompletedInvocation {
            response_result, ..
        }) = test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await?
        else {
            panic!("expected a completed invocation");
        };
        let output_entry = journal_table_v2::ReadJournalTable::get_journal_entry(
            &mut test_env.storage,
            invocation_id,
            1,
        )
        .await?
        .expect("output entry");

        let ResponseResultRef::Completed(CompletedResult { blob_id, status }) = response_result
        else {
            // Inlined in both the status and the journal
            let expected = if kill {
                ResponseResultRef::Failure(KILLED_INVOCATION_ERROR)
            } else {
                ResponseResultRef::Failure(expected_error)
            };
            assert_eq!(response_result, expected);
            assert_eq!(output_entry.content_blob(), None);
            continue;
        };
        assert_eq!(message, long_message);
        // The status keeps the first 200 characters, the blob the full message
        assert_eq!(
            status,
            CompletionStatus::Failure {
                code: codes::INTERNAL,
                message: format!("{}{}", "é".repeat(150), "x".repeat(50)).into(),
            }
        );
        // The status points to the blob storing the content of the Output entry
        assert_eq!(
            output_entry.content_blob().map(|blob| blob.id),
            Some(blob_id)
        );
        // Referenced by both the completed status and the journal
        let (_, ref_count, _) = test_env
            .storage
            .get_blob::<RawParts>(blob_id)
            .await?
            .expect("output blob");
        assert_eq!(ref_count, 2);
    }

    test_env.shutdown().await;
    Ok(())
}

/// Unpinned invocations get no synthesized Output on kill/failure: their Input can still be in
/// the v1 journal table, so appending to v2 would split the journal. The result is inlined.
#[test(restate_core::test)]
async fn no_output_entry_on_kill_and_failure_of_unpinned_v1_journal() -> TestResult {
    // Without journal v2 as default, the Input is written to the v1 table
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableInvocationStatusOutputBlob,
    ]))
    .await;

    let failure = InvocationError::new(codes::INTERNAL, "x".repeat(5000));
    for (kill, expected) in [
        (true, ResponseResultRef::Failure(KILLED_INVOCATION_ERROR)),
        (false, ResponseResultRef::Failure(failure.clone())),
    ] {
        let invocation_id = InvocationId::mock_generate(&InvocationTarget::mock_service());
        let end = if kill {
            commands::TerminateInvocationCommand::test_envelope(InvocationTermination {
                invocation_id,
                flavor: TerminationFlavor::Kill,
                response_sink: None,
            })
        } else {
            commands::InvokerEffectCommand::test_envelope(Effect {
                invocation_id,
                kind: InvokerEffectKind::Failed(failure.clone()),
            })
        };
        test_env
            .apply_multiple([
                commands::InvokeCommand::test_envelope(ServiceInvocation {
                    invocation_id,
                    invocation_target: InvocationTarget::mock_service(),
                    completion_retention_duration: Duration::from_secs(120),
                    journal_retention_duration: Duration::from_secs(120),
                    ..ServiceInvocation::mock()
                }),
                end,
            ])
            .await;

        let InvocationStatus::Completed(CompletedInvocation {
            response_result,
            journal_metadata,
            ..
        }) = test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await?
        else {
            panic!("expected a completed invocation");
        };
        assert_eq!(response_result, expected);
        // The journal is still only the Input, in the v1 table
        assert_eq!(journal_metadata.length, 1);
        std::assert!(
            ReadJournalTable::get_journal_entry(&mut test_env.storage, &invocation_id, 0)
                .await?
                .is_some()
        );
        for index in 0..2 {
            std::assert!(
                journal_table_v2::ReadJournalTable::get_journal_entry(
                    &mut test_env.storage,
                    invocation_id,
                    index,
                )
                .await?
                .is_none()
            );
        }
    }

    test_env.shutdown().await;
    Ok(())
}

/// A large output is stored in a blob even if the journal is dropped right away. The completed
/// status keeps the blob alive.
#[test(restate_core::test)]
async fn output_blob_outlives_dropped_journal() -> TestResult {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableInvocationStatusOutputBlob,
    ]))
    .await;

    let invocation_id = InvocationId::mock_generate(&InvocationTarget::mock_service());
    let result = Bytes::from(vec![7; 5000]);
    test_env
        .apply_multiple([
            commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                completion_retention_duration: Duration::from_secs(120),
                journal_retention_duration: Duration::ZERO,
                ..ServiceInvocation::mock()
            }),
            fixtures::pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
            fixtures::invoker_entry_effect(
                invocation_id,
                OutputCommand {
                    result: OutputResult::Success(result.clone()),
                    name: Default::default(),
                },
            ),
        ])
        .await;

    let InvocationStatus::Completed(CompletedInvocation {
        response_result, ..
    }) = test_env
        .storage
        .get_invocation_status(&invocation_id)
        .await?
    else {
        panic!("expected a completed invocation");
    };
    let ResponseResultRef::Completed(CompletedResult { blob_id, .. }) = response_result else {
        panic!("expected the output in a blob, got {response_result:?}");
    };
    // The journal is gone, and released its reference
    let input_entry = journal_table_v2::ReadJournalTable::get_journal_entry(
        &mut test_env.storage,
        invocation_id,
        0,
    )
    .await?;
    assert_eq!(input_entry, None);
    let (_, ref_count, _) = test_env
        .storage
        .get_blob::<RawParts>(blob_id)
        .await?
        .expect("output blob");
    assert_eq!(ref_count, 1);
    assert_eq!(
        response_result.resolve(&mut test_env.storage).await?,
        Some(ResponseResult::Success(result))
    );

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn clear_all_user_states() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;
    let service_id = ServiceId::new(None, "MySvc", "my-key");

    // Fill with some state the service K/V store
    let mut txn = test_env.storage.transaction();
    txn.put_user_state(&service_id, &Bytes::from_static(b"my-key-1"), b"my-val-1")?;
    txn.put_user_state(&service_id, &Bytes::from_static(b"my-key-2"), b"my-val-2")?;
    txn.commit().await.unwrap();
    drop(txn);

    let invocation_id =
        fixtures::mock_start_invocation_with_service_id(&mut test_env, service_id.clone()).await;

    test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::clear_all_state()),
            },
        }))
        .await;

    let states: Vec<restate_storage_api::Result<(Bytes, Bytes)>> = test_env
        .storage
        .get_all_user_states_for_service(&service_id)
        .unwrap()
        .collect()
        .await;
    assert_that!(states, empty());

    test_env.shutdown().await;
    Ok(())
}

#[restate_core::test]
async fn attach_invocation_entry() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

    let callee_invocation_id = InvocationId::mock_random();

    let actions = test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::AttachInvocation(
                    AttachInvocationEntry {
                        target: AttachInvocationTarget::InvocationId(
                            callee_invocation_id.to_string().into(),
                        ),
                        result: None,
                    },
                )),
            },
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(pat!(
                    restate_types::invocation::AttachInvocationRequest {
                        invocation_query: eq(InvocationQuery::Invocation(callee_invocation_id)),
                        block_on_inflight: eq(true),
                        response_sink: eq(ServiceInvocationResponseSink::partition_processor(
                            invocation_id,
                            1,
                        )),
                    }
                ))
            )
        }))
    );

    test_env.shutdown().await;
}

#[test(restate_core::test)]
async fn send_ingress_response_to_multiple_targets() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::mock_generate(&invocation_target);

    let request_id_1 = PartitionProcessorRpcRequestId::default();
    let request_id_2 = PartitionProcessorRpcRequestId::default();
    let request_id_3 = PartitionProcessorRpcRequestId::default();

    test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            ..ServiceInvocation::initialize(
                invocation_id,
                invocation_target.clone(),
                Source::Ingress(request_id_1),
            )
        }))
        .await;
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Invoked(_))
    );

    // Let's add another ingress
    let mut txn = test_env.storage.transaction();
    let mut invocation_status = txn.get_invocation_status(&invocation_id).await.unwrap();
    invocation_status.get_response_sinks_mut().unwrap().insert(
        ServiceInvocationResponseSink::Ingress {
            request_id: request_id_2,
        },
    );
    invocation_status.get_response_sinks_mut().unwrap().insert(
        ServiceInvocationResponseSink::Ingress {
            request_id: request_id_3,
        },
    );
    txn.put_invocation_status(&invocation_id, &invocation_status)?;
    txn.commit().await.unwrap();
    drop(txn);

    // Now let's send the output entry
    let response_bytes = Bytes::from_static(b"123");
    let actions = test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                    EntryResult::Success(response_bytes.clone()),
                )),
            },
        }))
        .await;
    // No ingress response is expected at this point because the invocation did not end yet
    assert_that!(
        actions,
        not(contains(pat!(Action::ReplyRpc {
            reply: pat!(RpcReply::Output(pat!(InvocationOutput { .. })))
        })))
    );

    // Send the End Effect
    let actions = test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id,
            kind: InvokerEffectKind::End,
        }))
        .await;
    // At this point we expect the completion to be forwarded to the invoker
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::ReplyRpc {
                reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                    request_id: eq(request_id_1),
                    response: eq(InvocationOutputResponse::Success(
                        invocation_target.clone(),
                        response_bytes.clone()
                    ))
                })))
            })),
            contains(pat!(Action::ReplyRpc {
                reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                    request_id: eq(request_id_2),
                    response: eq(InvocationOutputResponse::Success(
                        invocation_target.clone(),
                        response_bytes.clone()
                    ))
                })))
            })),
            contains(pat!(Action::ReplyRpc {
                reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                    request_id: eq(request_id_3),
                    response: eq(InvocationOutputResponse::Success(
                        invocation_target.clone(),
                        response_bytes.clone()
                    ))
                })))
            })),
        )
    );

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn consecutive_exclusive_handler_invocations_will_use_inbox() -> TestResult {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_virtual_object();
    let first_invocation_id = InvocationId::mock_generate(&invocation_target);
    let keyed_service_id = invocation_target.as_keyed_service_id().unwrap();
    let second_invocation_id = InvocationId::mock_generate(&invocation_target);

    // Let's start the first invocation
    test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            invocation_id: first_invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&first_invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Invoked(_))
    );
    assert_that!(
        test_env
            .storage
            .get_virtual_object_status(&keyed_service_id)
            .await,
        ok(eq(VirtualObjectStatus::Locked(first_invocation_id)))
    );

    // Let's start the second invocation
    test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            invocation_id: second_invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;

    // This should have not been invoked, but it should rather be in the inbox
    assert_that!(
        test_env
            .storage
            .inbox(&keyed_service_id)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await,
        ok(contains(matchers::storage::invocation_inbox_entry(
            second_invocation_id,
            &invocation_target
        )))
    );
    assert_that!(
        test_env
            .storage
            .get_virtual_object_status(&keyed_service_id)
            .await,
        ok(eq(VirtualObjectStatus::Locked(first_invocation_id)))
    );

    // Send the End Effect to terminate the first invocation
    test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id: first_invocation_id,
            kind: InvokerEffectKind::End,
        }))
        .await;
    // At this point we expect the second to be invoked, and also the lock updated
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&second_invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Invoked(_))
    );
    assert_that!(
        test_env
            .storage
            .get_virtual_object_status(&keyed_service_id)
            .await,
        ok(eq(VirtualObjectStatus::Locked(second_invocation_id)))
    );

    let _ = test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id: second_invocation_id,
            kind: InvokerEffectKind::End,
        }))
        .await;

    // After the second was completed too, the inbox is empty and the service is unlocked
    assert_that!(
        test_env
            .storage
            .inbox(&keyed_service_id)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await,
        ok(empty())
    );
    assert_that!(
        test_env
            .storage
            .get_virtual_object_status(&keyed_service_id)
            .await,
        ok(eq(VirtualObjectStatus::Unlocked))
    );

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn deduplicate_requests_with_same_pp_rpc_request_id() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let invocation_id = InvocationId::mock_random();

    let request_id = PartitionProcessorRpcRequestId::default();
    let service_invocation = {
        let mut si = ServiceInvocation::mock();
        si.invocation_id = invocation_id;
        si.response_sink = Some(ServiceInvocationResponseSink::Ingress { request_id });
        si.submit_notification_sink = Some(SubmitNotificationSink::Ingress { request_id });
        si.source = Source::Ingress(request_id);
        si
    };
    let actions = test_env
        .apply(commands::InvokeCommand::test_envelope(
            service_invocation.clone(),
        ))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::ReplyRpc {
            reply: pat!(RpcReply::Submitted(pat!(SubmittedInvocationNotification {
                request_id: eq(request_id),
                is_new_invocation: eq(true)
            })))
        }))
    );

    // Applying this again will return the same submit notification.
    let actions = test_env
        .apply(commands::InvokeCommand::test_envelope(service_invocation))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::ReplyRpc {
            reply: pat!(RpcReply::Submitted(pat!(SubmittedInvocationNotification {
                request_id: eq(request_id),
                is_new_invocation: eq(true)
            })))
        }))
    );

    test_env.shutdown().await;
    Ok(())
}

#[test]
fn quantized_actions_count_every_started_chunk() {
    let size_64k = NonZeroUsize::new(64 * 1024).unwrap();
    assert_eq!(quantized_actions(0, size_64k), 1);
    assert_eq!(quantized_actions(1, size_64k), 1);
    assert_eq!(quantized_actions(64 * 1024, size_64k), 1);
    assert_eq!(quantized_actions(64 * 1024 + 1, size_64k), 2);
    assert_eq!(quantized_actions(1024 * 1024, size_64k), 16);

    let size_32k = NonZeroUsize::new(32 * 1024).unwrap();
    assert_eq!(quantized_actions(64 * 1024 + 1, size_32k), 3);
}
