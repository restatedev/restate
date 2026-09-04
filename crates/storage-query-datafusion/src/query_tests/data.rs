// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use bytestring::ByteString;
use prost::Message;

use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InvocationStatus, JournalMetadata,
    JournalRetentionPolicy, StatusTimestamps,
};
use restate_storage_api::journal_events::EventView;
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_api::vqueue_table::metadata::{
    Action, MoveMetrics, Update, VQueueLink, VQueueMeta,
};
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryId, EntryKey, EntryMetadata, EntryValue, Stage, Status,
};
use restate_types::LimitKey;
use restate_types::LockName;
use restate_types::Scope;
use restate_types::ServiceName;
use restate_types::clock::UniqueTimestamp;
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::InvocationError;
use restate_types::identifiers::{DeploymentId, InvocationId, InvocationUuid, WithPartitionKey};
use restate_types::invocation::{
    InvocationTarget, ResponseResult, ServiceInvocationSpanContext, Source,
    VirtualObjectHandlerType,
};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use restate_types::journal::{Entry, InputEntry};
use restate_types::journal_events::{Event, PausedEvent, TransientErrorEvent};
use restate_types::service_protocol;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_util_string::{ReString, RestateString};
use restate_worker_api::invoker::status_handle::InvocationStatusReportInner;

const VQUEUE_BASE_TIMESTAMP_MILLIS: u64 = 1_744_000_000_000;
const TEXT_TABLE_INVOCATION_SEQUENCE_START: u128 = 1 << 64;

pub(super) struct FixtureFactory {
    next_invocation: u128,
    next_vqueue: u64,
}

impl Default for FixtureFactory {
    fn default() -> Self {
        Self {
            next_invocation: 1,
            next_vqueue: 1,
        }
    }
}

impl FixtureFactory {
    pub(super) fn for_text_tables() -> Self {
        Self {
            next_invocation: TEXT_TABLE_INVOCATION_SEQUENCE_START,
            next_vqueue: 1,
        }
    }

    pub(super) fn invocations<'factory, 'fixture, const N: usize>(
        &'factory mut self,
    ) -> InvocationFixturesBuilder<'factory, 'fixture, N> {
        InvocationFixturesBuilder {
            factory: self,
            options: InvocationOptions::default(),
        }
    }

    pub(super) fn create_vqueue(&mut self) -> VQueueFixture {
        self.create_vqueue_with(VQueueOptions::default())
    }

    pub(super) fn create_vqueue_with(&mut self, options: VQueueOptions) -> VQueueFixture {
        let scope = Scope::try_from_static(options.scope).unwrap();
        let id = VQueueId::custom(
            scope.partition_key(),
            format!("query-fixture-{}", self.next_vqueue),
        );
        let link = match options.service_key {
            Some(service_key) => VQueueLink::Lock(LockName::new(
                ServiceName::new(options.service_name),
                ReString::new(service_key),
            )),
            None => VQueueLink::Service(ServiceName::new(options.service_name)),
        };
        let created_at = unique_timestamp(VQUEUE_BASE_TIMESTAMP_MILLIS + 10_000 + self.next_vqueue);
        self.next_vqueue += 1;
        VQueueFixture {
            id,
            scope,
            limit_key: options.limit_key.parse().unwrap(),
            link,
            created_at,
            entry_stages: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(super) fn create_invocation(
        &mut self,
        options: InvocationOptions<'_>,
    ) -> InvocationFixture {
        let sequence = self.next_invocation;
        self.next_invocation += 1;

        let scope = options
            .vqueue
            .map(|vqueue| vqueue.scope.clone())
            .unwrap_or_else(|| Scope::try_from_static("scope-a").unwrap());
        let partition_key = scope.partition_key();
        let id = InvocationId::from_parts(partition_key, InvocationUuid::from_u128(sequence));
        let caller_id = InvocationId::from_parts(0, InvocationUuid::from_u128(10_000 + sequence));
        let deployment_id = DeploymentId::from_parts(1_000, 1);
        let vqueue_id = options.vqueue.map(|vqueue| vqueue.id.clone());

        let target = InvocationTarget::scoped_virtual_object(
            options.service_name,
            options.service_key,
            options.handler_name,
            VirtualObjectHandlerType::Exclusive,
            scope,
        );
        let source = Source::Service(
            caller_id,
            InvocationTarget::service("CallerService", "call"),
        );
        let timestamps = StatusTimestamps::new(
            MillisSinceEpoch::from(1_000),
            MillisSinceEpoch::from(6_000),
            Some(MillisSinceEpoch::from(2_000)),
            Some(MillisSinceEpoch::from(3_000)),
            Some(MillisSinceEpoch::from(5_000)),
            None,
        );
        let state = match options.status {
            InvocationFixtureStatus::Running => InvocationStatusReportInner {
                in_flight: true,
                start_count: 1,
                last_start_at: UNIX_EPOCH + Duration::from_secs(5),
                last_attempt_deployment_id: Some(deployment_id),
                last_attempt_protocol_version: Some(ServiceProtocolVersion::V5),
                last_attempt_server: Some("restate-sdk-rust/0.1.0".to_owned()),
                ..InvocationStatusReportInner::default()
            },
            InvocationFixtureStatus::CompletedSuccess
            | InvocationFixtureStatus::CompletedFailure => InvocationStatusReportInner::default(),
        };

        let vqueue_entry = options.vqueue.map(|vqueue| {
            vqueue.record_entry(options.entry_stage);
            let entry_id = EntryId::from(&id);
            let run_at =
                MillisSinceEpoch::from(VQUEUE_BASE_TIMESTAMP_MILLIS + 20_000 + sequence as u64);
            let created_at =
                unique_timestamp(VQUEUE_BASE_TIMESTAMP_MILLIS + 15_000 + sequence as u64);
            let mut stats = EntryStatistics::new(created_at, run_at.into());
            stats.transitioned_at =
                unique_timestamp(VQUEUE_BASE_TIMESTAMP_MILLIS + 16_000 + sequence as u64);
            if !matches!(options.entry_status, Status::New | Status::Scheduled) {
                stats.num_attempts = 1;
                stats.first_attempt_at = Some(unique_timestamp(
                    VQUEUE_BASE_TIMESTAMP_MILLIS + 15_500 + sequence as u64,
                ));
                stats.latest_attempt_at = stats.first_attempt_at;
            }
            if options.entry_status == Status::BackingOff {
                stats.num_errors = 1;
            }

            VQueueEntryFixture {
                key: EntryKey::new(options.has_lock, run_at, sequence as u64, entry_id),
                value: EntryValue {
                    status: options.entry_status,
                    metadata: EntryMetadata {
                        deployment: Some(deployment_id.to_string().into()),
                        ..EntryMetadata::default()
                    },
                    stats,
                },
                stage: options.entry_stage,
            }
        });

        let journal_entries = vec![
            JournalEntryFixture {
                index: 0,
                entry: JournalEntry::Entry(ProtobufRawEntryCodec::serialize_enriched(
                    Entry::Input(InputEntry {
                        headers: vec![],
                        value: Bytes::from_static(b"fixture-input"),
                    }),
                )),
            },
            JournalEntryFixture {
                index: 1,
                entry: JournalEntry::Entry(EnrichedRawEntry::new(
                    EnrichedEntryHeader::Run {},
                    service_protocol::RunEntryMessage {
                        name: format!("fixture-step-{sequence}"),
                        result: None,
                    }
                    .encode_to_vec()
                    .into(),
                )),
            },
        ];
        let journal_events = vec![
            EventView::new(
                MillisSinceEpoch::from(30_000 + sequence as u64),
                0,
                Event::TransientError(TransientErrorEvent {
                    error_code: 500u16.into(),
                    error_message: format!("fixture failure {sequence}"),
                    error_stacktrace: None,
                    restate_doc_error_code: None,
                    related_command_index: Some(1),
                    related_command_name: Some(format!("fixture-step-{sequence}")),
                    related_command_type: None,
                }),
            ),
            EventView::new(
                MillisSinceEpoch::from(31_000 + sequence as u64),
                1,
                Event::Paused(PausedEvent { last_failure: None }),
            ),
        ];

        InvocationFixture {
            id,
            status: options.status,
            target,
            vqueue_id,
            limit_key: "tenant/eu".parse::<LimitKey<ReString>>().unwrap(),
            source,
            execution_time: Some(MillisSinceEpoch::from(4_000)),
            idempotency_key: Some("request-1".into()),
            timestamps,
            completion_retention: Duration::from_secs(30),
            journal_retention: Duration::from_secs(10),
            journal: JournalMetadata::new(
                journal_entries.len() as u32,
                journal_entries.len() as u32,
                ServiceInvocationSpanContext::empty(),
            ),
            pinned_deployment: Some(PinnedDeployment::new(
                deployment_id,
                ServiceProtocolVersion::V5,
            )),
            state,
            vqueue_entry,
            journal_entries,
            journal_events,
        }
    }
}

fn unique_timestamp(millis: u64) -> UniqueTimestamp {
    UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::from(millis)).unwrap()
}

#[derive(Clone)]
pub(super) struct VQueueFixture {
    pub(super) id: VQueueId,
    scope: Scope,
    limit_key: LimitKey<ReString>,
    link: VQueueLink,
    created_at: UniqueTimestamp,
    entry_stages: Arc<Mutex<Vec<Stage>>>,
}

impl VQueueFixture {
    fn record_entry(&self, stage: Stage) {
        self.entry_stages.lock().unwrap().push(stage);
    }

    pub(super) fn metadata(&self) -> VQueueMeta {
        let mut metadata = VQueueMeta::new(
            self.created_at,
            Some(self.scope.clone()),
            self.limit_key.clone(),
            self.link.clone(),
        );

        for (index, stage) in self.entry_stages.lock().unwrap().iter().enumerate() {
            let timestamp =
                unique_timestamp(self.created_at.to_unix_millis().as_u64() + index as u64);
            metadata.apply_update(&Update::new(
                timestamp,
                Action::Move {
                    prev_stage: None,
                    next_stage: *stage,
                    metrics: MoveMetrics {
                        last_transition_at: timestamp,
                        has_started: false,
                        first_runnable_at: timestamp.to_unix_millis(),
                        scheduler_wait_stats: None,
                    },
                },
            ));
        }

        metadata
    }
}

pub(super) struct VQueueOptions {
    pub(super) scope: &'static str,
    pub(super) service_name: &'static str,
    pub(super) service_key: Option<&'static str>,
    pub(super) limit_key: &'static str,
}

impl Default for VQueueOptions {
    fn default() -> Self {
        Self {
            scope: "scope-a",
            service_name: "TestService",
            service_key: Some("key-1"),
            limit_key: "tenant/eu",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum InvocationFixtureStatus {
    Running,
    CompletedSuccess,
    CompletedFailure,
}

impl TryFrom<(&str, Option<&str>)> for InvocationFixtureStatus {
    type Error = anyhow::Error;

    fn try_from((status, completion_result): (&str, Option<&str>)) -> Result<Self, Self::Error> {
        match (status, completion_result) {
            ("invoked", None) => Ok(Self::Running),
            ("completed", Some("success")) => Ok(Self::CompletedSuccess),
            ("completed", Some("failure")) => Ok(Self::CompletedFailure),
            _ => anyhow::bail!(
                "unsupported invocation status values: status={status:?}, completion_result={completion_result:?}"
            ),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct InvocationOptions<'a> {
    pub(super) vqueue: Option<&'a VQueueFixture>,
    pub(super) service_name: &'a str,
    pub(super) service_key: &'a str,
    pub(super) handler_name: &'a str,
    pub(super) status: InvocationFixtureStatus,
    pub(super) entry_stage: Stage,
    pub(super) entry_status: Status,
    pub(super) has_lock: bool,
}

pub(super) struct InvocationFixturesBuilder<'factory, 'fixture, const N: usize> {
    factory: &'factory mut FixtureFactory,
    options: InvocationOptions<'fixture>,
}

impl<'fixture, const N: usize> InvocationFixturesBuilder<'_, 'fixture, N> {
    pub(super) fn with_vqueue(mut self, vqueue: &'fixture VQueueFixture) -> Self {
        self.options.vqueue = Some(vqueue);
        self
    }

    pub(super) fn with_status(mut self, status: InvocationFixtureStatus) -> Self {
        self.options.status = status;
        self
    }

    pub(super) fn create(self) -> [InvocationFixture; N] {
        let factory = self.factory;
        let options = self.options;
        std::array::from_fn(|_| factory.create_invocation(options))
    }
}

impl Default for InvocationOptions<'_> {
    fn default() -> Self {
        Self {
            vqueue: None,
            service_name: "TestService",
            service_key: "key-1",
            handler_name: "run",
            status: InvocationFixtureStatus::Running,
            entry_stage: Stage::Inbox,
            entry_status: Status::New,
            has_lock: true,
        }
    }
}

pub(super) struct VQueueEntryFixture {
    pub(super) key: EntryKey,
    pub(super) value: EntryValue,
    pub(super) stage: Stage,
}

pub(super) struct JournalEntryFixture {
    pub(super) index: u32,
    pub(super) entry: JournalEntry,
}

pub(super) struct InvocationFixture {
    pub(super) id: InvocationId,
    pub(super) status: InvocationFixtureStatus,
    pub(super) target: InvocationTarget,
    pub(super) vqueue_id: Option<VQueueId>,
    pub(super) limit_key: LimitKey<ReString>,
    pub(super) source: Source,
    pub(super) execution_time: Option<MillisSinceEpoch>,
    pub(super) idempotency_key: Option<ByteString>,
    pub(super) timestamps: StatusTimestamps,
    pub(super) completion_retention: Duration,
    pub(super) journal_retention: Duration,
    pub(super) journal: JournalMetadata,
    pub(super) pinned_deployment: Option<PinnedDeployment>,
    pub(super) state: InvocationStatusReportInner,
    pub(super) vqueue_entry: Option<VQueueEntryFixture>,
    pub(super) journal_entries: Vec<JournalEntryFixture>,
    pub(super) journal_events: Vec<EventView>,
}

impl InvocationFixture {
    pub(super) fn invocation_status(&self) -> InvocationStatus {
        let metadata = InFlightInvocationMetadata {
            invocation_target: self.target.clone(),
            vqueue_id: self.vqueue_id.clone(),
            limit_key: self.limit_key.clone(),
            source: self.source.clone(),
            execution_time: self.execution_time,
            idempotency_key: self.idempotency_key.clone(),
            timestamps: self.timestamps.clone(),
            completion_retention_duration: self.completion_retention,
            journal_retention_duration: self.journal_retention,
            journal_metadata: self.journal.clone(),
            pinned_deployment: self.pinned_deployment.clone(),
            ..InFlightInvocationMetadata::mock()
        };

        match self.status {
            InvocationFixtureStatus::Running => InvocationStatus::Invoked(metadata),
            InvocationFixtureStatus::CompletedSuccess => InvocationStatus::Completed(
                CompletedInvocation::from_in_flight_invocation_metadata(
                    metadata,
                    JournalRetentionPolicy::Retain,
                    ResponseResult::Success(Bytes::from_static(b"fixture result")),
                    MillisSinceEpoch::from(7_000),
                ),
            ),
            InvocationFixtureStatus::CompletedFailure => InvocationStatus::Completed(
                CompletedInvocation::from_in_flight_invocation_metadata(
                    metadata,
                    JournalRetentionPolicy::Retain,
                    ResponseResult::Failure(InvocationError::internal("fixture failure")),
                    MillisSinceEpoch::from(7_000),
                ),
            ),
        }
    }
}
