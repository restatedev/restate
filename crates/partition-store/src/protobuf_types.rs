// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BytesMut};
use restate_storage_api::StorageError;
use restate_types::errors::IdDecodeError;
use restate_types::storage::{StorageCodec, StorageDecode, StorageDecodeError, StorageEncode};

/// Marker trait to specify the Protobuf equivalent of a user facing type
pub(crate) trait PartitionStoreProtobufValue: Sized {
    type ProtobufType: From<Self> + TryInto<Self> + prost::Message + Default;

    /// Helper to use StorageCodec::decode
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, StorageError>
    where
        <<Self as PartitionStoreProtobufValue>::ProtobufType as TryInto<Self>>::Error:
            Into<anyhow::Error>,
    {
        StorageCodec::decode::<ProtobufStorageWrapper<Self::ProtobufType>, _>(buf)
            .map_err(|err| StorageError::Conversion(err.into()))
            .and_then(|v| {
                v.0.try_into()
                    .map_err(|e| StorageError::Conversion(e.into()))
            })
    }
}

pub(crate) struct ProtobufStorageWrapper<T>(pub(crate) T);

impl<T: prost::Message + 'static> StorageEncode for ProtobufStorageWrapper<T> {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), restate_types::storage::StorageEncodeError> {
        T::encode(&self.0, buf)
            .map_err(|err| restate_types::storage::StorageEncodeError::EncodeValue(err.into()))
    }

    fn default_codec(&self) -> restate_types::storage::StorageCodecKind {
        restate_types::storage::StorageCodecKind::Protobuf
    }
}

impl<T: prost::Message + Default> StorageDecode for ProtobufStorageWrapper<T> {
    fn decode<B: bytes::Buf>(
        buf: &mut B,
        kind: restate_types::storage::StorageCodecKind,
    ) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        match kind {
            restate_types::storage::StorageCodecKind::Protobuf => {
                Ok(ProtobufStorageWrapper(T::decode(buf).map_err(|err| {
                    StorageDecodeError::DecodeValue(err.into())
                })?))
            }
            codec => Err(StorageDecodeError::UnsupportedCodecKind(codec)),
        }
    }
}

/// Error type for conversion related problems (e.g. Rust <-> Protobuf)
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("missing field '{0}'")]
    MissingField(&'static str),
    #[error("unexpected enum variant {1} for field '{0}'")]
    UnexpectedEnumVariant(&'static str, i32),
    #[error("invalid data: {0}")]
    InvalidData(anyhow::Error),
}

impl ConversionError {
    pub fn invalid_data(source: impl Into<anyhow::Error>) -> Self {
        ConversionError::InvalidData(source.into())
    }

    pub fn missing_field(field: &'static str) -> Self {
        ConversionError::MissingField(field)
    }

    pub fn unexpected_enum_variant(field: &'static str, enum_variant: impl Into<i32>) -> Self {
        ConversionError::UnexpectedEnumVariant(field, enum_variant.into())
    }
}

impl From<IdDecodeError> for ConversionError {
    fn from(value: IdDecodeError) -> Self {
        ConversionError::invalid_data(value)
    }
}

impl From<ConversionError> for StorageError {
    fn from(value: ConversionError) -> Self {
        StorageError::Conversion(value.into())
    }
}

pub mod v1 {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]

    include!(concat!(
        env!("OUT_DIR"),
        "/dev.restate.storage.domain.v1.rs"
    ));

    pub mod pb_conversion {
        use crate::protobuf_types::v1::entry::EntryType;
        use crate::protobuf_types::v1::entry::EventEntry;
        use std::collections::{HashMap, HashSet};
        use std::str::FromStr;

        use anyhow::anyhow;
        use bytes::{Buf, Bytes};
        use bytestring::ByteString;
        use opentelemetry::trace::TraceState;
        use prost::Message;
        use restate_types::deployment::PinnedDeployment;

        use crate::protobuf_types::v1::dedup_sequence_number::Variant;
        use crate::protobuf_types::v1::enriched_entry_header::{
            AttachInvocation, Awakeable, BackgroundCall, CancelInvocation, ClearAllState,
            ClearState, CompleteAwakeable, CompletePromise, Custom, GetCallInvocationId,
            GetInvocationOutput, GetPromise, GetState, GetStateKeys, Input, Invoke, Output,
            PeekPromise, SetState, SideEffect, Sleep,
        };
        use crate::protobuf_types::v1::invocation_status::{
            Completed, Free, Inboxed, Invoked, Suspended,
        };
        use crate::protobuf_types::v1::journal_entry::completion_result::{
            Empty, Failure, Success,
        };
        use crate::protobuf_types::v1::journal_entry::{completion_result, CompletionResult, Kind};
        use crate::protobuf_types::v1::outbox_message::{
            NotifySignal, OutboxCancel, OutboxKill, OutboxServiceInvocation,
            OutboxServiceInvocationResponse,
        };
        use crate::protobuf_types::v1::service_invocation_response_sink::{
            Ingress, PartitionProcessor, ResponseSink,
        };
        use crate::protobuf_types::v1::{
            enriched_entry_header, entry, entry_result, inbox_entry, invocation_resolution_result,
            invocation_status, invocation_status_v2, invocation_target, journal_entry,
            outbox_message, promise, response_result, source, span_relation,
            submit_notification_sink, timer, virtual_object_status, BackgroundCallResolutionResult,
            DedupSequenceNumber, Duration, EnrichedEntryHeader, Entry, EntryResult,
            EpochSequenceNumber, Header, IdempotencyId, IdempotencyMetadata, InboxEntry,
            InvocationId, InvocationResolutionResult, InvocationStatus, InvocationStatusV2,
            InvocationTarget, InvocationV2Lite, JournalEntry, JournalEntryId, JournalEntryIndex,
            JournalMeta, KvPair, OutboxMessage, Promise, ResponseResult, SequenceNumber, ServiceId,
            ServiceInvocation, ServiceInvocationResponseSink, Source, SpanContext, SpanRelation,
            StateMutation, SubmitNotificationSink, Timer, VirtualObjectStatus,
        };
        use crate::protobuf_types::ConversionError;
        use restate_storage_api::StorageError;
        use restate_types::errors::{IdDecodeError, InvocationError};
        use restate_types::identifiers::{
            PartitionProcessorRpcRequestId, WithInvocationId, WithPartitionKey,
        };
        use restate_types::invocation::{InvocationTermination, TerminationFlavor};
        use restate_types::journal::enriched::AwakeableEnrichmentResult;
        use restate_types::journal::raw::RawEntry;
        use restate_types::journal_v2::{EntryMetadata, NotificationId, NotificationType};
        use restate_types::service_protocol::ServiceProtocolVersion;
        use restate_types::storage::{
            StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
        };
        use restate_types::time::MillisSinceEpoch;
        use restate_types::{journal_v2, GenerationalNodeId};

        impl TryFrom<VirtualObjectStatus>
            for restate_storage_api::service_status_table::VirtualObjectStatus
        {
            type Error = ConversionError;

            fn try_from(value: VirtualObjectStatus) -> Result<Self, ConversionError> {
                Ok(
                    match value
                        .status
                        .ok_or(ConversionError::missing_field("status"))?
                    {
                        virtual_object_status::Status::Locked(locked) => {
                            restate_storage_api::service_status_table::VirtualObjectStatus::Locked(
                                restate_types::identifiers::InvocationId::try_from(
                                    locked
                                        .invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                            )
                        }
                    },
                )
            }
        }

        impl From<restate_storage_api::service_status_table::VirtualObjectStatus> for VirtualObjectStatus {
            fn from(value: restate_storage_api::service_status_table::VirtualObjectStatus) -> Self {
                match value {
                    restate_storage_api::service_status_table::VirtualObjectStatus::Locked(
                        invocation_id,
                    ) => VirtualObjectStatus {
                        status: Some(virtual_object_status::Status::Locked(
                            virtual_object_status::Locked {
                                invocation_id: Some(invocation_id.into()),
                            },
                        )),
                    },
                    restate_storage_api::service_status_table::VirtualObjectStatus::Unlocked => {
                        unreachable!("Nothing should be stored for unlocked")
                    }
                }
            }
        }

        impl From<restate_types::identifiers::InvocationId> for InvocationId {
            fn from(value: restate_types::identifiers::InvocationId) -> Self {
                InvocationId {
                    partition_key: value.partition_key(),
                    invocation_uuid: value.invocation_uuid().to_bytes().to_vec().into(),
                }
            }
        }

        impl TryFrom<InvocationId> for restate_types::identifiers::InvocationId {
            type Error = ConversionError;

            fn try_from(value: InvocationId) -> Result<Self, ConversionError> {
                Ok(restate_types::identifiers::InvocationId::from_parts(
                    value.partition_key,
                    try_bytes_into_invocation_uuid(value.invocation_uuid)?,
                ))
            }
        }

        impl From<restate_types::identifiers::IdempotencyId> for IdempotencyId {
            fn from(value: restate_types::identifiers::IdempotencyId) -> Self {
                IdempotencyId {
                    service_name: value.service_name.into(),
                    service_key: value.service_key.map(Into::into),
                    handler_name: value.service_handler.into(),
                    idempotency_key: value.idempotency_key.into(),
                }
            }
        }

        impl TryFrom<IdempotencyId> for restate_types::identifiers::IdempotencyId {
            type Error = ConversionError;

            fn try_from(value: IdempotencyId) -> Result<Self, ConversionError> {
                Ok(restate_types::identifiers::IdempotencyId::new(
                    value.service_name.into(),
                    value.service_key.map(Into::into),
                    value.handler_name.into(),
                    value.idempotency_key.into(),
                ))
            }
        }

        impl From<restate_types::identifiers::JournalEntryId> for JournalEntryId {
            fn from(value: restate_types::identifiers::JournalEntryId) -> Self {
                JournalEntryId {
                    partition_key: value.partition_key(),
                    invocation_uuid: value
                        .invocation_id()
                        .invocation_uuid()
                        .to_bytes()
                        .to_vec()
                        .into(),
                    entry_index: value.journal_index(),
                }
            }
        }

        impl TryFrom<JournalEntryId> for restate_types::identifiers::JournalEntryId {
            type Error = ConversionError;

            fn try_from(value: JournalEntryId) -> Result<Self, ConversionError> {
                Ok(restate_types::identifiers::JournalEntryId::from_parts(
                    restate_types::identifiers::InvocationId::from_parts(
                        value.partition_key,
                        try_bytes_into_invocation_uuid(value.invocation_uuid)?,
                    ),
                    value.entry_index,
                ))
            }
        }

        impl From<restate_types::journal::EntryResult> for EntryResult {
            fn from(value: restate_types::journal::EntryResult) -> Self {
                match value {
                    restate_types::journal::EntryResult::Success(s) => EntryResult {
                        result: Some(entry_result::Result::Value(s)),
                    },
                    restate_types::journal::EntryResult::Failure(code, message) => EntryResult {
                        result: Some(entry_result::Result::Failure(entry_result::Failure {
                            error_code: code.into(),
                            message: message.into_bytes(),
                        })),
                    },
                }
            }
        }

        impl TryFrom<EntryResult> for restate_types::journal::EntryResult {
            type Error = ConversionError;

            fn try_from(value: EntryResult) -> Result<Self, ConversionError> {
                Ok(
                    match value
                        .result
                        .ok_or(ConversionError::missing_field("result"))?
                    {
                        entry_result::Result::Value(s) => {
                            restate_types::journal::EntryResult::Success(s)
                        }
                        entry_result::Result::Failure(entry_result::Failure {
                            error_code,
                            message,
                        }) => restate_types::journal::EntryResult::Failure(
                            error_code.into(),
                            ByteString::try_from(message).map_err(ConversionError::invalid_data)?,
                        ),
                    },
                )
            }
        }

        impl From<restate_storage_api::promise_table::PromiseResult> for EntryResult {
            fn from(value: restate_storage_api::promise_table::PromiseResult) -> Self {
                match value {
                    restate_storage_api::promise_table::PromiseResult::Success(s) => EntryResult {
                        result: Some(entry_result::Result::Value(s)),
                    },
                    restate_storage_api::promise_table::PromiseResult::Failure(code, message) => {
                        EntryResult {
                            result: Some(entry_result::Result::Failure(entry_result::Failure {
                                error_code: code.into(),
                                message: message.into_bytes(),
                            })),
                        }
                    }
                }
            }
        }

        impl TryFrom<EntryResult> for restate_storage_api::promise_table::PromiseResult {
            type Error = ConversionError;

            fn try_from(value: EntryResult) -> Result<Self, ConversionError> {
                Ok(
                    match value
                        .result
                        .ok_or(ConversionError::missing_field("result"))?
                    {
                        entry_result::Result::Value(s) => {
                            restate_storage_api::promise_table::PromiseResult::Success(s)
                        }
                        entry_result::Result::Failure(entry_result::Failure {
                            error_code,
                            message,
                        }) => restate_storage_api::promise_table::PromiseResult::Failure(
                            error_code.into(),
                            ByteString::try_from(message).map_err(ConversionError::invalid_data)?,
                        ),
                    },
                )
            }
        }

        // Little macro to try conversion or fail
        macro_rules! expect_or_fail {
            ($field:ident) => {
                $field.ok_or(ConversionError::missing_field(stringify!($field)))
            };
        }

        impl TryFrom<InvocationStatusV2>
            for restate_storage_api::invocation_status_table::InvocationStatus
        {
            type Error = ConversionError;

            fn try_from(value: InvocationStatusV2) -> Result<Self, ConversionError> {
                let InvocationStatusV2 {
                    status,
                    invocation_target,
                    source,
                    span_context,
                    creation_time,
                    modification_time,
                    response_sinks,
                    inboxed_transition_time,
                    scheduled_transition_time,
                    running_transition_time,
                    completed_transition_time,
                    argument,
                    headers,
                    execution_time,
                    completion_retention_duration,
                    idempotency_key,
                    inbox_sequence_number,
                    journal_length,
                    deployment_id,
                    service_protocol_version,
                    waiting_for_completions,
                    waiting_for_signal_indexes,
                    waiting_for_signal_names,
                    result,
                } = value;

                let invocation_target = expect_or_fail!(invocation_target)?.try_into()?;
                let timestamps =
                    restate_storage_api::invocation_status_table::StatusTimestamps::new(
                        MillisSinceEpoch::new(creation_time),
                        MillisSinceEpoch::new(modification_time),
                        inboxed_transition_time.map(MillisSinceEpoch::new),
                        scheduled_transition_time.map(MillisSinceEpoch::new),
                        running_transition_time.map(MillisSinceEpoch::new),
                        completed_transition_time.map(MillisSinceEpoch::new),
                    );
                let source = expect_or_fail!(source)?.try_into()?;
                let response_sinks = response_sinks
                    .into_iter()
                    .map(|s| {
                        Ok::<_, ConversionError>(Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(s)
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))??)
                    })
                    .collect::<Result<HashSet<_>, _>>()?;
                let headers = headers
                    .into_iter()
                    .map(|h| restate_types::invocation::Header::try_from(h))
                    .collect::<Result<Vec<_>, ConversionError>>()?;

                match status.try_into().unwrap_or_default() {
                    invocation_status_v2::Status::Scheduled => {
                        Ok(restate_storage_api::invocation_status_table::InvocationStatus::Scheduled(
                            restate_storage_api::invocation_status_table::ScheduledInvocation {
                                metadata:
                                    restate_storage_api::invocation_status_table::PreFlightInvocationMetadata {
                                        response_sinks,
                                        timestamps,
                                        invocation_target,
                                        argument: expect_or_fail!(argument)?,
                                        source,
                                        span_context: expect_or_fail!(span_context)?.try_into()?,
                                        headers,
                                        execution_time: execution_time.map(MillisSinceEpoch::new),
                                        completion_retention_duration:
                                            completion_retention_duration
                                                .unwrap_or_default()
                                                .try_into()?,
                                        idempotency_key: idempotency_key.map(ByteString::from),
                                    },
                            },
                        ))
                    }
                    invocation_status_v2::Status::Inboxed => {
                        Ok(restate_storage_api::invocation_status_table::InvocationStatus::Inboxed(
                            restate_storage_api::invocation_status_table::InboxedInvocation {
                                inbox_sequence_number: expect_or_fail!(inbox_sequence_number)?,
                                metadata:
                                    restate_storage_api::invocation_status_table::PreFlightInvocationMetadata {
                                        response_sinks,
                                        timestamps,
                                        invocation_target,
                                        argument: expect_or_fail!(argument)?,
                                        source,
                                        span_context: expect_or_fail!(span_context)?.try_into()?,
                                        headers,
                                        execution_time: execution_time.map(MillisSinceEpoch::new),
                                        completion_retention_duration:
                                            completion_retention_duration
                                                .unwrap_or_default()
                                                .try_into()?,
                                        idempotency_key: idempotency_key.map(ByteString::from),
                                    },
                            },
                        ))
                    }
                    invocation_status_v2::Status::Invoked => {
                        Ok(restate_storage_api::invocation_status_table::InvocationStatus::Invoked(
                            restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                                response_sinks,
                                timestamps,
                                invocation_target,
                                journal_metadata: restate_storage_api::invocation_status_table::JournalMetadata {
                                    length: journal_length,
                                    span_context: expect_or_fail!(span_context)?.try_into()?,
                                },
                                pinned_deployment: derive_pinned_deployment(
                                    deployment_id,
                                    service_protocol_version,
                                )?,
                                source,
                                completion_retention_duration: completion_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                idempotency_key: idempotency_key.map(ByteString::from),
                            },
                        ))
                    }
                    invocation_status_v2::Status::Suspended => Ok(
                        restate_storage_api::invocation_status_table::InvocationStatus::Suspended {
                            metadata: restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                                response_sinks,
                                timestamps,
                                invocation_target,
                                journal_metadata: restate_storage_api::invocation_status_table::JournalMetadata {
                                    length: journal_length,
                                    span_context: expect_or_fail!(span_context)?.try_into()?,
                                },
                                pinned_deployment: derive_pinned_deployment(
                                    deployment_id,
                                    service_protocol_version,
                                )?,
                                source,
                                completion_retention_duration: completion_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                idempotency_key: idempotency_key.map(ByteString::from),
                            },
                            waiting_for_notifications: waiting_for_completions
                                .into_iter()
                                .map(NotificationId::for_completion)
                                .chain(
                                    waiting_for_signal_indexes
                                        .into_iter()
                                        .map(|s| journal_v2::SignalId::for_index(s.into()))
                                        .map(NotificationId::for_signal),
                                )
                                .chain(
                                    waiting_for_signal_names
                                        .into_iter()
                                        .map(|s| journal_v2::SignalId::for_name(s.into()))
                                        .map(NotificationId::for_signal),
                                )
                                .collect(),
                        },
                    ),
                    invocation_status_v2::Status::Killed => {
                        Ok(restate_storage_api::invocation_status_table::InvocationStatus::Killed(
                            restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                                response_sinks,
                                timestamps,
                                invocation_target,
                                journal_metadata: restate_storage_api::invocation_status_table::JournalMetadata {
                                    length: journal_length,
                                    span_context: expect_or_fail!(span_context)?.try_into()?,
                                },
                                pinned_deployment: derive_pinned_deployment(
                                    deployment_id,
                                    service_protocol_version,
                                )?,
                                source,
                                completion_retention_duration: completion_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                idempotency_key: idempotency_key.map(ByteString::from),
                            },
                        ))
                    }
                    invocation_status_v2::Status::Completed => {
                        Ok(restate_storage_api::invocation_status_table::InvocationStatus::Completed(
                            restate_storage_api::invocation_status_table::CompletedInvocation {
                                timestamps,
                                invocation_target,
                                span_context: expect_or_fail!(span_context)?.try_into()?,
                                source,
                                idempotency_key: idempotency_key.map(ByteString::from),
                                response_result: expect_or_fail!(result)?.try_into()?,
                                completion_retention_duration: completion_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                            },
                        ))
                    }
                    _ => Err(ConversionError::unexpected_enum_variant(
                        "status",
                        value.status,
                    )),
                }
            }
        }

        impl From<restate_storage_api::invocation_status_table::InvocationStatus> for InvocationStatusV2 {
            fn from(value: restate_storage_api::invocation_status_table::InvocationStatus) -> Self {
                match value {
                    restate_storage_api::invocation_status_table::InvocationStatus::Scheduled(
                        restate_storage_api::invocation_status_table::ScheduledInvocation {
                            metadata:
                                restate_storage_api::invocation_status_table::PreFlightInvocationMetadata {
                                    response_sinks,
                                    timestamps,
                                    invocation_target,
                                    argument,
                                    source,
                                    span_context,
                                    headers,
                                    execution_time,
                                    completion_retention_duration,
                                    idempotency_key,
                                },
                        },
                    ) => InvocationStatusV2 {
                        status: invocation_status_v2::Status::Scheduled.into(),
                        invocation_target: Some(invocation_target.into()),
                        source: Some(source.into()),
                        span_context: Some(span_context.into()),
                        // SAFETY: We're only mapping data types here
                        creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                        modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                        inboxed_transition_time: unsafe { timestamps.inboxed_transition_time() }
                            .map(|t| t.as_u64()),
                        scheduled_transition_time: unsafe {
                            timestamps.scheduled_transition_time()
                        }
                        .map(|t| t.as_u64()),
                        running_transition_time: unsafe { timestamps.running_transition_time() }
                            .map(|t| t.as_u64()),
                        completed_transition_time: unsafe {
                            timestamps.completed_transition_time()
                        }
                        .map(|t| t.as_u64()),
                        response_sinks: response_sinks
                            .into_iter()
                            .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                            .collect(),
                        argument: Some(argument),
                        headers: headers.into_iter().map(Into::into).collect(),
                        execution_time: execution_time.map(|t| t.as_u64()),
                        completion_retention_duration: Some(completion_retention_duration.into()),
                        idempotency_key: idempotency_key.map(|key| key.to_string()),
                        inbox_sequence_number: None,
                        journal_length: 0,
                        deployment_id: None,
                        service_protocol_version: None,
                        waiting_for_completions: vec![],
                        waiting_for_signal_indexes: vec![],
                        waiting_for_signal_names: vec![],
                        result: None,
                    },
                    restate_storage_api::invocation_status_table::InvocationStatus::Inboxed(
                        restate_storage_api::invocation_status_table::InboxedInvocation {
                            metadata:
                                restate_storage_api::invocation_status_table::PreFlightInvocationMetadata {
                                    response_sinks,
                                    timestamps,
                                    invocation_target,
                                    argument,
                                    source,
                                    span_context,
                                    headers,
                                    execution_time,
                                    completion_retention_duration,
                                    idempotency_key,
                                },
                            inbox_sequence_number,
                        },
                    ) => InvocationStatusV2 {
                        status: invocation_status_v2::Status::Inboxed.into(),
                        invocation_target: Some(invocation_target.into()),
                        source: Some(source.into()),
                        span_context: Some(span_context.into()),
                        // SAFETY: We're only mapping data types here
                        creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                        modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                        inboxed_transition_time: unsafe { timestamps.inboxed_transition_time() }
                            .map(|t| t.as_u64()),
                        scheduled_transition_time: unsafe {
                            timestamps.scheduled_transition_time()
                        }
                        .map(|t| t.as_u64()),
                        running_transition_time: unsafe { timestamps.running_transition_time() }
                            .map(|t| t.as_u64()),
                        completed_transition_time: unsafe {
                            timestamps.completed_transition_time()
                        }
                        .map(|t| t.as_u64()),
                        response_sinks: response_sinks
                            .into_iter()
                            .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                            .collect(),
                        argument: Some(argument),
                        headers: headers.into_iter().map(Into::into).collect(),
                        execution_time: execution_time.map(|t| t.as_u64()),
                        completion_retention_duration: Some(completion_retention_duration.into()),
                        idempotency_key: idempotency_key.map(|key| key.to_string()),
                        inbox_sequence_number: Some(inbox_sequence_number),
                        journal_length: 0,
                        deployment_id: None,
                        service_protocol_version: None,
                        waiting_for_completions: vec![],
                        waiting_for_signal_indexes: vec![],
                        waiting_for_signal_names: vec![],
                        result: None,
                    },
                    restate_storage_api::invocation_status_table::InvocationStatus::Invoked(
                        restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                            invocation_target,
                            journal_metadata,
                            pinned_deployment,
                            response_sinks,
                            timestamps,
                            source,
                            completion_retention_duration,
                            idempotency_key,
                        },
                    ) => {
                        let (deployment_id, service_protocol_version) = match pinned_deployment {
                            None => (None, None),
                            Some(pinned_deployment) => (
                                Some(pinned_deployment.deployment_id.to_string()),
                                Some(pinned_deployment.service_protocol_version.as_repr()),
                            ),
                        };

                        InvocationStatusV2 {
                            status: invocation_status_v2::Status::Invoked.into(),
                            invocation_target: Some(invocation_target.into()),
                            source: Some(source.into()),
                            span_context: Some(journal_metadata.span_context.into()),
                            // SAFETY: We're only mapping data types here
                            creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                            modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                            inboxed_transition_time: unsafe {
                                timestamps.inboxed_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            scheduled_transition_time: unsafe {
                                timestamps.scheduled_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            running_transition_time: unsafe {
                                timestamps.running_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            completed_transition_time: unsafe {
                                timestamps.completed_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            response_sinks: response_sinks
                                .into_iter()
                                .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                                .collect(),
                            argument: None,
                            headers: vec![],
                            execution_time: None,
                            completion_retention_duration: Some(
                                completion_retention_duration.into(),
                            ),
                            idempotency_key: idempotency_key.map(|key| key.to_string()),
                            inbox_sequence_number: None,
                            journal_length: journal_metadata.length,
                            deployment_id,
                            service_protocol_version,
                            waiting_for_completions: vec![],
                            waiting_for_signal_indexes: vec![],
                            waiting_for_signal_names: vec![],
                            result: None,
                        }
                    }
                    restate_storage_api::invocation_status_table::InvocationStatus::Suspended {
                        metadata:
                            restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                                invocation_target,
                                journal_metadata,
                                pinned_deployment,
                                response_sinks,
                                timestamps,
                                source,
                                completion_retention_duration,
                                idempotency_key,
                            },
                        waiting_for_notifications,
                    } => {
                        let (deployment_id, service_protocol_version) = match pinned_deployment {
                            None => (None, None),
                            Some(pinned_deployment) => (
                                Some(pinned_deployment.deployment_id.to_string()),
                                Some(pinned_deployment.service_protocol_version.as_repr()),
                            ),
                        };

                        let mut waiting_for_completions: Vec<u32> = Default::default();
                        let mut waiting_for_signal_indexes: Vec<u32> = Default::default();
                        let mut waiting_for_signal_names: Vec<String> = Default::default();
                        for id in waiting_for_notifications {
                            match id {
                                journal_v2::NotificationId::CompletionId(c) => {
                                    waiting_for_completions.push(c);
                                }
                                journal_v2::NotificationId::SignalIndex(c) => {
                                    waiting_for_signal_indexes.push(c);
                                }
                                journal_v2::NotificationId::SignalName(s) => {
                                    waiting_for_signal_names.push(s.to_string());
                                }
                            };
                        }

                        InvocationStatusV2 {
                            status: invocation_status_v2::Status::Suspended.into(),
                            invocation_target: Some(invocation_target.into()),
                            source: Some(source.into()),
                            span_context: Some(journal_metadata.span_context.into()),
                            // SAFETY: We're only mapping data types here
                            creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                            modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                            inboxed_transition_time: unsafe {
                                timestamps.inboxed_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            scheduled_transition_time: unsafe {
                                timestamps.scheduled_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            running_transition_time: unsafe {
                                timestamps.running_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            completed_transition_time: unsafe {
                                timestamps.completed_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            response_sinks: response_sinks
                                .into_iter()
                                .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                                .collect(),
                            argument: None,
                            headers: vec![],
                            execution_time: None,
                            completion_retention_duration: Some(
                                completion_retention_duration.into(),
                            ),
                            idempotency_key: idempotency_key.map(|key| key.to_string()),
                            inbox_sequence_number: None,
                            journal_length: journal_metadata.length,
                            deployment_id,
                            service_protocol_version,
                            waiting_for_completions,
                            waiting_for_signal_indexes,
                            waiting_for_signal_names,
                            result: None,
                        }
                    }
                    restate_storage_api::invocation_status_table::InvocationStatus::Killed(
                        restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                            invocation_target,
                            journal_metadata,
                            pinned_deployment,
                            response_sinks,
                            timestamps,
                            source,
                            completion_retention_duration,
                            idempotency_key,
                        },
                    ) => {
                        let (deployment_id, service_protocol_version) = match pinned_deployment {
                            None => (None, None),
                            Some(pinned_deployment) => (
                                Some(pinned_deployment.deployment_id.to_string()),
                                Some(pinned_deployment.service_protocol_version.as_repr()),
                            ),
                        };

                        InvocationStatusV2 {
                            status: invocation_status_v2::Status::Killed.into(),
                            invocation_target: Some(invocation_target.into()),
                            source: Some(source.into()),
                            span_context: Some(journal_metadata.span_context.into()),
                            // SAFETY: We're only mapping data types here
                            creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                            modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                            inboxed_transition_time: unsafe {
                                timestamps.inboxed_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            scheduled_transition_time: unsafe {
                                timestamps.scheduled_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            running_transition_time: unsafe {
                                timestamps.running_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            completed_transition_time: unsafe {
                                timestamps.completed_transition_time()
                            }
                            .map(|t| t.as_u64()),
                            response_sinks: response_sinks
                                .into_iter()
                                .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                                .collect(),
                            argument: None,
                            headers: vec![],
                            execution_time: None,
                            completion_retention_duration: Some(
                                completion_retention_duration.into(),
                            ),
                            idempotency_key: idempotency_key.map(|key| key.to_string()),
                            inbox_sequence_number: None,
                            journal_length: journal_metadata.length,
                            deployment_id,
                            service_protocol_version,
                            waiting_for_completions: vec![],
                            waiting_for_signal_indexes: vec![],
                            waiting_for_signal_names: vec![],
                            result: None,
                        }
                    }
                    restate_storage_api::invocation_status_table::InvocationStatus::Completed(
                        restate_storage_api::invocation_status_table::CompletedInvocation {
                            invocation_target,
                            span_context,
                            source,
                            idempotency_key,
                            timestamps,
                            response_result,
                            completion_retention_duration,
                        },
                    ) => InvocationStatusV2 {
                        status: invocation_status_v2::Status::Completed.into(),
                        invocation_target: Some(invocation_target.into()),
                        source: Some(source.into()),
                        span_context: Some(span_context.into()),
                        // SAFETY: We're only mapping data types here
                        creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                        modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                        inboxed_transition_time: unsafe { timestamps.inboxed_transition_time() }
                            .map(|t| t.as_u64()),
                        scheduled_transition_time: unsafe {
                            timestamps.scheduled_transition_time()
                        }
                        .map(|t| t.as_u64()),
                        running_transition_time: unsafe { timestamps.running_transition_time() }
                            .map(|t| t.as_u64()),
                        completed_transition_time: unsafe {
                            timestamps.completed_transition_time()
                        }
                        .map(|t| t.as_u64()),
                        response_sinks: vec![],
                        argument: None,
                        headers: vec![],
                        execution_time: None,
                        completion_retention_duration: Some(completion_retention_duration.into()),
                        idempotency_key: idempotency_key.map(|key| key.to_string()),
                        inbox_sequence_number: None,
                        journal_length: 0,
                        deployment_id: None,
                        service_protocol_version: None,
                        waiting_for_completions: vec![],
                        waiting_for_signal_indexes: vec![],
                        waiting_for_signal_names: vec![],
                        result: Some(response_result.into()),
                    },
                    restate_storage_api::invocation_status_table::InvocationStatus::Free => {
                        panic!("Unexpected serialization of Free status. This is a bug of the invocation status table")
                    }
                }
            }
        }

        impl TryFrom<InvocationV2Lite> for crate::invocation_status_table::InvocationLite {
            type Error = ConversionError;

            fn try_from(value: InvocationV2Lite) -> Result<Self, ConversionError> {
                let InvocationV2Lite {
                    status,
                    invocation_target,
                } = value;

                let invocation_target = expect_or_fail!(invocation_target)?.try_into()?;
                let status = match status.try_into().unwrap_or_default() {
                    invocation_status_v2::Status::Scheduled => {
                        restate_storage_api::invocation_status_table::InvocationStatusDiscriminants::Scheduled
                    }
                    invocation_status_v2::Status::Inboxed => {
                        restate_storage_api::invocation_status_table::InvocationStatusDiscriminants::Inboxed
                    }
                    invocation_status_v2::Status::Invoked => {
                        restate_storage_api::invocation_status_table::InvocationStatusDiscriminants::Invoked
                    }
                    invocation_status_v2::Status::Suspended => {
                        restate_storage_api::invocation_status_table::InvocationStatusDiscriminants::Suspended
                    }
                    invocation_status_v2::Status::Killed => {
                        restate_storage_api::invocation_status_table::InvocationStatusDiscriminants::Killed
                    }
                    invocation_status_v2::Status::Completed => {
                        restate_storage_api::invocation_status_table::InvocationStatusDiscriminants::Completed
                    }
                    _ => {
                        return Err(ConversionError::unexpected_enum_variant(
                            "status",
                            value.status,
                        ))
                    }
                };

                Ok((crate::invocation_status_table::InvocationLite {
                    status,
                    invocation_target,
                }))
            }
        }

        impl From<crate::invocation_status_table::InvocationLite> for InvocationV2Lite {
            fn from(_: crate::invocation_status_table::InvocationLite) -> Self {
                panic!("Unexpected usage of InvocationLite, this data structure can be used only for reading, and never for writing")
            }
        }

        impl TryFrom<InvocationStatus> for crate::invocation_status_table::InvocationStatusV1 {
            type Error = ConversionError;

            fn try_from(value: InvocationStatus) -> Result<Self, ConversionError> {
                let result = match value
                    .status
                    .ok_or(ConversionError::missing_field("status"))?
                {
                    invocation_status::Status::Inboxed(inboxed) => {
                        let invocation_metadata =
                            restate_storage_api::invocation_status_table::InboxedInvocation::try_from(inboxed)?;
                        restate_storage_api::invocation_status_table::InvocationStatus::Inboxed(
                            invocation_metadata,
                        )
                    }
                    invocation_status::Status::Invoked(invoked) => {
                        let invocation_metadata =
                            restate_storage_api::invocation_status_table::InFlightInvocationMetadata::try_from(
                                invoked,
                            )?;
                        restate_storage_api::invocation_status_table::InvocationStatus::Invoked(
                            invocation_metadata,
                        )
                    }
                    invocation_status::Status::Suspended(suspended) => {
                        let (metadata, waiting_for_completed_entries) = suspended.try_into()?;
                        restate_storage_api::invocation_status_table::InvocationStatus::Suspended {
                            metadata,
                            waiting_for_notifications: waiting_for_completed_entries
                                .into_iter()
                                .map(|i| NotificationId::for_completion(i.into()))
                                .collect(),
                        }
                    }
                    invocation_status::Status::Completed(completed) => {
                        restate_storage_api::invocation_status_table::InvocationStatus::Completed(
                            completed.try_into()?,
                        )
                    }
                    invocation_status::Status::Free(_) => {
                        restate_storage_api::invocation_status_table::InvocationStatus::Free
                    }
                };

                Ok(crate::invocation_status_table::InvocationStatusV1(result))
            }
        }

        #[cfg(not(test))]
        impl From<crate::invocation_status_table::InvocationStatusV1> for InvocationStatus {
            fn from(_: crate::invocation_status_table::InvocationStatusV1) -> Self {
                panic!("Unexpected conversion to old InvocationStatus, this is not expected to happen.")
            }
        }

        // We need this for the test_migration in invocation_status_table_test
        #[cfg(test)]
        impl From<crate::invocation_status_table::InvocationStatusV1> for InvocationStatus {
            fn from(value: crate::invocation_status_table::InvocationStatusV1) -> Self {
                let status = match value.0 {
                    restate_storage_api::invocation_status_table::InvocationStatus::Inboxed(
                        inboxed_status,
                    ) => invocation_status::Status::Inboxed(Inboxed::from(inboxed_status)),
                    restate_storage_api::invocation_status_table::InvocationStatus::Invoked(
                        invoked_status,
                    ) => invocation_status::Status::Invoked(Invoked::from(invoked_status)),
                    restate_storage_api::invocation_status_table::InvocationStatus::Suspended {
                        metadata,
                        waiting_for_notifications,
                    } => invocation_status::Status::Suspended(Suspended::from((
                        metadata,
                        waiting_for_notifications
                            .into_iter()
                            .map(|notification_id| match notification_id {
                                NotificationId::CompletionId(c) => c,
                                _ => {
                                    panic!("Unsupported waiting signals with old invocation status")
                                }
                            })
                            .collect(),
                    ))),
                    restate_storage_api::invocation_status_table::InvocationStatus::Completed(
                        completed,
                    ) => invocation_status::Status::Completed(Completed::from(completed)),
                    restate_storage_api::invocation_status_table::InvocationStatus::Free => {
                        invocation_status::Status::Free(Free {})
                    }
                    restate_storage_api::invocation_status_table::InvocationStatus::Scheduled(
                        _,
                    ) => {
                        panic!("Unexpected conversion to old InvocationStatus when using Scheduled variant. This is a bug in the table implementation.")
                    }
                    restate_storage_api::invocation_status_table::InvocationStatus::Killed(_) => {
                        panic!("Unexpected conversion to old InvocationStatus when using Killed variant. This is a bug in the table implementation.")
                    }
                };

                InvocationStatus {
                    status: Some(status),
                }
            }
        }

        fn derive_pinned_deployment(
            deployment_id: Option<String>,
            service_protocol_version: Option<i32>,
        ) -> Result<Option<PinnedDeployment>, ConversionError> {
            let deployment_id = deployment_id
                .map(|deployment_id| deployment_id.parse().expect("valid deployment id"));

            if let Some(deployment_id) = deployment_id {
                let service_protocol_version = service_protocol_version.ok_or_else(|| {
                    ConversionError::invalid_data(anyhow!(
                        "service_protocol_version has not been set"
                    ))
                })?;
                let service_protocol_version =
                    ServiceProtocolVersion::try_from(service_protocol_version).map_err(|_| {
                        ConversionError::unexpected_enum_variant(
                            "service_protocol_version",
                            service_protocol_version,
                        )
                    })?;
                Ok(Some(PinnedDeployment::new(
                    deployment_id,
                    service_protocol_version,
                )))
            } else {
                Ok(None)
            }
        }

        impl TryFrom<Invoked> for restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
            type Error = ConversionError;

            fn try_from(value: Invoked) -> Result<Self, ConversionError> {
                let invocation_target = restate_types::invocation::InvocationTarget::try_from(
                    value
                        .invocation_target
                        .ok_or(ConversionError::missing_field("invocation_target"))?,
                )?;

                let pinned_deployment =
                    derive_pinned_deployment(value.deployment_id, value.service_protocol_version)?;

                let journal_metadata =
                    restate_storage_api::invocation_status_table::JournalMetadata::try_from(
                        value
                            .journal_meta
                            .ok_or(ConversionError::missing_field("journal_meta"))?,
                    )?;
                let response_sinks = value
                    .response_sinks
                    .into_iter()
                    .map(|s| {
                        Ok::<_, ConversionError>(Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(s)
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))??)
                    })
                    .collect::<Result<HashSet<_>, _>>()?;

                let source = restate_types::invocation::Source::try_from(
                    value
                        .source
                        .ok_or(ConversionError::missing_field("source"))?,
                )?;

                let completion_retention_time = std::time::Duration::try_from(
                    value.completion_retention_time.unwrap_or_default(),
                )?;

                let idempotency_key = value.idempotency_key.map(ByteString::from);

                Ok(
                    restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                        invocation_target,
                        journal_metadata,
                        pinned_deployment,
                        response_sinks,
                        timestamps:
                            restate_storage_api::invocation_status_table::StatusTimestamps::new(
                                MillisSinceEpoch::new(value.creation_time),
                                MillisSinceEpoch::new(value.modification_time),
                                None,
                                None,
                                None,
                                None,
                            ),
                        source,
                        completion_retention_duration: completion_retention_time,
                        idempotency_key,
                    },
                )
            }
        }

        impl From<restate_storage_api::invocation_status_table::InFlightInvocationMetadata> for Invoked {
            fn from(
                value: restate_storage_api::invocation_status_table::InFlightInvocationMetadata,
            ) -> Self {
                let restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                    invocation_target,
                    pinned_deployment,
                    response_sinks,
                    journal_metadata,
                    timestamps,
                    source,
                    completion_retention_duration: completion_retention_time,
                    idempotency_key,
                    ..
                } = value;

                let (deployment_id, service_protocol_version) = match pinned_deployment {
                    None => (None, None),
                    Some(pinned_deployment) => (
                        Some(pinned_deployment.deployment_id.to_string()),
                        Some(pinned_deployment.service_protocol_version.as_repr()),
                    ),
                };

                Invoked {
                    invocation_target: Some(invocation_target.into()),
                    response_sinks: response_sinks
                        .into_iter()
                        .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                        .collect(),
                    deployment_id,
                    service_protocol_version,
                    journal_meta: Some(JournalMeta::from(journal_metadata)),
                    creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                    modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                    source: Some(Source::from(source)),
                    completion_retention_time: Some(Duration::from(completion_retention_time)),
                    idempotency_key: idempotency_key.map(|key| key.to_string()),
                }
            }
        }

        impl TryFrom<Suspended>
            for (
                restate_storage_api::invocation_status_table::InFlightInvocationMetadata,
                HashSet<restate_types::identifiers::EntryIndex>,
            )
        {
            type Error = ConversionError;

            fn try_from(value: Suspended) -> Result<Self, ConversionError> {
                let invocation_target = restate_types::invocation::InvocationTarget::try_from(
                    value
                        .invocation_target
                        .ok_or(ConversionError::missing_field("invocation_target"))?,
                )?;

                let pinned_deployment =
                    derive_pinned_deployment(value.deployment_id, value.service_protocol_version)?;

                let journal_metadata =
                    restate_storage_api::invocation_status_table::JournalMetadata::try_from(
                        value
                            .journal_meta
                            .ok_or(ConversionError::missing_field("journal_meta"))?,
                    )?;
                let response_sinks = value
                    .response_sinks
                    .into_iter()
                    .map(|s| {
                        Ok::<_, ConversionError>(Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(s)
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))??)
                    })
                    .collect::<Result<HashSet<_>, _>>()?;

                let waiting_for_completed_entries =
                    value.waiting_for_completed_entries.into_iter().collect();

                let caller = restate_types::invocation::Source::try_from(
                    value
                        .source
                        .ok_or(ConversionError::missing_field("source"))?,
                )?;

                let completion_retention_time = std::time::Duration::try_from(
                    value.completion_retention_time.unwrap_or_default(),
                )?;

                let idempotency_key = value.idempotency_key.map(ByteString::from);

                Ok((
                    restate_storage_api::invocation_status_table::InFlightInvocationMetadata {
                        invocation_target,
                        journal_metadata,
                        pinned_deployment,
                        response_sinks,
                        timestamps:
                            restate_storage_api::invocation_status_table::StatusTimestamps::new(
                                MillisSinceEpoch::new(value.creation_time),
                                MillisSinceEpoch::new(value.modification_time),
                                None,
                                None,
                                None,
                                None,
                            ),
                        source: caller,
                        completion_retention_duration: completion_retention_time,
                        idempotency_key,
                    },
                    waiting_for_completed_entries,
                ))
            }
        }

        impl
            From<(
                restate_storage_api::invocation_status_table::InFlightInvocationMetadata,
                HashSet<restate_types::identifiers::EntryIndex>,
            )> for Suspended
        {
            fn from(
                (metadata, waiting_for_completed_entries): (
                    restate_storage_api::invocation_status_table::InFlightInvocationMetadata,
                    HashSet<restate_types::identifiers::EntryIndex>,
                ),
            ) -> Self {
                let journal_meta = JournalMeta::from(metadata.journal_metadata);
                let waiting_for_completed_entries =
                    waiting_for_completed_entries.into_iter().collect();

                let (deployment_id, service_protocol_version) = match metadata.pinned_deployment {
                    None => (None, None),
                    Some(pinned_deployment) => (
                        Some(pinned_deployment.deployment_id.to_string()),
                        Some(pinned_deployment.service_protocol_version.as_repr()),
                    ),
                };

                Suspended {
                    invocation_target: Some(metadata.invocation_target.into()),
                    response_sinks: metadata
                        .response_sinks
                        .into_iter()
                        .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                        .collect(),
                    journal_meta: Some(journal_meta),
                    deployment_id,
                    service_protocol_version,
                    creation_time: unsafe { metadata.timestamps.creation_time() }.as_u64(),
                    modification_time: unsafe { metadata.timestamps.modification_time() }.as_u64(),
                    waiting_for_completed_entries,
                    source: Some(Source::from(metadata.source)),
                    completion_retention_time: Some(Duration::from(
                        metadata.completion_retention_duration,
                    )),
                    idempotency_key: metadata.idempotency_key.map(|key| key.to_string()),
                }
            }
        }

        impl TryFrom<Inboxed> for restate_storage_api::invocation_status_table::InboxedInvocation {
            type Error = ConversionError;

            fn try_from(value: Inboxed) -> Result<Self, ConversionError> {
                let invocation_target = restate_types::invocation::InvocationTarget::try_from(
                    value
                        .invocation_target
                        .ok_or(ConversionError::missing_field("invocation_target"))?,
                )?;

                let response_sinks = value
                    .response_sinks
                    .into_iter()
                    .map(|s| {
                        Ok::<_, ConversionError>(Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(s)
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))??)
                    })
                    .collect::<Result<HashSet<_>, _>>()?;

                let source = restate_types::invocation::Source::try_from(
                    value
                        .source
                        .ok_or(ConversionError::missing_field("source"))?,
                )?;

                let span_context =
                    restate_types::invocation::ServiceInvocationSpanContext::try_from(
                        value
                            .span_context
                            .ok_or(ConversionError::missing_field("span_context"))?,
                    )?;
                let headers = value
                    .headers
                    .into_iter()
                    .map(|h| restate_types::invocation::Header::try_from(h))
                    .collect::<Result<Vec<_>, ConversionError>>()?;

                let execution_time = if value.execution_time == 0 {
                    None
                } else {
                    Some(MillisSinceEpoch::new(value.execution_time))
                };

                let completion_retention_time = std::time::Duration::try_from(
                    value.completion_retention_time.unwrap_or_default(),
                )?;

                let idempotency_key = value.idempotency_key.map(ByteString::from);

                Ok(restate_storage_api::invocation_status_table::InboxedInvocation {
                    inbox_sequence_number: value.inbox_sequence_number,
                    metadata: restate_storage_api::invocation_status_table::PreFlightInvocationMetadata {
                        response_sinks,
                        timestamps: restate_storage_api::invocation_status_table::StatusTimestamps::new(
                            MillisSinceEpoch::new(value.creation_time),
                            MillisSinceEpoch::new(value.modification_time),
                            None,
                            None,
                            None,
                            None,
                        ),
                        source,
                        span_context,
                        headers,
                        argument: value.argument,
                        execution_time,
                        idempotency_key,
                        completion_retention_duration: completion_retention_time,
                        invocation_target,
                    },
                })
            }
        }

        impl From<restate_storage_api::invocation_status_table::InboxedInvocation> for Inboxed {
            fn from(
                value: restate_storage_api::invocation_status_table::InboxedInvocation,
            ) -> Self {
                let restate_storage_api::invocation_status_table::InboxedInvocation {
                    metadata:
                        restate_storage_api::invocation_status_table::PreFlightInvocationMetadata {
                            response_sinks,
                            timestamps,
                            invocation_target,
                            argument,
                            source,
                            span_context,
                            headers,
                            execution_time,
                            completion_retention_duration: completion_retention_time,
                            idempotency_key,
                        },
                    inbox_sequence_number,
                } = value;

                let headers = headers.into_iter().map(Into::into).collect();

                Inboxed {
                    invocation_target: Some(invocation_target.into()),
                    inbox_sequence_number,
                    response_sinks: response_sinks
                        .into_iter()
                        .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                        .collect(),
                    creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                    modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                    source: Some(Source::from(source)),
                    span_context: Some(SpanContext::from(span_context)),
                    headers,
                    argument,
                    execution_time: execution_time.map(|m| m.as_u64()).unwrap_or_default(),
                    completion_retention_time: Some(Duration::from(completion_retention_time)),
                    idempotency_key: idempotency_key.map(|s| s.to_string()),
                }
            }
        }

        impl TryFrom<Completed> for restate_storage_api::invocation_status_table::CompletedInvocation {
            type Error = ConversionError;

            fn try_from(value: Completed) -> Result<Self, ConversionError> {
                let invocation_target = restate_types::invocation::InvocationTarget::try_from(
                    value
                        .invocation_target
                        .ok_or(ConversionError::missing_field("invocation_target"))?,
                )?;

                let source = restate_types::invocation::Source::try_from(
                    value
                        .source
                        .ok_or(ConversionError::missing_field("source"))?,
                )?;

                let idempotency_key = value.idempotency_key.map(ByteString::from);

                Ok(
                    restate_storage_api::invocation_status_table::CompletedInvocation {
                        invocation_target,
                        span_context: Default::default(),
                        source,
                        timestamps:
                            restate_storage_api::invocation_status_table::StatusTimestamps::new(
                                MillisSinceEpoch::new(value.creation_time),
                                MillisSinceEpoch::new(value.modification_time),
                                None,
                                None,
                                None,
                                None,
                            ),
                        response_result: value
                            .result
                            .ok_or(ConversionError::missing_field("result"))?
                            .try_into()?,
                        idempotency_key,
                        // The value Duration::MAX here disables the new cleaner task business logic.
                        // Look at crates/worker/src/partition/cleaner.rs for more details.
                        completion_retention_duration: std::time::Duration::MAX,
                    },
                )
            }
        }

        impl From<restate_storage_api::invocation_status_table::CompletedInvocation> for Completed {
            fn from(
                value: restate_storage_api::invocation_status_table::CompletedInvocation,
            ) -> Self {
                let restate_storage_api::invocation_status_table::CompletedInvocation {
                    invocation_target,
                    source,
                    idempotency_key,
                    timestamps,
                    response_result,
                    // We don't store this in the old invocation status table
                    completion_retention_duration: _,
                    // The old invocation status table doesn't support span context on Completed
                    span_context: _,
                } = value;

                Completed {
                    invocation_target: Some(InvocationTarget::from(invocation_target)),
                    source: Some(Source::from(source)),
                    result: Some(ResponseResult::from(response_result)),
                    creation_time: unsafe { timestamps.creation_time() }.as_u64(),
                    modification_time: unsafe { timestamps.modification_time() }.as_u64(),
                    idempotency_key: idempotency_key.map(|s| s.to_string()),
                }
            }
        }

        impl TryFrom<JournalMeta> for restate_storage_api::invocation_status_table::JournalMetadata {
            type Error = ConversionError;

            fn try_from(value: JournalMeta) -> Result<Self, ConversionError> {
                let length = value.length;
                let span_context =
                    restate_types::invocation::ServiceInvocationSpanContext::try_from(
                        value
                            .span_context
                            .ok_or(ConversionError::missing_field("span_context"))?,
                    )?;
                Ok(
                    restate_storage_api::invocation_status_table::JournalMetadata {
                        length,
                        span_context,
                    },
                )
            }
        }

        impl From<restate_storage_api::invocation_status_table::JournalMetadata> for JournalMeta {
            fn from(value: restate_storage_api::invocation_status_table::JournalMetadata) -> Self {
                let restate_storage_api::invocation_status_table::JournalMetadata {
                    span_context,
                    length,
                } = value;

                JournalMeta {
                    length,
                    span_context: Some(SpanContext::from(span_context)),
                }
            }
        }

        impl TryFrom<Source> for restate_types::invocation::Source {
            type Error = ConversionError;

            fn try_from(value: Source) -> Result<Self, ConversionError> {
                let source = match value
                    .source
                    .ok_or(ConversionError::missing_field("source"))?
                {
                    source::Source::Ingress(ingress) => restate_types::invocation::Source::Ingress(
                        PartitionProcessorRpcRequestId::from_slice(&ingress.rpc_id)
                            // TODO this should become an hard error in Restate 1.3
                            .unwrap_or_default(),
                    ),
                    source::Source::Subscription(subscription) => {
                        restate_types::invocation::Source::Subscription(
                            restate_types::identifiers::SubscriptionId::from_slice(
                                &subscription.subscription_id,
                            )
                            .map_err(|e| ConversionError::invalid_data(e))?,
                        )
                    }
                    source::Source::Service(service) => restate_types::invocation::Source::Service(
                        restate_types::identifiers::InvocationId::try_from(
                            service
                                .invocation_id
                                .ok_or(ConversionError::missing_field("invocation_id"))?,
                        )?,
                        restate_types::invocation::InvocationTarget::try_from(
                            service
                                .invocation_target
                                .ok_or(ConversionError::missing_field("invocation_target"))?,
                        )?,
                    ),
                    source::Source::Internal(_) => restate_types::invocation::Source::Internal,
                };

                Ok(source)
            }
        }

        impl From<restate_types::invocation::Source> for Source {
            fn from(value: restate_types::invocation::Source) -> Self {
                let source = match value {
                    restate_types::invocation::Source::Ingress(rpc_id) => {
                        source::Source::Ingress(source::Ingress {
                            rpc_id: rpc_id.to_bytes().to_vec().into(),
                        })
                    }
                    restate_types::invocation::Source::Subscription(sub_id) => {
                        source::Source::Subscription(source::Subscription {
                            subscription_id: sub_id.to_bytes().to_vec().into(),
                        })
                    }
                    restate_types::invocation::Source::Service(
                        invocation_id,
                        invocation_target,
                    ) => source::Source::Service(source::Service {
                        invocation_id: Some(InvocationId::from(invocation_id)),
                        invocation_target: Some(InvocationTarget::from(invocation_target)),
                    }),
                    restate_types::invocation::Source::Internal => source::Source::Internal(()),
                };

                Source {
                    source: Some(source),
                }
            }
        }

        impl TryFrom<InboxEntry> for restate_storage_api::inbox_table::InboxEntry {
            type Error = ConversionError;

            fn try_from(value: InboxEntry) -> Result<Self, ConversionError> {
                Ok(
                    match value.entry.ok_or(ConversionError::missing_field("entry"))? {
                        inbox_entry::Entry::Invocation(invocation) => {
                            restate_storage_api::inbox_table::InboxEntry::Invocation(
                                restate_types::identifiers::ServiceId::try_from(
                                    invocation
                                        .service_id
                                        .ok_or(ConversionError::missing_field("service_id"))?,
                                )?,
                                restate_types::identifiers::InvocationId::try_from(
                                    invocation
                                        .invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                            )
                        }
                        inbox_entry::Entry::StateMutation(state_mutation) => {
                            restate_storage_api::inbox_table::InboxEntry::StateMutation(
                                restate_types::state_mut::ExternalStateMutation::try_from(
                                    state_mutation,
                                )?,
                            )
                        }
                    },
                )
            }
        }

        impl From<restate_storage_api::inbox_table::InboxEntry> for InboxEntry {
            fn from(inbox_entry: restate_storage_api::inbox_table::InboxEntry) -> Self {
                let inbox_entry = match inbox_entry {
                    restate_storage_api::inbox_table::InboxEntry::Invocation(
                        service_id,
                        invocation_id,
                    ) => inbox_entry::Entry::Invocation(inbox_entry::Invocation {
                        service_id: Some(service_id.into()),
                        invocation_id: Some(InvocationId::from(invocation_id)),
                    }),
                    restate_storage_api::inbox_table::InboxEntry::StateMutation(state_mutation) => {
                        inbox_entry::Entry::StateMutation(StateMutation::from(state_mutation))
                    }
                };

                InboxEntry {
                    entry: Some(inbox_entry),
                }
            }
        }

        impl TryFrom<ServiceInvocation> for restate_types::invocation::ServiceInvocation {
            type Error = ConversionError;

            fn try_from(value: ServiceInvocation) -> Result<Self, ConversionError> {
                let ServiceInvocation {
                    invocation_id,
                    invocation_target,
                    response_sink,
                    span_context,
                    argument,
                    source,
                    headers,
                    execution_time,
                    idempotency_key,
                    completion_retention_time,
                    submit_notification_sink,
                } = value;

                let invocation_id = restate_types::identifiers::InvocationId::try_from(
                    invocation_id.ok_or(ConversionError::missing_field("invocation_id"))?,
                )?;

                let invocation_target = restate_types::invocation::InvocationTarget::try_from(
                    invocation_target.ok_or(ConversionError::missing_field("invocation_target"))?,
                )?;

                let span_context =
                    restate_types::invocation::ServiceInvocationSpanContext::try_from(
                        span_context.ok_or(ConversionError::missing_field("span_context"))?,
                    )?;

                let response_sink =
                    Option::<restate_types::invocation::ServiceInvocationResponseSink>::try_from(
                        response_sink.ok_or(ConversionError::missing_field("response_sink"))?,
                    )?;

                let source = restate_types::invocation::Source::try_from(
                    source.ok_or(ConversionError::missing_field("source"))?,
                )?;

                let headers = headers
                    .into_iter()
                    .map(|h| restate_types::invocation::Header::try_from(h))
                    .collect::<Result<Vec<_>, ConversionError>>()?;

                let execution_time = if execution_time == 0 {
                    None
                } else {
                    Some(MillisSinceEpoch::new(execution_time))
                };

                let completion_retention_time = completion_retention_time
                    .map(std::time::Duration::try_from)
                    .transpose()?;

                let idempotency_key = idempotency_key.map(ByteString::from);

                let submit_notification_sink = submit_notification_sink
                    .map(TryInto::try_into)
                    .transpose()?;

                Ok(restate_types::invocation::ServiceInvocation {
                    invocation_id,
                    invocation_target,
                    argument,
                    source,
                    response_sink,
                    span_context,
                    headers,
                    execution_time,
                    completion_retention_duration: completion_retention_time,
                    idempotency_key,
                    submit_notification_sink: submit_notification_sink,
                })
            }
        }

        impl From<restate_types::invocation::ServiceInvocation> for ServiceInvocation {
            fn from(value: restate_types::invocation::ServiceInvocation) -> Self {
                let invocation_target = InvocationTarget::from(value.invocation_target);
                let span_context = SpanContext::from(value.span_context);
                let response_sink = ServiceInvocationResponseSink::from(value.response_sink);
                let source = Source::from(value.source);
                let headers = value.headers.into_iter().map(Into::into).collect();

                ServiceInvocation {
                    invocation_id: Some(InvocationId::from(value.invocation_id)),
                    invocation_target: Some(invocation_target),
                    span_context: Some(span_context),
                    response_sink: Some(response_sink),
                    argument: value.argument,
                    source: Some(source),
                    headers,
                    execution_time: value.execution_time.map(|m| m.as_u64()).unwrap_or_default(),
                    completion_retention_time: value
                        .completion_retention_duration
                        .map(Duration::from),
                    idempotency_key: value.idempotency_key.map(|s| s.to_string()),
                    submit_notification_sink: value.submit_notification_sink.map(Into::into),
                }
            }
        }

        impl TryFrom<SubmitNotificationSink> for restate_types::invocation::SubmitNotificationSink {
            type Error = ConversionError;

            fn try_from(value: SubmitNotificationSink) -> Result<Self, ConversionError> {
                let notification_sink = match value
                    .notification_sink
                    .ok_or(ConversionError::missing_field("notification_sink"))?
                {
                    submit_notification_sink::NotificationSink::Ingress(
                        submit_notification_sink::Ingress { request_id },
                    ) => restate_types::invocation::SubmitNotificationSink::Ingress {
                        request_id:
                            restate_types::identifiers::PartitionProcessorRpcRequestId::from_slice(
                                request_id.as_ref(),
                            )
                            .map_err(ConversionError::invalid_data)?,
                    },
                };

                Ok(notification_sink)
            }
        }

        impl From<restate_types::invocation::SubmitNotificationSink> for SubmitNotificationSink {
            fn from(value: restate_types::invocation::SubmitNotificationSink) -> Self {
                let notification_sink = match value {
                    restate_types::invocation::SubmitNotificationSink::Ingress { request_id } => {
                        submit_notification_sink::NotificationSink::Ingress(
                            submit_notification_sink::Ingress {
                                request_id: Bytes::copy_from_slice(&request_id.to_bytes()),
                            },
                        )
                    }
                };

                SubmitNotificationSink {
                    notification_sink: Some(notification_sink),
                }
            }
        }

        impl TryFrom<StateMutation> for restate_types::state_mut::ExternalStateMutation {
            type Error = ConversionError;

            fn try_from(state_mutation: StateMutation) -> Result<Self, ConversionError> {
                let service_id = restate_types::identifiers::ServiceId::try_from(
                    state_mutation
                        .service_id
                        .ok_or(ConversionError::missing_field("service_id"))?,
                )?;
                let state = state_mutation
                    .kv_pairs
                    .into_iter()
                    .map(|kv| (kv.key, kv.value))
                    .collect();

                Ok(restate_types::state_mut::ExternalStateMutation {
                    service_id: service_id,
                    version: state_mutation.version,
                    state,
                })
            }
        }

        impl From<restate_types::state_mut::ExternalStateMutation> for StateMutation {
            fn from(state_mutation: restate_types::state_mut::ExternalStateMutation) -> Self {
                let service_id = ServiceId::from(state_mutation.service_id);
                let kv_pairs = state_mutation
                    .state
                    .into_iter()
                    .map(|(key, value)| KvPair { key, value })
                    .collect();

                StateMutation {
                    service_id: Some(service_id),
                    version: state_mutation.version,
                    kv_pairs,
                }
            }
        }

        impl TryFrom<InvocationTarget> for restate_types::invocation::InvocationTarget {
            type Error = ConversionError;

            fn try_from(value: InvocationTarget) -> Result<Self, ConversionError> {
                let name =
                    ByteString::try_from(value.name).map_err(ConversionError::invalid_data)?;
                let handler =
                    ByteString::try_from(value.handler).map_err(ConversionError::invalid_data)?;

                match invocation_target::Ty::try_from(value.service_and_handler_ty) {
                    Ok(invocation_target::Ty::Service) => {
                        Ok(restate_types::invocation::InvocationTarget::Service { name, handler })
                    }
                    Ok(invocation_target::Ty::VirtualObjectExclusive) => {
                        Ok(restate_types::invocation::InvocationTarget::VirtualObject {
                            name,
                            handler,
                            key: ByteString::try_from(value.key)
                                .map_err(ConversionError::invalid_data)?,
                            handler_ty:
                                restate_types::invocation::VirtualObjectHandlerType::Exclusive,
                        })
                    }
                    Ok(invocation_target::Ty::VirtualObjectShared) => {
                        Ok(restate_types::invocation::InvocationTarget::VirtualObject {
                            name,
                            handler,
                            key: ByteString::try_from(value.key)
                                .map_err(ConversionError::invalid_data)?,
                            handler_ty: restate_types::invocation::VirtualObjectHandlerType::Shared,
                        })
                    }
                    Ok(invocation_target::Ty::WorkflowWorkflow) => {
                        Ok(restate_types::invocation::InvocationTarget::Workflow {
                            name,
                            handler,
                            key: ByteString::try_from(value.key)
                                .map_err(ConversionError::invalid_data)?,
                            handler_ty: restate_types::invocation::WorkflowHandlerType::Workflow,
                        })
                    }
                    Ok(invocation_target::Ty::WorkflowShared) => {
                        Ok(restate_types::invocation::InvocationTarget::Workflow {
                            name,
                            handler,
                            key: ByteString::try_from(value.key)
                                .map_err(ConversionError::invalid_data)?,
                            handler_ty: restate_types::invocation::WorkflowHandlerType::Shared,
                        })
                    }
                    _ => Err(ConversionError::unexpected_enum_variant(
                        "ty",
                        value.service_and_handler_ty,
                    )),
                }
            }
        }

        impl From<restate_types::invocation::InvocationTarget> for InvocationTarget {
            fn from(value: restate_types::invocation::InvocationTarget) -> Self {
                match value {
                    restate_types::invocation::InvocationTarget::Service { name, handler } => {
                        InvocationTarget {
                            name: name.into_bytes(),
                            handler: handler.into_bytes(),
                            service_and_handler_ty: invocation_target::Ty::Service.into(),
                            ..InvocationTarget::default()
                        }
                    }
                    restate_types::invocation::InvocationTarget::VirtualObject {
                        name,
                        key,
                        handler,
                        handler_ty,
                    } => InvocationTarget {
                        name: name.into_bytes(),
                        handler: handler.into_bytes(),
                        key: key.into_bytes(),
                        service_and_handler_ty: match handler_ty {
                            restate_types::invocation::VirtualObjectHandlerType::Shared => {
                                invocation_target::Ty::VirtualObjectShared
                            }
                            restate_types::invocation::VirtualObjectHandlerType::Exclusive => {
                                invocation_target::Ty::VirtualObjectExclusive
                            }
                        }
                        .into(),
                    },
                    restate_types::invocation::InvocationTarget::Workflow {
                        name,
                        key,
                        handler,
                        handler_ty,
                    } => InvocationTarget {
                        name: name.into_bytes(),
                        handler: handler.into_bytes(),
                        key: key.into_bytes(),
                        service_and_handler_ty: match handler_ty {
                            restate_types::invocation::WorkflowHandlerType::Shared => {
                                invocation_target::Ty::WorkflowShared
                            }
                            restate_types::invocation::WorkflowHandlerType::Workflow => {
                                invocation_target::Ty::WorkflowWorkflow
                            }
                        }
                        .into(),
                    },
                }
            }
        }

        impl TryFrom<ServiceId> for restate_types::identifiers::ServiceId {
            type Error = ConversionError;

            fn try_from(service_id: ServiceId) -> Result<Self, ConversionError> {
                Ok(restate_types::identifiers::ServiceId::new(
                    ByteString::try_from(service_id.service_name)
                        .map_err(ConversionError::invalid_data)?,
                    ByteString::try_from(service_id.service_key)
                        .map_err(ConversionError::invalid_data)?,
                ))
            }
        }

        impl From<restate_types::identifiers::ServiceId> for ServiceId {
            fn from(service_id: restate_types::identifiers::ServiceId) -> Self {
                ServiceId {
                    service_key: service_id.key.into_bytes(),
                    service_name: service_id.service_name.into_bytes(),
                }
            }
        }

        fn try_bytes_into_invocation_uuid(
            bytes: Bytes,
        ) -> Result<restate_types::identifiers::InvocationUuid, ConversionError> {
            restate_types::identifiers::InvocationUuid::from_slice(bytes.as_ref())
                .map_err(ConversionError::invalid_data)
        }

        impl TryFrom<SpanContext> for restate_types::invocation::ServiceInvocationSpanContext {
            type Error = ConversionError;

            fn try_from(value: SpanContext) -> Result<Self, ConversionError> {
                let SpanContext {
                    trace_id,
                    span_id,
                    trace_flags,
                    is_remote,
                    trace_state,
                    span_relation,
                } = value;

                let trace_id = try_bytes_into_trace_id(trace_id)?;
                let span_id = opentelemetry::trace::SpanId::from_bytes(span_id.to_be_bytes());
                let trace_flags = opentelemetry::trace::TraceFlags::new(
                    u8::try_from(trace_flags).map_err(ConversionError::invalid_data)?,
                );

                let trace_state =
                    TraceState::from_str(&trace_state).map_err(ConversionError::invalid_data)?;

                let span_relation = span_relation
                    .map(|span_relation| span_relation.try_into())
                    .transpose()
                    .map_err(ConversionError::invalid_data)?;

                Ok(
                    restate_types::invocation::ServiceInvocationSpanContext::new(
                        opentelemetry::trace::SpanContext::new(
                            trace_id,
                            span_id,
                            trace_flags,
                            is_remote,
                            trace_state,
                        ),
                        span_relation,
                    ),
                )
            }
        }

        impl From<restate_types::invocation::ServiceInvocationSpanContext> for SpanContext {
            fn from(value: restate_types::invocation::ServiceInvocationSpanContext) -> Self {
                let span_context = value.span_context();
                let trace_state = span_context.trace_state().header();
                let span_id = u64::from_be_bytes(span_context.span_id().to_bytes());
                let trace_flags = u32::from(span_context.trace_flags().to_u8());
                let trace_id = Bytes::copy_from_slice(&span_context.trace_id().to_bytes());
                let is_remote = span_context.is_remote();
                let span_relation = value
                    .span_cause()
                    .map(|span_relation| SpanRelation::from(span_relation.clone()));

                SpanContext {
                    trace_state,
                    span_id,
                    trace_flags,
                    trace_id,
                    is_remote,
                    span_relation,
                }
            }
        }

        impl TryFrom<SpanRelation> for restate_types::invocation::SpanRelationCause {
            type Error = ConversionError;

            fn try_from(value: SpanRelation) -> Result<Self, ConversionError> {
                match value.kind.ok_or(ConversionError::missing_field("kind"))? {
                    span_relation::Kind::Parent(span_relation::Parent { span_id }) => {
                        let span_id =
                            opentelemetry::trace::SpanId::from_bytes(span_id.to_be_bytes());
                        Ok(Self::Parent(span_id))
                    }
                    span_relation::Kind::Linked(span_relation::Linked { trace_id, span_id }) => {
                        let trace_id = try_bytes_into_trace_id(trace_id)?;
                        let span_id =
                            opentelemetry::trace::SpanId::from_bytes(span_id.to_be_bytes());
                        Ok(Self::Linked(trace_id, span_id))
                    }
                }
            }
        }

        impl From<restate_types::invocation::SpanRelationCause> for SpanRelation {
            fn from(value: restate_types::invocation::SpanRelationCause) -> Self {
                let kind = match value {
                    restate_types::invocation::SpanRelationCause::Parent(span_id) => {
                        let span_id = u64::from_be_bytes(span_id.to_bytes());
                        span_relation::Kind::Parent(span_relation::Parent { span_id })
                    }
                    restate_types::invocation::SpanRelationCause::Linked(trace_id, span_id) => {
                        let span_id = u64::from_be_bytes(span_id.to_bytes());
                        let trace_id = Bytes::copy_from_slice(&trace_id.to_bytes());
                        span_relation::Kind::Linked(span_relation::Linked { trace_id, span_id })
                    }
                };

                Self { kind: Some(kind) }
            }
        }

        fn try_bytes_into_trace_id(
            mut bytes: Bytes,
        ) -> Result<opentelemetry::trace::TraceId, ConversionError> {
            if bytes.len() != 16 {
                return Err(ConversionError::InvalidData(anyhow!(
                    "trace id pb definition needs to contain exactly 16 bytes"
                )));
            }

            let mut bytes_array = [0; 16];
            bytes.copy_to_slice(&mut bytes_array);

            Ok(opentelemetry::trace::TraceId::from_bytes(bytes_array))
        }

        impl TryFrom<ServiceInvocationResponseSink>
            for Option<restate_types::invocation::ServiceInvocationResponseSink>
        {
            type Error = ConversionError;

            fn try_from(value: ServiceInvocationResponseSink) -> Result<Self, ConversionError> {
                let response_sink = match value
                    .response_sink
                    .ok_or(ConversionError::missing_field("response_sink"))?
                {
                    ResponseSink::PartitionProcessor(partition_processor) => {
                        Some(
                            restate_types::invocation::ServiceInvocationResponseSink::PartitionProcessor {
                                caller: restate_types::identifiers::InvocationId::from_slice(&partition_processor.caller)?,
                                entry_index: partition_processor.entry_index,
                            },
                        )
                    }
                    ResponseSink::Ingress(ingress) => {
                        Some(
                            restate_types::invocation::ServiceInvocationResponseSink::Ingress {
                                request_id: restate_types::identifiers::PartitionProcessorRpcRequestId::from_slice(ingress.request_id.as_ref())
                                    .map_err(ConversionError::invalid_data)?

                            },
                        )
                    }
                    ResponseSink::None(_) => None,
                };

                Ok(response_sink)
            }
        }

        impl From<Option<restate_types::invocation::ServiceInvocationResponseSink>>
            for ServiceInvocationResponseSink
        {
            fn from(
                value: Option<restate_types::invocation::ServiceInvocationResponseSink>,
            ) -> Self {
                let response_sink = match value {
                    Some(
                        restate_types::invocation::ServiceInvocationResponseSink::PartitionProcessor {
                            caller,
                            entry_index,
                        },
                    ) => ResponseSink::PartitionProcessor(PartitionProcessor {
                        entry_index,
                        caller: caller.into(),
                    }),
                    Some(restate_types::invocation::ServiceInvocationResponseSink::Ingress {  request_id }) => {
                        ResponseSink::Ingress(Ingress {
                            request_id: Bytes::copy_from_slice(&request_id.to_bytes())
                        })
                    },
                    None => ResponseSink::None(Default::default()),
                };

                ServiceInvocationResponseSink {
                    response_sink: Some(response_sink),
                }
            }
        }

        impl TryFrom<Header> for restate_types::invocation::Header {
            type Error = ConversionError;

            fn try_from(value: Header) -> Result<Self, ConversionError> {
                let Header { name, value } = value;

                Ok(restate_types::invocation::Header::new(name, value))
            }
        }

        impl From<restate_types::invocation::Header> for Header {
            fn from(value: restate_types::invocation::Header) -> Self {
                Self {
                    name: value.name.to_string(),
                    value: value.value.to_string(),
                }
            }
        }

        impl From<GenerationalNodeId> for super::GenerationalNodeId {
            fn from(value: GenerationalNodeId) -> Self {
                super::GenerationalNodeId {
                    id: value.raw_id(),
                    generation: value.raw_generation(),
                }
            }
        }

        impl From<super::GenerationalNodeId> for GenerationalNodeId {
            fn from(value: super::GenerationalNodeId) -> Self {
                GenerationalNodeId::new(value.id, value.generation)
            }
        }

        impl TryFrom<JournalEntry> for restate_storage_api::journal_table::JournalEntry {
            type Error = ConversionError;

            fn try_from(value: JournalEntry) -> Result<Self, ConversionError> {
                let journal_entry = match value
                    .kind
                    .ok_or(ConversionError::missing_field("kind"))?
                {
                    Kind::Entry(journal_entry) => {
                        restate_storage_api::journal_table::JournalEntry::Entry(
                            restate_types::journal::enriched::EnrichedRawEntry::try_from(
                                journal_entry,
                            )?,
                        )
                    }
                    Kind::CompletionResult(completion_result) => {
                        restate_storage_api::journal_table::JournalEntry::Completion(
                            restate_types::journal::CompletionResult::try_from(completion_result)?,
                        )
                    }
                };

                Ok(journal_entry)
            }
        }

        impl From<restate_storage_api::journal_table::JournalEntry> for JournalEntry {
            fn from(value: restate_storage_api::journal_table::JournalEntry) -> Self {
                match value {
                    restate_storage_api::journal_table::JournalEntry::Entry(entry) => {
                        JournalEntry::from(entry)
                    }
                    restate_storage_api::journal_table::JournalEntry::Completion(completion) => {
                        JournalEntry::from(completion)
                    }
                }
            }
        }

        impl From<restate_types::journal::enriched::EnrichedRawEntry> for JournalEntry {
            fn from(value: restate_types::journal::enriched::EnrichedRawEntry) -> Self {
                JournalEntry {
                    kind: Some(Kind::Entry(value.into())),
                }
            }
        }

        impl From<restate_types::journal::CompletionResult> for JournalEntry {
            fn from(value: restate_types::journal::CompletionResult) -> Self {
                let completion_result = CompletionResult::from(value);

                JournalEntry {
                    kind: Some(Kind::CompletionResult(completion_result)),
                }
            }
        }

        impl TryFrom<journal_entry::Entry> for restate_types::journal::enriched::EnrichedRawEntry {
            type Error = ConversionError;

            fn try_from(value: journal_entry::Entry) -> Result<Self, ConversionError> {
                let journal_entry::Entry { header, raw_entry } = value;

                let header = restate_types::journal::enriched::EnrichedEntryHeader::try_from(
                    header.ok_or(ConversionError::missing_field("header"))?,
                )?;

                Ok(restate_types::journal::enriched::EnrichedRawEntry::new(
                    header, raw_entry,
                ))
            }
        }

        impl From<restate_types::journal::enriched::EnrichedRawEntry> for journal_entry::Entry {
            fn from(value: restate_types::journal::enriched::EnrichedRawEntry) -> Self {
                let (header, entry) = value.into_inner();
                journal_entry::Entry {
                    header: Some(EnrichedEntryHeader::from(header)),
                    raw_entry: entry,
                }
            }
        }

        impl TryFrom<CompletionResult> for restate_types::journal::CompletionResult {
            type Error = ConversionError;

            fn try_from(value: CompletionResult) -> Result<Self, ConversionError> {
                let result = match value
                    .result
                    .ok_or(ConversionError::missing_field("result"))?
                {
                    completion_result::Result::Empty(_) => {
                        restate_types::journal::CompletionResult::Empty
                    }
                    completion_result::Result::Success(success) => {
                        restate_types::journal::CompletionResult::Success(success.value)
                    }
                    completion_result::Result::Failure(failure) => {
                        let failure_message = ByteString::try_from(failure.message)
                            .map_err(ConversionError::invalid_data);

                        restate_types::journal::CompletionResult::Failure(
                            failure.error_code.into(),
                            failure_message?,
                        )
                    }
                };

                Ok(result)
            }
        }

        impl From<restate_types::journal::CompletionResult> for CompletionResult {
            fn from(value: restate_types::journal::CompletionResult) -> Self {
                let result = match value {
                    restate_types::journal::CompletionResult::Empty => {
                        completion_result::Result::Empty(Empty {})
                    }
                    restate_types::journal::CompletionResult::Success(value) => {
                        completion_result::Result::Success(Success { value })
                    }
                    restate_types::journal::CompletionResult::Failure(error_code, message) => {
                        completion_result::Result::Failure(Failure {
                            error_code: error_code.into(),
                            message: message.into_bytes(),
                        })
                    }
                };

                CompletionResult {
                    result: Some(result),
                }
            }
        }

        impl TryFrom<EnrichedEntryHeader> for restate_types::journal::enriched::EnrichedEntryHeader {
            type Error = ConversionError;

            fn try_from(value: EnrichedEntryHeader) -> Result<Self, ConversionError> {
                // By definition of requires_ack, if it reached the journal storage then
                // either there is one in-flight stream that already got notified of this entry ack,
                // or there are no in-flight streams and the entry won't need any ack because it's in the replayed journal.

                let enriched_header = match value
                    .kind
                    .ok_or(ConversionError::missing_field("kind"))?
                {
                    enriched_entry_header::Kind::Input(_) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::Input {}
                    }
                    enriched_entry_header::Kind::Output(_) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::Output {}
                    }
                    enriched_entry_header::Kind::GetState(get_state) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::GetState {
                            is_completed: get_state.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::SetState(_) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::SetState {}
                    }
                    enriched_entry_header::Kind::ClearState(_) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::ClearState {}
                    }
                    enriched_entry_header::Kind::ClearAllState(_) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::ClearAllState {}
                    }
                    enriched_entry_header::Kind::GetStateKeys(get_state_keys) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::GetStateKeys {
                            is_completed: get_state_keys.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::GetPromise(get_promise) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::GetPromise {
                            is_completed: get_promise.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::PeekPromise(peek_promise) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::PeekPromise {
                            is_completed: peek_promise.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::CompletePromise(complete_promise) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::CompletePromise {
                            is_completed: complete_promise.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::Sleep(sleep) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::Sleep {
                            is_completed: sleep.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::Invoke(invoke) => {
                        let enrichment_result = Option::<
                            restate_types::journal::enriched::CallEnrichmentResult,
                        >::try_from(
                            invoke
                                .resolution_result
                                .ok_or(ConversionError::missing_field("resolution_result"))?,
                        )?;

                        restate_types::journal::enriched::EnrichedEntryHeader::Call {
                            is_completed: invoke.is_completed,
                            enrichment_result,
                        }
                    }
                    enriched_entry_header::Kind::BackgroundCall(background_call) => {
                        let enrichment_result =
                            restate_types::journal::enriched::CallEnrichmentResult::try_from(
                                background_call
                                    .resolution_result
                                    .ok_or(ConversionError::missing_field("resolution_result"))?,
                            )?;

                        restate_types::journal::enriched::EnrichedEntryHeader::OneWayCall {
                            enrichment_result,
                        }
                    }
                    enriched_entry_header::Kind::Awakeable(awakeable) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::Awakeable {
                            is_completed: awakeable.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::CompleteAwakeable(CompleteAwakeable {
                        invocation_id,
                        entry_index,
                    }) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::CompleteAwakeable {
                            enrichment_result: AwakeableEnrichmentResult {
                                invocation_id: restate_types::identifiers::InvocationId::try_from(
                                    invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )
                                .map_err(ConversionError::invalid_data)?,
                                entry_index,
                            },
                        }
                    }
                    enriched_entry_header::Kind::SideEffect(_) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::Run {}
                    }
                    enriched_entry_header::Kind::CancelInvocation(_) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::CancelInvocation {}
                    }
                    enriched_entry_header::Kind::GetCallInvocationId(entry) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::GetCallInvocationId {
                            is_completed: entry.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::AttachInvocation(entry) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::AttachInvocation {
                            is_completed: entry.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::GetInvocationOutput(entry) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::GetInvocationOutput {
                            is_completed: entry.is_completed,
                        }
                    }
                    enriched_entry_header::Kind::Custom(custom) => {
                        restate_types::journal::enriched::EnrichedEntryHeader::Custom {
                            code: u16::try_from(custom.code)
                                .map_err(ConversionError::invalid_data)?,
                        }
                    }
                };

                Ok(enriched_header)
            }
        }

        impl From<restate_types::journal::enriched::EnrichedEntryHeader> for EnrichedEntryHeader {
            fn from(value: restate_types::journal::enriched::EnrichedEntryHeader) -> Self {
                // No need to write down the requires_ack field for any of the entries because
                // when reading an entry from storage, we never need to send the ack back for it.

                let kind = match value {
                    restate_types::journal::enriched::EnrichedEntryHeader::Input { .. } => {
                        enriched_entry_header::Kind::Input(Input {})
                    }
                    restate_types::journal::enriched::EnrichedEntryHeader::Output { .. } => {
                        enriched_entry_header::Kind::Output(Output {})
                    }
                    restate_types::journal::enriched::EnrichedEntryHeader::GetState {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::GetState(GetState { is_completed }),
                    restate_types::journal::enriched::EnrichedEntryHeader::SetState { .. } => {
                        enriched_entry_header::Kind::SetState(SetState {})
                    }
                    restate_types::journal::enriched::EnrichedEntryHeader::ClearState {
                        ..
                    } => enriched_entry_header::Kind::ClearState(ClearState {}),
                    restate_types::journal::enriched::EnrichedEntryHeader::GetStateKeys {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::GetStateKeys(GetStateKeys { is_completed }),
                    restate_types::journal::enriched::EnrichedEntryHeader::ClearAllState {
                        ..
                    } => enriched_entry_header::Kind::ClearAllState(ClearAllState {}),
                    restate_types::journal::enriched::EnrichedEntryHeader::Sleep {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::Sleep(Sleep { is_completed }),
                    restate_types::journal::enriched::EnrichedEntryHeader::Call {
                        is_completed,
                        enrichment_result,
                        ..
                    } => enriched_entry_header::Kind::Invoke(Invoke {
                        is_completed,
                        resolution_result: Some(InvocationResolutionResult::from(
                            enrichment_result,
                        )),
                    }),
                    restate_types::journal::enriched::EnrichedEntryHeader::OneWayCall {
                        enrichment_result,
                        ..
                    } => enriched_entry_header::Kind::BackgroundCall(BackgroundCall {
                        resolution_result: Some(BackgroundCallResolutionResult::from(
                            enrichment_result,
                        )),
                    }),
                    restate_types::journal::enriched::EnrichedEntryHeader::Awakeable {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::Awakeable(Awakeable { is_completed }),
                    restate_types::journal::enriched::EnrichedEntryHeader::CompleteAwakeable {
                        enrichment_result,
                        ..
                    } => enriched_entry_header::Kind::CompleteAwakeable(CompleteAwakeable {
                        invocation_id: Some(InvocationId::from(enrichment_result.invocation_id)),
                        entry_index: enrichment_result.entry_index,
                    }),
                    restate_types::journal::enriched::EnrichedEntryHeader::Run { .. } => {
                        enriched_entry_header::Kind::SideEffect(SideEffect {})
                    }
                    restate_types::journal::enriched::EnrichedEntryHeader::Custom {
                        code, ..
                    } => enriched_entry_header::Kind::Custom(Custom {
                        code: u32::from(code),
                    }),
                    restate_types::journal::enriched::EnrichedEntryHeader::GetPromise {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::GetPromise(GetPromise { is_completed }),
                    restate_types::journal::enriched::EnrichedEntryHeader::PeekPromise {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::PeekPromise(PeekPromise { is_completed }),
                    restate_types::journal::enriched::EnrichedEntryHeader::CompletePromise {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::CompletePromise(CompletePromise {
                        is_completed,
                    }),
                    restate_types::journal::enriched::EnrichedEntryHeader::CancelInvocation {
                        ..
                    } => enriched_entry_header::Kind::CancelInvocation(CancelInvocation {}),
                    restate_types::journal::enriched::EnrichedEntryHeader::GetCallInvocationId {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::GetCallInvocationId(GetCallInvocationId { is_completed }),
                    restate_types::journal::enriched::EnrichedEntryHeader::AttachInvocation {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::AttachInvocation(AttachInvocation { is_completed }),
                    restate_types::journal::enriched::EnrichedEntryHeader::GetInvocationOutput {
                        is_completed,
                        ..
                    } => enriched_entry_header::Kind::GetInvocationOutput(GetInvocationOutput { is_completed }),
                };

                EnrichedEntryHeader { kind: Some(kind) }
            }
        }

        impl TryFrom<InvocationResolutionResult>
            for Option<restate_types::journal::enriched::CallEnrichmentResult>
        {
            type Error = ConversionError;

            fn try_from(value: InvocationResolutionResult) -> Result<Self, ConversionError> {
                let result = match value
                    .result
                    .ok_or(ConversionError::missing_field("result"))?
                {
                    invocation_resolution_result::Result::None(_) => None,
                    invocation_resolution_result::Result::Success(success) => {
                        let invocation_id = restate_types::identifiers::InvocationId::try_from(
                            success
                                .invocation_id
                                .ok_or(ConversionError::missing_field("invocation_id"))?,
                        )?;

                        let invocation_target =
                            restate_types::invocation::InvocationTarget::try_from(
                                success
                                    .invocation_target
                                    .ok_or(ConversionError::missing_field("invocation_target"))?,
                            )?;

                        let span_context =
                            restate_types::invocation::ServiceInvocationSpanContext::try_from(
                                success
                                    .span_context
                                    .ok_or(ConversionError::missing_field("span_context"))?,
                            )?;

                        let completion_retention_time = Some(std::time::Duration::try_from(
                            success.completion_retention_time.unwrap_or_default(),
                        )?);

                        Some(restate_types::journal::enriched::CallEnrichmentResult {
                            invocation_id,
                            invocation_target,
                            span_context,
                            completion_retention_time,
                        })
                    }
                };

                Ok(result)
            }
        }

        impl From<Option<restate_types::journal::enriched::CallEnrichmentResult>>
            for InvocationResolutionResult
        {
            fn from(value: Option<restate_types::journal::enriched::CallEnrichmentResult>) -> Self {
                let result = match value {
                    None => invocation_resolution_result::Result::None(Default::default()),
                    Some(resolution_result) => match resolution_result {
                        restate_types::journal::enriched::CallEnrichmentResult {
                            invocation_id,
                            invocation_target,
                            span_context,
                            completion_retention_time,
                        } => invocation_resolution_result::Result::Success(
                            invocation_resolution_result::Success {
                                invocation_id: Some(InvocationId::from(invocation_id)),
                                invocation_target: Some(invocation_target.into()),
                                span_context: Some(SpanContext::from(span_context)),
                                completion_retention_time: Some(Duration::from(
                                    completion_retention_time.unwrap_or_default(),
                                )),
                            },
                        ),
                    },
                };

                InvocationResolutionResult {
                    result: Some(result),
                }
            }
        }

        impl TryFrom<BackgroundCallResolutionResult>
            for restate_types::journal::enriched::CallEnrichmentResult
        {
            type Error = ConversionError;

            fn try_from(value: BackgroundCallResolutionResult) -> Result<Self, ConversionError> {
                let invocation_id = restate_types::identifiers::InvocationId::try_from(
                    value
                        .invocation_id
                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                )?;

                let invocation_target = restate_types::invocation::InvocationTarget::try_from(
                    value
                        .invocation_target
                        .ok_or(ConversionError::missing_field("invocation_target"))?,
                )?;
                let span_context =
                    restate_types::invocation::ServiceInvocationSpanContext::try_from(
                        value
                            .span_context
                            .ok_or(ConversionError::missing_field("span_context"))?,
                    )?;

                let completion_retention_time = Some(std::time::Duration::try_from(
                    value.completion_retention_time.unwrap_or_default(),
                )?);

                Ok(restate_types::journal::enriched::CallEnrichmentResult {
                    invocation_id,
                    span_context,
                    invocation_target,
                    completion_retention_time,
                })
            }
        }

        impl From<restate_types::journal::enriched::CallEnrichmentResult>
            for BackgroundCallResolutionResult
        {
            fn from(value: restate_types::journal::enriched::CallEnrichmentResult) -> Self {
                BackgroundCallResolutionResult {
                    invocation_id: Some(InvocationId::from(value.invocation_id)),
                    invocation_target: Some(value.invocation_target.into()),
                    span_context: Some(SpanContext::from(value.span_context)),
                    completion_retention_time: Some(Duration::from(
                        value.completion_retention_time.unwrap_or_default(),
                    )),
                }
            }
        }

        impl TryFrom<entry::CallOrSendCommandMetadata> for journal_v2::raw::CallOrSendMetadata {
            type Error = ConversionError;

            fn try_from(value: entry::CallOrSendCommandMetadata) -> Result<Self, Self::Error> {
                let invocation_id = restate_types::identifiers::InvocationId::try_from(
                    value
                        .invocation_id
                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                )?;

                let invocation_target = restate_types::invocation::InvocationTarget::try_from(
                    value
                        .invocation_target
                        .ok_or(ConversionError::missing_field("invocation_target"))?,
                )?;
                let span_context =
                    restate_types::invocation::ServiceInvocationSpanContext::try_from(
                        value
                            .span_context
                            .ok_or(ConversionError::missing_field("span_context"))?,
                    )?;

                let completion_retention_duration = std::time::Duration::try_from(
                    value.completion_retention_duration.unwrap_or_default(),
                )?;

                Ok(journal_v2::raw::CallOrSendMetadata {
                    invocation_id,
                    span_context,
                    invocation_target,
                    completion_retention_duration,
                })
            }
        }

        impl From<journal_v2::raw::CallOrSendMetadata> for entry::CallOrSendCommandMetadata {
            fn from(value: journal_v2::raw::CallOrSendMetadata) -> Self {
                entry::CallOrSendCommandMetadata {
                    invocation_id: Some(InvocationId::from(value.invocation_id)),
                    invocation_target: Some(value.invocation_target.into()),
                    span_context: Some(SpanContext::from(value.span_context)),
                    completion_retention_duration: Some(Duration::from(
                        value.completion_retention_duration,
                    )),
                }
            }
        }

        impl TryFrom<EntryType> for journal_v2::EntryType {
            type Error = ConversionError;

            fn try_from(value: EntryType) -> Result<Self, Self::Error> {
                Ok(match value {
                    EntryType::Unknown => {
                        return Err(ConversionError::unexpected_enum_variant(
                            "ty",
                            EntryType::Unknown,
                        ))
                    }
                    EntryType::Event => journal_v2::EntryType::Event,
                    EntryType::InputCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::Input)
                    }
                    EntryType::OutputCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::Output)
                    }
                    EntryType::RunCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::Run)
                    }
                    EntryType::CallCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::Call)
                    }
                    EntryType::OneWayCallCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::OneWayCall)
                    }
                    EntryType::SleepCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::Sleep)
                    }
                    EntryType::GetLazyStateCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::GetLazyState)
                    }
                    EntryType::SetStateCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::SetState)
                    }
                    EntryType::ClearStateCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::ClearState)
                    }
                    EntryType::ClearAllStateCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::ClearAllState)
                    }
                    EntryType::GetLazyStateKeysCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::GetLazyStateKeys)
                    }
                    EntryType::GetEagerStateCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::GetEagerState)
                    }
                    EntryType::GetEagerStateKeysCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::GetEagerStateKeys)
                    }
                    EntryType::GetPromiseCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::GetPromise)
                    }
                    EntryType::PeekPromiseCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::PeekPromise)
                    }
                    EntryType::CompletePromiseCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::CompletePromise)
                    }
                    EntryType::SendSignalCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::SendSignal)
                    }
                    EntryType::AttachInvocationCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::AttachInvocation)
                    }
                    EntryType::GetInvocationOutputCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::GetInvocationOutput)
                    }
                    EntryType::CompleteAwakeableCommand => {
                        journal_v2::EntryType::Command(journal_v2::CommandType::CompleteAwakeable)
                    }
                    EntryType::Signal => {
                        journal_v2::EntryType::Notification(journal_v2::NotificationType::Signal)
                    }
                    EntryType::GetLazyStateCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::GetLazyState,
                        ),
                    ),
                    EntryType::GetLazyStateKeysCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::GetLazyStateKeys,
                        ),
                    ),
                    EntryType::GetPromiseCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::GetPromise,
                        ),
                    ),
                    EntryType::PeekPromiseCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::PeekPromise,
                        ),
                    ),
                    EntryType::CompletePromiseCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::CompletePromise,
                        ),
                    ),
                    EntryType::SleepCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(journal_v2::CompletionType::Sleep),
                    ),
                    EntryType::CallInvocationIdCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::CallInvocationId,
                        ),
                    ),
                    EntryType::CallCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(journal_v2::CompletionType::Call),
                    ),
                    EntryType::RunCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(journal_v2::CompletionType::Run),
                    ),
                    EntryType::AttachInvocationCompletion => journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::AttachInvocation,
                        ),
                    ),
                    EntryType::GetInvocationOutputCompletion => {
                        journal_v2::EntryType::Notification(
                            journal_v2::NotificationType::Completion(
                                journal_v2::CompletionType::GetInvocationOutput,
                            ),
                        )
                    }
                })
            }
        }

        impl From<journal_v2::EntryType> for EntryType {
            fn from(value: journal_v2::EntryType) -> Self {
                match value {
                    journal_v2::EntryType::Command(journal_v2::CommandType::Input) => {
                        EntryType::InputCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::Run) => {
                        EntryType::RunCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::Call) => {
                        EntryType::CallCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::OneWayCall) => {
                        EntryType::OneWayCallCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::Sleep) => {
                        EntryType::SleepCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::Output) => {
                        EntryType::OutputCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::GetLazyState) => {
                        EntryType::GetLazyStateCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::SetState) => {
                        EntryType::SetStateCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::ClearState) => {
                        EntryType::ClearStateCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::ClearAllState) => {
                        EntryType::ClearAllStateCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::GetLazyStateKeys) => {
                        EntryType::GetLazyStateCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::GetEagerState) => {
                        EntryType::GetEagerStateCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::GetEagerStateKeys) => {
                        EntryType::GetEagerStateKeysCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::GetPromise) => {
                        EntryType::GetPromiseCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::PeekPromise) => {
                        EntryType::PeekPromiseCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::CompletePromise) => {
                        EntryType::CompletePromiseCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::SendSignal) => {
                        EntryType::SendSignalCommand
                    }
                    journal_v2::EntryType::Command(journal_v2::CommandType::AttachInvocation) => {
                        EntryType::AttachInvocationCommand
                    }
                    journal_v2::EntryType::Command(
                        journal_v2::CommandType::GetInvocationOutput,
                    ) => EntryType::GetInvocationOutputCommand,
                    journal_v2::EntryType::Command(journal_v2::CommandType::CompleteAwakeable) => {
                        EntryType::CompleteAwakeableCommand
                    }
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::GetLazyState,
                        ),
                    ) => EntryType::GetLazyStateCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::GetLazyStateKeys,
                        ),
                    ) => EntryType::GetLazyStateKeysCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::GetPromise,
                        ),
                    ) => EntryType::GetPromiseCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::PeekPromise,
                        ),
                    ) => EntryType::PeekPromiseCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::CompletePromise,
                        ),
                    ) => EntryType::CompletePromiseCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(journal_v2::CompletionType::Sleep),
                    ) => EntryType::SleepCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::CallInvocationId,
                        ),
                    ) => EntryType::CallInvocationIdCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(journal_v2::CompletionType::Call),
                    ) => EntryType::CallCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(journal_v2::CompletionType::Run),
                    ) => EntryType::RunCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::AttachInvocation,
                        ),
                    ) => EntryType::AttachInvocationCompletion,
                    journal_v2::EntryType::Notification(
                        journal_v2::NotificationType::Completion(
                            journal_v2::CompletionType::GetInvocationOutput,
                        ),
                    ) => EntryType::GetInvocationOutputCompletion,
                    journal_v2::EntryType::Notification(journal_v2::NotificationType::Signal) => {
                        EntryType::Signal
                    }
                    journal_v2::EntryType::Event => EntryType::Event,
                }
            }
        }

        impl TryFrom<Entry> for crate::journal_table_v2::StoredEntry {
            type Error = ConversionError;

            fn try_from(value: Entry) -> Result<Self, Self::Error> {
                let header = journal_v2::raw::RawEntryHeader {
                    append_time: value.append_time.into(),
                };

                Ok(crate::journal_table_v2::StoredEntry(
                    match EntryType::try_from(value.ty)
                        .map_err(|e| ConversionError::unexpected_enum_variant("ty", e.0))?
                        .try_into()?
                    {
                        journal_v2::EntryType::Event => {
                            let event_entry = EventEntry::decode(value.content)
                                .map_err(|e| ConversionError::InvalidData(e.into()))?;
                            journal_v2::raw::RawEntry::new(
                                header,
                                journal_v2::Event {
                                    ty: event_entry
                                        .ty
                                        .parse::<journal_v2::EventType>()
                                        .map_err(|e| ConversionError::InvalidData(e.into()))?,
                                    metadata: event_entry
                                        .metadata
                                        .into_iter()
                                        .map(|(k, v)| (k, v.into()))
                                        .collect(),
                                },
                            )
                        }
                        journal_v2::EntryType::Notification(notification_ty) => {
                            let notification_id = match value
                                .notification_id
                                .ok_or(ConversionError::missing_field("notification_id"))?
                            {
                                entry::NotificationId::CompletionIdx(c) => {
                                    journal_v2::NotificationId::CompletionId(c)
                                }
                                entry::NotificationId::SignalIdx(c) => {
                                    journal_v2::NotificationId::SignalIndex(c)
                                }
                                entry::NotificationId::SignalName(s) => {
                                    journal_v2::NotificationId::SignalName(s.into())
                                }
                            };

                            journal_v2::raw::RawEntry::new(
                                header,
                                journal_v2::raw::RawNotification::new(
                                    notification_ty,
                                    notification_id,
                                    value.content,
                                ),
                            )
                        }
                        journal_v2::EntryType::Command(ct @ journal_v2::CommandType::Call)
                        | journal_v2::EntryType::Command(
                            ct @ journal_v2::CommandType::OneWayCall,
                        ) => journal_v2::raw::RawEntry::new(
                            header,
                            journal_v2::raw::RawCommand::new(ct, value.content)
                                .with_command_specific_metadata(
                                    journal_v2::raw::RawCommandSpecificMetadata::CallOrSend(
                                        journal_v2::raw::CallOrSendMetadata::try_from(
                                            value
                                                .call_or_send_command_metadata
                                                .ok_or(ConversionError::missing_field(
                                                "call_command_journal_entry_additional_metadata",
                                            ))?,
                                        )?,
                                    ),
                                ),
                        ),
                        journal_v2::EntryType::Command(ct) => journal_v2::raw::RawEntry::new(
                            header,
                            journal_v2::raw::RawCommand::new(ct, value.content),
                        ),
                    },
                ))
            }
        }

        impl From<crate::journal_table_v2::StoredEntry> for Entry {
            fn from(
                crate::journal_table_v2::StoredEntry(raw_entry): crate::journal_table_v2::StoredEntry,
            ) -> Self {
                let ty = EntryType::from(raw_entry.ty());
                let append_time = raw_entry.header().append_time.into();

                let mut call_or_send_command_metadata: Option<entry::CallOrSendCommandMetadata> =
                    None;
                let mut notification_id: Option<entry::NotificationId> = None;
                let content = match raw_entry.inner {
                    journal_v2::raw::RawEntryInner::Command(cmd) => {
                        match cmd.command_specific_metadata() {
                            journal_v2::raw::RawCommandSpecificMetadata::CallOrSend(
                                call_or_send_metadata,
                            ) => {
                                call_or_send_command_metadata =
                                    Some(call_or_send_metadata.clone().into())
                            }
                            journal_v2::raw::RawCommandSpecificMetadata::None => {}
                        };

                        cmd.serialized_content()
                    }
                    journal_v2::raw::RawEntryInner::Notification(notification) => {
                        notification_id = Some(match notification.id() {
                            journal_v2::NotificationId::CompletionId(c) => {
                                entry::NotificationId::CompletionIdx(c)
                            }
                            journal_v2::NotificationId::SignalIndex(c) => {
                                entry::NotificationId::SignalIdx(c)
                            }
                            journal_v2::NotificationId::SignalName(s) => {
                                entry::NotificationId::SignalName(s.into())
                            }
                        });

                        notification.serialized_content()
                    }
                    journal_v2::raw::RawEntryInner::Event(event) => EventEntry {
                        ty: event.ty.to_string(),
                        metadata: event
                            .metadata
                            .into_iter()
                            .map(|(k, v)| (k, v.to_string()))
                            .collect(),
                    }
                    .encode_to_vec()
                    .into(),
                };

                Entry {
                    ty: ty.into(),
                    content,
                    append_time,
                    call_or_send_command_metadata,
                    notification_id,
                }
            }
        }

        impl TryFrom<OutboxMessage> for restate_storage_api::outbox_table::OutboxMessage {
            type Error = ConversionError;

            fn try_from(value: OutboxMessage) -> Result<Self, ConversionError> {
                let result = match value
                    .outbox_message
                    .ok_or(ConversionError::missing_field("outbox_message"))?
                {
                    outbox_message::OutboxMessage::ServiceInvocationCase(service_invocation) => {
                        restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(
                            restate_types::invocation::ServiceInvocation::try_from(
                                service_invocation
                                    .service_invocation
                                    .ok_or(ConversionError::missing_field("service_invocation"))?,
                            )?,
                        )
                    }
                    outbox_message::OutboxMessage::ServiceInvocationResponse(
                        invocation_response,
                    ) => restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(
                        restate_types::invocation::InvocationResponse {
                            entry_index: invocation_response.entry_index,
                            id: restate_types::identifiers::InvocationId::try_from(
                                invocation_response
                                    .invocation_id
                                    .ok_or(ConversionError::missing_field("invocation_id"))?,
                            )?,
                            result: restate_types::invocation::ResponseResult::try_from(
                                invocation_response
                                    .response_result
                                    .ok_or(ConversionError::missing_field("response_result"))?,
                            )?,
                        },
                    ),
                    outbox_message::OutboxMessage::Kill(outbox_kill) => {
                        restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(
                            InvocationTermination::kill(
                                restate_types::identifiers::InvocationId::try_from(
                                    outbox_kill
                                        .invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                            ),
                        )
                    }
                    outbox_message::OutboxMessage::Cancel(outbox_cancel) => {
                        restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(
                            InvocationTermination::cancel(
                                restate_types::identifiers::InvocationId::try_from(
                                    outbox_cancel
                                        .invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                            ),
                        )
                    }
                    outbox_message::OutboxMessage::AttachInvocationRequest(
                        outbox_message::AttachInvocationRequest {
                            block_on_inflight,
                            response_sink,
                            query,
                        },
                    ) => restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(
                        restate_types::invocation::AttachInvocationRequest {
                            invocation_query: match query
                                .ok_or(ConversionError::missing_field("query"))?
                            {
                                outbox_message::attach_invocation_request::Query::InvocationId(
                                    id,
                                ) => restate_types::invocation::InvocationQuery::Invocation(
                                    id.try_into()?,
                                ),
                                outbox_message::attach_invocation_request::Query::IdempotencyId(
                                    id,
                                ) => restate_types::invocation::InvocationQuery::IdempotencyId(
                                    id.try_into()?,
                                ),
                                outbox_message::attach_invocation_request::Query::WorkflowId(
                                    id,
                                ) => restate_types::invocation::InvocationQuery::Workflow(
                                    id.try_into()?,
                                ),
                            },
                            block_on_inflight,
                            response_sink: Option::<
                                restate_types::invocation::ServiceInvocationResponseSink,
                            >::try_from(
                                response_sink
                                    .ok_or(ConversionError::missing_field("response_sink"))?,
                            )
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))??,
                        },
                    ),
                    outbox_message::OutboxMessage::NotifySignal(NotifySignal {
                        invocation_id,
                        signal_id,
                        result,
                    }) => restate_storage_api::outbox_table::OutboxMessage::NotifySignal(
                        restate_types::invocation::NotifySignalRequest {
                            invocation_id: restate_types::identifiers::InvocationId::try_from(
                                expect_or_fail!(invocation_id)?,
                            )?,
                            signal: journal_v2::Signal::new(
                                match expect_or_fail!(signal_id)? {
                                    outbox_message::notify_signal::SignalId::Idx(idx) => {
                                        journal_v2::SignalId::Index(idx)
                                    }
                                    outbox_message::notify_signal::SignalId::Name(name) => {
                                        journal_v2::SignalId::Name(name.into())
                                    }
                                },
                                match expect_or_fail!(result)? {
                                    outbox_message::notify_signal::Result::None(_) => {
                                        journal_v2::SignalResult::Void
                                    }
                                    outbox_message::notify_signal::Result::Success(b) => {
                                        journal_v2::SignalResult::Success(b)
                                    }
                                    outbox_message::notify_signal::Result::Failure(
                                        outbox_message::notify_signal::Failure {
                                            error_code,
                                            message,
                                        },
                                    ) => journal_v2::SignalResult::Failure(journal_v2::Failure {
                                        code: error_code.into(),
                                        message: message.into(),
                                    }),
                                },
                            ),
                        },
                    ),
                };

                Ok(result)
            }
        }

        impl From<restate_storage_api::outbox_table::OutboxMessage> for OutboxMessage {
            fn from(value: restate_storage_api::outbox_table::OutboxMessage) -> Self {
                let outbox_message = match value {
                    restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(
                        service_invocation,
                    ) => outbox_message::OutboxMessage::ServiceInvocationCase(
                        OutboxServiceInvocation {
                            service_invocation: Some(ServiceInvocation::from(service_invocation)),
                        },
                    ),
                    restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(
                        invocation_response,
                    ) => outbox_message::OutboxMessage::ServiceInvocationResponse(
                        OutboxServiceInvocationResponse {
                            entry_index: invocation_response.entry_index,
                            invocation_id: Some(InvocationId::from(invocation_response.id)),
                            response_result: Some(ResponseResult::from(invocation_response.result)),
                        },
                    ),
                    restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(
                        invocation_termination,
                    ) => match invocation_termination.flavor {
                        TerminationFlavor::Kill => {
                            outbox_message::OutboxMessage::Kill(OutboxKill {
                                invocation_id: Some(InvocationId::from(
                                    invocation_termination.invocation_id,
                                )),
                            })
                        }
                        TerminationFlavor::Cancel => {
                            outbox_message::OutboxMessage::Cancel(OutboxCancel {
                                invocation_id: Some(InvocationId::from(
                                    invocation_termination.invocation_id,
                                )),
                            })
                        }
                    },
                    restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(
                        restate_types::invocation::AttachInvocationRequest {
                            invocation_query,
                            block_on_inflight,
                            response_sink,
                        },
                    ) => outbox_message::OutboxMessage::AttachInvocationRequest(
                        outbox_message::AttachInvocationRequest {
                            block_on_inflight,
                            query: Some(match invocation_query {
                                restate_types::invocation::InvocationQuery::Invocation(id) => {
                                    outbox_message::attach_invocation_request::Query::InvocationId(
                                        id.into(),
                                    )
                                }
                                restate_types::invocation::InvocationQuery::IdempotencyId(id) => {
                                    outbox_message::attach_invocation_request::Query::IdempotencyId(
                                        id.into(),
                                    )
                                }
                                restate_types::invocation::InvocationQuery::Workflow(id) => {
                                    outbox_message::attach_invocation_request::Query::WorkflowId(
                                        id.into(),
                                    )
                                }
                            }),
                            response_sink: Some(Some(response_sink).into()),
                        },
                    ),
                    restate_storage_api::outbox_table::OutboxMessage::NotifySignal(
                        notify_signal,
                    ) => {
                        outbox_message::OutboxMessage::NotifySignal(outbox_message::NotifySignal {
                            invocation_id: Some(InvocationId::from(notify_signal.invocation_id)),
                            signal_id: Some(match notify_signal.signal.id {
                                journal_v2::SignalId::Index(idx) => {
                                    outbox_message::notify_signal::SignalId::Idx(idx)
                                }
                                journal_v2::SignalId::Name(name) => {
                                    outbox_message::notify_signal::SignalId::Name(name.to_string())
                                }
                            }),
                            result: Some(match notify_signal.signal.result {
                                journal_v2::SignalResult::Void => {
                                    outbox_message::notify_signal::Result::None(())
                                }
                                journal_v2::SignalResult::Success(b) => {
                                    outbox_message::notify_signal::Result::Success(b)
                                }
                                journal_v2::SignalResult::Failure(f) => {
                                    outbox_message::notify_signal::Result::Failure(
                                        outbox_message::notify_signal::Failure {
                                            error_code: f.code.into(),
                                            message: f.message.into(),
                                        },
                                    )
                                }
                            }),
                        })
                    }
                };

                OutboxMessage {
                    outbox_message: Some(outbox_message),
                }
            }
        }

        impl TryFrom<ResponseResult> for restate_types::invocation::ResponseResult {
            type Error = ConversionError;

            fn try_from(value: ResponseResult) -> Result<Self, ConversionError> {
                let result = match value
                    .response_result
                    .ok_or(ConversionError::missing_field("response_result"))?
                {
                    response_result::ResponseResult::ResponseSuccess(success) => {
                        restate_types::invocation::ResponseResult::Success(success.value)
                    }
                    response_result::ResponseResult::ResponseFailure(failure) => {
                        restate_types::invocation::ResponseResult::Failure(InvocationError::new(
                            failure.failure_code,
                            ByteString::try_from(failure.failure_message)
                                .map_err(ConversionError::invalid_data)?,
                        ))
                    }
                };

                Ok(result)
            }
        }

        impl From<restate_types::invocation::ResponseResult> for ResponseResult {
            fn from(value: restate_types::invocation::ResponseResult) -> Self {
                let response_result = match value {
                    restate_types::invocation::ResponseResult::Success(value) => {
                        response_result::ResponseResult::ResponseSuccess(
                            response_result::ResponseSuccess { value },
                        )
                    }
                    restate_types::invocation::ResponseResult::Failure(err) => {
                        response_result::ResponseResult::ResponseFailure(
                            response_result::ResponseFailure {
                                failure_code: err.code().into(),
                                failure_message: Bytes::copy_from_slice(err.message().as_ref()),
                            },
                        )
                    }
                };

                ResponseResult {
                    response_result: Some(response_result),
                }
            }
        }

        impl TryFrom<Timer> for restate_storage_api::timer_table::Timer {
            type Error = ConversionError;

            fn try_from(value: Timer) -> Result<Self, ConversionError> {
                Ok(
                    match value.value.ok_or(ConversionError::missing_field("value"))? {
                        timer::Value::CompleteSleepEntry(cse) => {
                            restate_storage_api::timer_table::Timer::CompleteJournalEntry(
                                restate_types::identifiers::InvocationId::try_from(
                                    cse.invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                                cse.entry_index,
                            )
                        }
                        timer::Value::Invoke(si) => {
                            restate_storage_api::timer_table::Timer::Invoke(
                                restate_types::invocation::ServiceInvocation::try_from(si)?,
                            )
                        }
                        timer::Value::ScheduledInvoke(id) => {
                            restate_storage_api::timer_table::Timer::NeoInvoke(
                                restate_types::identifiers::InvocationId::try_from(id)?,
                            )
                        }
                        timer::Value::CleanInvocationStatus(clean_invocation_status) => {
                            restate_storage_api::timer_table::Timer::CleanInvocationStatus(
                                restate_types::identifiers::InvocationId::try_from(
                                    clean_invocation_status
                                        .invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                            )
                        }
                    },
                )
            }
        }

        impl From<restate_storage_api::timer_table::Timer> for Timer {
            fn from(value: restate_storage_api::timer_table::Timer) -> Self {
                Timer {
                    value: Some(match value {
                        restate_storage_api::timer_table::Timer::CompleteJournalEntry(
                            invocation_id,
                            entry_index,
                        ) => timer::Value::CompleteSleepEntry(timer::CompleteSleepEntry {
                            invocation_id: Some(InvocationId::from(invocation_id)),
                            entry_index,
                        }),
                        restate_storage_api::timer_table::Timer::NeoInvoke(invocation_id) => {
                            timer::Value::ScheduledInvoke(InvocationId::from(invocation_id))
                        }
                        restate_storage_api::timer_table::Timer::Invoke(si) => {
                            timer::Value::Invoke(ServiceInvocation::from(si))
                        }
                        restate_storage_api::timer_table::Timer::CleanInvocationStatus(
                            invocation_id,
                        ) => timer::Value::CleanInvocationStatus(timer::CleanInvocationStatus {
                            invocation_id: Some(InvocationId::from(invocation_id)),
                        }),
                    }),
                }
            }
        }

        impl From<restate_storage_api::deduplication_table::DedupSequenceNumber> for DedupSequenceNumber {
            fn from(value: restate_storage_api::deduplication_table::DedupSequenceNumber) -> Self {
                match value {
                    restate_storage_api::deduplication_table::DedupSequenceNumber::Sn(sn) => {
                        DedupSequenceNumber {
                            variant: Some(Variant::SequenceNumber(sn)),
                        }
                    }
                    restate_storage_api::deduplication_table::DedupSequenceNumber::Esn(esn) => {
                        DedupSequenceNumber {
                            variant: Some(Variant::EpochSequenceNumber(EpochSequenceNumber::from(
                                esn,
                            ))),
                        }
                    }
                }
            }
        }

        impl TryFrom<DedupSequenceNumber>
            for restate_storage_api::deduplication_table::DedupSequenceNumber
        {
            type Error = ConversionError;

            fn try_from(value: DedupSequenceNumber) -> Result<Self, ConversionError> {
                Ok(
                    match value
                        .variant
                        .ok_or(ConversionError::missing_field("variant"))?
                    {
                        Variant::SequenceNumber(sn) => {
                            restate_storage_api::deduplication_table::DedupSequenceNumber::Sn(sn)
                        }
                        Variant::EpochSequenceNumber(esn) => {
                            restate_storage_api::deduplication_table::DedupSequenceNumber::Esn(
                                restate_storage_api::deduplication_table::EpochSequenceNumber::try_from(esn)?,
                            )
                        }
                    },
                )
            }
        }

        impl From<restate_storage_api::deduplication_table::EpochSequenceNumber> for EpochSequenceNumber {
            fn from(value: restate_storage_api::deduplication_table::EpochSequenceNumber) -> Self {
                EpochSequenceNumber {
                    leader_epoch: value.leader_epoch.into(),
                    sequence_number: value.sequence_number,
                }
            }
        }

        impl TryFrom<EpochSequenceNumber>
            for restate_storage_api::deduplication_table::EpochSequenceNumber
        {
            type Error = ConversionError;

            fn try_from(value: EpochSequenceNumber) -> Result<Self, ConversionError> {
                Ok(
                    restate_storage_api::deduplication_table::EpochSequenceNumber {
                        leader_epoch: value.leader_epoch.into(),
                        sequence_number: value.sequence_number,
                    },
                )
            }
        }

        impl From<std::time::Duration> for Duration {
            fn from(value: std::time::Duration) -> Self {
                Duration {
                    secs: value.as_secs(),
                    nanos: value.subsec_nanos(),
                }
            }
        }

        impl TryFrom<Duration> for std::time::Duration {
            type Error = ConversionError;

            fn try_from(value: Duration) -> Result<Self, ConversionError> {
                Ok(std::time::Duration::new(value.secs, value.nanos))
            }
        }

        impl From<restate_storage_api::idempotency_table::IdempotencyMetadata> for IdempotencyMetadata {
            fn from(value: restate_storage_api::idempotency_table::IdempotencyMetadata) -> Self {
                IdempotencyMetadata {
                    invocation_id: Some(InvocationId::from(value.invocation_id)),
                }
            }
        }

        impl TryFrom<IdempotencyMetadata> for restate_storage_api::idempotency_table::IdempotencyMetadata {
            type Error = ConversionError;

            fn try_from(value: IdempotencyMetadata) -> Result<Self, ConversionError> {
                Ok(
                    restate_storage_api::idempotency_table::IdempotencyMetadata {
                        invocation_id: restate_types::identifiers::InvocationId::try_from(
                            value
                                .invocation_id
                                .ok_or(ConversionError::missing_field("invocation_id"))?,
                        )
                        .map_err(|e| ConversionError::invalid_data(e))?,
                    },
                )
            }
        }

        impl From<restate_storage_api::promise_table::Promise> for Promise {
            fn from(value: restate_storage_api::promise_table::Promise) -> Self {
                match value.state {
                    restate_storage_api::promise_table::PromiseState::Completed(e) => Promise {
                        state: Some(promise::State::CompletedState(promise::CompletedState {
                            result: Some(e.into()),
                        })),
                    },
                    restate_storage_api::promise_table::PromiseState::NotCompleted(listeners) => {
                        Promise {
                            state: Some(promise::State::NotCompletedState(
                                promise::NotCompletedState {
                                    listening_journal_entries: listeners
                                        .into_iter()
                                        .map(Into::into)
                                        .collect(),
                                },
                            )),
                        }
                    }
                }
            }
        }

        impl TryFrom<Promise> for restate_storage_api::promise_table::Promise {
            type Error = ConversionError;

            fn try_from(value: Promise) -> Result<Self, ConversionError> {
                Ok(restate_storage_api::promise_table::Promise {
                    state: match value.state.ok_or(ConversionError::missing_field("state"))? {
                        promise::State::CompletedState(s) => {
                            restate_storage_api::promise_table::PromiseState::Completed(
                                s.result
                                    .ok_or(ConversionError::missing_field("result"))?
                                    .try_into()?,
                            )
                        }
                        promise::State::NotCompletedState(s) => {
                            restate_storage_api::promise_table::PromiseState::NotCompleted(
                                s.listening_journal_entries
                                    .into_iter()
                                    .map(TryInto::try_into)
                                    .collect::<Result<Vec<_>, _>>()?,
                            )
                        }
                    },
                })
            }
        }

        impl From<crate::fsm_table::SequenceNumber> for SequenceNumber {
            fn from(value: crate::fsm_table::SequenceNumber) -> Self {
                SequenceNumber {
                    sequence_number: value.into(),
                }
            }
        }

        impl From<SequenceNumber> for crate::fsm_table::SequenceNumber {
            fn from(value: SequenceNumber) -> Self {
                Self::from(value.sequence_number)
            }
        }

        impl From<crate::journal_table_v2::JournalEntryIndex> for JournalEntryIndex {
            fn from(value: crate::journal_table_v2::JournalEntryIndex) -> Self {
                Self {
                    entry_index: value.into(),
                }
            }
        }

        impl From<JournalEntryIndex> for crate::journal_table_v2::JournalEntryIndex {
            fn from(value: JournalEntryIndex) -> Self {
                Self::from(value.entry_index)
            }
        }
    }
}
