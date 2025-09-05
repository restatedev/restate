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

use restate_types::SemanticRestateVersion;
use restate_types::errors::IdDecodeError;
use restate_types::storage::{StorageCodec, StorageDecode, StorageDecodeError, StorageEncode};

use crate::StorageError;

/// Marker trait to specify the Protobuf equivalent of a user facing type
pub trait PartitionStoreProtobufValue: Sized {
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

impl PartitionStoreProtobufValue for SemanticRestateVersion {
    type ProtobufType = crate::protobuf_types::v1::RestateVersion;
}

pub struct ProtobufStorageWrapper<T>(pub T);

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

impl From<ConversionError> for StorageDecodeError {
    fn from(value: ConversionError) -> Self {
        StorageDecodeError::DecodeValue(value.into())
    }
}

pub mod v1 {
    #![allow(clippy::large_enum_variant)]

    include!(concat!(
        env!("OUT_DIR"),
        "/dev.restate.storage.domain.v1.rs"
    ));

    pub mod pb_conversion {
        use std::collections::HashSet;

        use anyhow::anyhow;
        use bytes::{Buf, Bytes};
        use bytestring::ByteString;

        use restate_types::deployment::PinnedDeployment;
        use restate_types::errors::InvocationError;
        use restate_types::identifiers::{
            PartitionProcessorRpcRequestId, WithInvocationId, WithPartitionKey,
        };
        use restate_types::invocation::{InvocationTermination, TerminationFlavor};
        use restate_types::journal::enriched::AwakeableEnrichmentResult;
        use restate_types::journal_v2::{EntryMetadata, NotificationId};
        use restate_types::logs::Lsn;
        use restate_types::service_protocol::ServiceProtocolVersion;
        use restate_types::time::MillisSinceEpoch;
        use restate_types::{GenerationalNodeId, journal_events, journal_v2};

        use super::dedup_sequence_number::Variant;
        use super::enriched_entry_header::{
            AttachInvocation, Awakeable, BackgroundCall, CancelInvocation, ClearAllState,
            ClearState, CompleteAwakeable, CompletePromise, Custom, GetCallInvocationId,
            GetInvocationOutput, GetPromise, GetState, GetStateKeys, Input, Invoke, Output,
            PeekPromise, SetState, SideEffect, Sleep,
        };
        use super::entry::EntryType;
        use super::invocation_status::{Completed, Inboxed, Invoked, Suspended};
        use super::invocation_status_v2::JournalTrimPoint;
        use super::journal_entry::completion_result::{Empty, Failure, Success};
        use super::journal_entry::{CompletionResult, Kind, completion_result};
        use super::outbox_message::{
            OutboxCancel, OutboxKill, OutboxServiceInvocation, OutboxServiceInvocationResponse,
        };
        use super::service_invocation_response_sink::{Ingress, PartitionProcessor, ResponseSink};
        use super::{
            BackgroundCallResolutionResult, DedupSequenceNumber, Duration, EnrichedEntryHeader,
            Entry, EntryResult, EpochSequenceNumber, Header, IdempotencyId, IdempotencyMetadata,
            InboxEntry, InvocationId, InvocationResolutionResult, InvocationStatus,
            InvocationStatusV2, InvocationTarget, InvocationV2Lite, JournalCompletionTarget,
            JournalEntry, JournalEntryIndex, JournalMeta, KvPair, OutboxMessage,
            PartitionDurability, Promise, ResponseResult, RestateVersion, SequenceNumber,
            ServiceId, ServiceInvocation, ServiceInvocationResponseSink, Source, SpanContext,
            SpanRelation, StateMutation, SubmitNotificationSink, Timer, VirtualObjectStatus,
            enriched_entry_header, entry, entry_result, inbox_entry, invocation_resolution_result,
            invocation_status, invocation_status_v2, invocation_target, journal_entry,
            outbox_message, promise, response_result, source, span_relation,
            submit_notification_sink, timer, virtual_object_status,
        };
        use super::{Event, event};
        use crate::invocation_status_table::{CompletionRangeEpochMap, JournalMetadata};
        use crate::protobuf_types::ConversionError;

        impl TryFrom<VirtualObjectStatus> for crate::service_status_table::VirtualObjectStatus {
            type Error = ConversionError;

            fn try_from(value: VirtualObjectStatus) -> Result<Self, ConversionError> {
                Ok(
                    match value
                        .status
                        .ok_or(ConversionError::missing_field("status"))?
                    {
                        virtual_object_status::Status::Locked(locked) => {
                            crate::service_status_table::VirtualObjectStatus::Locked(
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

        impl From<crate::service_status_table::VirtualObjectStatus> for VirtualObjectStatus {
            fn from(value: crate::service_status_table::VirtualObjectStatus) -> Self {
                match value {
                    crate::service_status_table::VirtualObjectStatus::Locked(invocation_id) => {
                        VirtualObjectStatus {
                            status: Some(virtual_object_status::Status::Locked(
                                virtual_object_status::Locked {
                                    invocation_id: Some(invocation_id.into()),
                                },
                            )),
                        }
                    }
                    crate::service_status_table::VirtualObjectStatus::Unlocked => {
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

        impl TryFrom<&InvocationId> for restate_types::identifiers::InvocationId {
            type Error = ConversionError;

            fn try_from(value: &InvocationId) -> Result<Self, ConversionError> {
                Ok(restate_types::identifiers::InvocationId::from_parts(
                    value.partition_key,
                    try_bytes_into_invocation_uuid(&value.invocation_uuid)?,
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

        impl From<restate_types::invocation::JournalCompletionTarget> for JournalCompletionTarget {
            fn from(value: restate_types::invocation::JournalCompletionTarget) -> Self {
                Self {
                    partition_key: value.partition_key(),
                    invocation_uuid: value
                        .invocation_id()
                        .invocation_uuid()
                        .to_bytes()
                        .to_vec()
                        .into(),
                    entry_index: value.caller_completion_id,
                    caller_invocation_epoch: value.caller_invocation_epoch,
                }
            }
        }

        impl TryFrom<JournalCompletionTarget> for restate_types::invocation::JournalCompletionTarget {
            type Error = ConversionError;

            fn try_from(value: JournalCompletionTarget) -> Result<Self, ConversionError> {
                Ok(
                    restate_types::invocation::JournalCompletionTarget::from_parts(
                        restate_types::identifiers::InvocationId::from_parts(
                            value.partition_key,
                            try_bytes_into_invocation_uuid(value.invocation_uuid)?,
                        ),
                        value.entry_index,
                        value.caller_invocation_epoch,
                    ),
                )
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

        impl From<crate::promise_table::PromiseResult> for EntryResult {
            fn from(value: crate::promise_table::PromiseResult) -> Self {
                match value {
                    crate::promise_table::PromiseResult::Success(s) => EntryResult {
                        result: Some(entry_result::Result::Value(s)),
                    },
                    crate::promise_table::PromiseResult::Failure(code, message) => EntryResult {
                        result: Some(entry_result::Result::Failure(entry_result::Failure {
                            error_code: code.into(),
                            message: message.into_bytes(),
                        })),
                    },
                }
            }
        }

        impl TryFrom<EntryResult> for crate::promise_table::PromiseResult {
            type Error = ConversionError;

            fn try_from(value: EntryResult) -> Result<Self, ConversionError> {
                Ok(
                    match value
                        .result
                        .ok_or(ConversionError::missing_field("result"))?
                    {
                        entry_result::Result::Value(s) => {
                            crate::promise_table::PromiseResult::Success(s)
                        }
                        entry_result::Result::Failure(entry_result::Failure {
                            error_code,
                            message,
                        }) => crate::promise_table::PromiseResult::Failure(
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
        pub(super) use expect_or_fail;

        impl TryFrom<InvocationStatusV2> for crate::invocation_status_table::InvocationStatus {
            type Error = ConversionError;

            fn try_from(value: InvocationStatusV2) -> Result<Self, ConversionError> {
                let InvocationStatusV2 {
                    status,
                    invocation_target,
                    source,
                    span_context,
                    creation_time,
                    created_using_restate_version,
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
                    journal_retention_duration,
                    idempotency_key,
                    inbox_sequence_number,
                    journal_length,
                    commands,
                    events,
                    deployment_id,
                    service_protocol_version,
                    current_invocation_epoch,
                    trim_points,
                    waiting_for_completions,
                    waiting_for_signal_indexes,
                    waiting_for_signal_names,
                    result,
                    hotfix_apply_cancellation_after_deployment_is_pinned,
                } = value;

                let invocation_target = expect_or_fail!(invocation_target)?.try_into()?;
                let created_using_restate_version =
                    restate_version_from_pb(created_using_restate_version);
                let timestamps = crate::invocation_status_table::StatusTimestamps::new(
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
                        Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(s)
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))?
                    })
                    .collect::<Result<HashSet<_>, _>>()?;
                let headers = headers
                    .into_iter()
                    .map(restate_types::invocation::Header::try_from)
                    .collect::<Result<Vec<_>, ConversionError>>()?;

                match status.try_into().unwrap_or_default() {
                    invocation_status_v2::Status::Scheduled => {
                        Ok(crate::invocation_status_table::InvocationStatus::Scheduled(
                            crate::invocation_status_table::ScheduledInvocation {
                                metadata:
                                    crate::invocation_status_table::PreFlightInvocationMetadata {
                                        response_sinks,
                                        timestamps,
                                        invocation_target,
                                        created_using_restate_version,
                                        argument: expect_or_fail!(argument)?,
                                        source,
                                        span_context: expect_or_fail!(span_context)?.try_into()?,
                                        headers,
                                        execution_time: execution_time.map(MillisSinceEpoch::new),
                                        completion_retention_duration:
                                            completion_retention_duration
                                                .unwrap_or_default()
                                                .try_into()?,
                                        journal_retention_duration: journal_retention_duration
                                            .unwrap_or_default()
                                            .try_into()?,
                                        idempotency_key: idempotency_key.map(ByteString::from),
                                    },
                            },
                        ))
                    }
                    invocation_status_v2::Status::Inboxed => {
                        Ok(crate::invocation_status_table::InvocationStatus::Inboxed(
                            crate::invocation_status_table::InboxedInvocation {
                                inbox_sequence_number: expect_or_fail!(inbox_sequence_number)?,
                                metadata:
                                    crate::invocation_status_table::PreFlightInvocationMetadata {
                                        response_sinks,
                                        timestamps,
                                        invocation_target,
                                        created_using_restate_version,
                                        argument: expect_or_fail!(argument)?,
                                        source,
                                        span_context: expect_or_fail!(span_context)?.try_into()?,
                                        headers,
                                        execution_time: execution_time.map(MillisSinceEpoch::new),
                                        completion_retention_duration:
                                            completion_retention_duration
                                                .unwrap_or_default()
                                                .try_into()?,
                                        journal_retention_duration: journal_retention_duration
                                            .unwrap_or_default()
                                            .try_into()?,
                                        idempotency_key: idempotency_key.map(ByteString::from),
                                    },
                            },
                        ))
                    }
                    invocation_status_v2::Status::Invoked => {
                        Ok(crate::invocation_status_table::InvocationStatus::Invoked(
                            crate::invocation_status_table::InFlightInvocationMetadata {
                                response_sinks,
                                timestamps,
                                invocation_target,
                                created_using_restate_version,
                                journal_metadata: crate::invocation_status_table::JournalMetadata {
                                    length: journal_length,
                                    commands,
                                    events,
                                    span_context: expect_or_fail!(span_context)?.try_into()?,
                                },
                                pinned_deployment: derive_pinned_deployment(
                                    deployment_id,
                                    service_protocol_version,
                                )?,
                                source,
                                execution_time: execution_time.map(MillisSinceEpoch::new),
                                completion_retention_duration: completion_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                journal_retention_duration: journal_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                idempotency_key: idempotency_key.map(ByteString::from),
                                hotfix_apply_cancellation_after_deployment_is_pinned,
                                current_invocation_epoch,
                                completion_range_epoch_map:
                                    CompletionRangeEpochMap::from_trim_points(
                                        trim_points.into_iter().map(|trim_point| {
                                            (trim_point.completion_id, trim_point.invocation_epoch)
                                        }),
                                    ),
                            },
                        ))
                    }
                    invocation_status_v2::Status::Suspended => Ok(
                        crate::invocation_status_table::InvocationStatus::Suspended {
                            metadata: crate::invocation_status_table::InFlightInvocationMetadata {
                                response_sinks,
                                timestamps,
                                invocation_target,
                                created_using_restate_version,
                                journal_metadata: crate::invocation_status_table::JournalMetadata {
                                    length: journal_length,
                                    commands,
                                    events,
                                    span_context: expect_or_fail!(span_context)?.try_into()?,
                                },
                                pinned_deployment: derive_pinned_deployment(
                                    deployment_id,
                                    service_protocol_version,
                                )?,
                                source,
                                execution_time: execution_time.map(MillisSinceEpoch::new),
                                completion_retention_duration: completion_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                journal_retention_duration: journal_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                idempotency_key: idempotency_key.map(ByteString::from),
                                hotfix_apply_cancellation_after_deployment_is_pinned,
                                current_invocation_epoch,
                                completion_range_epoch_map:
                                    CompletionRangeEpochMap::from_trim_points(
                                        trim_points.into_iter().map(|trim_point| {
                                            (trim_point.completion_id, trim_point.invocation_epoch)
                                        }),
                                    ),
                            },
                            waiting_for_notifications: waiting_for_completions
                                .into_iter()
                                .map(NotificationId::for_completion)
                                .chain(
                                    waiting_for_signal_indexes
                                        .into_iter()
                                        .map(journal_v2::SignalId::for_index)
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
                    invocation_status_v2::Status::Paused => {
                        Ok(crate::invocation_status_table::InvocationStatus::Paused(
                            crate::invocation_status_table::InFlightInvocationMetadata {
                                response_sinks,
                                timestamps,
                                invocation_target,
                                created_using_restate_version,
                                journal_metadata: crate::invocation_status_table::JournalMetadata {
                                    length: journal_length,
                                    commands,
                                    events,
                                    span_context: expect_or_fail!(span_context)?.try_into()?,
                                },
                                pinned_deployment: derive_pinned_deployment(
                                    deployment_id,
                                    service_protocol_version,
                                )?,
                                source,
                                execution_time: execution_time.map(MillisSinceEpoch::new),
                                completion_retention_duration: completion_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                journal_retention_duration: journal_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                idempotency_key: idempotency_key.map(ByteString::from),
                                hotfix_apply_cancellation_after_deployment_is_pinned,
                                current_invocation_epoch,
                                completion_range_epoch_map:
                                    CompletionRangeEpochMap::from_trim_points(
                                        trim_points.into_iter().map(|trim_point| {
                                            (trim_point.completion_id, trim_point.invocation_epoch)
                                        }),
                                    ),
                            },
                        ))
                    }
                    invocation_status_v2::Status::Completed => {
                        Ok(crate::invocation_status_table::InvocationStatus::Completed(
                            crate::invocation_status_table::CompletedInvocation {
                                timestamps,
                                invocation_target,
                                created_using_restate_version,
                                source,
                                execution_time: execution_time.map(MillisSinceEpoch::new),
                                idempotency_key: idempotency_key.map(ByteString::from),
                                response_result: expect_or_fail!(result)?.try_into()?,
                                completion_retention_duration: completion_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                journal_retention_duration: journal_retention_duration
                                    .unwrap_or_default()
                                    .try_into()?,
                                journal_metadata: crate::invocation_status_table::JournalMetadata {
                                    length: journal_length,
                                    commands,
                                    events,
                                    span_context: expect_or_fail!(span_context)?.try_into()?,
                                },
                                pinned_deployment: derive_pinned_deployment(
                                    deployment_id,
                                    service_protocol_version,
                                )?,
                            },
                        ))
                    }
                    invocation_status_v2::Status::UnknownStatus => Err(
                        ConversionError::unexpected_enum_variant("status", value.status),
                    ),
                }
            }
        }

        impl From<crate::invocation_status_table::InvocationStatus> for InvocationStatusV2 {
            fn from(value: crate::invocation_status_table::InvocationStatus) -> Self {
                match value {
                    crate::invocation_status_table::InvocationStatus::Scheduled(
                        crate::invocation_status_table::ScheduledInvocation {
                            metadata:
                                crate::invocation_status_table::PreFlightInvocationMetadata {
                                    response_sinks,
                                    timestamps,
                                    invocation_target,
                                    created_using_restate_version,
                                    argument,
                                    source,
                                    span_context,
                                    headers,
                                    execution_time,
                                    completion_retention_duration,
                                    journal_retention_duration,
                                    idempotency_key,
                                },
                        },
                    ) => InvocationStatusV2 {
                        status: invocation_status_v2::Status::Scheduled.into(),
                        invocation_target: Some(invocation_target.into()),
                        source: Some(source.into()),
                        span_context: Some(span_context.into()),
                        creation_time: timestamps.creation_time().as_u64(),
                        created_using_restate_version: created_using_restate_version.into_string(),
                        modification_time: timestamps.modification_time().as_u64(),
                        inboxed_transition_time: timestamps
                            .inboxed_transition_time()
                            .map(|t| t.as_u64()),
                        scheduled_transition_time: timestamps
                            .scheduled_transition_time()
                            .map(|t| t.as_u64()),
                        running_transition_time: timestamps
                            .running_transition_time()
                            .map(|t| t.as_u64()),
                        completed_transition_time: timestamps
                            .completed_transition_time()
                            .map(|t| t.as_u64()),
                        response_sinks: response_sinks
                            .into_iter()
                            .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                            .collect(),
                        argument: Some(argument),
                        headers: headers.into_iter().map(Into::into).collect(),
                        execution_time: execution_time.map(|t| t.as_u64()),
                        completion_retention_duration: Some(completion_retention_duration.into()),
                        journal_retention_duration: Some(journal_retention_duration.into()),
                        idempotency_key: idempotency_key.map(|key| key.to_string()),
                        inbox_sequence_number: None,
                        journal_length: 0,
                        commands: 0,
                        events: 0,
                        deployment_id: None,
                        service_protocol_version: None,
                        hotfix_apply_cancellation_after_deployment_is_pinned: false,
                        current_invocation_epoch: 0,
                        trim_points: vec![],
                        waiting_for_completions: vec![],
                        waiting_for_signal_indexes: vec![],
                        waiting_for_signal_names: vec![],
                        result: None,
                    },
                    crate::invocation_status_table::InvocationStatus::Inboxed(
                        crate::invocation_status_table::InboxedInvocation {
                            metadata:
                                crate::invocation_status_table::PreFlightInvocationMetadata {
                                    response_sinks,
                                    timestamps,
                                    invocation_target,
                                    created_using_restate_version,
                                    argument,
                                    source,
                                    span_context,
                                    headers,
                                    execution_time,
                                    completion_retention_duration,
                                    journal_retention_duration,
                                    idempotency_key,
                                },
                            inbox_sequence_number,
                        },
                    ) => InvocationStatusV2 {
                        status: invocation_status_v2::Status::Inboxed.into(),
                        invocation_target: Some(invocation_target.into()),
                        source: Some(source.into()),
                        span_context: Some(span_context.into()),
                        creation_time: timestamps.creation_time().as_u64(),
                        created_using_restate_version: created_using_restate_version.into_string(),
                        modification_time: timestamps.modification_time().as_u64(),
                        inboxed_transition_time: timestamps
                            .inboxed_transition_time()
                            .map(|t| t.as_u64()),
                        scheduled_transition_time: timestamps
                            .scheduled_transition_time()
                            .map(|t| t.as_u64()),
                        running_transition_time: timestamps
                            .running_transition_time()
                            .map(|t| t.as_u64()),
                        completed_transition_time: timestamps
                            .completed_transition_time()
                            .map(|t| t.as_u64()),
                        response_sinks: response_sinks
                            .into_iter()
                            .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                            .collect(),
                        argument: Some(argument),
                        headers: headers.into_iter().map(Into::into).collect(),
                        execution_time: execution_time.map(|t| t.as_u64()),
                        completion_retention_duration: Some(completion_retention_duration.into()),
                        journal_retention_duration: Some(journal_retention_duration.into()),
                        idempotency_key: idempotency_key.map(|key| key.to_string()),
                        inbox_sequence_number: Some(inbox_sequence_number),
                        journal_length: 0,
                        commands: 0,
                        events: 0,
                        deployment_id: None,
                        service_protocol_version: None,
                        hotfix_apply_cancellation_after_deployment_is_pinned: false,
                        current_invocation_epoch: 0,
                        trim_points: vec![],
                        waiting_for_completions: vec![],
                        waiting_for_signal_indexes: vec![],
                        waiting_for_signal_names: vec![],
                        result: None,
                    },
                    crate::invocation_status_table::InvocationStatus::Invoked(
                        crate::invocation_status_table::InFlightInvocationMetadata {
                            invocation_target,
                            created_using_restate_version,
                            journal_metadata,
                            pinned_deployment,
                            response_sinks,
                            timestamps,
                            source,
                            execution_time,
                            completion_retention_duration,
                            journal_retention_duration,
                            idempotency_key,
                            hotfix_apply_cancellation_after_deployment_is_pinned,
                            current_invocation_epoch,
                            completion_range_epoch_map,
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
                            creation_time: timestamps.creation_time().as_u64(),
                            created_using_restate_version: created_using_restate_version
                                .into_string(),
                            modification_time: timestamps.modification_time().as_u64(),
                            inboxed_transition_time: timestamps
                                .inboxed_transition_time()
                                .map(|t| t.as_u64()),
                            scheduled_transition_time: timestamps
                                .scheduled_transition_time()
                                .map(|t| t.as_u64()),
                            running_transition_time: timestamps
                                .running_transition_time()
                                .map(|t| t.as_u64()),
                            completed_transition_time: timestamps
                                .completed_transition_time()
                                .map(|t| t.as_u64()),
                            response_sinks: response_sinks
                                .into_iter()
                                .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                                .collect(),
                            argument: None,
                            headers: vec![],
                            execution_time: execution_time.map(|t| t.as_u64()),
                            completion_retention_duration: Some(
                                completion_retention_duration.into(),
                            ),
                            journal_retention_duration: Some(journal_retention_duration.into()),
                            idempotency_key: idempotency_key.map(|key| key.to_string()),
                            inbox_sequence_number: None,
                            journal_length: journal_metadata.length,
                            commands: journal_metadata.commands,
                            events: journal_metadata.events,
                            deployment_id,
                            service_protocol_version,
                            waiting_for_completions: vec![],
                            waiting_for_signal_indexes: vec![],
                            waiting_for_signal_names: vec![],
                            result: None,
                            hotfix_apply_cancellation_after_deployment_is_pinned,
                            current_invocation_epoch,
                            trim_points: completion_range_epoch_map
                                .into_trim_points_iter()
                                .map(|(completion_id, invocation_epoch)| JournalTrimPoint {
                                    completion_id,
                                    invocation_epoch,
                                })
                                .collect(),
                        }
                    }
                    crate::invocation_status_table::InvocationStatus::Suspended {
                        metadata:
                            crate::invocation_status_table::InFlightInvocationMetadata {
                                invocation_target,
                                created_using_restate_version,
                                journal_metadata,
                                pinned_deployment,
                                response_sinks,
                                timestamps,
                                source,
                                execution_time,
                                completion_retention_duration,
                                journal_retention_duration,
                                idempotency_key,
                                hotfix_apply_cancellation_after_deployment_is_pinned,
                                current_invocation_epoch,
                                completion_range_epoch_map,
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
                            creation_time: timestamps.creation_time().as_u64(),
                            created_using_restate_version: created_using_restate_version
                                .into_string(),
                            modification_time: timestamps.modification_time().as_u64(),
                            inboxed_transition_time: timestamps
                                .inboxed_transition_time()
                                .map(|t| t.as_u64()),
                            scheduled_transition_time: timestamps
                                .scheduled_transition_time()
                                .map(|t| t.as_u64()),
                            running_transition_time: timestamps
                                .running_transition_time()
                                .map(|t| t.as_u64()),
                            completed_transition_time: timestamps
                                .completed_transition_time()
                                .map(|t| t.as_u64()),
                            response_sinks: response_sinks
                                .into_iter()
                                .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                                .collect(),
                            argument: None,
                            headers: vec![],
                            execution_time: execution_time.map(|t| t.as_u64()),
                            completion_retention_duration: Some(
                                completion_retention_duration.into(),
                            ),
                            journal_retention_duration: Some(journal_retention_duration.into()),
                            idempotency_key: idempotency_key.map(|key| key.to_string()),
                            inbox_sequence_number: None,
                            journal_length: journal_metadata.length,
                            commands: journal_metadata.commands,
                            events: journal_metadata.events,
                            deployment_id,
                            service_protocol_version,
                            waiting_for_completions,
                            waiting_for_signal_indexes,
                            waiting_for_signal_names,
                            result: None,
                            hotfix_apply_cancellation_after_deployment_is_pinned,
                            current_invocation_epoch,
                            trim_points: completion_range_epoch_map
                                .into_trim_points_iter()
                                .map(|(completion_id, invocation_epoch)| JournalTrimPoint {
                                    completion_id,
                                    invocation_epoch,
                                })
                                .collect(),
                        }
                    }
                    crate::invocation_status_table::InvocationStatus::Paused(
                        crate::invocation_status_table::InFlightInvocationMetadata {
                            invocation_target,
                            created_using_restate_version,
                            journal_metadata,
                            pinned_deployment,
                            response_sinks,
                            timestamps,
                            source,
                            execution_time,
                            completion_retention_duration,
                            journal_retention_duration,
                            idempotency_key,
                            hotfix_apply_cancellation_after_deployment_is_pinned,
                            current_invocation_epoch,
                            completion_range_epoch_map,
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
                            status: invocation_status_v2::Status::Paused.into(),
                            invocation_target: Some(invocation_target.into()),
                            source: Some(source.into()),
                            span_context: Some(journal_metadata.span_context.into()),
                            creation_time: timestamps.creation_time().as_u64(),
                            created_using_restate_version: created_using_restate_version
                                .into_string(),
                            modification_time: timestamps.modification_time().as_u64(),
                            inboxed_transition_time: timestamps
                                .inboxed_transition_time()
                                .map(|t| t.as_u64()),
                            scheduled_transition_time: timestamps
                                .scheduled_transition_time()
                                .map(|t| t.as_u64()),
                            running_transition_time: timestamps
                                .running_transition_time()
                                .map(|t| t.as_u64()),
                            completed_transition_time: timestamps
                                .completed_transition_time()
                                .map(|t| t.as_u64()),
                            response_sinks: response_sinks
                                .into_iter()
                                .map(|s| ServiceInvocationResponseSink::from(Some(s)))
                                .collect(),
                            argument: None,
                            headers: vec![],
                            execution_time: execution_time.map(|t| t.as_u64()),
                            completion_retention_duration: Some(
                                completion_retention_duration.into(),
                            ),
                            journal_retention_duration: Some(journal_retention_duration.into()),
                            idempotency_key: idempotency_key.map(|key| key.to_string()),
                            inbox_sequence_number: None,
                            journal_length: journal_metadata.length,
                            commands: journal_metadata.commands,
                            events: journal_metadata.events,
                            deployment_id,
                            service_protocol_version,
                            waiting_for_completions: vec![],
                            waiting_for_signal_indexes: vec![],
                            waiting_for_signal_names: vec![],
                            result: None,
                            hotfix_apply_cancellation_after_deployment_is_pinned,
                            current_invocation_epoch,
                            trim_points: completion_range_epoch_map
                                .into_trim_points_iter()
                                .map(|(completion_id, invocation_epoch)| JournalTrimPoint {
                                    completion_id,
                                    invocation_epoch,
                                })
                                .collect(),
                        }
                    }
                    crate::invocation_status_table::InvocationStatus::Completed(
                        crate::invocation_status_table::CompletedInvocation {
                            invocation_target,
                            created_using_restate_version,
                            source,
                            execution_time,
                            idempotency_key,
                            timestamps,
                            response_result,
                            completion_retention_duration,
                            journal_retention_duration,
                            journal_metadata,
                            pinned_deployment,
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
                            status: invocation_status_v2::Status::Completed.into(),
                            invocation_target: Some(invocation_target.into()),
                            source: Some(source.into()),
                            span_context: Some(journal_metadata.span_context.into()),
                            creation_time: timestamps.creation_time().as_u64(),
                            created_using_restate_version: created_using_restate_version
                                .into_string(),
                            modification_time: timestamps.modification_time().as_u64(),
                            inboxed_transition_time: timestamps
                                .inboxed_transition_time()
                                .map(|t| t.as_u64()),
                            scheduled_transition_time: timestamps
                                .scheduled_transition_time()
                                .map(|t| t.as_u64()),
                            running_transition_time: timestamps
                                .running_transition_time()
                                .map(|t| t.as_u64()),
                            completed_transition_time: timestamps
                                .completed_transition_time()
                                .map(|t| t.as_u64()),
                            response_sinks: vec![],
                            argument: None,
                            headers: vec![],
                            execution_time: execution_time.map(|t| t.as_u64()),
                            completion_retention_duration: Some(
                                completion_retention_duration.into(),
                            ),
                            journal_retention_duration: Some(journal_retention_duration.into()),
                            idempotency_key: idempotency_key.map(|key| key.to_string()),
                            inbox_sequence_number: None,
                            journal_length: journal_metadata.length,
                            commands: journal_metadata.commands,
                            events: journal_metadata.events,
                            deployment_id,
                            service_protocol_version,
                            hotfix_apply_cancellation_after_deployment_is_pinned: false,
                            current_invocation_epoch: 0,
                            trim_points: vec![],
                            waiting_for_completions: vec![],
                            waiting_for_signal_indexes: vec![],
                            waiting_for_signal_names: vec![],
                            result: Some(response_result.into()),
                        }
                    }
                    crate::invocation_status_table::InvocationStatus::Free => {
                        panic!(
                            "Unexpected serialization of Free status. This is a bug of the invocation status table"
                        )
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
                    current_invocation_epoch,
                } = value;

                let invocation_target = expect_or_fail!(invocation_target)?.try_into()?;
                let status = match status.try_into().unwrap_or_default() {
                    invocation_status_v2::Status::Scheduled => {
                        crate::invocation_status_table::InvocationStatusDiscriminants::Scheduled
                    }
                    invocation_status_v2::Status::Inboxed => {
                        crate::invocation_status_table::InvocationStatusDiscriminants::Inboxed
                    }
                    invocation_status_v2::Status::Invoked => {
                        crate::invocation_status_table::InvocationStatusDiscriminants::Invoked
                    }
                    invocation_status_v2::Status::Suspended => {
                        crate::invocation_status_table::InvocationStatusDiscriminants::Suspended
                    }
                    invocation_status_v2::Status::Completed => {
                        crate::invocation_status_table::InvocationStatusDiscriminants::Completed
                    }
                    invocation_status_v2::Status::Paused => {
                        crate::invocation_status_table::InvocationStatusDiscriminants::Paused
                    }
                    invocation_status_v2::Status::UnknownStatus => {
                        return Err(ConversionError::unexpected_enum_variant(
                            "status",
                            value.status,
                        ));
                    }
                };

                Ok(crate::invocation_status_table::InvocationLite {
                    status,
                    invocation_target,
                    current_invocation_epoch,
                })
            }
        }

        impl From<crate::invocation_status_table::InvocationLite> for InvocationV2Lite {
            fn from(_: crate::invocation_status_table::InvocationLite) -> Self {
                panic!(
                    "Unexpected usage of InvocationLite, this data structure can be used only for reading, and never for writing"
                )
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
                            crate::invocation_status_table::InboxedInvocation::try_from(inboxed)?;
                        crate::invocation_status_table::InvocationStatus::Inboxed(
                            invocation_metadata,
                        )
                    }
                    invocation_status::Status::Invoked(invoked) => {
                        let invocation_metadata =
                            crate::invocation_status_table::InFlightInvocationMetadata::try_from(
                                invoked,
                            )?;
                        crate::invocation_status_table::InvocationStatus::Invoked(
                            invocation_metadata,
                        )
                    }
                    invocation_status::Status::Suspended(suspended) => {
                        let (metadata, waiting_for_completed_entries) = suspended.try_into()?;
                        crate::invocation_status_table::InvocationStatus::Suspended {
                            metadata,
                            waiting_for_notifications: waiting_for_completed_entries
                                .into_iter()
                                .map(NotificationId::for_completion)
                                .collect(),
                        }
                    }
                    invocation_status::Status::Completed(completed) => {
                        crate::invocation_status_table::InvocationStatus::Completed(
                            completed.try_into()?,
                        )
                    }
                    invocation_status::Status::Free(_) => {
                        crate::invocation_status_table::InvocationStatus::Free
                    }
                };

                Ok(crate::invocation_status_table::InvocationStatusV1(result))
            }
        }

        #[cfg(not(any(test, feature = "test-util")))]
        impl From<crate::invocation_status_table::InvocationStatusV1> for InvocationStatus {
            fn from(_: crate::invocation_status_table::InvocationStatusV1) -> Self {
                panic!(
                    "Unexpected conversion to old InvocationStatus, this is not expected to happen."
                )
            }
        }

        // We need this for the test_migration in invocation_status_table_test
        #[cfg(any(test, feature = "test-util"))]
        impl From<crate::invocation_status_table::InvocationStatusV1> for InvocationStatus {
            fn from(value: crate::invocation_status_table::InvocationStatusV1) -> Self {
                let status = match value.0 {
                    crate::invocation_status_table::InvocationStatus::Inboxed(inboxed_status) => {
                        invocation_status::Status::Inboxed(Inboxed::from(inboxed_status))
                    }
                    crate::invocation_status_table::InvocationStatus::Invoked(invoked_status) => {
                        invocation_status::Status::Invoked(Invoked::from(invoked_status))
                    }
                    crate::invocation_status_table::InvocationStatus::Suspended {
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
                    crate::invocation_status_table::InvocationStatus::Completed(completed) => {
                        invocation_status::Status::Completed(Completed::from(completed))
                    }
                    crate::invocation_status_table::InvocationStatus::Free => {
                        invocation_status::Status::Free(invocation_status::Free {})
                    }
                    crate::invocation_status_table::InvocationStatus::Scheduled(_)
                    | crate::invocation_status_table::InvocationStatus::Paused(_) => {
                        panic!(
                            "Unexpected conversion to old InvocationStatus when using Scheduled variant. This is a bug in the table implementation."
                        )
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

        impl TryFrom<Invoked> for crate::invocation_status_table::InFlightInvocationMetadata {
            type Error = ConversionError;

            fn try_from(value: Invoked) -> Result<Self, ConversionError> {
                let invocation_target = restate_types::invocation::InvocationTarget::try_from(
                    value
                        .invocation_target
                        .ok_or(ConversionError::missing_field("invocation_target"))?,
                )?;

                let pinned_deployment =
                    derive_pinned_deployment(value.deployment_id, value.service_protocol_version)?;

                let journal_metadata = crate::invocation_status_table::JournalMetadata::try_from(
                    value
                        .journal_meta
                        .ok_or(ConversionError::missing_field("journal_meta"))?,
                )?;
                let response_sinks = value
                    .response_sinks
                    .into_iter()
                    .map(|s| {
                        Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(s)
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))?
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

                Ok(crate::invocation_status_table::InFlightInvocationMetadata {
                    invocation_target,
                    journal_metadata,
                    pinned_deployment,
                    response_sinks,
                    created_using_restate_version: restate_types::RestateVersion::unknown(),
                    timestamps: crate::invocation_status_table::StatusTimestamps::new(
                        MillisSinceEpoch::new(value.creation_time),
                        MillisSinceEpoch::new(value.modification_time),
                        None,
                        None,
                        None,
                        None,
                    ),
                    source,
                    execution_time: None,
                    completion_retention_duration: completion_retention_time,
                    journal_retention_duration: Default::default(),
                    idempotency_key,
                    hotfix_apply_cancellation_after_deployment_is_pinned: false,
                    current_invocation_epoch: 0,
                    completion_range_epoch_map: Default::default(),
                })
            }
        }

        impl From<crate::invocation_status_table::InFlightInvocationMetadata> for Invoked {
            fn from(value: crate::invocation_status_table::InFlightInvocationMetadata) -> Self {
                let crate::invocation_status_table::InFlightInvocationMetadata {
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
                    creation_time: timestamps.creation_time().as_u64(),
                    modification_time: timestamps.modification_time().as_u64(),
                    source: Some(Source::from(source)),
                    completion_retention_time: Some(Duration::from(completion_retention_time)),
                    idempotency_key: idempotency_key.map(|key| key.to_string()),
                }
            }
        }

        impl TryFrom<Suspended>
            for (
                crate::invocation_status_table::InFlightInvocationMetadata,
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

                let journal_metadata = crate::invocation_status_table::JournalMetadata::try_from(
                    value
                        .journal_meta
                        .ok_or(ConversionError::missing_field("journal_meta"))?,
                )?;
                let response_sinks = value
                    .response_sinks
                    .into_iter()
                    .map(|s| {
                        Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(s)
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))?
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
                    crate::invocation_status_table::InFlightInvocationMetadata {
                        invocation_target,
                        created_using_restate_version: restate_types::RestateVersion::unknown(),
                        journal_metadata,
                        pinned_deployment,
                        response_sinks,
                        timestamps: crate::invocation_status_table::StatusTimestamps::new(
                            MillisSinceEpoch::new(value.creation_time),
                            MillisSinceEpoch::new(value.modification_time),
                            None,
                            None,
                            None,
                            None,
                        ),
                        source: caller,
                        execution_time: None,
                        completion_retention_duration: completion_retention_time,
                        journal_retention_duration: Default::default(),
                        idempotency_key,
                        hotfix_apply_cancellation_after_deployment_is_pinned: false,
                        current_invocation_epoch: 0,
                        completion_range_epoch_map: Default::default(),
                    },
                    waiting_for_completed_entries,
                ))
            }
        }

        impl
            From<(
                crate::invocation_status_table::InFlightInvocationMetadata,
                HashSet<restate_types::identifiers::EntryIndex>,
            )> for Suspended
        {
            fn from(
                (metadata, waiting_for_completed_entries): (
                    crate::invocation_status_table::InFlightInvocationMetadata,
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
                    creation_time: metadata.timestamps.creation_time().as_u64(),
                    modification_time: metadata.timestamps.modification_time().as_u64(),
                    waiting_for_completed_entries,
                    source: Some(Source::from(metadata.source)),
                    completion_retention_time: Some(Duration::from(
                        metadata.completion_retention_duration,
                    )),
                    idempotency_key: metadata.idempotency_key.map(|key| key.to_string()),
                }
            }
        }

        impl TryFrom<Inboxed> for crate::invocation_status_table::InboxedInvocation {
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
                        Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(s)
                            .transpose()
                            .ok_or(ConversionError::missing_field("response_sink"))?
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
                    .map(restate_types::invocation::Header::try_from)
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

                Ok(crate::invocation_status_table::InboxedInvocation {
                    inbox_sequence_number: value.inbox_sequence_number,
                    metadata: crate::invocation_status_table::PreFlightInvocationMetadata {
                        created_using_restate_version: restate_types::RestateVersion::unknown(),
                        response_sinks,
                        timestamps: crate::invocation_status_table::StatusTimestamps::new(
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
                        journal_retention_duration: Default::default(),
                    },
                })
            }
        }

        impl From<crate::invocation_status_table::InboxedInvocation> for Inboxed {
            fn from(value: crate::invocation_status_table::InboxedInvocation) -> Self {
                let crate::invocation_status_table::InboxedInvocation {
                    metadata:
                        crate::invocation_status_table::PreFlightInvocationMetadata {
                            response_sinks,
                            timestamps,
                            invocation_target,
                            created_using_restate_version: _,
                            argument,
                            source,
                            span_context,
                            headers,
                            execution_time,
                            completion_retention_duration: completion_retention_time,
                            journal_retention_duration: _,
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
                    creation_time: timestamps.creation_time().as_u64(),
                    modification_time: timestamps.modification_time().as_u64(),
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

        impl TryFrom<Completed> for crate::invocation_status_table::CompletedInvocation {
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

                Ok(crate::invocation_status_table::CompletedInvocation {
                    invocation_target,
                    created_using_restate_version: restate_types::RestateVersion::unknown(),
                    source,
                    timestamps: crate::invocation_status_table::StatusTimestamps::new(
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
                    execution_time: None,
                    journal_retention_duration: Default::default(),
                    journal_metadata: JournalMetadata::empty(),
                    pinned_deployment: None,
                })
            }
        }

        impl From<crate::invocation_status_table::CompletedInvocation> for Completed {
            fn from(value: crate::invocation_status_table::CompletedInvocation) -> Self {
                let crate::invocation_status_table::CompletedInvocation {
                    invocation_target,
                    created_using_restate_version: _,
                    source,
                    execution_time: _,
                    idempotency_key,
                    timestamps,
                    response_result,
                    // We don't store this in the old invocation status table
                    completion_retention_duration: _,
                    // The old invocation status table doesn't support journal metadata on Completed
                    journal_retention_duration: _,
                    journal_metadata: _,
                    // The old invocation status table doesn't support PinnedDeployment on Completed
                    pinned_deployment: _,
                } = value;

                Completed {
                    invocation_target: Some(InvocationTarget::from(invocation_target)),
                    source: Some(Source::from(source)),
                    result: Some(ResponseResult::from(response_result)),
                    creation_time: timestamps.creation_time().as_u64(),
                    modification_time: timestamps.modification_time().as_u64(),
                    idempotency_key: idempotency_key.map(|s| s.to_string()),
                }
            }
        }

        impl TryFrom<JournalMeta> for crate::invocation_status_table::JournalMetadata {
            type Error = ConversionError;

            fn try_from(value: JournalMeta) -> Result<Self, ConversionError> {
                let length = value.length;
                let span_context =
                    restate_types::invocation::ServiceInvocationSpanContext::try_from(
                        value
                            .span_context
                            .ok_or(ConversionError::missing_field("span_context"))?,
                    )?;
                Ok(crate::invocation_status_table::JournalMetadata {
                    length,
                    commands: 0,
                    events: 0,
                    span_context,
                })
            }
        }

        impl From<crate::invocation_status_table::JournalMetadata> for JournalMeta {
            fn from(value: crate::invocation_status_table::JournalMetadata) -> Self {
                let crate::invocation_status_table::JournalMetadata {
                    span_context,
                    length,
                    ..
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
                            .map_err(ConversionError::invalid_data)?,
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
                    source::Source::RestartAsNew(service) => {
                        restate_types::invocation::Source::RestartAsNew(
                            restate_types::identifiers::InvocationId::try_from(
                                service
                                    .invocation_id
                                    .ok_or(ConversionError::missing_field("invocation_id"))?,
                            )?,
                        )
                    }
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
                    restate_types::invocation::Source::RestartAsNew(invocation_id) => {
                        source::Source::RestartAsNew(source::RestartAsNew {
                            invocation_id: Some(InvocationId::from(invocation_id)),
                        })
                    }
                    restate_types::invocation::Source::Internal => source::Source::Internal(()),
                };

                Source {
                    source: Some(source),
                }
            }
        }

        impl From<&restate_types::invocation::Source> for Source {
            fn from(value: &restate_types::invocation::Source) -> Self {
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
                        invocation_id: Some(InvocationId::from(*invocation_id)),
                        invocation_target: Some(InvocationTarget::from(invocation_target)),
                    }),
                    restate_types::invocation::Source::RestartAsNew(invocation_id) => {
                        source::Source::RestartAsNew(source::RestartAsNew {
                            invocation_id: Some(InvocationId::from(*invocation_id)),
                        })
                    }
                    restate_types::invocation::Source::Internal => source::Source::Internal(()),
                };

                Source {
                    source: Some(source),
                }
            }
        }

        impl TryFrom<InboxEntry> for crate::inbox_table::InboxEntry {
            type Error = ConversionError;

            fn try_from(value: InboxEntry) -> Result<Self, ConversionError> {
                Ok(
                    match value.entry.ok_or(ConversionError::missing_field("entry"))? {
                        inbox_entry::Entry::Invocation(invocation) => {
                            crate::inbox_table::InboxEntry::Invocation(
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
                            crate::inbox_table::InboxEntry::StateMutation(
                                restate_types::state_mut::ExternalStateMutation::try_from(
                                    state_mutation,
                                )?,
                            )
                        }
                    },
                )
            }
        }

        impl From<crate::inbox_table::InboxEntry> for InboxEntry {
            fn from(inbox_entry: crate::inbox_table::InboxEntry) -> Self {
                let inbox_entry = match inbox_entry {
                    crate::inbox_table::InboxEntry::Invocation(service_id, invocation_id) => {
                        inbox_entry::Entry::Invocation(inbox_entry::Invocation {
                            service_id: Some(service_id.into()),
                            invocation_id: Some(InvocationId::from(invocation_id)),
                        })
                    }
                    crate::inbox_table::InboxEntry::StateMutation(state_mutation) => {
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
                    completion_retention_duration,
                    journal_retention_duration,
                    submit_notification_sink,
                    restate_version,
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
                    .map(restate_types::invocation::Header::try_from)
                    .collect::<Result<Vec<_>, ConversionError>>()?;

                let execution_time = if execution_time == 0 {
                    None
                } else {
                    Some(MillisSinceEpoch::new(execution_time))
                };

                let completion_retention_duration = completion_retention_duration
                    .map(std::time::Duration::try_from)
                    .transpose()?
                    .unwrap_or_default();
                let journal_retention_duration = journal_retention_duration
                    .map(std::time::Duration::try_from)
                    .transpose()?
                    .unwrap_or_default();

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
                    completion_retention_duration,
                    journal_retention_duration,
                    idempotency_key,
                    submit_notification_sink,
                    restate_version: restate_version_from_pb(restate_version),
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
                    completion_retention_duration: Some(value.completion_retention_duration.into()),
                    journal_retention_duration: Some(value.journal_retention_duration.into()),
                    idempotency_key: value.idempotency_key.map(|s| s.to_string()),
                    submit_notification_sink: value.submit_notification_sink.map(Into::into),
                    restate_version: value.restate_version.into_string(),
                }
            }
        }

        impl From<Box<restate_types::invocation::ServiceInvocation>> for ServiceInvocation {
            fn from(value: Box<restate_types::invocation::ServiceInvocation>) -> Self {
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
                    completion_retention_duration: Some(value.completion_retention_duration.into()),
                    journal_retention_duration: Some(value.journal_retention_duration.into()),
                    idempotency_key: value.idempotency_key.map(|s| s.to_string()),
                    submit_notification_sink: value.submit_notification_sink.map(Into::into),
                    restate_version: value.restate_version.into_string(),
                }
            }
        }

        impl From<&restate_types::invocation::ServiceInvocation> for ServiceInvocation {
            fn from(value: &restate_types::invocation::ServiceInvocation) -> Self {
                let invocation_target = InvocationTarget::from(&value.invocation_target);
                let span_context = SpanContext::from(&value.span_context);
                let response_sink =
                    ServiceInvocationResponseSink::from(value.response_sink.as_ref());
                let source = Source::from(&value.source);
                let headers = value.headers.iter().map(|h| h.clone().into()).collect();

                ServiceInvocation {
                    invocation_id: Some(InvocationId::from(value.invocation_id)),
                    invocation_target: Some(invocation_target),
                    span_context: Some(span_context),
                    response_sink: Some(response_sink),
                    argument: value.argument.clone(),
                    source: Some(source),
                    headers,
                    execution_time: value.execution_time.map(|m| m.as_u64()).unwrap_or_default(),
                    completion_retention_duration: Some(value.completion_retention_duration.into()),
                    journal_retention_duration: Some(value.journal_retention_duration.into()),
                    idempotency_key: value.idempotency_key.as_ref().map(|s| s.to_string()),
                    submit_notification_sink: value.submit_notification_sink.map(Into::into),
                    restate_version: value.restate_version.clone().into_string(),
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
                    service_id,
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

        impl From<&restate_types::invocation::InvocationTarget> for InvocationTarget {
            fn from(value: &restate_types::invocation::InvocationTarget) -> Self {
                match value {
                    restate_types::invocation::InvocationTarget::Service { name, handler } => {
                        InvocationTarget {
                            name: name.as_bytes().clone(),
                            handler: handler.as_bytes().clone(),
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
                        name: name.as_bytes().clone(),
                        handler: handler.as_bytes().clone(),
                        key: key.as_bytes().clone(),
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
                        name: name.as_bytes().clone(),
                        handler: handler.as_bytes().clone(),
                        key: key.as_bytes().clone(),
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
            bytes: impl AsRef<[u8]>,
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

                let span_relation = span_relation
                    .map(|span_relation| span_relation.try_into())
                    .transpose()
                    .map_err(ConversionError::invalid_data)?;

                Ok(
                    restate_types::invocation::ServiceInvocationSpanContext::new(
                        restate_types::invocation::SpanContextDef::new(
                            trace_id,
                            span_id,
                            trace_flags,
                            is_remote,
                            restate_types::invocation::TraceStateDef::new_header(trace_state),
                        ),
                        span_relation,
                    ),
                )
            }
        }

        impl From<restate_types::invocation::ServiceInvocationSpanContext> for SpanContext {
            fn from(value: restate_types::invocation::ServiceInvocationSpanContext) -> Self {
                let (span_context, span_cause) = value.into_span_context_and_cause();
                let span_id = u64::from_be_bytes(span_context.span_id().to_bytes());
                let trace_flags = u32::from(span_context.trace_flags().to_u8());
                let trace_id = Bytes::copy_from_slice(&span_context.trace_id().to_bytes());
                let is_remote = span_context.is_remote();
                let trace_state = span_context.into_trace_state().into_header();
                let span_relation =
                    span_cause.map(|span_relation| SpanRelation::from(span_relation.clone()));

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

        impl From<&restate_types::invocation::ServiceInvocationSpanContext> for SpanContext {
            fn from(value: &restate_types::invocation::ServiceInvocationSpanContext) -> Self {
                let span_context = value.span_context();
                let trace_state = span_context.trace_state().header().into_owned();
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

        pub(super) fn try_bytes_into_trace_id(
            mut bytes: impl Buf,
        ) -> Result<opentelemetry::trace::TraceId, ConversionError> {
            if bytes.remaining() != 16 {
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
                            restate_types::invocation::ServiceInvocationResponseSink::PartitionProcessor(
                                restate_types::invocation::JournalCompletionTarget {
                                    caller_id:  restate_types::identifiers::InvocationId::from_slice(&partition_processor.caller)?,
                                    caller_completion_id: partition_processor.entry_index,
                                    caller_invocation_epoch: partition_processor.caller_invocation_epoch,
                                }
                            ),
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
                        restate_types::invocation::ServiceInvocationResponseSink::PartitionProcessor(restate_types::invocation::JournalCompletionTarget {
                                                                                                         caller_id, caller_completion_id, caller_invocation_epoch
                                                                                                     }),
                    ) => ResponseSink::PartitionProcessor(PartitionProcessor {
                        entry_index: caller_completion_id,
                        caller: caller_id.into(),
                        caller_invocation_epoch
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

        impl From<Option<&restate_types::invocation::ServiceInvocationResponseSink>>
            for ServiceInvocationResponseSink
        {
            fn from(
                value: Option<&restate_types::invocation::ServiceInvocationResponseSink>,
            ) -> Self {
                let response_sink = match value {
                    Some(
                        restate_types::invocation::ServiceInvocationResponseSink::PartitionProcessor(restate_types::invocation::JournalCompletionTarget {
                                                                                                         caller_id, caller_completion_id, caller_invocation_epoch
                                                                                                     }),
                    ) => ResponseSink::PartitionProcessor(PartitionProcessor {
                        entry_index: *caller_completion_id,
                        caller: (*caller_id).into(),
                        caller_invocation_epoch: *caller_invocation_epoch,
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
                    generation: value.generation(),
                }
            }
        }

        impl From<super::GenerationalNodeId> for GenerationalNodeId {
            fn from(value: super::GenerationalNodeId) -> Self {
                GenerationalNodeId::new(value.id, value.generation)
            }
        }

        impl TryFrom<JournalEntry> for crate::journal_table::JournalEntry {
            type Error = ConversionError;

            fn try_from(value: JournalEntry) -> Result<Self, ConversionError> {
                let journal_entry = match value
                    .kind
                    .ok_or(ConversionError::missing_field("kind"))?
                {
                    Kind::Entry(journal_entry) => crate::journal_table::JournalEntry::Entry(
                        restate_types::journal::enriched::EnrichedRawEntry::try_from(
                            journal_entry,
                        )?,
                    ),
                    Kind::CompletionResult(completion_result) => {
                        crate::journal_table::JournalEntry::Completion(
                            restate_types::journal::CompletionResult::try_from(completion_result)?,
                        )
                    }
                };

                Ok(journal_entry)
            }
        }

        impl From<crate::journal_table::JournalEntry> for JournalEntry {
            fn from(value: crate::journal_table::JournalEntry) -> Self {
                match value {
                    crate::journal_table::JournalEntry::Entry(entry) => JournalEntry::from(entry),
                    crate::journal_table::JournalEntry::Completion(completion) => {
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
                    None => invocation_resolution_result::Result::None(()),
                    Some(resolution_result) => {
                        let restate_types::journal::enriched::CallEnrichmentResult {
                            invocation_id,
                            invocation_target,
                            span_context,
                            completion_retention_time,
                        } = resolution_result;

                        invocation_resolution_result::Result::Success(
                            invocation_resolution_result::Success {
                                invocation_id: Some(InvocationId::from(invocation_id)),
                                invocation_target: Some(invocation_target.into()),
                                span_context: Some(SpanContext::from(span_context)),
                                completion_retention_time: Some(Duration::from(
                                    completion_retention_time.unwrap_or_default(),
                                )),
                            },
                        )
                    }
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
                let journal_retention_duration = std::time::Duration::try_from(
                    value.journal_retention_duration.unwrap_or_default(),
                )?;

                Ok(journal_v2::raw::CallOrSendMetadata {
                    invocation_id,
                    span_context,
                    invocation_target,
                    completion_retention_duration,
                    journal_retention_duration,
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
                    journal_retention_duration: Some(Duration::from(
                        value.journal_retention_duration,
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
                        ));
                    }
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
                }
            }
        }

        impl TryFrom<Entry> for crate::journal_table_v2::StoredEntry {
            type Error = ConversionError;

            fn try_from(value: Entry) -> Result<Self, Self::Error> {
                let header =
                    restate_types::storage::StoredRawEntryHeader::new(value.append_time.into());

                Ok(crate::journal_table_v2::StoredEntry(
                    match EntryType::try_from(value.ty)
                        .map_err(|e| ConversionError::unexpected_enum_variant("ty", e.0))?
                        .try_into()?
                    {
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

                            restate_types::storage::StoredRawEntry::new(
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
                        ) => restate_types::storage::StoredRawEntry::new(
                            header,
                            journal_v2::raw::RawCommand::new(ct, value.content)
                                .with_command_specific_metadata(
                                journal_v2::raw::RawCommandSpecificMetadata::CallOrSend(Box::new(
                                    journal_v2::raw::CallOrSendMetadata::try_from(
                                        value.call_or_send_command_metadata.ok_or(
                                            ConversionError::missing_field(
                                                "call_command_journal_entry_additional_metadata",
                                            ),
                                        )?,
                                    )?,
                                )),
                            ),
                        ),
                        journal_v2::EntryType::Command(ct) => {
                            restate_types::storage::StoredRawEntry::new(
                                header,
                                journal_v2::raw::RawCommand::new(ct, value.content),
                            )
                        }
                    },
                ))
            }
        }

        impl From<crate::journal_table_v2::StoredEntry> for Entry {
            fn from(
                crate::journal_table_v2::StoredEntry(raw_entry): crate::journal_table_v2::StoredEntry,
            ) -> Self {
                let ty = EntryType::from(raw_entry.ty());
                let append_time = raw_entry.header.append_time.into();

                let mut call_or_send_command_metadata: Option<entry::CallOrSendCommandMetadata> =
                    None;
                let mut notification_id: Option<entry::NotificationId> = None;
                let content = match raw_entry.inner {
                    journal_v2::raw::RawEntry::Command(cmd) => {
                        match cmd.command_specific_metadata {
                            journal_v2::raw::RawCommandSpecificMetadata::CallOrSend(
                                call_or_send_metadata,
                            ) => {
                                call_or_send_command_metadata =
                                    Some((*call_or_send_metadata).into())
                            }
                            journal_v2::raw::RawCommandSpecificMetadata::None => {}
                        };

                        cmd.serialized_content
                    }
                    journal_v2::raw::RawEntry::Notification(notification) => {
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

        impl From<journal_events::EventType> for event::EventType {
            fn from(value: journal_events::EventType) -> Self {
                match value {
                    journal_events::EventType::TransientError => Self::TransientError,
                    journal_events::EventType::Paused => Self::Paused,
                    journal_events::EventType::Unknown => Self::UnknownEvent,
                }
            }
        }

        impl From<event::EventType> for journal_events::EventType {
            fn from(value: event::EventType) -> Self {
                match value {
                    event::EventType::TransientError => Self::TransientError,
                    event::EventType::Paused => Self::Paused,
                    event::EventType::UnknownEvent => Self::Unknown,
                }
            }
        }

        impl TryFrom<Event> for crate::journal_events::StoredEvent {
            type Error = ConversionError;

            fn try_from(value: Event) -> Result<Self, Self::Error> {
                let Event {
                    event_type,
                    event_deduplication_hash,
                    content,
                    append_time,
                    after_journal_entry_index,
                } = value;

                let event_type = journal_events::EventType::from(
                    event::EventType::try_from(event_type).unwrap_or_default(),
                );

                let mut event = journal_events::raw::RawEvent::new(event_type, content);
                if let Some(deduplication_hash) = event_deduplication_hash {
                    event.set_deduplication_hash(deduplication_hash);
                }

                Ok(Self {
                    after_journal_entry_index,
                    append_time: MillisSinceEpoch::from(append_time),
                    event,
                })
            }
        }

        impl From<crate::journal_events::StoredEvent> for Event {
            fn from(
                crate::journal_events::StoredEvent {
                    after_journal_entry_index,
                    append_time,
                    event,
                }: crate::journal_events::StoredEvent,
            ) -> Self {
                let (ty, deduplication_hash, value) = event.into_inner();
                Self {
                    event_type: event::EventType::from(ty).into(),
                    event_deduplication_hash: deduplication_hash,
                    content: value,
                    append_time: append_time.into(),
                    after_journal_entry_index,
                }
            }
        }

        impl From<restate_types::invocation::AttachInvocationRequest>
            for outbox_message::AttachInvocationRequest
        {
            fn from(value: restate_types::invocation::AttachInvocationRequest) -> Self {
                let restate_types::invocation::AttachInvocationRequest {
                    block_on_inflight,
                    response_sink,
                    invocation_query,
                } = value;

                Self {
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
                            outbox_message::attach_invocation_request::Query::WorkflowId(id.into())
                        }
                    }),
                    response_sink: Some(Some(response_sink).into()),
                }
            }
        }

        impl TryFrom<outbox_message::AttachInvocationRequest>
            for restate_types::invocation::AttachInvocationRequest
        {
            type Error = ConversionError;

            fn try_from(
                value: outbox_message::AttachInvocationRequest,
            ) -> Result<Self, Self::Error> {
                let outbox_message::AttachInvocationRequest {
                    block_on_inflight,
                    response_sink,
                    query,
                } = value;

                Ok(
                    Self {
                        invocation_query: match query
                            .ok_or(ConversionError::missing_field("query"))?
                        {
                            outbox_message::attach_invocation_request::Query::InvocationId(id) => {
                                restate_types::invocation::InvocationQuery::Invocation(
                                    id.try_into()?,
                                )
                            }
                            outbox_message::attach_invocation_request::Query::IdempotencyId(id) => {
                                restate_types::invocation::InvocationQuery::IdempotencyId(
                                    id.try_into()?,
                                )
                            }
                            outbox_message::attach_invocation_request::Query::WorkflowId(id) => {
                                restate_types::invocation::InvocationQuery::Workflow(id.try_into()?)
                            }
                        },
                        block_on_inflight,
                        response_sink: Option::<
                            restate_types::invocation::ServiceInvocationResponseSink,
                        >::try_from(
                            response_sink.ok_or(ConversionError::missing_field("response_sink"))?,
                        )
                        .transpose()
                        .ok_or(ConversionError::missing_field("response_sink"))??,
                    },
                )
            }
        }

        impl From<restate_types::invocation::InvocationResponse>
            for outbox_message::OutboxServiceInvocationResponse
        {
            fn from(value: restate_types::invocation::InvocationResponse) -> Self {
                let restate_types::invocation::InvocationResponse { target, result } = value;

                OutboxServiceInvocationResponse {
                    entry_index: target.caller_completion_id,
                    invocation_id: Some(InvocationId::from(target.caller_id)),
                    response_result: Some(ResponseResult::from(result)),
                    caller_invocation_epoch: target.caller_invocation_epoch,
                }
            }
        }

        impl TryFrom<outbox_message::OutboxServiceInvocationResponse>
            for restate_types::invocation::InvocationResponse
        {
            type Error = ConversionError;

            fn try_from(
                value: outbox_message::OutboxServiceInvocationResponse,
            ) -> Result<Self, Self::Error> {
                let outbox_message::OutboxServiceInvocationResponse {
                    caller_invocation_epoch,
                    entry_index,
                    invocation_id,
                    response_result,
                } = value;

                Ok(Self {
                    target: restate_types::invocation::JournalCompletionTarget {
                        caller_id: restate_types::identifiers::InvocationId::try_from(
                            invocation_id.ok_or(ConversionError::missing_field("invocation_id"))?,
                        )?,
                        caller_completion_id: entry_index,
                        caller_invocation_epoch,
                    },
                    result: restate_types::invocation::ResponseResult::try_from(
                        response_result.ok_or(ConversionError::missing_field("response_result"))?,
                    )?,
                })
            }
        }

        impl From<restate_types::invocation::NotifySignalRequest> for outbox_message::NotifySignal {
            fn from(value: restate_types::invocation::NotifySignalRequest) -> Self {
                let restate_types::invocation::NotifySignalRequest {
                    invocation_id,
                    signal,
                } = value;

                Self {
                    invocation_id: Some(InvocationId::from(invocation_id)),
                    signal_id: Some(match signal.id {
                        journal_v2::SignalId::Index(idx) => {
                            outbox_message::notify_signal::SignalId::Idx(idx)
                        }
                        journal_v2::SignalId::Name(name) => {
                            outbox_message::notify_signal::SignalId::Name(name.to_string())
                        }
                    }),
                    result: Some(match signal.result {
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
                }
            }
        }

        impl TryFrom<outbox_message::NotifySignal> for restate_types::invocation::NotifySignalRequest {
            type Error = ConversionError;

            fn try_from(value: outbox_message::NotifySignal) -> Result<Self, Self::Error> {
                let outbox_message::NotifySignal {
                    invocation_id,
                    signal_id,
                    result,
                } = value;

                Ok(Self {
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
                })
            }
        }

        impl TryFrom<OutboxMessage> for crate::outbox_table::OutboxMessage {
            type Error = ConversionError;

            fn try_from(value: OutboxMessage) -> Result<Self, ConversionError> {
                let result = match value
                    .outbox_message
                    .ok_or(ConversionError::missing_field("outbox_message"))?
                {
                    outbox_message::OutboxMessage::ServiceInvocationCase(service_invocation) => {
                        crate::outbox_table::OutboxMessage::ServiceInvocation(Box::new(
                            restate_types::invocation::ServiceInvocation::try_from(
                                service_invocation
                                    .service_invocation
                                    .ok_or(ConversionError::missing_field("service_invocation"))?,
                            )?,
                        ))
                    }
                    outbox_message::OutboxMessage::ServiceInvocationResponse(
                        invocation_response,
                    ) => crate::outbox_table::OutboxMessage::ServiceResponse(
                        invocation_response.try_into()?,
                    ),
                    outbox_message::OutboxMessage::Kill(outbox_kill) => {
                        crate::outbox_table::OutboxMessage::InvocationTermination(
                            InvocationTermination {
                                invocation_id: restate_types::identifiers::InvocationId::try_from(
                                    outbox_kill
                                        .invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                                flavor: TerminationFlavor::Kill,
                                response_sink: None,
                            },
                        )
                    }
                    outbox_message::OutboxMessage::Cancel(outbox_cancel) => {
                        crate::outbox_table::OutboxMessage::InvocationTermination(
                            InvocationTermination {
                                invocation_id: restate_types::identifiers::InvocationId::try_from(
                                    outbox_cancel
                                        .invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                                flavor: TerminationFlavor::Cancel,
                                response_sink: None,
                            },
                        )
                    }
                    outbox_message::OutboxMessage::AttachInvocationRequest(
                        attach_invocation_request,
                    ) => crate::outbox_table::OutboxMessage::AttachInvocation(
                        attach_invocation_request.try_into()?,
                    ),
                    outbox_message::OutboxMessage::NotifySignal(notify_signal) => {
                        crate::outbox_table::OutboxMessage::NotifySignal(notify_signal.try_into()?)
                    }
                };

                Ok(result)
            }
        }

        impl From<crate::outbox_table::OutboxMessage> for OutboxMessage {
            fn from(value: crate::outbox_table::OutboxMessage) -> Self {
                let outbox_message = match value {
                    crate::outbox_table::OutboxMessage::ServiceInvocation(service_invocation) => {
                        outbox_message::OutboxMessage::ServiceInvocationCase(
                            OutboxServiceInvocation {
                                service_invocation: Some(ServiceInvocation::from(
                                    service_invocation,
                                )),
                            },
                        )
                    }
                    crate::outbox_table::OutboxMessage::ServiceResponse(invocation_response) => {
                        outbox_message::OutboxMessage::ServiceInvocationResponse(
                            OutboxServiceInvocationResponse {
                                entry_index: invocation_response.target.caller_completion_id,
                                invocation_id: Some(InvocationId::from(
                                    invocation_response.target.caller_id,
                                )),
                                response_result: Some(ResponseResult::from(
                                    invocation_response.result,
                                )),
                                caller_invocation_epoch: invocation_response
                                    .target
                                    .caller_invocation_epoch,
                            },
                        )
                    }
                    crate::outbox_table::OutboxMessage::InvocationTermination(
                        invocation_termination,
                    ) => {
                        debug_assert!(
                            invocation_termination.response_sink.is_none(),
                            "Response sink is unsupported for outbox messages"
                        );
                        match invocation_termination.flavor {
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
                        }
                    }
                    crate::outbox_table::OutboxMessage::AttachInvocation(
                        attach_invocation_request,
                    ) => outbox_message::OutboxMessage::AttachInvocationRequest(
                        attach_invocation_request.into(),
                    ),
                    crate::outbox_table::OutboxMessage::NotifySignal(notify_signal) => {
                        outbox_message::OutboxMessage::NotifySignal(notify_signal.into())
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
                        // we should be able to turn the incoming Bytes into a String without a copy
                        let failure_message = Vec::<u8>::from(failure.failure_message);
                        let failure_message = String::from_utf8(failure_message)
                            .map_err(ConversionError::invalid_data)?;
                        restate_types::invocation::ResponseResult::Failure(InvocationError::new(
                            failure.failure_code,
                            failure_message,
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

        impl TryFrom<Timer> for crate::timer_table::Timer {
            type Error = ConversionError;

            fn try_from(value: Timer) -> Result<Self, ConversionError> {
                Ok(
                    match value.value.ok_or(ConversionError::missing_field("value"))? {
                        timer::Value::CompleteSleepEntry(cse) => {
                            crate::timer_table::Timer::CompleteJournalEntry(
                                restate_types::identifiers::InvocationId::try_from(
                                    cse.invocation_id
                                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                                )?,
                                cse.entry_index,
                                cse.caller_invocation_epoch,
                            )
                        }
                        timer::Value::Invoke(si) => crate::timer_table::Timer::Invoke(Box::new(
                            restate_types::invocation::ServiceInvocation::try_from(si)?,
                        )),
                        timer::Value::ScheduledInvoke(id) => crate::timer_table::Timer::NeoInvoke(
                            restate_types::identifiers::InvocationId::try_from(id)?,
                        ),
                        timer::Value::CleanInvocationStatus(clean_invocation_status) => {
                            crate::timer_table::Timer::CleanInvocationStatus(
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

        impl From<crate::timer_table::Timer> for Timer {
            fn from(value: crate::timer_table::Timer) -> Self {
                Timer {
                    value: Some(match value {
                        crate::timer_table::Timer::CompleteJournalEntry(
                            invocation_id,
                            entry_index,
                            caller_invocation_epoch,
                        ) => timer::Value::CompleteSleepEntry(timer::CompleteSleepEntry {
                            invocation_id: Some(InvocationId::from(invocation_id)),
                            entry_index,
                            caller_invocation_epoch,
                        }),
                        crate::timer_table::Timer::NeoInvoke(invocation_id) => {
                            timer::Value::ScheduledInvoke(InvocationId::from(invocation_id))
                        }
                        crate::timer_table::Timer::Invoke(si) => {
                            timer::Value::Invoke(ServiceInvocation::from(si))
                        }
                        crate::timer_table::Timer::CleanInvocationStatus(invocation_id) => {
                            timer::Value::CleanInvocationStatus(timer::CleanInvocationStatus {
                                invocation_id: Some(InvocationId::from(invocation_id)),
                            })
                        }
                    }),
                }
            }
        }

        impl From<crate::deduplication_table::DedupSequenceNumber> for DedupSequenceNumber {
            fn from(value: crate::deduplication_table::DedupSequenceNumber) -> Self {
                match value {
                    crate::deduplication_table::DedupSequenceNumber::Sn(sn) => {
                        DedupSequenceNumber {
                            variant: Some(Variant::SequenceNumber(sn)),
                        }
                    }
                    crate::deduplication_table::DedupSequenceNumber::Esn(esn) => {
                        DedupSequenceNumber {
                            variant: Some(Variant::EpochSequenceNumber(EpochSequenceNumber::from(
                                esn,
                            ))),
                        }
                    }
                }
            }
        }

        impl TryFrom<DedupSequenceNumber> for crate::deduplication_table::DedupSequenceNumber {
            type Error = ConversionError;

            fn try_from(value: DedupSequenceNumber) -> Result<Self, ConversionError> {
                Ok(
                    match value
                        .variant
                        .ok_or(ConversionError::missing_field("variant"))?
                    {
                        Variant::SequenceNumber(sn) => {
                            crate::deduplication_table::DedupSequenceNumber::Sn(sn)
                        }
                        Variant::EpochSequenceNumber(esn) => {
                            crate::deduplication_table::DedupSequenceNumber::Esn(
                                crate::deduplication_table::EpochSequenceNumber::try_from(esn)?,
                            )
                        }
                    },
                )
            }
        }

        impl From<crate::deduplication_table::EpochSequenceNumber> for EpochSequenceNumber {
            fn from(value: crate::deduplication_table::EpochSequenceNumber) -> Self {
                EpochSequenceNumber {
                    leader_epoch: value.leader_epoch.into(),
                    sequence_number: value.sequence_number,
                }
            }
        }

        impl TryFrom<EpochSequenceNumber> for crate::deduplication_table::EpochSequenceNumber {
            type Error = ConversionError;

            fn try_from(value: EpochSequenceNumber) -> Result<Self, ConversionError> {
                Ok(crate::deduplication_table::EpochSequenceNumber {
                    leader_epoch: value.leader_epoch.into(),
                    sequence_number: value.sequence_number,
                })
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

        impl From<crate::idempotency_table::IdempotencyMetadata> for IdempotencyMetadata {
            fn from(value: crate::idempotency_table::IdempotencyMetadata) -> Self {
                IdempotencyMetadata {
                    invocation_id: Some(InvocationId::from(value.invocation_id)),
                }
            }
        }

        impl TryFrom<IdempotencyMetadata> for crate::idempotency_table::IdempotencyMetadata {
            type Error = ConversionError;

            fn try_from(value: IdempotencyMetadata) -> Result<Self, ConversionError> {
                Ok(crate::idempotency_table::IdempotencyMetadata {
                    invocation_id: restate_types::identifiers::InvocationId::try_from(
                        value
                            .invocation_id
                            .ok_or(ConversionError::missing_field("invocation_id"))?,
                    )
                    .map_err(ConversionError::invalid_data)?,
                })
            }
        }

        impl From<crate::promise_table::Promise> for Promise {
            fn from(value: crate::promise_table::Promise) -> Self {
                match value.state {
                    crate::promise_table::PromiseState::Completed(e) => Promise {
                        state: Some(promise::State::CompletedState(promise::CompletedState {
                            result: Some(e.into()),
                        })),
                    },
                    crate::promise_table::PromiseState::NotCompleted(listeners) => Promise {
                        state: Some(promise::State::NotCompletedState(
                            promise::NotCompletedState {
                                listening_journal_entries: listeners
                                    .into_iter()
                                    .map(Into::into)
                                    .collect(),
                            },
                        )),
                    },
                }
            }
        }

        impl TryFrom<Promise> for crate::promise_table::Promise {
            type Error = ConversionError;

            fn try_from(value: Promise) -> Result<Self, ConversionError> {
                Ok(crate::promise_table::Promise {
                    state: match value.state.ok_or(ConversionError::missing_field("state"))? {
                        promise::State::CompletedState(s) => {
                            crate::promise_table::PromiseState::Completed(
                                s.result
                                    .ok_or(ConversionError::missing_field("result"))?
                                    .try_into()?,
                            )
                        }
                        promise::State::NotCompletedState(s) => {
                            crate::promise_table::PromiseState::NotCompleted(
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

        impl From<RestateVersion> for restate_types::SemanticRestateVersion {
            fn from(value: RestateVersion) -> Self {
                Self::parse(&value.version).unwrap_or_default()
            }
        }

        impl From<restate_types::SemanticRestateVersion> for RestateVersion {
            fn from(value: restate_types::SemanticRestateVersion) -> Self {
                RestateVersion {
                    version: value.to_string(),
                }
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

        impl From<crate::fsm_table::PartitionDurability> for PartitionDurability {
            fn from(value: crate::fsm_table::PartitionDurability) -> Self {
                PartitionDurability {
                    durable_point: Some(SequenceNumber {
                        sequence_number: value.durable_point.as_u64(),
                    }),
                    modification_time: value.modification_time.as_u64(),
                }
            }
        }

        impl From<PartitionDurability> for crate::fsm_table::PartitionDurability {
            fn from(value: PartitionDurability) -> Self {
                crate::fsm_table::PartitionDurability {
                    durable_point: Lsn::from(
                        value.durable_point.unwrap_or_default().sequence_number,
                    ),
                    modification_time: MillisSinceEpoch::new(value.modification_time),
                }
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

        fn restate_version_from_pb(restate_version: String) -> restate_types::RestateVersion {
            if restate_version.is_empty() {
                restate_types::RestateVersion::unknown()
            } else {
                restate_types::RestateVersion::new(restate_version)
            }
        }
    }

    pub mod lazy {
        use std::fmt::Display;

        use bytes::Bytes;
        use prost::Message;
        use restate_types::{
            errors::ConversionError,
            identifiers::{DeploymentId, InvocationId, SubscriptionId},
            invocation::ServiceType,
            service_protocol::ServiceProtocolVersion,
        };

        use crate::protobuf_types::v1::{
            InvocationTarget, ResponseResult, Source, SpanContextLite,
            pb_conversion::{expect_or_fail, try_bytes_into_trace_id},
            response_result, source,
        };

        fn merge_bytes_zerocopy<'a>(
            wire_type: prost::encoding::WireType,
            value: &mut &'a [u8],
            buf: &mut &'a [u8],
            _ctx: prost::encoding::DecodeContext,
        ) -> Result<(), prost::DecodeError> {
            use prost::encoding::*;
            check_wire_type(WireType::LengthDelimited, wire_type)?;
            let len = decode_varint(buf)?;
            if len > buf.len() as u64 {
                return Err(prost::DecodeError::new("buffer underflow"));
            }
            let len = len as usize;

            (*value, *buf) = buf.split_at(len);
            Ok(())
        }

        #[derive(Debug, Default)]
        pub struct InvocationStatusV2Lazy<'a> {
            pub inner: super::InvocationStatusV2Lazy,
            // 2
            pub invocation_target_lazy: Option<&'a [u8]>,
            // 3
            pub source_lazy: Option<&'a [u8]>,
            // 4
            pub span_context_lazy: Option<&'a [u8]>,
            // 11
            pub completion_retention_duration_lazy: Option<&'a [u8]>,
            // 12
            pub idempotency_key: Option<&'a [u8]>,
            // 15
            pub deployment_id: Option<&'a [u8]>,
            // 18
            pub result_lazy: Option<&'a [u8]>,
            // 29
            pub journal_retention_duration_lazy: Option<&'a [u8]>,
            // 30
            pub created_using_restate_version: &'a [u8],
        }

        impl<'a> InvocationStatusV2Lazy<'a> {
            pub fn decode(
                mut buf: &'a [u8],
            ) -> Result<InvocationStatusV2Lazy<'a>, prost::DecodeError> {
                let mut message = InvocationStatusV2Lazy::default();
                let ctx = prost::encoding::DecodeContext::default();
                while !buf.is_empty() {
                    let (tag, wire_type) = prost::encoding::decode_key(&mut buf)?;
                    message.merge_field(tag, wire_type, &mut buf, ctx.clone())?;
                }

                Ok(message)
            }

            fn merge_field<'b>(
                &mut self,
                tag: u32,
                wire_type: ::prost::encoding::wire_type::WireType,
                buf: &'b mut &'a [u8],
                ctx: ::prost::encoding::DecodeContext,
            ) -> ::core::result::Result<(), ::prost::DecodeError> {
                const STRUCT_NAME: &str = "InvocationStatusV2Lazy";
                match tag {
                    2u32 => {
                        let value = &mut self.invocation_target_lazy;
                        merge_bytes_zerocopy(wire_type, value.get_or_insert_default(), buf, ctx)
                            .map_err(|mut error| {
                                error.push(STRUCT_NAME, "invocation_target_lazy");
                                error
                            })
                    }
                    3u32 => {
                        let value = &mut self.source_lazy;
                        merge_bytes_zerocopy(wire_type, value.get_or_insert_default(), buf, ctx)
                            .map_err(|mut error| {
                                error.push(STRUCT_NAME, "source_lazy");
                                error
                            })
                    }
                    4u32 => {
                        let value = &mut self.span_context_lazy;
                        merge_bytes_zerocopy(wire_type, value.get_or_insert_default(), buf, ctx)
                            .map_err(|mut error| {
                                error.push(STRUCT_NAME, "span_context_lazy");
                                error
                            })
                    }
                    11u32 => {
                        let value = &mut self.completion_retention_duration_lazy;
                        merge_bytes_zerocopy(wire_type, value.get_or_insert_default(), buf, ctx)
                            .map_err(|mut error| {
                                error.push(STRUCT_NAME, "completion_retention_duration_lazy");
                                error
                            })
                    }
                    12u32 => {
                        let value = &mut self.idempotency_key;
                        merge_bytes_zerocopy(wire_type, value.get_or_insert_default(), buf, ctx)
                            .map_err(|mut error| {
                                error.push(STRUCT_NAME, "idempotency_key");
                                error
                            })
                    }
                    15u32 => {
                        let value = &mut self.deployment_id;
                        merge_bytes_zerocopy(wire_type, value.get_or_insert_default(), buf, ctx)
                            .map_err(|mut error| {
                                error.push(STRUCT_NAME, "deployment_id");
                                error
                            })
                    }
                    18u32 => {
                        let value = &mut self.result_lazy;
                        merge_bytes_zerocopy(wire_type, value.get_or_insert_default(), buf, ctx)
                            .map_err(|mut error| {
                                error.push(STRUCT_NAME, "result_lazy");
                                error
                            })
                    }
                    29u32 => {
                        let value = &mut self.journal_retention_duration_lazy;
                        merge_bytes_zerocopy(wire_type, value.get_or_insert_default(), buf, ctx)
                            .map_err(|mut error| {
                                error.push(STRUCT_NAME, "journal_retention_duration_lazy");
                                error
                            })
                    }
                    30u32 => {
                        let value = &mut self.created_using_restate_version;
                        merge_bytes_zerocopy(wire_type, value, buf, ctx).map_err(|mut error| {
                            error.push(STRUCT_NAME, "created_using_restate_version");
                            error
                        })
                    }
                    _ => prost::Message::merge_field(&mut self.inner, tag, wire_type, buf, ctx),
                }
            }

            pub fn service_protocol_version(
                &self,
            ) -> std::result::Result<std::option::Option<ServiceProtocolVersion>, ConversionError>
            {
                match self.inner.service_protocol_version {
                    Some(service_protocol_version) => {
                        ServiceProtocolVersion::try_from(service_protocol_version)
                            .map_err(|_| ConversionError::invalid_data("service_protocol_version"))
                            .map(Some)
                    }
                    None => Ok(None),
                }
            }

            pub fn invocation_target(
                &self,
            ) -> std::result::Result<Option<InvocationTarget>, ConversionError> {
                use super::invocation_status_v2::Status;
                match self.inner.status() {
                    Status::Scheduled
                    | Status::Inboxed
                    | Status::Invoked
                    | Status::Suspended
                    | Status::Paused
                    | Status::Completed => {}
                    Status::UnknownStatus => return Ok(None),
                }

                let invocation_target = self.invocation_target_lazy.as_ref();
                let invocation_target = expect_or_fail!(invocation_target)?;

                // almost of invocation_target is bytes in 2 or 3 fields. if we provide the data as a slice,
                // prost will copy those fields out separately into bytes (2 or 3 small allocations). if we instead
                // do one copy now, prost will make each field a shallow reference into one heap-allocated buffer.

                let invocation_target = Bytes::copy_from_slice(invocation_target);

                InvocationTarget::decode(invocation_target)
                    .map_err(|_| ConversionError::invalid_data("invocation_target"))
                    .map(Some)
            }

            pub fn source(&self) -> std::result::Result<source::Source, ConversionError> {
                use super::invocation_status_v2::Status;
                match self.inner.status() {
                    Status::Scheduled
                    | Status::Inboxed
                    | Status::Invoked
                    | Status::Suspended
                    | Status::Paused
                    | Status::Completed => {}
                    Status::UnknownStatus => {
                        return Err(ConversionError::invalid_data("status"));
                    }
                }

                let source = self.source_lazy;
                let source = expect_or_fail!(source)?;

                // almost of source is bytes (ids and the invocation target). if we provide the data as a slice,
                // prost will copy those fields out separately into bytes. if we instead
                // do one copy now, prost will make each field a shallow reference into one heap-allocated buffer.

                let source = Bytes::copy_from_slice(source);

                let source =
                    Source::decode(source).map_err(|_| ConversionError::invalid_data("source"))?;

                source
                    .source
                    .ok_or(ConversionError::missing_field("source"))
            }

            pub fn trace_id(&self) -> Result<opentelemetry::trace::TraceId, ConversionError> {
                use ConversionError;
                let span_context = self.span_context_lazy.as_ref();
                let span_context = expect_or_fail!(span_context)?;

                let span_context = SpanContextLite::decode(*span_context)
                    .map_err(|_| ConversionError::invalid_data("span_context"))?;

                let trace_id = try_bytes_into_trace_id(span_context.trace_id.as_ref())
                    .map_err(|_| ConversionError::InvalidData("trace_id"))?;
                Ok(trace_id)
            }

            pub fn completion_retention_duration(
                &self,
            ) -> Result<std::time::Duration, ConversionError> {
                match self.completion_retention_duration_lazy {
                    Some(completion_retention_duration) => {
                        super::Duration::decode(completion_retention_duration).map_err(|_| {
                            ConversionError::invalid_data("completion_retention_duration")
                        })?
                    }
                    None => super::Duration::default(),
                }
                .try_into()
                .map_err(|_| ConversionError::invalid_data("completion_retention_duration"))
            }

            pub fn idempotency_key(&self) -> std::result::Result<Option<&str>, ConversionError> {
                self.idempotency_key
                    .map(str::from_utf8)
                    .transpose()
                    .map_err(|_| ConversionError::invalid_data("idempotency_key"))
            }

            pub fn deployment_id(
                &self,
            ) -> std::result::Result<Option<DeploymentId>, ConversionError> {
                use ConversionError;

                match self.deployment_id {
                    Some(deployment_id) => str::from_utf8(deployment_id)
                        .map_err(|_| ConversionError::invalid_data("deployment_id"))?
                        .parse()
                        .map_err(|_| ConversionError::invalid_data("deployment_id"))
                        .map(Some),
                    None => Ok(None),
                }
            }

            pub fn response_result(
                &self,
            ) -> Result<response_result::ResponseResult, ConversionError> {
                use ConversionError;

                let result = self.result_lazy.as_ref();
                let result = expect_or_fail!(result)?;

                let result = ResponseResult::decode(*result)
                    .map_err(|_| ConversionError::invalid_data("result"))?;

                let response_result = result.response_result.as_ref();
                let response_result = expect_or_fail!(response_result)?;

                Ok(response_result.clone())
            }

            pub fn journal_retention_duration(
                &self,
            ) -> Result<std::time::Duration, ConversionError> {
                match self.journal_retention_duration_lazy {
                    Some(journal_retention_duration) => {
                        super::Duration::decode(journal_retention_duration).map_err(|_| {
                            ConversionError::invalid_data("journal_retention_duration")
                        })?
                    }
                    None => super::Duration::default(),
                }
                .try_into()
                .map_err(|_| ConversionError::invalid_data("journal_retention_duration"))
            }

            pub fn created_using_restate_version(
                &self,
            ) -> std::result::Result<&str, ConversionError> {
                if self.created_using_restate_version.is_empty() {
                    Ok(restate_types::RestateVersion::UNKNOWN_STR)
                } else {
                    str::from_utf8(self.created_using_restate_version)
                        .map_err(|_| ConversionError::invalid_data("created_using_restate_version"))
                }
            }
        }

        impl super::source::Service {
            pub fn invocation_target(
                &self,
            ) -> std::result::Result<&InvocationTarget, ConversionError> {
                self.invocation_target
                    .as_ref()
                    .ok_or(ConversionError::missing_field("invocation_target"))
            }

            pub fn invocation_id(&self) -> std::result::Result<InvocationId, ConversionError> {
                InvocationId::try_from(
                    self.invocation_id
                        .as_ref()
                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                )
                .map_err(|_| ConversionError::invalid_data("invocation_id"))
            }
        }

        impl super::source::Subscription {
            pub fn subscription_id(&self) -> std::result::Result<SubscriptionId, ConversionError> {
                SubscriptionId::from_slice(self.subscription_id.as_ref())
                    .map_err(|_| ConversionError::invalid_data("subscription_id"))
            }
        }

        impl super::source::RestartAsNew {
            pub fn invocation_id(&self) -> std::result::Result<InvocationId, ConversionError> {
                InvocationId::try_from(
                    self.invocation_id
                        .as_ref()
                        .ok_or(ConversionError::missing_field("invocation_id"))?,
                )
                .map_err(|_| ConversionError::invalid_data("invocation_id"))
            }
        }

        impl InvocationTarget {
            pub fn service_name(&self) -> std::result::Result<&str, ConversionError> {
                str::from_utf8(self.name.as_ref())
                    .map_err(|_| ConversionError::invalid_data("name"))
            }

            pub fn key(&self) -> std::result::Result<Option<&str>, ConversionError> {
                use super::invocation_target::Ty;
                match Ty::try_from(self.service_and_handler_ty) {
                    Ok(
                        Ty::VirtualObjectExclusive
                        | Ty::VirtualObjectShared
                        | Ty::WorkflowWorkflow
                        | Ty::WorkflowShared,
                    ) => {
                        let key = str::from_utf8(self.key.as_ref())
                            .map_err(|_| ConversionError::invalid_data("key"))?;

                        Ok(Some(key))
                    }
                    Ok(Ty::Service) => Ok(None),
                    Err(_) | Ok(Ty::UnknownTy) => {
                        Err(ConversionError::invalid_data("service_and_handler_ty"))
                    }
                }
            }

            pub fn handler_name(&self) -> std::result::Result<&str, ConversionError> {
                str::from_utf8(self.handler.as_ref())
                    .map_err(|_| ConversionError::invalid_data("name"))
            }

            pub fn service_ty(&self) -> Result<ServiceType, ConversionError> {
                use super::invocation_target::Ty;
                match Ty::try_from(self.service_and_handler_ty) {
                    Ok(Ty::Service) => Ok(ServiceType::Service),
                    Ok(Ty::VirtualObjectExclusive | Ty::VirtualObjectShared) => {
                        Ok(ServiceType::VirtualObject)
                    }
                    Ok(Ty::WorkflowWorkflow | Ty::WorkflowShared) => Ok(ServiceType::Workflow),
                    Err(_) | Ok(Ty::UnknownTy) => {
                        Err(ConversionError::invalid_data("service_and_handler_ty"))
                    }
                }
            }

            pub fn target_fmt<'a>(
                &'a self,
            ) -> std::result::Result<TargetFormatter<'a>, ConversionError> {
                let service_name = self.service_name()?;
                let key = self.key()?;
                let handler_name = self.handler_name()?;
                Ok(TargetFormatter(service_name, key, handler_name))
            }
        }

        pub struct TargetFormatter<'a>(&'a str, Option<&'a str>, &'a str);

        impl<'a> Display for TargetFormatter<'a> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                if let Some(key) = self.1 {
                    write!(f, "{}/{}/{}", self.0, key, self.2)
                } else {
                    write!(f, "{}/{}", self.0, self.2)
                }
            }
        }
    }
}
