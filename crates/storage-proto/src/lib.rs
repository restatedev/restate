// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod storage {
    pub mod v1 {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(
            env!("OUT_DIR"),
            "/dev.restate.storage.domain.v1.rs"
        ));

        #[cfg(feature = "conversion")]
        pub mod pb_conversion {
            use crate::storage::v1::enriched_entry_header::{
                Awakeable, BackgroundCall, ClearAllState, ClearState, CompleteAwakeable, Custom,
                GetState, GetStateKeys, Invoke, OutputStream, PollInputStream, SetState, Sleep,
            };
            use crate::storage::v1::invocation_status::{Free, Invoked, Suspended, Virtual};
            use crate::storage::v1::journal_entry::completion_result::{Empty, Failure, Success};
            use crate::storage::v1::journal_entry::{
                completion_result, CompletionResult, Entry, Kind,
            };
            use crate::storage::v1::outbox_message::{
                OutboxCancel, OutboxIngressResponse, OutboxKill, OutboxServiceInvocation,
                OutboxServiceInvocationResponse,
            };
            use crate::storage::v1::service_invocation_response_sink::{
                Ingress, NewInvocation, PartitionProcessor, ResponseSink,
            };
            use crate::storage::v1::{
                enriched_entry_header, inbox_entry, invocation_resolution_result,
                invocation_status, maybe_full_invocation_id, outbox_message, response_result,
                service_status, source, span_relation, timer, BackgroundCallResolutionResult,
                EnrichedEntryHeader, FullInvocationId, InboxEntry, InvocationResolutionResult,
                InvocationStatus, JournalEntry, JournalMeta, KvPair, MaybeFullInvocationId,
                OutboxMessage, ResponseResult, ServiceId, ServiceInvocation,
                ServiceInvocationResponseSink, ServiceStatus, Source, SpanContext, SpanRelation,
                StateMutation, Timer,
            };
            use anyhow::anyhow;
            use bytes::{Buf, Bytes};
            use bytestring::ByteString;
            use opentelemetry_api::trace::TraceState;
            use restate_storage_api::StorageError;
            use restate_types::identifiers::InvocationUuid;
            use restate_types::invocation::{InvocationTermination, TerminationFlavor};
            use restate_types::journal::enriched::AwakeableEnrichmentResult;
            use restate_types::time::MillisSinceEpoch;
            use restate_types::GenerationalNodeId;
            use std::collections::HashSet;
            use std::str::FromStr;

            /// Error type for conversion related problems (e.g. Rust <-> Protobuf)
            #[derive(Debug, thiserror::Error)]
            pub enum ConversionError {
                #[error("missing field '{0}'")]
                MissingField(&'static str),
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
            }

            impl From<ConversionError> for StorageError {
                fn from(value: ConversionError) -> Self {
                    StorageError::Conversion(value.into())
                }
            }

            impl TryFrom<ServiceStatus> for InvocationUuid {
                type Error = ConversionError;

                fn try_from(value: ServiceStatus) -> Result<Self, Self::Error> {
                    Ok(
                        match value
                            .status
                            .ok_or(ConversionError::missing_field("status"))?
                        {
                            service_status::Status::Locked(locked) => {
                                try_bytes_into_invocation_uuid(locked.invocation_uuid)?
                            }
                        },
                    )
                }
            }

            impl From<restate_storage_api::service_status_table::ServiceStatus> for ServiceStatus {
                fn from(value: restate_storage_api::service_status_table::ServiceStatus) -> Self {
                    match value {
                        restate_storage_api::service_status_table::ServiceStatus::Locked(
                            invocation_id,
                        ) => ServiceStatus {
                            status: Some(service_status::Status::Locked(service_status::Locked {
                                invocation_uuid: invocation_id
                                    .invocation_uuid()
                                    .to_bytes()
                                    .to_vec()
                                    .into(),
                            })),
                        },
                        restate_storage_api::service_status_table::ServiceStatus::Unlocked => {
                            unreachable!("Nothing should be stored for unlocked")
                        }
                    }
                }
            }

            impl TryFrom<InvocationStatus> for restate_storage_api::invocation_status_table::InvocationStatus {
                type Error = ConversionError;

                fn try_from(value: InvocationStatus) -> Result<Self, Self::Error> {
                    let result = match value
                        .status
                        .ok_or(ConversionError::missing_field("status"))?
                    {
                        invocation_status::Status::Invoked(invoked) => {
                            let invocation_metadata =
                                restate_storage_api::invocation_status_table::InvocationMetadata::try_from(
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
                                waiting_for_completed_entries,
                            }
                        }
                        invocation_status::Status::Virtual(r#virtual) => {
                            let (
                                journal_metadata,
                                completion_notification_target,
                                kill_notification_target,
                                timestamps,
                            ) = r#virtual.try_into()?;
                            restate_storage_api::invocation_status_table::InvocationStatus::Virtual {
                                journal_metadata,
                                completion_notification_target,
                                kill_notification_target,
                                timestamps,
                            }
                        }
                        invocation_status::Status::Free(_) => {
                            restate_storage_api::invocation_status_table::InvocationStatus::Free
                        }
                    };

                    Ok(result)
                }
            }

            impl From<restate_storage_api::invocation_status_table::InvocationStatus> for InvocationStatus {
                fn from(
                    value: restate_storage_api::invocation_status_table::InvocationStatus,
                ) -> Self {
                    let status = match value {
                        restate_storage_api::invocation_status_table::InvocationStatus::Invoked(
                            invoked_status,
                        ) => invocation_status::Status::Invoked(Invoked::from(invoked_status)),
                        restate_storage_api::invocation_status_table::InvocationStatus::Suspended {
                            metadata,
                            waiting_for_completed_entries,
                        } => invocation_status::Status::Suspended(Suspended::from((
                            metadata,
                            waiting_for_completed_entries,
                        ))),
                        restate_storage_api::invocation_status_table::InvocationStatus::Virtual {
                            journal_metadata,
                            completion_notification_target,
                            kill_notification_target,
                            timestamps,
                        } => invocation_status::Status::Virtual(Virtual::from((
                            journal_metadata,
                            completion_notification_target,
                            kill_notification_target,
                            timestamps,
                        ))),
                        restate_storage_api::invocation_status_table::InvocationStatus::Free => {
                            invocation_status::Status::Free(Free {})
                        }
                    };

                    InvocationStatus {
                        status: Some(status),
                    }
                }
            }

            impl TryFrom<Invoked> for restate_storage_api::invocation_status_table::InvocationMetadata {
                type Error = ConversionError;

                fn try_from(value: Invoked) -> Result<Self, Self::Error> {
                    let service_id = value
                        .service_id
                        .ok_or(ConversionError::missing_field("service_id"))?
                        .try_into()?;

                    let method_name = value.method_name.try_into().map_err(|e| {
                        ConversionError::InvalidData(anyhow!(
                            "Cannot decode method_name string {e}"
                        ))
                    })?;
                    let deployment_id = value.deployment_id.and_then(|one_of_deployment_id| {
                        match one_of_deployment_id {
                            invocation_status::invoked::DeploymentId::None(_) => None,
                            invocation_status::invoked::DeploymentId::Value(id) => {
                                Some(id.parse().expect("valid deployment id"))
                            }
                        }
                    });

                    let journal_metadata =
                        restate_storage_api::invocation_status_table::JournalMetadata::try_from(
                            value
                                .journal_meta
                                .ok_or(ConversionError::missing_field("journal_meta"))?,
                        )?;
                    let response_sink = Option::<
                        restate_types::invocation::ServiceInvocationResponseSink,
                    >::try_from(
                        value
                            .response_sink
                            .ok_or(ConversionError::missing_field("response_sink"))?,
                    )?;

                    let source = restate_types::invocation::Source::try_from(
                        value
                            .source
                            .ok_or(ConversionError::missing_field("source"))?,
                    )?;

                    Ok(
                        restate_storage_api::invocation_status_table::InvocationMetadata::new(
                            service_id,
                            journal_metadata,
                            deployment_id,
                            method_name,
                            response_sink,
                            restate_storage_api::invocation_status_table::StatusTimestamps::new(
                                MillisSinceEpoch::new(value.creation_time),
                                MillisSinceEpoch::new(value.modification_time),
                            ),
                            source,
                        ),
                    )
                }
            }

            impl From<restate_storage_api::invocation_status_table::InvocationMetadata> for Invoked {
                fn from(
                    value: restate_storage_api::invocation_status_table::InvocationMetadata,
                ) -> Self {
                    let restate_storage_api::invocation_status_table::InvocationMetadata {
                        service_id,
                        deployment_id,
                        method,
                        response_sink,
                        journal_metadata,
                        timestamps,
                        source,
                    } = value;

                    Invoked {
                        service_id: Some(service_id.into()),
                        response_sink: Some(ServiceInvocationResponseSink::from(response_sink)),
                        method_name: method.into_bytes(),
                        deployment_id: Some(match deployment_id {
                            None => invocation_status::invoked::DeploymentId::None(()),
                            Some(deployment_id) => invocation_status::invoked::DeploymentId::Value(
                                deployment_id.to_string(),
                            ),
                        }),
                        journal_meta: Some(JournalMeta::from(journal_metadata)),
                        creation_time: timestamps.creation_time().as_u64(),
                        modification_time: timestamps.modification_time().as_u64(),
                        source: Some(Source::from(source)),
                    }
                }
            }

            impl TryFrom<Suspended>
                for (
                    restate_storage_api::invocation_status_table::InvocationMetadata,
                    HashSet<restate_types::identifiers::EntryIndex>,
                )
            {
                type Error = ConversionError;

                fn try_from(value: Suspended) -> Result<Self, Self::Error> {
                    let service_id = value
                        .service_id
                        .ok_or(ConversionError::missing_field("service_id"))?
                        .try_into()?;

                    let method_name = value.method_name.try_into().map_err(|e| {
                        ConversionError::InvalidData(anyhow!(
                            "Cannot decode method_name string {e}"
                        ))
                    })?;
                    let deployment_id = value.deployment_id.and_then(|one_of_deployment_id| {
                        match one_of_deployment_id {
                            invocation_status::suspended::DeploymentId::None(_) => None,
                            invocation_status::suspended::DeploymentId::Value(id) => Some(id),
                        }
                    });

                    let journal_metadata =
                        restate_storage_api::invocation_status_table::JournalMetadata::try_from(
                            value
                                .journal_meta
                                .ok_or(ConversionError::missing_field("journal_meta"))?,
                        )?;
                    let response_sink = Option::<
                        restate_types::invocation::ServiceInvocationResponseSink,
                    >::try_from(
                        value
                            .response_sink
                            .ok_or(ConversionError::missing_field("response_sink"))?,
                    )?;

                    let waiting_for_completed_entries =
                        value.waiting_for_completed_entries.into_iter().collect();

                    let caller = restate_types::invocation::Source::try_from(
                        value
                            .source
                            .ok_or(ConversionError::missing_field("source"))?,
                    )?;

                    Ok((
                        restate_storage_api::invocation_status_table::InvocationMetadata::new(
                            service_id,
                            journal_metadata,
                            deployment_id.map(|d| d.parse().expect("valid deployment id")),
                            method_name,
                            response_sink,
                            restate_storage_api::invocation_status_table::StatusTimestamps::new(
                                MillisSinceEpoch::new(value.creation_time),
                                MillisSinceEpoch::new(value.modification_time),
                            ),
                            caller,
                        ),
                        waiting_for_completed_entries,
                    ))
                }
            }

            impl
                From<(
                    restate_storage_api::invocation_status_table::InvocationMetadata,
                    HashSet<restate_types::identifiers::EntryIndex>,
                )> for Suspended
            {
                fn from(
                    (metadata, waiting_for_completed_entries): (
                        restate_storage_api::invocation_status_table::InvocationMetadata,
                        HashSet<restate_types::identifiers::EntryIndex>,
                    ),
                ) -> Self {
                    let response_sink = ServiceInvocationResponseSink::from(metadata.response_sink);
                    let journal_meta = JournalMeta::from(metadata.journal_metadata);
                    let waiting_for_completed_entries =
                        waiting_for_completed_entries.into_iter().collect();

                    Suspended {
                        service_id: Some(metadata.service_id.into()),
                        response_sink: Some(response_sink),
                        journal_meta: Some(journal_meta),
                        method_name: metadata.method.into_bytes(),
                        deployment_id: Some(match metadata.deployment_id {
                            None => invocation_status::suspended::DeploymentId::None(()),
                            Some(deployment_id) => {
                                invocation_status::suspended::DeploymentId::Value(
                                    deployment_id.to_string(),
                                )
                            }
                        }),
                        creation_time: metadata.timestamps.creation_time().as_u64(),
                        modification_time: metadata.timestamps.modification_time().as_u64(),
                        waiting_for_completed_entries,
                        source: Some(Source::from(metadata.source)),
                    }
                }
            }

            impl TryFrom<Virtual>
                for (
                    restate_storage_api::invocation_status_table::JournalMetadata,
                    restate_storage_api::invocation_status_table::NotificationTarget,
                    restate_storage_api::invocation_status_table::NotificationTarget,
                    restate_storage_api::invocation_status_table::StatusTimestamps,
                )
            {
                type Error = ConversionError;

                fn try_from(value: Virtual) -> Result<Self, Self::Error> {
                    let journal_metadata =
                        restate_storage_api::invocation_status_table::JournalMetadata::try_from(
                            value
                                .journal_meta
                                .ok_or(ConversionError::missing_field("journal_meta"))?,
                        )?;
                    let completion_notification_target =
                        restate_storage_api::invocation_status_table::NotificationTarget {
                            service: restate_types::identifiers::ServiceId::new(
                                value.completion_notification_target_service_name,
                                value.completion_notification_target_service_key,
                            ),
                            method: value.completion_notification_target_method,
                        };
                    let kill_notification_target =
                        restate_storage_api::invocation_status_table::NotificationTarget {
                            service: restate_types::identifiers::ServiceId::new(
                                value.kill_notification_target_service_name,
                                value.kill_notification_target_service_key,
                            ),
                            method: value.kill_notification_target_method,
                        };
                    let timestamps =
                        restate_storage_api::invocation_status_table::StatusTimestamps::new(
                            MillisSinceEpoch::new(value.creation_time),
                            MillisSinceEpoch::new(value.modification_time),
                        );

                    Ok((
                        journal_metadata,
                        completion_notification_target,
                        kill_notification_target,
                        timestamps,
                    ))
                }
            }

            impl
                From<(
                    restate_storage_api::invocation_status_table::JournalMetadata,
                    restate_storage_api::invocation_status_table::NotificationTarget,
                    restate_storage_api::invocation_status_table::NotificationTarget,
                    restate_storage_api::invocation_status_table::StatusTimestamps,
                )> for Virtual
            {
                fn from(
                    (
                        journal_metadata,
                        completion_notification_target,
                        kill_notification_target,
                        timestamps,
                    ): (
                        restate_storage_api::invocation_status_table::JournalMetadata,
                        restate_storage_api::invocation_status_table::NotificationTarget,
                        restate_storage_api::invocation_status_table::NotificationTarget,
                        restate_storage_api::invocation_status_table::StatusTimestamps,
                    ),
                ) -> Self {
                    let journal_meta = JournalMeta::from(journal_metadata);

                    Virtual {
                        journal_meta: Some(journal_meta),
                        completion_notification_target_service_name: completion_notification_target
                            .service
                            .service_name
                            .to_string(),
                        completion_notification_target_service_key: completion_notification_target
                            .service
                            .key,
                        completion_notification_target_method: completion_notification_target
                            .method,
                        kill_notification_target_service_name: kill_notification_target
                            .service
                            .service_name
                            .to_string(),
                        kill_notification_target_service_key: kill_notification_target.service.key,
                        kill_notification_target_method: kill_notification_target.method,
                        creation_time: timestamps.creation_time().as_u64(),
                        modification_time: timestamps.modification_time().as_u64(),
                    }
                }
            }

            impl TryFrom<JournalMeta> for restate_storage_api::invocation_status_table::JournalMetadata {
                type Error = ConversionError;

                fn try_from(value: JournalMeta) -> Result<Self, Self::Error> {
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
                fn from(
                    value: restate_storage_api::invocation_status_table::JournalMetadata,
                ) -> Self {
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

                fn try_from(value: Source) -> Result<Self, Self::Error> {
                    let source = match value
                        .source
                        .ok_or(ConversionError::missing_field("source"))?
                    {
                        source::Source::Ingress(_) => restate_types::invocation::Source::Ingress,
                        source::Source::Service(fid) => restate_types::invocation::Source::Service(
                            restate_types::identifiers::FullInvocationId::try_from(fid)?,
                        ),
                        source::Source::Internal(_) => restate_types::invocation::Source::Internal,
                    };

                    Ok(source)
                }
            }

            impl From<restate_types::invocation::Source> for Source {
                fn from(value: restate_types::invocation::Source) -> Self {
                    let source = match value {
                        restate_types::invocation::Source::Ingress => source::Source::Ingress(()),
                        restate_types::invocation::Source::Service(fid) => {
                            source::Source::Service(FullInvocationId::from(fid))
                        }
                        restate_types::invocation::Source::Internal => source::Source::Internal(()),
                    };

                    Source {
                        source: Some(source),
                    }
                }
            }

            impl TryFrom<InboxEntry> for restate_storage_api::inbox_table::InboxEntry {
                type Error = ConversionError;

                fn try_from(value: InboxEntry) -> Result<Self, Self::Error> {
                    // Backwards compatibility to support Restate <= 0.7
                    let inbox_entry = if let Some(service_invocation) = value.service_invocation {
                        restate_storage_api::inbox_table::InboxEntry::Invocation(
                            restate_types::invocation::ServiceInvocation::try_from(
                                service_invocation,
                            )?,
                        )
                    } else {
                        // All InboxEntries starting with Restate >= 0.7.1 should have the entry field set
                        match value.entry.ok_or(ConversionError::missing_field("entry"))? {
                            inbox_entry::Entry::Invocation(service_invocation) => {
                                restate_storage_api::inbox_table::InboxEntry::Invocation(
                                    restate_types::invocation::ServiceInvocation::try_from(
                                        service_invocation,
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
                        }
                    };

                    Ok(inbox_entry)
                }
            }

            impl From<restate_storage_api::inbox_table::InboxEntry> for InboxEntry {
                fn from(inbox_entry: restate_storage_api::inbox_table::InboxEntry) -> Self {
                    let inbox_entry = match inbox_entry {
                        restate_storage_api::inbox_table::InboxEntry::Invocation(
                            service_invocation,
                        ) => inbox_entry::Entry::Invocation(ServiceInvocation::from(
                            service_invocation,
                        )),
                        restate_storage_api::inbox_table::InboxEntry::StateMutation(
                            state_mutation,
                        ) => inbox_entry::Entry::StateMutation(StateMutation::from(state_mutation)),
                    };

                    InboxEntry {
                        // Backwards compatibility to support Restate <= 0.7
                        service_invocation: None,
                        entry: Some(inbox_entry),
                    }
                }
            }

            impl TryFrom<ServiceInvocation> for restate_types::invocation::ServiceInvocation {
                type Error = ConversionError;

                fn try_from(value: ServiceInvocation) -> Result<Self, Self::Error> {
                    let ServiceInvocation {
                        id,
                        method_name,
                        response_sink,
                        span_context,
                        argument,
                        source,
                    } = value;

                    let id = restate_types::identifiers::FullInvocationId::try_from(
                        id.ok_or(ConversionError::missing_field("id"))?,
                    )?;

                    let span_context =
                        restate_types::invocation::ServiceInvocationSpanContext::try_from(
                            span_context.ok_or(ConversionError::missing_field("span_context"))?,
                        )?;

                    let response_sink = Option::<
                        restate_types::invocation::ServiceInvocationResponseSink,
                    >::try_from(
                        response_sink.ok_or(ConversionError::missing_field("response_sink"))?,
                    )?;

                    let method_name =
                        ByteString::try_from(method_name).map_err(ConversionError::invalid_data)?;

                    let source = restate_types::invocation::Source::try_from(
                        source.ok_or(ConversionError::missing_field("source"))?,
                    )?;

                    Ok(restate_types::invocation::ServiceInvocation {
                        fid: id,
                        method_name,
                        argument,
                        source,
                        response_sink,
                        span_context,
                    })
                }
            }

            impl From<restate_types::invocation::ServiceInvocation> for ServiceInvocation {
                fn from(value: restate_types::invocation::ServiceInvocation) -> Self {
                    let id = FullInvocationId::from(value.fid);
                    let span_context = SpanContext::from(value.span_context);
                    let response_sink = ServiceInvocationResponseSink::from(value.response_sink);
                    let method_name = value.method_name.into_bytes();
                    let source = Source::from(value.source);

                    ServiceInvocation {
                        id: Some(id),
                        span_context: Some(span_context),
                        response_sink: Some(response_sink),
                        method_name,
                        argument: value.argument,
                        source: Some(source),
                    }
                }
            }

            impl TryFrom<StateMutation> for restate_types::state_mut::ExternalStateMutation {
                type Error = ConversionError;

                fn try_from(state_mutation: StateMutation) -> Result<Self, Self::Error> {
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

            impl TryFrom<ServiceId> for restate_types::identifiers::ServiceId {
                type Error = ConversionError;

                fn try_from(service_id: ServiceId) -> Result<Self, Self::Error> {
                    Ok(restate_types::identifiers::ServiceId::new(
                        ByteString::try_from(service_id.service_name)
                            .map_err(ConversionError::invalid_data)?,
                        service_id.service_key,
                    ))
                }
            }

            impl From<restate_types::identifiers::ServiceId> for ServiceId {
                fn from(service_id: restate_types::identifiers::ServiceId) -> Self {
                    ServiceId {
                        service_key: service_id.key,
                        service_name: service_id.service_name.into_bytes(),
                    }
                }
            }

            impl TryFrom<FullInvocationId> for restate_types::identifiers::FullInvocationId {
                type Error = ConversionError;

                fn try_from(value: FullInvocationId) -> Result<Self, Self::Error> {
                    let FullInvocationId {
                        service_name,
                        service_key,
                        invocation_uuid,
                    } = value;

                    let service_name = ByteString::try_from(service_name)
                        .map_err(ConversionError::invalid_data)?;
                    let invocation_uuid = try_bytes_into_invocation_uuid(invocation_uuid)?;

                    Ok(restate_types::identifiers::FullInvocationId::new(
                        service_name,
                        service_key,
                        invocation_uuid,
                    ))
                }
            }

            impl From<restate_types::identifiers::FullInvocationId> for FullInvocationId {
                fn from(value: restate_types::identifiers::FullInvocationId) -> Self {
                    let service_key = value.service_id.key;
                    let service_name = value.service_id.service_name.into_bytes();

                    FullInvocationId {
                        invocation_uuid: value.invocation_uuid.into(),
                        service_key,
                        service_name,
                    }
                }
            }

            impl TryFrom<MaybeFullInvocationId> for restate_types::invocation::MaybeFullInvocationId {
                type Error = ConversionError;

                fn try_from(value: MaybeFullInvocationId) -> Result<Self, Self::Error> {
                    match value.kind.ok_or(ConversionError::missing_field("kind"))? {
                        maybe_full_invocation_id::Kind::FullInvocationId(fid) => {
                            Ok(restate_types::invocation::MaybeFullInvocationId::Full(
                                restate_types::identifiers::FullInvocationId::try_from(fid)?,
                            ))
                        }
                        maybe_full_invocation_id::Kind::InvocationId(invocation_id) => {
                            Ok(restate_types::invocation::MaybeFullInvocationId::Partial(
                                restate_types::identifiers::InvocationId::from_slice(
                                    &invocation_id,
                                )
                                .map_err(|e| ConversionError::invalid_data(e))?,
                            ))
                        }
                    }
                }
            }

            impl From<restate_types::invocation::MaybeFullInvocationId> for MaybeFullInvocationId {
                fn from(value: restate_types::invocation::MaybeFullInvocationId) -> Self {
                    match value {
                        restate_types::invocation::MaybeFullInvocationId::Full(fid) => {
                            MaybeFullInvocationId {
                                kind: Some(maybe_full_invocation_id::Kind::FullInvocationId(
                                    FullInvocationId::from(fid),
                                )),
                            }
                        }
                        restate_types::invocation::MaybeFullInvocationId::Partial(
                            invocation_id,
                        ) => MaybeFullInvocationId {
                            kind: Some(maybe_full_invocation_id::Kind::InvocationId(
                                Bytes::copy_from_slice(&invocation_id.to_bytes()),
                            )),
                        },
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

                fn try_from(value: SpanContext) -> Result<Self, Self::Error> {
                    let SpanContext {
                        trace_id,
                        span_id,
                        trace_flags,
                        is_remote,
                        trace_state,
                        span_relation,
                    } = value;

                    let trace_id = try_bytes_into_trace_id(trace_id)?;
                    let span_id =
                        opentelemetry_api::trace::SpanId::from_bytes(span_id.to_be_bytes());
                    let trace_flags = opentelemetry_api::trace::TraceFlags::new(
                        u8::try_from(trace_flags).map_err(ConversionError::invalid_data)?,
                    );

                    let trace_state = TraceState::from_str(&trace_state)
                        .map_err(ConversionError::invalid_data)?;

                    let span_relation = span_relation
                        .map(|span_relation| span_relation.try_into())
                        .transpose()
                        .map_err(ConversionError::invalid_data)?;

                    Ok(
                        restate_types::invocation::ServiceInvocationSpanContext::new(
                            opentelemetry_api::trace::SpanContext::new(
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

                fn try_from(value: SpanRelation) -> Result<Self, Self::Error> {
                    match value.kind.ok_or(ConversionError::missing_field("kind"))? {
                        span_relation::Kind::Parent(span_relation::Parent { span_id }) => {
                            let span_id =
                                opentelemetry_api::trace::SpanId::from_bytes(span_id.to_be_bytes());
                            Ok(Self::Parent(span_id))
                        }
                        span_relation::Kind::Linked(span_relation::Linked {
                            trace_id,
                            span_id,
                        }) => {
                            let trace_id = try_bytes_into_trace_id(trace_id)?;
                            let span_id =
                                opentelemetry_api::trace::SpanId::from_bytes(span_id.to_be_bytes());
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
            ) -> Result<opentelemetry_api::trace::TraceId, ConversionError> {
                if bytes.len() != 16 {
                    return Err(ConversionError::InvalidData(anyhow!(
                        "trace id pb definition needs to contain exactly 16 bytes"
                    )));
                }

                let mut bytes_array = [0; 16];
                bytes.copy_to_slice(&mut bytes_array);

                Ok(opentelemetry_api::trace::TraceId::from_bytes(bytes_array))
            }

            impl TryFrom<ServiceInvocationResponseSink>
                for Option<restate_types::invocation::ServiceInvocationResponseSink>
            {
                type Error = ConversionError;

                fn try_from(value: ServiceInvocationResponseSink) -> Result<Self, Self::Error> {
                    let response_sink = match value
                        .response_sink
                        .ok_or(ConversionError::missing_field("response_sink"))?
                    {
                        ResponseSink::PartitionProcessor(partition_processor) => {
                            let caller = restate_types::identifiers::FullInvocationId::try_from(
                                partition_processor
                                    .caller
                                    .ok_or(ConversionError::missing_field("caller"))?,
                            )?;
                            Some(
                                restate_types::invocation::ServiceInvocationResponseSink::PartitionProcessor {
                                    caller,
                                    entry_index: partition_processor.entry_index,
                                },
                            )
                        }
                        ResponseSink::Ingress(ingress) => {
                            let proto_id = ingress
                                .node_id
                                .ok_or(ConversionError::missing_field("node_id"))?;

                            Some(
                                restate_types::invocation::ServiceInvocationResponseSink::Ingress(
                                    GenerationalNodeId::new(proto_id.id, proto_id.generation),
                                ),
                            )
                        }
                        ResponseSink::NewInvocation(new_invocation) => {
                            let target = restate_types::identifiers::FullInvocationId::try_from(
                                new_invocation
                                    .target
                                    .ok_or(ConversionError::missing_field("target"))?,
                            )?;
                            Some(
                                restate_types::invocation::ServiceInvocationResponseSink::NewInvocation {
                                    target,
                                    method: new_invocation.method,
                                    caller_context: new_invocation.caller_context,
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
                            caller: Some(FullInvocationId::from(caller)),
                        }),
                        Some(restate_types::invocation::ServiceInvocationResponseSink::Ingress(node_id)) => {
                            ResponseSink::Ingress(Ingress {
                                node_id: Some(super::GenerationalNodeId::from(node_id)),
                            })
                        },
                        Some(
                            restate_types::invocation::ServiceInvocationResponseSink::NewInvocation {
                               target, method, caller_context
                            },
                        ) => ResponseSink::NewInvocation(NewInvocation {
                            method,
                            target: Some(FullInvocationId::from(target)),
                            caller_context
                        }),
                        None => ResponseSink::None(Default::default()),
                    };

                    ServiceInvocationResponseSink {
                        response_sink: Some(response_sink),
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

                fn try_from(value: JournalEntry) -> Result<Self, Self::Error> {
                    let journal_entry =
                        match value.kind.ok_or(ConversionError::missing_field("kind"))? {
                            Kind::Entry(journal_entry) => {
                                restate_storage_api::journal_table::JournalEntry::Entry(
                                    restate_types::journal::enriched::EnrichedRawEntry::try_from(
                                        journal_entry,
                                    )?,
                                )
                            }
                            Kind::CompletionResult(completion_result) => {
                                restate_storage_api::journal_table::JournalEntry::Completion(
                                    restate_types::journal::CompletionResult::try_from(
                                        completion_result,
                                    )?,
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
                        restate_storage_api::journal_table::JournalEntry::Completion(
                            completion,
                        ) => JournalEntry::from(completion),
                    }
                }
            }

            impl From<restate_types::journal::enriched::EnrichedRawEntry> for JournalEntry {
                fn from(value: restate_types::journal::enriched::EnrichedRawEntry) -> Self {
                    let entry = Entry::from(value);

                    JournalEntry {
                        kind: Some(Kind::Entry(entry)),
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

            impl TryFrom<Entry> for restate_types::journal::enriched::EnrichedRawEntry {
                type Error = ConversionError;

                fn try_from(value: Entry) -> Result<Self, Self::Error> {
                    let Entry { header, raw_entry } = value;

                    let header = restate_types::journal::enriched::EnrichedEntryHeader::try_from(
                        header.ok_or(ConversionError::missing_field("header"))?,
                    )?;

                    Ok(restate_types::journal::enriched::EnrichedRawEntry::new(
                        header, raw_entry,
                    ))
                }
            }

            impl From<restate_types::journal::enriched::EnrichedRawEntry> for Entry {
                fn from(value: restate_types::journal::enriched::EnrichedRawEntry) -> Self {
                    let (header, entry) = value.into_inner();
                    Entry {
                        header: Some(EnrichedEntryHeader::from(header)),
                        raw_entry: entry,
                    }
                }
            }

            impl TryFrom<CompletionResult> for restate_types::journal::CompletionResult {
                type Error = ConversionError;

                fn try_from(value: CompletionResult) -> Result<Self, Self::Error> {
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

                fn try_from(value: EnrichedEntryHeader) -> Result<Self, Self::Error> {
                    // By definition of requires_ack, if it reached the journal storage then
                    // either there is one in-flight stream that already got notified of this entry ack,
                    // or there are no in-flight streams and the entry won't need any ack because it's in the replayed journal.

                    let enriched_header = match value
                        .kind
                        .ok_or(ConversionError::missing_field("kind"))?
                    {
                        enriched_entry_header::Kind::PollInputStream(poll_input_stream) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::PollInputStream {
                                                            is_completed: poll_input_stream.is_completed,
                            }
                        }
                        enriched_entry_header::Kind::OutputStream(_) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::OutputStream {
                                                        }
                        }
                        enriched_entry_header::Kind::GetState(get_state) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::GetState {
                                                            is_completed: get_state.is_completed,
                            }
                        }
                        enriched_entry_header::Kind::SetState(_) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::SetState {
                                                        }
                        }
                        enriched_entry_header::Kind::ClearState(_) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::ClearState {
                                                        }
                        },
                        enriched_entry_header::Kind::ClearAllState(_) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::ClearAllState {
                            }
                        }
                        enriched_entry_header::Kind::GetStateKeys(get_state_keys) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::GetStateKeys {
                                is_completed: get_state_keys.is_completed,
                            }
                        }
                        enriched_entry_header::Kind::Sleep(sleep) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::Sleep {
                                                            is_completed: sleep.is_completed,
                            }
                        }
                        enriched_entry_header::Kind::Invoke(invoke) => {
                            let enrichment_result = Option::<
                                restate_types::journal::enriched::InvokeEnrichmentResult,
                            >::try_from(
                                invoke
                                    .resolution_result
                                    .ok_or(ConversionError::missing_field("resolution_result"))?,
                            )?;

                            restate_types::journal::enriched::EnrichedEntryHeader::Invoke {
                                                            is_completed: invoke.is_completed,
                                enrichment_result,
                            }
                        }
                        enriched_entry_header::Kind::BackgroundCall(background_call) => {
                            let enrichment_result =
                                restate_types::journal::enriched::InvokeEnrichmentResult::try_from(
                                    background_call.resolution_result.ok_or(
                                        ConversionError::missing_field("resolution_result"),
                                    )?,
                                )?;

                            restate_types::journal::enriched::EnrichedEntryHeader::BackgroundInvoke {
                                                            enrichment_result,
                            }
                        }
                        enriched_entry_header::Kind::Awakeable(awakeable) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::Awakeable {
                                                            is_completed: awakeable.is_completed,
                            }
                        }
                        enriched_entry_header::Kind::CompleteAwakeable(CompleteAwakeable { invocation_id, entry_index }) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::CompleteAwakeable {
                                                            enrichment_result: AwakeableEnrichmentResult {
                                    invocation_id: restate_types::identifiers::InvocationId::from_slice(&invocation_id).map_err(ConversionError::invalid_data)?,
                                    entry_index,
                                },
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
                        restate_types::journal::enriched::EnrichedEntryHeader::PollInputStream {
                            is_completed,
                            ..
                        } => enriched_entry_header::Kind::PollInputStream(PollInputStream {
                            is_completed,
                        }),
                        restate_types::journal::enriched::EnrichedEntryHeader::OutputStream{..} => {
                            enriched_entry_header::Kind::OutputStream(OutputStream {})
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::GetState { is_completed, .. } => {
                            enriched_entry_header::Kind::GetState(GetState { is_completed })
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::SetState{..} => {
                            enriched_entry_header::Kind::SetState(SetState {})
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::ClearState{..} => {
                            enriched_entry_header::Kind::ClearState(ClearState {})
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::GetStateKeys { is_completed, .. } => {
                            enriched_entry_header::Kind::GetStateKeys(GetStateKeys { is_completed })
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::ClearAllState{..} => {
                            enriched_entry_header::Kind::ClearAllState(ClearAllState {})
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::Sleep { is_completed, .. } => {
                            enriched_entry_header::Kind::Sleep(Sleep { is_completed })
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::Invoke {
                            is_completed,
                            enrichment_result,
                            ..
                        } => enriched_entry_header::Kind::Invoke(Invoke {
                            is_completed,
                            resolution_result: Some(InvocationResolutionResult::from(
                                enrichment_result,
                            )),
                        }),
                        restate_types::journal::enriched::EnrichedEntryHeader::BackgroundInvoke {
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
                        restate_types::journal::enriched::EnrichedEntryHeader::CompleteAwakeable { enrichment_result, .. } => {
                            enriched_entry_header::Kind::CompleteAwakeable(CompleteAwakeable {
                                invocation_id: Bytes::copy_from_slice(&enrichment_result.invocation_id.to_bytes()),
                                entry_index: enrichment_result.entry_index
                            })
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::Custom {
                            code,
                            ..
                        } => enriched_entry_header::Kind::Custom(Custom {
                            code: u32::from(code),
                        }),
                    };

                    EnrichedEntryHeader { kind: Some(kind) }
                }
            }

            impl TryFrom<InvocationResolutionResult>
                for Option<restate_types::journal::enriched::InvokeEnrichmentResult>
            {
                type Error = ConversionError;

                fn try_from(value: InvocationResolutionResult) -> Result<Self, Self::Error> {
                    let result = match value
                        .result
                        .ok_or(ConversionError::missing_field("result"))?
                    {
                        invocation_resolution_result::Result::None(_) => None,
                        invocation_resolution_result::Result::Success(success) => {
                            let span_context =
                                restate_types::invocation::ServiceInvocationSpanContext::try_from(
                                    success
                                        .span_context
                                        .ok_or(ConversionError::missing_field("span_context"))?,
                                )?;
                            let invocation_uuid =
                                try_bytes_into_invocation_uuid(success.invocation_uuid)?;
                            let service_key = success.service_key;
                            let service_name = ByteString::try_from(success.service_name)
                                .map_err(ConversionError::invalid_data)?;

                            Some(restate_types::journal::enriched::InvokeEnrichmentResult {
                                span_context,
                                invocation_uuid,
                                service_key,
                                service_name,
                            })
                        }
                    };

                    Ok(result)
                }
            }

            impl From<Option<restate_types::journal::enriched::InvokeEnrichmentResult>>
                for InvocationResolutionResult
            {
                fn from(
                    value: Option<restate_types::journal::enriched::InvokeEnrichmentResult>,
                ) -> Self {
                    let result = match value {
                        None => invocation_resolution_result::Result::None(Default::default()),
                        Some(resolution_result) => match resolution_result {
                            restate_types::journal::enriched::InvokeEnrichmentResult {
                                invocation_uuid,
                                service_key,
                                service_name,
                                span_context,
                            } => invocation_resolution_result::Result::Success(
                                invocation_resolution_result::Success {
                                    invocation_uuid: invocation_uuid.into(),
                                    service_key,
                                    service_name: service_name.into_bytes(),
                                    span_context: Some(SpanContext::from(span_context)),
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
                for restate_types::journal::enriched::InvokeEnrichmentResult
            {
                type Error = ConversionError;

                fn try_from(value: BackgroundCallResolutionResult) -> Result<Self, Self::Error> {
                    let span_context =
                        restate_types::invocation::ServiceInvocationSpanContext::try_from(
                            value
                                .span_context
                                .ok_or(ConversionError::missing_field("span_context"))?,
                        )?;
                    let invocation_uuid = try_bytes_into_invocation_uuid(value.invocation_uuid)?;
                    let service_key = value.service_key;
                    let service_name = ByteString::try_from(value.service_name)
                        .map_err(ConversionError::invalid_data)?;

                    Ok(restate_types::journal::enriched::InvokeEnrichmentResult {
                        span_context,
                        invocation_uuid,
                        service_key,
                        service_name,
                    })
                }
            }

            impl From<restate_types::journal::enriched::InvokeEnrichmentResult>
                for BackgroundCallResolutionResult
            {
                fn from(value: restate_types::journal::enriched::InvokeEnrichmentResult) -> Self {
                    BackgroundCallResolutionResult {
                        invocation_uuid: value.invocation_uuid.into(),
                        service_key: value.service_key,
                        service_name: value.service_name.into_bytes(),
                        span_context: Some(SpanContext::from(value.span_context)),
                    }
                }
            }

            impl TryFrom<OutboxMessage> for restate_storage_api::outbox_table::OutboxMessage {
                type Error = ConversionError;

                fn try_from(value: OutboxMessage) -> Result<Self, Self::Error> {
                    let result = match value
                        .outbox_message
                        .ok_or(ConversionError::missing_field("outbox_message"))?
                    {
                        outbox_message::OutboxMessage::ServiceInvocationCase(
                            service_invocation,
                        ) => restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(
                            restate_types::invocation::ServiceInvocation::try_from(
                                service_invocation
                                    .service_invocation
                                    .ok_or(ConversionError::missing_field("service_invocation"))?,
                            )?,
                        ),
                        outbox_message::OutboxMessage::ServiceInvocationResponse(
                            invocation_response,
                        ) => restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(
                            restate_types::invocation::InvocationResponse {
                                entry_index: invocation_response.entry_index,
                                id: invocation_response
                                    .maybe_fid
                                    .ok_or(ConversionError::missing_field("maybe_fid"))?
                                    .try_into()?,
                                result: restate_types::invocation::ResponseResult::try_from(
                                    invocation_response
                                        .response_result
                                        .ok_or(ConversionError::missing_field("response_result"))?,
                                )?,
                            },
                        ),
                        outbox_message::OutboxMessage::Kill(outbox_kill) => {
                            let maybe_fid = outbox_kill.maybe_full_invocation_id.ok_or(
                                ConversionError::missing_field("maybe_full_invocation_id"),
                            )?;
                            restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(
                                InvocationTermination::kill(
                                    restate_types::invocation::MaybeFullInvocationId::try_from(
                                        maybe_fid,
                                    )?,
                                ),
                            )
                        }
                        outbox_message::OutboxMessage::Cancel(outbox_cancel) => {
                            let maybe_fid = outbox_cancel.maybe_full_invocation_id.ok_or(
                                ConversionError::missing_field("maybe_full_invocation_id"),
                            )?;
                            restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(
                                InvocationTermination::cancel(
                                    restate_types::invocation::MaybeFullInvocationId::try_from(
                                        maybe_fid,
                                    )?,
                                ),
                            )
                        }
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
                                service_invocation: Some(ServiceInvocation::from(
                                    service_invocation,
                                )),
                            },
                        ),
                        restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(
                            invocation_response,
                        ) => outbox_message::OutboxMessage::ServiceInvocationResponse(
                            OutboxServiceInvocationResponse {
                                entry_index: invocation_response.entry_index,
                                maybe_fid: Some(MaybeFullInvocationId::from(
                                    invocation_response.id,
                                )),
                                response_result: Some(ResponseResult::from(
                                    invocation_response.result,
                                )),
                            },
                        ),
                        restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(
                            invocation_termination,
                        ) => match invocation_termination.flavor {
                            TerminationFlavor::Kill => {
                                outbox_message::OutboxMessage::Kill(OutboxKill {
                                    maybe_full_invocation_id: Some(MaybeFullInvocationId::from(
                                        invocation_termination.maybe_fid,
                                    )),
                                })
                            }
                            TerminationFlavor::Cancel => {
                                outbox_message::OutboxMessage::Cancel(OutboxCancel {
                                    maybe_full_invocation_id: Some(MaybeFullInvocationId::from(
                                        invocation_termination.maybe_fid,
                                    )),
                                })
                            }
                        },
                    };

                    OutboxMessage {
                        outbox_message: Some(outbox_message),
                    }
                }
            }

            impl TryFrom<ResponseResult> for restate_types::invocation::ResponseResult {
                type Error = ConversionError;

                fn try_from(value: ResponseResult) -> Result<Self, Self::Error> {
                    let result = match value
                        .response_result
                        .ok_or(ConversionError::missing_field("response_result"))?
                    {
                        response_result::ResponseResult::ResponseSuccess(success) => {
                            restate_types::invocation::ResponseResult::Success(success.value)
                        }
                        response_result::ResponseResult::ResponseFailure(failure) => {
                            restate_types::invocation::ResponseResult::Failure(
                                failure.failure_code.into(),
                                ByteString::try_from(failure.failure_message)
                                    .map_err(ConversionError::invalid_data)?,
                            )
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
                        restate_types::invocation::ResponseResult::Failure(error_code, error) => {
                            response_result::ResponseResult::ResponseFailure(
                                response_result::ResponseFailure {
                                    failure_code: error_code.into(),
                                    failure_message: error.into_bytes(),
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

                fn try_from(value: Timer) -> Result<Self, Self::Error> {
                    let service_name = ByteString::try_from(value.service_name)
                        .map_err(ConversionError::invalid_data)?;
                    let service_id =
                        restate_types::identifiers::ServiceId::new(service_name, value.service_key);

                    Ok(
                        match value.value.ok_or(ConversionError::missing_field("value"))? {
                            timer::Value::CompleteSleepEntry(_) => {
                                restate_storage_api::timer_table::Timer::CompleteSleepEntry(
                                    service_id,
                                )
                            }
                            timer::Value::Invoke(si) => {
                                restate_storage_api::timer_table::Timer::Invoke(
                                    service_id,
                                    restate_types::invocation::ServiceInvocation::try_from(si)?,
                                )
                            }
                        },
                    )
                }
            }

            impl From<restate_storage_api::timer_table::Timer> for Timer {
                fn from(value: restate_storage_api::timer_table::Timer) -> Self {
                    match value {
                        restate_storage_api::timer_table::Timer::CompleteSleepEntry(service_id) => {
                            Timer {
                                service_name: service_id.service_name.into_bytes(),
                                service_key: service_id.key,
                                value: Some(timer::Value::CompleteSleepEntry(Default::default())),
                            }
                        }
                        restate_storage_api::timer_table::Timer::Invoke(service_id, si) => Timer {
                            service_name: service_id.service_name.into_bytes(),
                            service_key: service_id.key,
                            value: Some(timer::Value::Invoke(ServiceInvocation::from(si))),
                        },
                    }
                }
            }
        }
    }
}
