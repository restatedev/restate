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
                Awakeable, BackgroundCall, ClearState, CompleteAwakeable, Custom, GetState, Invoke,
                OutputStream, PollInputStream, SetState, Sleep,
            };
            use crate::storage::v1::invocation_status::{Free, Invoked, Suspended};
            use crate::storage::v1::journal_entry::completion_result::{
                Ack, Empty, Failure, Success,
            };
            use crate::storage::v1::journal_entry::{
                completion_result, CompletionResult, Entry, Kind,
            };
            use crate::storage::v1::journal_meta::EndpointId;
            use crate::storage::v1::outbox_message::{
                OutboxIngressResponse, OutboxServiceInvocation, OutboxServiceInvocationResponse,
            };
            use crate::storage::v1::service_invocation_response_sink::{
                Ingress, PartitionProcessor, ResponseSink,
            };
            use crate::storage::v1::{
                enriched_entry_header, invocation_resolution_result, invocation_status,
                outbox_message, outbox_message::outbox_service_invocation_response,
                response_result, span_relation, timer, BackgroundCallResolutionResult,
                EnrichedEntryHeader, FullInvocationId, InboxEntry, InvocationResolutionResult,
                InvocationStatus, JournalEntry, JournalMeta, OutboxMessage, ResponseResult,
                SequencedTimer, ServiceInvocation, ServiceInvocationResponseSink, SpanContext,
                SpanRelation, Timer,
            };
            use anyhow::anyhow;
            use bytes::{Buf, Bytes};
            use bytestring::ByteString;
            use opentelemetry_api::trace::TraceState;
            use restate_storage_api::StorageError;
            use restate_types::invocation::MaybeFullInvocationId;
            use restate_types::time::MillisSinceEpoch;
            use std::collections::{HashSet, VecDeque};
            use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
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

            impl TryFrom<InvocationStatus> for restate_storage_api::status_table::InvocationStatus {
                type Error = ConversionError;

                fn try_from(value: InvocationStatus) -> Result<Self, Self::Error> {
                    let result = match value
                        .status
                        .ok_or(ConversionError::missing_field("status"))?
                    {
                        invocation_status::Status::Invoked(invoked) => {
                            let invoked_status =
                                restate_storage_api::status_table::InvocationMetadata::try_from(
                                    invoked,
                                )?;
                            restate_storage_api::status_table::InvocationStatus::Invoked(
                                invoked_status,
                            )
                        }
                        invocation_status::Status::Suspended(suspended) => {
                            let (metadata, waiting_for_completed_entries) = suspended.try_into()?;
                            restate_storage_api::status_table::InvocationStatus::Suspended {
                                metadata,
                                waiting_for_completed_entries,
                            }
                        }
                        invocation_status::Status::Free(_) => {
                            restate_storage_api::status_table::InvocationStatus::Free
                        }
                    };

                    Ok(result)
                }
            }

            impl From<restate_storage_api::status_table::InvocationStatus> for InvocationStatus {
                fn from(value: restate_storage_api::status_table::InvocationStatus) -> Self {
                    let status = match value {
                        restate_storage_api::status_table::InvocationStatus::Invoked(
                            invoked_status,
                        ) => invocation_status::Status::Invoked(Invoked::from(invoked_status)),
                        restate_storage_api::status_table::InvocationStatus::Suspended {
                            metadata,
                            waiting_for_completed_entries,
                        } => invocation_status::Status::Suspended(Suspended::from((
                            metadata,
                            waiting_for_completed_entries,
                        ))),
                        restate_storage_api::status_table::InvocationStatus::Free => {
                            invocation_status::Status::Free(Free {})
                        }
                    };

                    InvocationStatus {
                        status: Some(status),
                    }
                }
            }

            impl TryFrom<Invoked> for restate_storage_api::status_table::InvocationMetadata {
                type Error = ConversionError;

                fn try_from(value: Invoked) -> Result<Self, Self::Error> {
                    let invocation_uuid = try_bytes_into_invocation_uuid(value.invocation_uuid)?;
                    let journal_metadata = restate_types::journal::JournalMetadata::try_from(
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

                    Ok(restate_storage_api::status_table::InvocationMetadata::new(
                        invocation_uuid,
                        journal_metadata,
                        response_sink,
                        MillisSinceEpoch::new(value.creation_time),
                        MillisSinceEpoch::new(value.modification_time),
                    ))
                }
            }

            impl From<restate_storage_api::status_table::InvocationMetadata> for Invoked {
                fn from(value: restate_storage_api::status_table::InvocationMetadata) -> Self {
                    let restate_storage_api::status_table::InvocationMetadata {
                        invocation_uuid,
                        response_sink,
                        journal_metadata,
                        creation_time,
                        modification_time,
                    } = value;

                    Invoked {
                        response_sink: Some(ServiceInvocationResponseSink::from(response_sink)),
                        invocation_uuid: invocation_uuid_to_bytes(&invocation_uuid),
                        journal_meta: Some(JournalMeta::from(journal_metadata)),
                        creation_time: creation_time.as_u64(),
                        modification_time: modification_time.as_u64(),
                    }
                }
            }

            impl TryFrom<Suspended>
                for (
                    restate_storage_api::status_table::InvocationMetadata,
                    HashSet<restate_types::identifiers::EntryIndex>,
                )
            {
                type Error = ConversionError;

                fn try_from(value: Suspended) -> Result<Self, Self::Error> {
                    let invocation_uuid = try_bytes_into_invocation_uuid(value.invocation_uuid)?;
                    let journal_metadata = restate_types::journal::JournalMetadata::try_from(
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

                    Ok((
                        restate_storage_api::status_table::InvocationMetadata::new(
                            invocation_uuid,
                            journal_metadata,
                            response_sink,
                            MillisSinceEpoch::new(value.creation_time),
                            MillisSinceEpoch::new(value.modification_time),
                        ),
                        waiting_for_completed_entries,
                    ))
                }
            }

            impl
                From<(
                    restate_storage_api::status_table::InvocationMetadata,
                    HashSet<restate_types::identifiers::EntryIndex>,
                )> for Suspended
            {
                fn from(
                    (metadata, waiting_for_completed_entries): (
                        restate_storage_api::status_table::InvocationMetadata,
                        HashSet<restate_types::identifiers::EntryIndex>,
                    ),
                ) -> Self {
                    let invocation_uuid = invocation_uuid_to_bytes(&metadata.invocation_uuid);
                    let response_sink = ServiceInvocationResponseSink::from(metadata.response_sink);
                    let journal_meta = JournalMeta::from(metadata.journal_metadata);
                    let waiting_for_completed_entries =
                        waiting_for_completed_entries.into_iter().collect();

                    Suspended {
                        invocation_uuid,
                        response_sink: Some(response_sink),
                        journal_meta: Some(journal_meta),
                        creation_time: metadata.creation_time.as_u64(),
                        modification_time: metadata.modification_time.as_u64(),
                        waiting_for_completed_entries,
                    }
                }
            }

            impl TryFrom<JournalMeta> for restate_types::journal::JournalMetadata {
                type Error = ConversionError;

                fn try_from(value: JournalMeta) -> Result<Self, Self::Error> {
                    let length = value.length;
                    // TODO: replace with ByteString to avoid allocation of String
                    let method = String::from_utf8(value.method_name.to_vec())
                        .map_err(ConversionError::invalid_data)?;
                    let span_context =
                        restate_types::invocation::ServiceInvocationSpanContext::try_from(
                            value
                                .span_context
                                .ok_or(ConversionError::missing_field("span_context"))?,
                        )?;
                    let endpoint_id =
                        value
                            .endpoint_id
                            .and_then(|one_of_endpoint_id| match one_of_endpoint_id {
                                EndpointId::None(_) => None,
                                EndpointId::Value(id) => Some(id),
                            });
                    Ok(restate_types::journal::JournalMetadata {
                        endpoint_id,
                        length,
                        method,
                        span_context,
                    })
                }
            }

            impl From<restate_types::journal::JournalMetadata> for JournalMeta {
                fn from(value: restate_types::journal::JournalMetadata) -> Self {
                    let restate_types::journal::JournalMetadata {
                        span_context,
                        length,
                        method,
                        endpoint_id,
                    } = value;

                    JournalMeta {
                        length,
                        method_name: Bytes::from(method.into_bytes()),
                        span_context: Some(SpanContext::from(span_context)),
                        endpoint_id: Some(match endpoint_id {
                            None => EndpointId::None(()),
                            Some(endpoint_id) => EndpointId::Value(endpoint_id),
                        }),
                    }
                }
            }

            impl TryFrom<InboxEntry> for restate_types::invocation::ServiceInvocation {
                type Error = ConversionError;

                fn try_from(value: InboxEntry) -> Result<Self, Self::Error> {
                    let service_invocation =
                        restate_types::invocation::ServiceInvocation::try_from(
                            value
                                .service_invocation
                                .ok_or(ConversionError::missing_field("service_invocation"))?,
                        )?;

                    Ok(service_invocation)
                }
            }

            impl From<restate_types::invocation::ServiceInvocation> for InboxEntry {
                fn from(value: restate_types::invocation::ServiceInvocation) -> Self {
                    let service_invocation = ServiceInvocation::from(value);

                    InboxEntry {
                        service_invocation: Some(service_invocation),
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

                    Ok(restate_types::invocation::ServiceInvocation {
                        fid: id,
                        method_name,
                        argument,
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

                    ServiceInvocation {
                        id: Some(id),
                        span_context: Some(span_context),
                        response_sink: Some(response_sink),
                        method_name,
                        argument: value.argument,
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
                    let invocation_uuid = invocation_uuid_to_bytes(&value.invocation_uuid);
                    let service_key = value.service_id.key;
                    let service_name = value.service_id.service_name.into_bytes();

                    FullInvocationId {
                        invocation_uuid,
                        service_key,
                        service_name,
                    }
                }
            }

            fn try_bytes_into_invocation_uuid(
                bytes: Bytes,
            ) -> Result<restate_types::identifiers::InvocationUuid, ConversionError> {
                restate_types::identifiers::InvocationUuid::from_slice(bytes.as_ref())
                    .map_err(ConversionError::invalid_data)
            }

            fn invocation_uuid_to_bytes(
                invocation_id: &restate_types::identifiers::InvocationUuid,
            ) -> Bytes {
                Bytes::copy_from_slice(invocation_id.as_bytes())
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
                            let ingress_dispatcher_id = try_string_into_ingress_dispatcher_id(
                                ingress.ingress_dispatcher_id,
                            )?;

                            Some(
                                restate_types::invocation::ServiceInvocationResponseSink::Ingress(
                                    ingress_dispatcher_id,
                                ),
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
                        Some(restate_types::invocation::ServiceInvocationResponseSink::Ingress(ingress_dispatcher_id)) => {
                            ResponseSink::Ingress(Ingress {
                                ingress_dispatcher_id: ingress_dispatcher_id.to_string(),
                            })
                        }
                        None => ResponseSink::None(Default::default()),
                    };

                    ServiceInvocationResponseSink {
                        response_sink: Some(response_sink),
                    }
                }
            }

            fn try_string_into_ingress_dispatcher_id(
                value: String,
            ) -> Result<restate_types::identifiers::IngressDispatcherId, ConversionError>
            {
                Ok(value.parse().map_err(ConversionError::invalid_data)?)
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
                    Entry {
                        header: Some(EnrichedEntryHeader::from(value.header)),
                        raw_entry: value.entry,
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
                        completion_result::Result::Ack(_) => {
                            restate_types::journal::CompletionResult::Ack
                        }
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
                        restate_types::journal::CompletionResult::Ack => {
                            completion_result::Result::Ack(Ack {})
                        }
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
                            restate_types::journal::enriched::EnrichedEntryHeader::OutputStream
                        }
                        enriched_entry_header::Kind::GetState(get_state) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::GetState {
                                is_completed: get_state.is_completed,
                            }
                        }
                        enriched_entry_header::Kind::SetState(_) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::SetState
                        }
                        enriched_entry_header::Kind::ClearState(_) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::ClearState
                        }
                        enriched_entry_header::Kind::Sleep(sleep) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::Sleep {
                                is_completed: sleep.is_completed,
                            }
                        }
                        enriched_entry_header::Kind::Invoke(invoke) => {
                            let resolution_result = Option::<
                                restate_types::journal::enriched::ResolutionResult,
                            >::try_from(
                                invoke
                                    .resolution_result
                                    .ok_or(ConversionError::missing_field("resolution_result"))?,
                            )?;

                            restate_types::journal::enriched::EnrichedEntryHeader::Invoke {
                                is_completed: invoke.is_completed,
                                resolution_result,
                            }
                        }
                        enriched_entry_header::Kind::BackgroundCall(background_call) => {
                            let resolution_result =
                                restate_types::journal::enriched::ResolutionResult::try_from(
                                    background_call.resolution_result.ok_or(
                                        ConversionError::missing_field("resolution_result"),
                                    )?,
                                )?;

                            restate_types::journal::enriched::EnrichedEntryHeader::BackgroundInvoke {
                                    resolution_result,
                                }
                        }
                        enriched_entry_header::Kind::Awakeable(awakeable) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::Awakeable {
                                is_completed: awakeable.is_completed,
                            }
                        }
                        enriched_entry_header::Kind::CompleteAwakeable(CompleteAwakeable { invocation_id, entry_index }) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::CompleteAwakeable {
                                invocation_id: restate_types::identifiers::InvocationId::from_slice(&invocation_id).map_err(ConversionError::invalid_data)?,
                                entry_index,
                            }
                        }
                        enriched_entry_header::Kind::Custom(custom) => {
                            restate_types::journal::enriched::EnrichedEntryHeader::Custom {
                                code: u16::try_from(custom.code)
                                    .map_err(ConversionError::invalid_data)?,
                                requires_ack: custom.requires_ack,
                            }
                        }
                    };

                    Ok(enriched_header)
                }
            }

            impl From<restate_types::journal::enriched::EnrichedEntryHeader> for EnrichedEntryHeader {
                fn from(value: restate_types::journal::enriched::EnrichedEntryHeader) -> Self {
                    let kind = match value {
                        restate_types::journal::enriched::EnrichedEntryHeader::PollInputStream {
                            is_completed,
                        } => enriched_entry_header::Kind::PollInputStream(PollInputStream {
                            is_completed,
                        }),
                        restate_types::journal::enriched::EnrichedEntryHeader::OutputStream => {
                            enriched_entry_header::Kind::OutputStream(OutputStream {})
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::GetState { is_completed } => {
                            enriched_entry_header::Kind::GetState(GetState { is_completed })
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::SetState => {
                            enriched_entry_header::Kind::SetState(SetState {})
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::ClearState => {
                            enriched_entry_header::Kind::ClearState(ClearState {})
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::Sleep { is_completed } => {
                            enriched_entry_header::Kind::Sleep(Sleep { is_completed })
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::Invoke {
                            is_completed,
                            resolution_result,
                        } => enriched_entry_header::Kind::Invoke(Invoke {
                            is_completed,
                            resolution_result: Some(InvocationResolutionResult::from(
                                resolution_result,
                            )),
                        }),
                        restate_types::journal::enriched::EnrichedEntryHeader::BackgroundInvoke {
                            resolution_result,
                        } => enriched_entry_header::Kind::BackgroundCall(BackgroundCall {
                            resolution_result: Some(BackgroundCallResolutionResult::from(
                                resolution_result,
                            )),
                        }),
                        restate_types::journal::enriched::EnrichedEntryHeader::Awakeable {
                            is_completed,
                        } => enriched_entry_header::Kind::Awakeable(Awakeable { is_completed }),
                        restate_types::journal::enriched::EnrichedEntryHeader::CompleteAwakeable { invocation_id, entry_index } => {
                            enriched_entry_header::Kind::CompleteAwakeable(CompleteAwakeable {
                                invocation_id: Bytes::copy_from_slice(&invocation_id.as_bytes()),
                                entry_index
                            })
                        }
                        restate_types::journal::enriched::EnrichedEntryHeader::Custom {
                            requires_ack,
                            code,
                        } => enriched_entry_header::Kind::Custom(Custom {
                            requires_ack,
                            code: u32::from(code),
                        }),
                    };

                    EnrichedEntryHeader { kind: Some(kind) }
                }
            }

            impl TryFrom<InvocationResolutionResult>
                for Option<restate_types::journal::enriched::ResolutionResult>
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

                            Some(restate_types::journal::enriched::ResolutionResult {
                                span_context,
                                invocation_uuid,
                                service_key,
                            })
                        }
                    };

                    Ok(result)
                }
            }

            impl From<Option<restate_types::journal::enriched::ResolutionResult>>
                for InvocationResolutionResult
            {
                fn from(value: Option<restate_types::journal::enriched::ResolutionResult>) -> Self {
                    let result = match value {
                        None => invocation_resolution_result::Result::None(Default::default()),
                        Some(resolution_result) => match resolution_result {
                            restate_types::journal::enriched::ResolutionResult {
                                invocation_uuid,
                                service_key,
                                span_context,
                            } => invocation_resolution_result::Result::Success(
                                invocation_resolution_result::Success {
                                    invocation_uuid: invocation_uuid_to_bytes(&invocation_uuid),
                                    service_key,
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
                for restate_types::journal::enriched::ResolutionResult
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

                    Ok(restate_types::journal::enriched::ResolutionResult {
                        span_context,
                        invocation_uuid,
                        service_key,
                    })
                }
            }

            impl From<restate_types::journal::enriched::ResolutionResult> for BackgroundCallResolutionResult {
                fn from(value: restate_types::journal::enriched::ResolutionResult) -> Self {
                    BackgroundCallResolutionResult {
                        invocation_uuid: invocation_uuid_to_bytes(&value.invocation_uuid),
                        service_key: value.service_key,
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
                                id: match invocation_response
                                    .id
                                    .ok_or(ConversionError::missing_field("id"))?
                                {
                                    outbox_service_invocation_response::Id::FullInvocationId(
                                        fid,
                                    ) => MaybeFullInvocationId::Full(
                                        restate_types::identifiers::FullInvocationId::try_from(
                                            fid,
                                        )?,
                                    ),
                                    outbox_service_invocation_response::Id::InvocationId(
                                        invocation_id_bytes,
                                    ) => MaybeFullInvocationId::Partial(
                                        restate_types::identifiers::InvocationId::from_slice(
                                            &invocation_id_bytes,
                                        )
                                        .map_err(ConversionError::invalid_data)?,
                                    ),
                                },
                                result: restate_types::invocation::ResponseResult::try_from(
                                    invocation_response
                                        .response_result
                                        .ok_or(ConversionError::missing_field("response_result"))?,
                                )?,
                            },
                        ),
                        outbox_message::OutboxMessage::IngressResponse(ingress_response) => {
                            restate_storage_api::outbox_table::OutboxMessage::IngressResponse {
                                full_invocation_id:
                                    restate_types::identifiers::FullInvocationId::try_from(
                                        ingress_response.full_invocation_id.ok_or(
                                            ConversionError::missing_field("full_invocation_id"),
                                        )?,
                                    )?,
                                ingress_dispatcher_id: try_string_into_ingress_dispatcher_id(
                                    ingress_response.ingress_dispatcher_id,
                                )?,
                                response: restate_types::invocation::ResponseResult::try_from(
                                    ingress_response
                                        .response_result
                                        .ok_or(ConversionError::missing_field("response_result"))?,
                                )?,
                            }
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
                                id: Some(match invocation_response.id {
                                    MaybeFullInvocationId::Partial(iid) => {
                                        outbox_service_invocation_response::Id::InvocationId(
                                            Bytes::copy_from_slice(&iid.as_bytes()),
                                        )
                                    }
                                    MaybeFullInvocationId::Full(fid) => {
                                        outbox_service_invocation_response::Id::FullInvocationId(
                                            FullInvocationId::from(fid),
                                        )
                                    }
                                }),
                                response_result: Some(ResponseResult::from(
                                    invocation_response.result,
                                )),
                            },
                        ),
                        restate_storage_api::outbox_table::OutboxMessage::IngressResponse {
                            ingress_dispatcher_id,
                            full_invocation_id,
                            response,
                        } => {
                            outbox_message::OutboxMessage::IngressResponse(OutboxIngressResponse {
                                full_invocation_id: Some(FullInvocationId::from(
                                    full_invocation_id,
                                )),
                                ingress_dispatcher_id: ingress_dispatcher_id.to_string(),
                                response_result: Some(ResponseResult::from(response)),
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
                    Ok(
                        match value.value.ok_or(ConversionError::missing_field("value"))? {
                            timer::Value::CompleteSleepEntry(_) => {
                                restate_storage_api::timer_table::Timer::CompleteSleepEntry
                            }
                            timer::Value::Invoke(si) => {
                                restate_storage_api::timer_table::Timer::Invoke(
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
                        restate_storage_api::timer_table::Timer::CompleteSleepEntry => Timer {
                            value: Some(timer::Value::CompleteSleepEntry(Default::default())),
                        },
                        restate_storage_api::timer_table::Timer::Invoke(si) => Timer {
                            value: Some(timer::Value::Invoke(ServiceInvocation::from(si))),
                        },
                    }
                }
            }
        }
    }
}
