pub mod storage {
    pub mod v1 {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/dev.restate.storage.v1.rs"));

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
            use crate::storage::v1::outbox_message::{
                OutboxIngressResponse, OutboxServiceInvocation, OutboxServiceInvocationResponse,
            };
            use crate::storage::v1::service_invocation_response_sink::{
                Ingress, PartitionProcessor, ResponseSink,
            };
            use crate::storage::v1::{
                background_call_resolution_result, enriched_entry_header,
                invocation_resolution_result, invocation_status, outbox_message, response_result,
                BackgroundCallResolutionResult, EnrichedEntryHeader, InboxEntry,
                InvocationResolutionResult, InvocationStatus, JournalEntry, JournalMeta,
                OutboxMessage, ResponseResult, ServiceInvocation, ServiceInvocationId,
                ServiceInvocationResponseSink, SpanContext, Timer,
            };
            use bytes::{Buf, Bytes};
            use bytestring::ByteString;
            use opentelemetry_api::trace::TraceState;
            use std::collections::VecDeque;
            use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
            use std::str::FromStr;

            #[derive(Debug, thiserror::Error)]
            pub enum ConversionError {
                #[error("missing field '{0}'")]
                MissingField(&'static str),
                #[error("invalid data: {0}")]
                InvalidData(common::utils::GenericError),
            }

            impl ConversionError {
                fn invalid_data(source: impl Into<common::utils::GenericError>) -> Self {
                    ConversionError::InvalidData(source.into())
                }

                fn missing_field(field: &'static str) -> Self {
                    ConversionError::MissingField(field)
                }
            }

            impl TryFrom<InvocationStatus> for common::types::InvocationStatus {
                type Error = ConversionError;

                fn try_from(value: InvocationStatus) -> Result<Self, Self::Error> {
                    let result = match value
                        .status
                        .ok_or(ConversionError::missing_field("status"))?
                    {
                        invocation_status::Status::Invoked(invoked) => {
                            let invoked_status = common::types::InvokedStatus::try_from(invoked)?;
                            common::types::InvocationStatus::Invoked(invoked_status)
                        }
                        invocation_status::Status::Suspended(suspended) => {
                            let suspended_status =
                                common::types::SuspendedStatus::try_from(suspended)?;
                            common::types::InvocationStatus::Suspended(suspended_status)
                        }
                        invocation_status::Status::Free(_) => common::types::InvocationStatus::Free,
                    };

                    Ok(result)
                }
            }

            impl From<common::types::InvocationStatus> for InvocationStatus {
                fn from(value: common::types::InvocationStatus) -> Self {
                    let status = match value {
                        common::types::InvocationStatus::Invoked(invoked_status) => {
                            invocation_status::Status::Invoked(Invoked::from(invoked_status))
                        }
                        common::types::InvocationStatus::Suspended(suspended_status) => {
                            invocation_status::Status::Suspended(Suspended::from(suspended_status))
                        }
                        common::types::InvocationStatus::Free => {
                            invocation_status::Status::Free(Free {})
                        }
                    };

                    InvocationStatus {
                        status: Some(status),
                    }
                }
            }

            impl TryFrom<Invoked> for common::types::InvokedStatus {
                type Error = ConversionError;

                fn try_from(value: Invoked) -> Result<Self, Self::Error> {
                    let invocation_id = try_bytes_into_invocation_id(value.invocation_id)?;
                    let journal_metadata = common::types::JournalMetadata::try_from(
                        value
                            .journal_meta
                            .ok_or(ConversionError::missing_field("journal_meta"))?,
                    )?;
                    let response_sink =
                        Option::<common::types::ServiceInvocationResponseSink>::try_from(
                            value
                                .response_sink
                                .ok_or(ConversionError::missing_field("response_sink"))?,
                        )?;

                    Ok(common::types::InvokedStatus::new(
                        invocation_id,
                        journal_metadata,
                        response_sink,
                    ))
                }
            }

            impl From<common::types::InvokedStatus> for Invoked {
                fn from(value: common::types::InvokedStatus) -> Self {
                    let common::types::InvokedStatus {
                        invocation_id,
                        response_sink,
                        journal_metadata,
                    } = value;

                    Invoked {
                        response_sink: Some(ServiceInvocationResponseSink::from(response_sink)),
                        invocation_id: invocation_id_to_bytes(&invocation_id),
                        journal_meta: Some(JournalMeta::from(journal_metadata)),
                    }
                }
            }

            impl TryFrom<Suspended> for common::types::SuspendedStatus {
                type Error = ConversionError;

                fn try_from(value: Suspended) -> Result<Self, Self::Error> {
                    let invocation_id = try_bytes_into_invocation_id(value.invocation_id)?;
                    let journal_metadata = common::types::JournalMetadata::try_from(
                        value
                            .journal_meta
                            .ok_or(ConversionError::missing_field("journal_meta"))?,
                    )?;
                    let response_sink =
                        Option::<common::types::ServiceInvocationResponseSink>::try_from(
                            value
                                .response_sink
                                .ok_or(ConversionError::missing_field("response_sink"))?,
                        )?;

                    let waiting_for_completed_entries =
                        value.waiting_for_completed_entries.into_iter().collect();

                    Ok(common::types::SuspendedStatus::new(
                        invocation_id,
                        journal_metadata,
                        response_sink,
                        waiting_for_completed_entries,
                    ))
                }
            }

            impl From<common::types::SuspendedStatus> for Suspended {
                fn from(value: common::types::SuspendedStatus) -> Self {
                    let invocation_id = invocation_id_to_bytes(&value.invocation_id);
                    let response_sink = ServiceInvocationResponseSink::from(value.response_sink);
                    let journal_meta = JournalMeta::from(value.journal_metadata);
                    let waiting_for_completed_entries =
                        value.waiting_for_completed_entries.into_iter().collect();

                    Suspended {
                        invocation_id,
                        response_sink: Some(response_sink),
                        journal_meta: Some(journal_meta),
                        waiting_for_completed_entries,
                    }
                }
            }

            impl TryFrom<JournalMeta> for common::types::JournalMetadata {
                type Error = ConversionError;

                fn try_from(value: JournalMeta) -> Result<Self, Self::Error> {
                    let length = value.length;
                    // TODO: replace with ByteString to avoid allocation of String
                    let method = String::from_utf8(value.method_name.to_vec())
                        .map_err(ConversionError::invalid_data)?;
                    let span_context = common::types::ServiceInvocationSpanContext::new(
                        opentelemetry_api::trace::SpanContext::try_from(
                            value
                                .span_context
                                .ok_or(ConversionError::missing_field("span_context"))?,
                        )?,
                    );
                    Ok(common::types::JournalMetadata {
                        length,
                        method,
                        span_context,
                    })
                }
            }

            impl From<common::types::JournalMetadata> for JournalMeta {
                fn from(value: common::types::JournalMetadata) -> Self {
                    let common::types::JournalMetadata {
                        span_context,
                        length,
                        method,
                    } = value;

                    JournalMeta {
                        length,
                        method_name: Bytes::from(method.into_bytes()),
                        span_context: Some(SpanContext::from(
                            opentelemetry_api::trace::SpanContext::from(span_context),
                        )),
                    }
                }
            }

            impl TryFrom<InboxEntry> for common::types::ServiceInvocation {
                type Error = ConversionError;

                fn try_from(value: InboxEntry) -> Result<Self, Self::Error> {
                    let service_invocation = common::types::ServiceInvocation::try_from(
                        value
                            .service_invocation
                            .ok_or(ConversionError::missing_field("service_invocation"))?,
                    )?;

                    Ok(service_invocation)
                }
            }

            impl From<common::types::ServiceInvocation> for InboxEntry {
                fn from(value: common::types::ServiceInvocation) -> Self {
                    let service_invocation = ServiceInvocation::from(value);

                    InboxEntry {
                        service_invocation: Some(service_invocation),
                    }
                }
            }

            impl TryFrom<ServiceInvocation> for common::types::ServiceInvocation {
                type Error = ConversionError;

                fn try_from(value: ServiceInvocation) -> Result<Self, Self::Error> {
                    let ServiceInvocation {
                        id,
                        method_name,
                        response_sink,
                        span_context,
                        argument,
                    } = value;

                    let id = common::types::ServiceInvocationId::try_from(
                        id.ok_or(ConversionError::missing_field("id"))?,
                    )?;

                    let span_context = opentelemetry_api::trace::SpanContext::try_from(
                        span_context.ok_or(ConversionError::missing_field("span_context"))?,
                    )?;

                    let response_sink =
                        Option::<common::types::ServiceInvocationResponseSink>::try_from(
                            response_sink.ok_or(ConversionError::missing_field("response_sink"))?,
                        )?;

                    let method_name =
                        ByteString::try_from(method_name).map_err(ConversionError::invalid_data)?;

                    Ok(common::types::ServiceInvocation {
                        id,
                        method_name,
                        argument,
                        response_sink,
                        span_context: common::types::ServiceInvocationSpanContext::new(
                            span_context,
                        ),
                    })
                }
            }

            impl From<common::types::ServiceInvocation> for ServiceInvocation {
                fn from(value: common::types::ServiceInvocation) -> Self {
                    let id = ServiceInvocationId::from(value.id);
                    let span_context = SpanContext::from(
                        opentelemetry_api::trace::SpanContext::from(value.span_context),
                    );
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

            impl TryFrom<ServiceInvocationId> for common::types::ServiceInvocationId {
                type Error = ConversionError;

                fn try_from(value: ServiceInvocationId) -> Result<Self, Self::Error> {
                    let ServiceInvocationId {
                        service_name,
                        service_key,
                        invocation_id,
                    } = value;

                    let service_name = ByteString::try_from(service_name)
                        .map_err(ConversionError::invalid_data)?;
                    let invocation_id = try_bytes_into_invocation_id(invocation_id)?;

                    Ok(common::types::ServiceInvocationId::new(
                        service_name,
                        service_key,
                        invocation_id,
                    ))
                }
            }

            impl From<common::types::ServiceInvocationId> for ServiceInvocationId {
                fn from(value: common::types::ServiceInvocationId) -> Self {
                    let invocation_id = invocation_id_to_bytes(&value.invocation_id);
                    let service_key = value.service_id.key;
                    let service_name = value.service_id.service_name.into_bytes();

                    ServiceInvocationId {
                        invocation_id,
                        service_key,
                        service_name,
                    }
                }
            }

            fn try_bytes_into_invocation_id(
                bytes: Bytes,
            ) -> Result<common::types::InvocationId, ConversionError> {
                common::types::InvocationId::from_slice(bytes.as_ref())
                    .map_err(ConversionError::invalid_data)
            }

            fn invocation_id_to_bytes(invocation_id: &common::types::InvocationId) -> Bytes {
                Bytes::copy_from_slice(invocation_id.as_bytes())
            }

            impl TryFrom<SpanContext> for opentelemetry_api::trace::SpanContext {
                type Error = ConversionError;

                fn try_from(value: SpanContext) -> Result<Self, Self::Error> {
                    let SpanContext {
                        trace_id,
                        span_id,
                        trace_flags,
                        is_remote,
                        trace_state,
                    } = value;

                    let trace_id = try_bytes_into_trace_id(trace_id)?;
                    let span_id =
                        opentelemetry_api::trace::SpanId::from_bytes(span_id.to_be_bytes());
                    let trace_flags = opentelemetry_api::trace::TraceFlags::new(
                        u8::try_from(trace_flags).map_err(ConversionError::invalid_data)?,
                    );

                    let trace_state = TraceState::from_str(&trace_state)
                        .map_err(ConversionError::invalid_data)?;

                    Ok(opentelemetry_api::trace::SpanContext::new(
                        trace_id,
                        span_id,
                        trace_flags,
                        is_remote,
                        trace_state,
                    ))
                }
            }

            impl From<opentelemetry_api::trace::SpanContext> for SpanContext {
                fn from(value: opentelemetry_api::trace::SpanContext) -> Self {
                    let trace_state = value.trace_state().header();
                    let span_id = u64::from_be_bytes(value.span_id().to_bytes());
                    let trace_flags = u32::from(value.trace_flags().to_u8());
                    let trace_id = Bytes::copy_from_slice(&value.trace_id().to_bytes());

                    SpanContext {
                        trace_state,
                        span_id,
                        trace_flags,
                        trace_id,
                        is_remote: value.is_remote(),
                    }
                }
            }

            fn try_bytes_into_trace_id(
                mut bytes: Bytes,
            ) -> Result<opentelemetry_api::trace::TraceId, ConversionError> {
                if bytes.len() != 16 {
                    return Err(ConversionError::invalid_data(
                        "trace id pb definition needs to contain exactly 16 bytes",
                    ));
                }

                let mut bytes_array = [0; 16];
                bytes.copy_to_slice(&mut bytes_array);

                Ok(opentelemetry_api::trace::TraceId::from_bytes(bytes_array))
            }

            impl TryFrom<ServiceInvocationResponseSink>
                for Option<common::types::ServiceInvocationResponseSink>
            {
                type Error = ConversionError;

                fn try_from(value: ServiceInvocationResponseSink) -> Result<Self, Self::Error> {
                    let response_sink = match value
                        .response_sink
                        .ok_or(ConversionError::missing_field("response_sink"))?
                    {
                        ResponseSink::PartitionProcessor(partition_processor) => {
                            let caller = common::types::ServiceInvocationId::try_from(
                                partition_processor
                                    .caller
                                    .ok_or(ConversionError::missing_field("caller"))?,
                            )?;
                            Some(
                                common::types::ServiceInvocationResponseSink::PartitionProcessor {
                                    caller,
                                    entry_index: partition_processor.entry_index,
                                },
                            )
                        }
                        ResponseSink::Ingress(ingress) => {
                            let ingress_id = try_string_into_ingress_id(ingress.ingress_id)?;

                            Some(common::types::ServiceInvocationResponseSink::Ingress(
                                ingress_id,
                            ))
                        }
                        ResponseSink::None(_) => None,
                    };

                    Ok(response_sink)
                }
            }

            impl From<Option<common::types::ServiceInvocationResponseSink>> for ServiceInvocationResponseSink {
                fn from(value: Option<common::types::ServiceInvocationResponseSink>) -> Self {
                    let response_sink = match value {
                        Some(
                            common::types::ServiceInvocationResponseSink::PartitionProcessor {
                                caller,
                                entry_index,
                            },
                        ) => ResponseSink::PartitionProcessor(PartitionProcessor {
                            entry_index,
                            caller: Some(ServiceInvocationId::from(caller)),
                        }),
                        Some(common::types::ServiceInvocationResponseSink::Ingress(ingress_id)) => {
                            ResponseSink::Ingress(Ingress {
                                ingress_id: ingress_id_to_string(ingress_id),
                            })
                        }
                        None => ResponseSink::None(Default::default()),
                    };

                    ServiceInvocationResponseSink {
                        response_sink: Some(response_sink),
                    }
                }
            }

            fn try_string_into_ingress_id(
                value: String,
            ) -> Result<common::types::IngressId, ConversionError> {
                Ok(common::types::IngressId(
                    value.parse().map_err(ConversionError::invalid_data)?,
                ))
            }

            fn ingress_id_to_string(ingress_id: common::types::IngressId) -> String {
                ingress_id.0.to_string()
            }

            impl TryFrom<JournalEntry> for common::types::JournalEntry {
                type Error = ConversionError;

                fn try_from(value: JournalEntry) -> Result<Self, Self::Error> {
                    let journal_entry =
                        match value.kind.ok_or(ConversionError::missing_field("kind"))? {
                            Kind::Entry(journal_entry) => common::types::JournalEntry::Entry(
                                common::types::EnrichedRawEntry::try_from(journal_entry)?,
                            ),
                            Kind::CompletionResult(completion_result) => {
                                common::types::JournalEntry::Completion(
                                    common::types::CompletionResult::try_from(completion_result)?,
                                )
                            }
                        };

                    Ok(journal_entry)
                }
            }

            impl From<common::types::JournalEntry> for JournalEntry {
                fn from(value: common::types::JournalEntry) -> Self {
                    match value {
                        common::types::JournalEntry::Entry(entry) => JournalEntry::from(entry),
                        common::types::JournalEntry::Completion(completion) => {
                            JournalEntry::from(completion)
                        }
                    }
                }
            }

            impl From<common::types::EnrichedRawEntry> for JournalEntry {
                fn from(value: common::types::EnrichedRawEntry) -> Self {
                    let entry = Entry::from(value);

                    JournalEntry {
                        kind: Some(Kind::Entry(entry)),
                    }
                }
            }

            impl From<common::types::CompletionResult> for JournalEntry {
                fn from(value: common::types::CompletionResult) -> Self {
                    let completion_result = CompletionResult::from(value);

                    JournalEntry {
                        kind: Some(Kind::CompletionResult(completion_result)),
                    }
                }
            }

            impl TryFrom<Entry> for common::types::EnrichedRawEntry {
                type Error = ConversionError;

                fn try_from(value: Entry) -> Result<Self, Self::Error> {
                    let Entry { header, raw_entry } = value;

                    let header = common::types::EnrichedEntryHeader::try_from(
                        header.ok_or(ConversionError::missing_field("header"))?,
                    )?;

                    Ok(common::types::EnrichedRawEntry::new(header, raw_entry))
                }
            }

            impl From<common::types::EnrichedRawEntry> for Entry {
                fn from(value: common::types::EnrichedRawEntry) -> Self {
                    Entry {
                        header: Some(EnrichedEntryHeader::from(value.header)),
                        raw_entry: value.entry,
                    }
                }
            }

            impl TryFrom<CompletionResult> for common::types::CompletionResult {
                type Error = ConversionError;

                fn try_from(value: CompletionResult) -> Result<Self, Self::Error> {
                    let result = match value
                        .result
                        .ok_or(ConversionError::missing_field("result"))?
                    {
                        completion_result::Result::Ack(_) => common::types::CompletionResult::Ack,
                        completion_result::Result::Empty(_) => {
                            common::types::CompletionResult::Empty
                        }
                        completion_result::Result::Success(success) => {
                            common::types::CompletionResult::Success(success.value)
                        }
                        completion_result::Result::Failure(failure) => {
                            let failure_message = ByteString::try_from(failure.message)
                                .map_err(ConversionError::invalid_data);

                            common::types::CompletionResult::Failure(
                                failure.error_code,
                                failure_message?,
                            )
                        }
                    };

                    Ok(result)
                }
            }

            impl From<common::types::CompletionResult> for CompletionResult {
                fn from(value: common::types::CompletionResult) -> Self {
                    let result = match value {
                        common::types::CompletionResult::Ack => {
                            completion_result::Result::Ack(Ack {})
                        }
                        common::types::CompletionResult::Empty => {
                            completion_result::Result::Empty(Empty {})
                        }
                        common::types::CompletionResult::Success(value) => {
                            completion_result::Result::Success(Success { value })
                        }
                        common::types::CompletionResult::Failure(error_code, message) => {
                            completion_result::Result::Failure(Failure {
                                error_code,
                                message: message.into_bytes(),
                            })
                        }
                    };

                    CompletionResult {
                        result: Some(result),
                    }
                }
            }

            impl TryFrom<EnrichedEntryHeader> for common::types::EnrichedEntryHeader {
                type Error = ConversionError;

                fn try_from(value: EnrichedEntryHeader) -> Result<Self, Self::Error> {
                    let enriched_header =
                        match value.kind.ok_or(ConversionError::missing_field("kind"))? {
                            enriched_entry_header::Kind::PollInputStream(poll_input_stream) => {
                                common::types::EnrichedEntryHeader::PollInputStream {
                                    is_completed: poll_input_stream.is_completed,
                                }
                            }
                            enriched_entry_header::Kind::OutputStream(_) => {
                                common::types::EnrichedEntryHeader::OutputStream
                            }
                            enriched_entry_header::Kind::GetState(get_state) => {
                                common::types::EnrichedEntryHeader::GetState {
                                    is_completed: get_state.is_completed,
                                }
                            }
                            enriched_entry_header::Kind::SetState(_) => {
                                common::types::EnrichedEntryHeader::SetState
                            }
                            enriched_entry_header::Kind::ClearState(_) => {
                                common::types::EnrichedEntryHeader::ClearState
                            }
                            enriched_entry_header::Kind::Sleep(sleep) => {
                                common::types::EnrichedEntryHeader::Sleep {
                                    is_completed: sleep.is_completed,
                                }
                            }
                            enriched_entry_header::Kind::Invoke(invoke) => {
                                let resolution_result =
                                    Option::<common::types::ResolutionResult>::try_from(
                                        invoke.resolution_result.ok_or(
                                            ConversionError::missing_field("resolution_result"),
                                        )?,
                                    )?;

                                common::types::EnrichedEntryHeader::Invoke {
                                    is_completed: invoke.is_completed,
                                    resolution_result,
                                }
                            }
                            enriched_entry_header::Kind::BackgroundCall(background_call) => {
                                let resolution_result = common::types::ResolutionResult::try_from(
                                    background_call.resolution_result.ok_or(
                                        ConversionError::missing_field("resolution_result"),
                                    )?,
                                )?;

                                common::types::EnrichedEntryHeader::BackgroundInvoke {
                                    resolution_result,
                                }
                            }
                            enriched_entry_header::Kind::Awakeable(awakeable) => {
                                common::types::EnrichedEntryHeader::Awakeable {
                                    is_completed: awakeable.is_completed,
                                }
                            }
                            enriched_entry_header::Kind::CompleteAwakeable(_) => {
                                common::types::EnrichedEntryHeader::CompleteAwakeable
                            }
                            enriched_entry_header::Kind::Custom(custom) => {
                                common::types::EnrichedEntryHeader::Custom {
                                    code: u16::try_from(custom.code)
                                        .map_err(ConversionError::invalid_data)?,
                                    requires_ack: custom.requires_ack,
                                }
                            }
                        };

                    Ok(enriched_header)
                }
            }

            impl From<common::types::EnrichedEntryHeader> for EnrichedEntryHeader {
                fn from(value: common::types::EnrichedEntryHeader) -> Self {
                    let kind = match value {
                        common::types::EnrichedEntryHeader::PollInputStream { is_completed } => {
                            enriched_entry_header::Kind::PollInputStream(PollInputStream {
                                is_completed,
                            })
                        }
                        common::types::EnrichedEntryHeader::OutputStream => {
                            enriched_entry_header::Kind::OutputStream(OutputStream {})
                        }
                        common::types::EnrichedEntryHeader::GetState { is_completed } => {
                            enriched_entry_header::Kind::GetState(GetState { is_completed })
                        }
                        common::types::EnrichedEntryHeader::SetState => {
                            enriched_entry_header::Kind::SetState(SetState {})
                        }
                        common::types::EnrichedEntryHeader::ClearState => {
                            enriched_entry_header::Kind::ClearState(ClearState {})
                        }
                        common::types::EnrichedEntryHeader::Sleep { is_completed } => {
                            enriched_entry_header::Kind::Sleep(Sleep { is_completed })
                        }
                        common::types::EnrichedEntryHeader::Invoke {
                            is_completed,
                            resolution_result,
                        } => enriched_entry_header::Kind::Invoke(Invoke {
                            is_completed,
                            resolution_result: Some(InvocationResolutionResult::from(
                                resolution_result,
                            )),
                        }),
                        common::types::EnrichedEntryHeader::BackgroundInvoke {
                            resolution_result,
                        } => enriched_entry_header::Kind::BackgroundCall(BackgroundCall {
                            resolution_result: Some(BackgroundCallResolutionResult::from(
                                resolution_result,
                            )),
                        }),
                        common::types::EnrichedEntryHeader::Awakeable { is_completed } => {
                            enriched_entry_header::Kind::Awakeable(Awakeable { is_completed })
                        }
                        common::types::EnrichedEntryHeader::CompleteAwakeable => {
                            enriched_entry_header::Kind::CompleteAwakeable(CompleteAwakeable {})
                        }
                        common::types::EnrichedEntryHeader::Custom { requires_ack, code } => {
                            enriched_entry_header::Kind::Custom(Custom {
                                requires_ack,
                                code: u32::from(code),
                            })
                        }
                    };

                    EnrichedEntryHeader { kind: Some(kind) }
                }
            }

            impl TryFrom<InvocationResolutionResult> for Option<common::types::ResolutionResult> {
                type Error = ConversionError;

                fn try_from(value: InvocationResolutionResult) -> Result<Self, Self::Error> {
                    let result = match value
                        .result
                        .ok_or(ConversionError::missing_field("result"))?
                    {
                        invocation_resolution_result::Result::None(_) => None,
                        invocation_resolution_result::Result::Success(success) => {
                            let span_context = opentelemetry_api::trace::SpanContext::try_from(
                                success
                                    .span_context
                                    .ok_or(ConversionError::missing_field("span_context"))?,
                            )?;
                            let invocation_id =
                                try_bytes_into_invocation_id(success.invocation_id)?;
                            let service_key = success.service_key;

                            Some(common::types::ResolutionResult::Success {
                                span_context: common::types::ServiceInvocationSpanContext::new(
                                    span_context,
                                ),
                                invocation_id,
                                service_key,
                            })
                        }
                        invocation_resolution_result::Result::Failure(failure) => {
                            let error = ByteString::try_from(failure.error)
                                .map_err(ConversionError::invalid_data)?;

                            Some(common::types::ResolutionResult::Failure {
                                error_code: failure.error_code,
                                error,
                            })
                        }
                    };

                    Ok(result)
                }
            }

            impl From<Option<common::types::ResolutionResult>> for InvocationResolutionResult {
                fn from(value: Option<common::types::ResolutionResult>) -> Self {
                    let result = match value {
                        None => invocation_resolution_result::Result::None(Default::default()),
                        Some(resolution_result) => match resolution_result {
                            common::types::ResolutionResult::Success {
                                invocation_id,
                                service_key,
                                span_context,
                            } => invocation_resolution_result::Result::Success(
                                invocation_resolution_result::Success {
                                    invocation_id: invocation_id_to_bytes(&invocation_id),
                                    service_key,
                                    span_context: Some(SpanContext::from(
                                        opentelemetry_api::trace::SpanContext::from(span_context),
                                    )),
                                },
                            ),
                            common::types::ResolutionResult::Failure { error_code, error } => {
                                invocation_resolution_result::Result::Failure(
                                    invocation_resolution_result::Failure {
                                        error_code,
                                        error: error.into_bytes(),
                                    },
                                )
                            }
                        },
                    };

                    InvocationResolutionResult {
                        result: Some(result),
                    }
                }
            }

            impl TryFrom<BackgroundCallResolutionResult> for common::types::ResolutionResult {
                type Error = ConversionError;

                fn try_from(value: BackgroundCallResolutionResult) -> Result<Self, Self::Error> {
                    let resolution_result = match value
                        .result
                        .ok_or(ConversionError::missing_field("result"))?
                    {
                        background_call_resolution_result::Result::Success(success) => {
                            let span_context = opentelemetry_api::trace::SpanContext::try_from(
                                success
                                    .span_context
                                    .ok_or(ConversionError::missing_field("span_context"))?,
                            )?;
                            let invocation_id =
                                try_bytes_into_invocation_id(success.invocation_id)?;
                            let service_key = success.service_key;
                            common::types::ResolutionResult::Success {
                                span_context: common::types::ServiceInvocationSpanContext::new(
                                    span_context,
                                ),
                                invocation_id,
                                service_key,
                            }
                        }
                        background_call_resolution_result::Result::Failure(failure) => {
                            let error = ByteString::try_from(failure.error)
                                .map_err(ConversionError::invalid_data)?;
                            common::types::ResolutionResult::Failure {
                                error_code: failure.error_code,
                                error,
                            }
                        }
                    };

                    Ok(resolution_result)
                }
            }

            impl From<common::types::ResolutionResult> for BackgroundCallResolutionResult {
                fn from(value: common::types::ResolutionResult) -> Self {
                    let result = match value {
                        common::types::ResolutionResult::Success {
                            invocation_id,
                            span_context,
                            service_key,
                        } => background_call_resolution_result::Result::Success(
                            background_call_resolution_result::Success {
                                invocation_id: invocation_id_to_bytes(&invocation_id),
                                service_key,
                                span_context: Some(SpanContext::from(
                                    opentelemetry_api::trace::SpanContext::from(span_context),
                                )),
                            },
                        ),
                        common::types::ResolutionResult::Failure { error_code, error } => {
                            background_call_resolution_result::Result::Failure(
                                background_call_resolution_result::Failure {
                                    error_code,
                                    error: error.into_bytes(),
                                },
                            )
                        }
                    };

                    BackgroundCallResolutionResult {
                        result: Some(result),
                    }
                }
            }

            impl TryFrom<OutboxMessage> for common::types::OutboxMessage {
                type Error = ConversionError;

                fn try_from(value: OutboxMessage) -> Result<Self, Self::Error> {
                    let result = match value
                        .outbox_message
                        .ok_or(ConversionError::missing_field("outbox_message"))?
                    {
                        outbox_message::OutboxMessage::ServiceInvocationCase(
                            service_invocation,
                        ) => common::types::OutboxMessage::ServiceInvocation(
                            common::types::ServiceInvocation::try_from(
                                service_invocation
                                    .service_invocation
                                    .ok_or(ConversionError::missing_field("service_invocation"))?,
                            )?,
                        ),
                        outbox_message::OutboxMessage::ServiceInvocationResponse(
                            invocation_response,
                        ) => common::types::OutboxMessage::ServiceResponse(
                            common::types::InvocationResponse {
                                entry_index: invocation_response.entry_index,
                                id: common::types::ServiceInvocationId::try_from(
                                    invocation_response.service_invocation_id.ok_or(
                                        ConversionError::missing_field("service_invocation_id"),
                                    )?,
                                )?,
                                result: common::types::ResponseResult::try_from(
                                    invocation_response
                                        .response_result
                                        .ok_or(ConversionError::missing_field("response_result"))?,
                                )?,
                            },
                        ),
                        outbox_message::OutboxMessage::IngressResponse(ingress_response) => {
                            common::types::OutboxMessage::IngressResponse {
                                service_invocation_id:
                                    common::types::ServiceInvocationId::try_from(
                                        ingress_response.service_invocation_id.ok_or(
                                            ConversionError::missing_field("service_invocation_id"),
                                        )?,
                                    )?,
                                ingress_id: try_string_into_ingress_id(
                                    ingress_response.ingress_id,
                                )?,
                                response: common::types::ResponseResult::try_from(
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

            impl From<common::types::OutboxMessage> for OutboxMessage {
                fn from(value: common::types::OutboxMessage) -> Self {
                    let outbox_message = match value {
                        common::types::OutboxMessage::ServiceInvocation(service_invocation) => {
                            outbox_message::OutboxMessage::ServiceInvocationCase(
                                OutboxServiceInvocation {
                                    service_invocation: Some(ServiceInvocation::from(
                                        service_invocation,
                                    )),
                                },
                            )
                        }
                        common::types::OutboxMessage::ServiceResponse(invocation_response) => {
                            outbox_message::OutboxMessage::ServiceInvocationResponse(
                                OutboxServiceInvocationResponse {
                                    entry_index: invocation_response.entry_index,
                                    service_invocation_id: Some(ServiceInvocationId::from(
                                        invocation_response.id,
                                    )),
                                    response_result: Some(ResponseResult::from(
                                        invocation_response.result,
                                    )),
                                },
                            )
                        }
                        common::types::OutboxMessage::IngressResponse {
                            ingress_id,
                            service_invocation_id,
                            response,
                        } => {
                            outbox_message::OutboxMessage::IngressResponse(OutboxIngressResponse {
                                service_invocation_id: Some(ServiceInvocationId::from(
                                    service_invocation_id,
                                )),
                                ingress_id: ingress_id_to_string(ingress_id),
                                response_result: Some(ResponseResult::from(response)),
                            })
                        }
                    };

                    OutboxMessage {
                        outbox_message: Some(outbox_message),
                    }
                }
            }

            impl TryFrom<ResponseResult> for common::types::ResponseResult {
                type Error = ConversionError;

                fn try_from(value: ResponseResult) -> Result<Self, Self::Error> {
                    let result = match value
                        .response_result
                        .ok_or(ConversionError::missing_field("response_result"))?
                    {
                        response_result::ResponseResult::ResponseSuccess(success) => {
                            common::types::ResponseResult::Success(success.value)
                        }
                        response_result::ResponseResult::ResponseFailure(failure) => {
                            common::types::ResponseResult::Failure(
                                failure.failure_code,
                                ByteString::try_from(failure.failure_message)
                                    .map_err(ConversionError::invalid_data)?,
                            )
                        }
                    };

                    Ok(result)
                }
            }

            impl From<common::types::ResponseResult> for ResponseResult {
                fn from(value: common::types::ResponseResult) -> Self {
                    let response_result = match value {
                        common::types::ResponseResult::Success(value) => {
                            response_result::ResponseResult::ResponseSuccess(
                                response_result::ResponseSuccess { value },
                            )
                        }
                        common::types::ResponseResult::Failure(error_code, error) => {
                            response_result::ResponseResult::ResponseFailure(
                                response_result::ResponseFailure {
                                    failure_code: error_code,
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

            impl TryFrom<Timer> for common::types::Timer {
                type Error = ConversionError;

                fn try_from(_value: Timer) -> Result<Self, Self::Error> {
                    Ok(common::types::Timer)
                }
            }

            impl From<common::types::Timer> for Timer {
                fn from(_value: common::types::Timer) -> Self {
                    Timer::default()
                }
            }
        }
    }
}
