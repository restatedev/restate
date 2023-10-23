// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use assert2::let_assert;
use bytes::{BufMut, BytesMut};
use bytestring::ByteString;
use prost::Message;
use restate_pb::builtin_service::ResponseSerializer;
use restate_pb::restate::internal::get_result_response::InvocationFailure;
use restate_pb::restate::internal::{
    get_result_response, journal_completion_notification_request, recv_response, send_response,
    start_response, CleanupRequest, GetResultRequest, GetResultResponse,
    InactivityTimeoutTimerRequest, JournalCompletionNotificationRequest, KillNotificationRequest,
    PingRequest, RecvRequest, RecvResponse, RemoteContextBuiltInService, SendRequest, SendResponse,
    StartRequest, StartResponse,
};
use restate_pb::{
    REMOTE_CONTEXT_INTERNAL_ON_COMPLETION_METHOD_NAME, REMOTE_CONTEXT_INTERNAL_ON_KILL_METHOD_NAME,
};
use restate_schema_api::key::KeyExtractor;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol::message::{
    Decoder, Encoder, EncodingError, MessageHeader, ProtocolMessage,
};
use restate_types::errors::KILLED_INVOCATION_ERROR;
use restate_types::identifiers::{InvocationId, InvocationUuid, WithPartitionKey};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext};
use restate_types::journal::enriched::{EnrichedEntryHeader, ResolutionResult};
use restate_types::journal::raw::{PlainRawEntry, RawEntryCodec, RawEntryHeader};
use restate_types::journal::{
    BackgroundInvokeEntry, ClearStateEntry, CompleteAwakeableEntry, Entry, GetStateEntry,
    InvokeEntry, InvokeRequest, OutputStreamEntry, SetStateEntry,
};
use restate_types::journal::{Completion, CompletionResult};
use serde::{Deserialize, Serialize};
use std::iter;
use std::time::{Duration, SystemTime};
use tracing::{debug, instrument, trace, warn};

#[derive(Serialize, Deserialize, Debug)]
enum InvocationStatus {
    Executing {
        invocation_uuid: InvocationUuid,
        stream_id: String,
        retention_period_sec: u32,
    },
    Done(
        #[serde(with = "serde_with::As::<restate_serde_util::ProtobufEncoded>")] GetResultResponse,
    ),
}
const STATUS: StateKey<Bincode<InvocationStatus>> = StateKey::new_bincode("_internal_status");

// There can be at most one sink pulling at the same time
const PENDING_RECV_SINK: StateKey<Bincode<(FullInvocationId, ServiceInvocationResponseSink)>> =
    StateKey::new_bincode("_internal_pull_sink");

// Stream of events to send yet
const PENDING_RECV_STREAM: StateKey<Raw> = StateKey::new_raw("_internal_pending_recv_stream");

// There can be many clients invoking GetResult
type SinksState = Vec<(FullInvocationId, ServiceInvocationResponseSink)>;
const PENDING_GET_RESULT_SINKS: StateKey<Bincode<SinksState>> =
    StateKey::new_bincode("_internal_result_sinks");

#[derive(Serialize, Deserialize, Debug)]
struct InvokeEntryContext {
    operation_id: String,
    entry_index: EntryIndex,
}

const DEFAULT_RETENTION_PERIOD_SEC: u32 = 30 * 60;

const PROTOCOL_VERSION: u16 = 0;

const STREAM_TIMEOUT_SEC: u32 = 60;

// TODO perhaps it makes sense to promote this to a "interval timer feature",
//  and include it directly in the PP timer support.
//  It could be used for a bunch of other things, such as cron scheduling,
//  and from the user itself to track inactivity (e.g. users inactivity!)
#[derive(Serialize, Deserialize, Debug, Default)]
struct InactivityTracker {
    timer_index: u64,
}
const INACTIVITY_TRACKER: StateKey<Bincode<InactivityTracker>> =
    StateKey::new_bincode("_inactivity");

impl<'a, State: StateReader> InvocationContext<'a, State> {
    #[allow(clippy::too_many_arguments)]
    async fn handle_protocol_message(
        &mut self,
        operation_id: &str,
        stream_id: &str,
        retention_period_sec: u32,
        entry_index: EntryIndex,
        journal_span_context: ServiceInvocationSpanContext,
        header: MessageHeader,
        message: ProtocolMessage,
    ) -> Result<(), InvocationError> {
        trace!(restate.protocol.message_header = ?header, restate.protocol.message = ?message, "Received message");
        match message {
            ProtocolMessage::Start { .. } => Err(InvocationError::new(
                UserErrorCode::FailedPrecondition,
                "Unexpected StartMessage received",
            )),
            ProtocolMessage::Completion(_) => Err(InvocationError::new(
                UserErrorCode::FailedPrecondition,
                "Unexpected CompletionMessage received",
            )),
            ProtocolMessage::Suspension(_) => Err(InvocationError::new(
                UserErrorCode::FailedPrecondition,
                "Unexpected SuspensionMessage received",
            )),
            ProtocolMessage::Error(error) => {
                warn!(
                    ?error,
                    restate.embedded_handler.operation_id = %operation_id,
                    restate.embedded_handler.stream_id = %stream_id,
                    "Error when executing the invocation");
                Ok(())
            }
            ProtocolMessage::UnparsedEntry(entry) => {
                self.handle_entry(
                    operation_id,
                    retention_period_sec,
                    entry_index,
                    journal_span_context,
                    entry,
                )
                .await
            }
        }
    }

    async fn handle_entry(
        &mut self,
        operation_id: &str,
        retention_period_sec: u32,
        entry_index: EntryIndex,
        journal_span_context: ServiceInvocationSpanContext,
        mut entry: PlainRawEntry,
    ) -> Result<(), InvocationError> {
        let enriched_entry = match entry.header {
            RawEntryHeader::OutputStream => {
                let_assert!(
                    Entry::OutputStream(OutputStreamEntry { result }) =
                        ProtobufRawEntryCodec::deserialize(&entry)
                            .map_err(InvocationError::internal)?
                );

                self.complete_invocation(
                    operation_id,
                    retention_period_sec,
                    entry_index,
                    result.into(),
                )
                .await?;
                EnrichedRawEntry::new(EnrichedEntryHeader::OutputStream, entry.entry)
            }
            RawEntryHeader::GetState { is_completed } => {
                let_assert!(
                    Entry::GetState(GetStateEntry { key, .. }) =
                        ProtobufRawEntryCodec::deserialize(&entry)
                            .map_err(InvocationError::internal)?
                );
                let state_key = check_state_key(key)?;

                if !is_completed {
                    let state_value = self.load_state(&state_key).await?;

                    let completion = Completion::new(
                        entry_index as EntryIndex,
                        match state_value {
                            Some(value) => CompletionResult::Success(value),
                            None => CompletionResult::Empty,
                        },
                    );

                    ProtobufRawEntryCodec::write_completion(&mut entry, completion.result.clone())
                        .map_err(InvocationError::internal)?;
                    self.enqueue_protocol_message(ProtocolMessage::from(completion))
                        .await?;
                }
                EnrichedRawEntry::new(
                    EnrichedEntryHeader::GetState { is_completed: true },
                    entry.entry,
                )
            }
            RawEntryHeader::SetState => {
                let_assert!(
                    Entry::SetState(SetStateEntry { key, value }) =
                        ProtobufRawEntryCodec::deserialize(&entry)
                            .map_err(InvocationError::internal)?
                );
                let state_key = check_state_key(key)?;
                self.set_state(&state_key, &value)?;
                EnrichedRawEntry::new(EnrichedEntryHeader::SetState, entry.entry)
            }
            RawEntryHeader::ClearState => {
                let_assert!(
                    Entry::ClearState(ClearStateEntry { key }) =
                        ProtobufRawEntryCodec::deserialize(&entry)
                            .map_err(InvocationError::internal)?
                );
                let state_key = check_state_key(key)?;
                self.clear_state(&state_key);
                EnrichedRawEntry::new(EnrichedEntryHeader::ClearState, entry.entry)
            }
            RawEntryHeader::Invoke { is_completed } => {
                if !is_completed {
                    let_assert!(
                        Entry::Invoke(InvokeEntry { request, .. }) =
                            ProtobufRawEntryCodec::deserialize(&entry)
                                .map_err(InvocationError::internal)?
                    );

                    let fid = self.generate_fid_from_invoke_request(&request)?;
                    let span_context =
                        ServiceInvocationSpanContext::start(&fid, journal_span_context.as_parent());

                    self.send_message(OutboxMessage::ServiceInvocation(ServiceInvocation {
                        fid: fid.clone(),
                        method_name: request.method_name,
                        argument: request.parameter,
                        response_sink: Some(ServiceInvocationResponseSink::NewInvocation {
                            target: FullInvocationId::with_service_id(
                                self.full_invocation_id.service_id.clone(),
                                InvocationUuid::now_v7(),
                            ),
                            method: REMOTE_CONTEXT_INTERNAL_ON_COMPLETION_METHOD_NAME.to_string(),
                            caller_context: Bincode::encode(&InvokeEntryContext {
                                operation_id: operation_id.to_string(),
                                entry_index,
                            })
                            .map_err(InvocationError::internal)?,
                        }),
                        span_context: span_context.clone(),
                    }));

                    EnrichedRawEntry::new(
                        EnrichedEntryHeader::Invoke {
                            is_completed,
                            resolution_result: Some(ResolutionResult {
                                invocation_uuid: fid.invocation_uuid,
                                service_key: fid.service_id.key,
                                service_name: fid.service_id.service_name,
                                span_context,
                            }),
                        },
                        entry.entry,
                    )
                } else {
                    EnrichedRawEntry::new(
                        EnrichedEntryHeader::Invoke {
                            is_completed,
                            resolution_result: None,
                        },
                        entry.entry,
                    )
                }
            }
            RawEntryHeader::BackgroundInvoke => {
                let_assert!(
                    Entry::BackgroundInvoke(BackgroundInvokeEntry {
                        request,
                        invoke_time,
                    }) = ProtobufRawEntryCodec::deserialize(&entry)
                        .map_err(InvocationError::internal)?
                );

                let fid = self.generate_fid_from_invoke_request(&request)?;
                let span_context =
                    ServiceInvocationSpanContext::start(&fid, journal_span_context.as_linked());

                if invoke_time != 0 {
                    self.delay_invoke(
                        fid.clone(),
                        request.method_name.into(),
                        request.parameter,
                        None,
                        invoke_time.into(),
                        entry_index,
                    )
                } else {
                    self.send_message(OutboxMessage::ServiceInvocation(ServiceInvocation {
                        fid: fid.clone(),
                        method_name: request.method_name,
                        argument: request.parameter,
                        response_sink: None,
                        span_context: span_context.clone(),
                    }))
                }
                EnrichedRawEntry::new(
                    EnrichedEntryHeader::BackgroundInvoke {
                        resolution_result: ResolutionResult {
                            invocation_uuid: fid.invocation_uuid,
                            service_key: fid.service_id.key,
                            service_name: fid.service_id.service_name,
                            span_context,
                        },
                    },
                    entry.entry,
                )
            }
            RawEntryHeader::Awakeable { is_completed } => {
                EnrichedRawEntry::new(EnrichedEntryHeader::Awakeable { is_completed }, entry.entry)
            }
            RawEntryHeader::CompleteAwakeable => {
                let_assert!(
                    Entry::CompleteAwakeable(CompleteAwakeableEntry { id, result }) =
                        ProtobufRawEntryCodec::deserialize(&entry)
                            .map_err(InvocationError::internal)?
                );

                let (invocation_id, entry_index) = AwakeableIdentifier::decode(id)
                    .map_err(InvocationError::internal)?
                    .into_inner();

                self.send_message(OutboxMessage::from_awakeable_completion(
                    invocation_id.clone(),
                    entry_index,
                    result.into(),
                ));

                EnrichedRawEntry::new(
                    EnrichedEntryHeader::CompleteAwakeable {
                        invocation_id,
                        entry_index,
                    },
                    entry.entry,
                )
            }
            RawEntryHeader::Custom { requires_ack, code } => {
                if requires_ack {
                    self.enqueue_protocol_message(ProtocolMessage::from(Completion::new(
                        entry_index,
                        CompletionResult::Ack,
                    )))
                    .await?;
                }
                EnrichedRawEntry::new(
                    EnrichedEntryHeader::Custom {
                        code,
                        requires_ack: false,
                    },
                    entry.entry,
                )
            }
            RawEntryHeader::PollInputStream { .. } => {
                return Err(InvocationError::new(
                    UserErrorCode::FailedPrecondition,
                    "Unexpected PollInputStream entry received",
                ))
            }
            RawEntryHeader::Sleep { .. } => {
                return Err(InvocationError::new(
                    UserErrorCode::Unimplemented,
                    "Unsupported entry type",
                ))
            }
        };

        self.store_journal_entry(self.journal_service_id(), entry_index, enriched_entry);
        Ok(())
    }

    async fn close_pending_recv(
        &mut self,
        res: recv_response::Response,
    ) -> Result<(), InvocationError> {
        if let Some((fid, recv_sink)) = self.pop_state(&PENDING_RECV_SINK).await? {
            trace!("Closing the previously listening client with {:?}", res);
            // Because the caller of Start becomes the leading client,
            // if the previous client is waiting on a recv, it must be excluded.
            self.send_message(OutboxMessage::from_response_sink(
                &fid,
                recv_sink,
                ResponseSerializer::<RecvResponse>::default().serialize_success(RecvResponse {
                    response: Some(res),
                }),
            ));
        }
        Ok(())
    }

    async fn close_pending_get_result(
        &mut self,
        res: GetResultResponse,
    ) -> Result<(), InvocationError> {
        for (fid, get_result_sink) in self
            .pop_state(&PENDING_GET_RESULT_SINKS)
            .await?
            .unwrap_or_default()
        {
            trace!("Closing the previously listening client with {:?}", res);
            // Because the caller of Start becomes the leading client,
            // if the previous client is waiting on a recv, it must be excluded.
            self.send_message(OutboxMessage::from_response_sink(
                &fid,
                get_result_sink,
                ResponseSerializer::<GetResultResponse>::default().serialize_success(res.clone()),
            ));
        }
        Ok(())
    }

    async fn enqueue_protocol_message(
        &mut self,
        msg: ProtocolMessage,
    ) -> Result<(), InvocationError> {
        let encoder = Encoder::new(PROTOCOL_VERSION);

        if let Some((fid, recv_sink)) = self.pop_state(&PENDING_RECV_SINK).await? {
            trace!(restate.protocol.message = ?msg, "Sending message");
            self.send_message(OutboxMessage::from_response_sink(
                &fid,
                recv_sink,
                ResponseSerializer::<RecvResponse>::default().serialize_success(RecvResponse {
                    response: Some(recv_response::Response::Messages(encoder.encode(msg))),
                }),
            ));
        } else {
            trace!(restate.protocol.message = ?msg, "Enqueuing message");
            // Enqueue in existing recv message stream
            let pending_recv_stream = self
                .load_state(&PENDING_RECV_STREAM)
                .await?
                .unwrap_or_default();

            let encoder = Encoder::new(PROTOCOL_VERSION);
            let mut new_pending_recv_stream =
                BytesMut::with_capacity(pending_recv_stream.len() + encoder.encoded_len(&msg));
            new_pending_recv_stream.put(pending_recv_stream);
            encoder
                .encode_to_buf_mut(&mut new_pending_recv_stream, msg)
                .expect(
                    "Encoding messages to a BytesMut should be infallible, unless OOM is reached.",
                );

            self.set_state(&PENDING_RECV_STREAM, &new_pending_recv_stream.freeze())?;
        }
        Ok(())
    }

    async fn complete_invocation(
        &mut self,
        operation_id: &str,
        retention_period_sec: u32,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> Result<(), InvocationError> {
        let expiry_time = SystemTime::now() + Duration::from_secs(retention_period_sec as u64);

        let get_result_response = GetResultResponse {
            expiry_time: humantime::format_rfc3339(expiry_time).to_string(),
            response: Some(match result {
                ResponseResult::Success(s) => get_result_response::Response::Success(s),
                ResponseResult::Failure(code, msg) => {
                    get_result_response::Response::Failure(InvocationFailure {
                        code: code.into(),
                        message: msg.to_string(),
                    })
                }
            }),
        };

        self.set_state(
            &STATUS,
            &InvocationStatus::Done(get_result_response.clone()),
        )?;
        self.close_pending_recv(recv_response::Response::Messages(Bytes::new()))
            .await?;
        self.close_pending_get_result(get_result_response).await?;
        self.delay_invoke(
            FullInvocationId::with_service_id(
                self.full_invocation_id.service_id.clone(),
                InvocationUuid::now_v7(),
            ),
            "Cleanup".to_string(),
            CleanupRequest {
                operation_id: operation_id.to_string(),
            }
            .encode_to_vec()
            .into(),
            None,
            expiry_time.into(),
            entry_index,
        );

        Ok(())
    }

    async fn reset_inactivity_timer(
        &mut self,
        operation_id: &str,
        stream_id: &str,
    ) -> Result<(), InvocationError> {
        let mut inactivity_tracker = self
            .load_state(&INACTIVITY_TRACKER)
            .await?
            .unwrap_or_default();
        inactivity_tracker.timer_index += 1;

        // TODO the timer support doesn't have any way to delete pending timers,
        //  and it's unclear whether writing a timer with same id will overwrite the previous one.
        self.delay_invoke(
            FullInvocationId::with_service_id(
                self.full_invocation_id.service_id.clone(),
                InvocationUuid::now_v7(),
            ),
            "InternalOnInactivityTimer".to_string(),
            InactivityTimeoutTimerRequest {
                operation_id: operation_id.to_string(),
                stream_id: stream_id.to_string(),
                // We add an incremental index every time we set this timer,
                // so once it fires we can use that to check if it's the current timer or not.
                inactivity_timer_index: inactivity_tracker.timer_index,
            }
            .encode_to_vec()
            .into(),
            None,
            (SystemTime::now() + Duration::from_secs(STREAM_TIMEOUT_SEC as u64)).into(),
            // The timer index is per "timer registrar", in this case the FID of the RemoteContext invocation.
            // The reason we use 0 is that no other delay_invoke is scheduled on 0 index.
            //
            // Should we get rid of this assumption?
            0,
        );

        self.set_state(&INACTIVITY_TRACKER, &inactivity_tracker)?;
        Ok(())
    }

    fn generate_fid_from_invoke_request(
        &self,
        request: &InvokeRequest,
    ) -> Result<FullInvocationId, InvocationError> {
        let service_key = self
            .schemas
            .extract(
                &request.service_name,
                &request.method_name,
                request.parameter.clone(),
            )
            .map_err(InvocationError::internal)?;

        Ok(FullInvocationId::generate(
            request.service_name.clone(),
            service_key,
        ))
    }

    fn journal_service_id(&self) -> ServiceId {
        // We use the same service key of the remote context to make sure the partition key is the same
        ServiceId::new(
            "dev.restate.EmbeddedHandlerJournal",
            self.full_invocation_id.service_id.key.clone(),
        )
    }
}

#[async_trait::async_trait]
impl<'a, State: StateReader + Send + Sync> RemoteContextBuiltInService
    for InvocationContext<'a, State>
{
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.remote_context.operation_id = %request.operation_id,
            restate.remote_context.stream_id = %request.stream_id,
        )
    )]
    async fn start(
        &mut self,
        request: StartRequest,
        response_serializer: ResponseSerializer<StartResponse>,
    ) -> Result<(), InvocationError> {
        if let Some(InvocationStatus::Done(get_result_response)) = self.load_state(&STATUS).await? {
            trace!("The result is already available, returning the known result");
            // Response is already here, so we're good, we simply send it back
            self.reply_to_caller(response_serializer.serialize_success(StartResponse {
                invocation_status: Some(start_response::InvocationStatus::Completed(
                    get_result_response,
                )),
                ..StartResponse::default()
            }));
            return Ok(());
        }

        self.close_pending_recv(recv_response::Response::InvalidStream(()))
            .await?;

        self.reset_inactivity_timer(&request.operation_id, &request.stream_id)
            .await?;

        // Make sure we have a journal
        let journal_service_id = self.journal_service_id();
        let (invocation_uuid, length, journal_entries) =
            if let Some((invocation_uuid, journal_metadata, journal_entries)) =
                self.load_journal(&journal_service_id).await?
            {
                (invocation_uuid, journal_metadata.length, journal_entries)
            } else {
                // If there isn't any journal, let's create one
                let input_entry = EnrichedRawEntry::new(
                    EnrichedEntryHeader::PollInputStream { is_completed: true },
                    ProtobufRawEntryCodec::serialize_as_unary_input_entry(request.argument).entry,
                );
                let invocation_uuid = InvocationUuid::now_v7();
                self.create_journal(
                    journal_service_id.clone(),
                    invocation_uuid,
                    self.span_context.clone(),
                    NotificationTarget {
                        service: self.full_invocation_id.service_id.clone(),
                        method: REMOTE_CONTEXT_INTERNAL_ON_COMPLETION_METHOD_NAME.to_string(),
                    },
                    NotificationTarget {
                        service: self.full_invocation_id.service_id.clone(),
                        method: REMOTE_CONTEXT_INTERNAL_ON_KILL_METHOD_NAME.to_string(),
                    },
                );
                self.store_journal_entry(journal_service_id, 0, input_entry.clone());
                (invocation_uuid, 1, vec![input_entry])
            };

        // We're now the leading client, let's write the status
        self.set_state(
            &STATUS,
            &InvocationStatus::Executing {
                invocation_uuid,
                stream_id: request.stream_id,
                retention_period_sec: if request.retention_period_sec != 0 {
                    request.retention_period_sec
                } else {
                    DEFAULT_RETENTION_PERIOD_SEC
                },
            },
        )?;

        // Let's create the messages and write them to a buffer
        let invocation_id =
            InvocationId::new(self.full_invocation_id.partition_key(), invocation_uuid);
        let encoder = Encoder::new(PROTOCOL_VERSION);
        let mut stream_buffer = BytesMut::new();
        encoder
            .encode_to_buf_mut(
                &mut stream_buffer,
                ProtocolMessage::new_start_message(
                    Bytes::copy_from_slice(&invocation_id.as_bytes()),
                    invocation_id.to_string(),
                    length,
                    true, // TODO add eager state
                    iter::empty(),
                ),
            )
            .expect("Encoding messages to a BytesMut should be infallible, unless OOM is reached.");
        for entry in journal_entries {
            encoder
                .encode_to_buf_mut(
                    &mut stream_buffer,
                    ProtocolMessage::from(PlainRawEntry::from(entry)),
                )
                .expect(
                    "Encoding messages to a BytesMut should be infallible, unless OOM is reached.",
                );
        }

        self.reply_to_caller(response_serializer.serialize_success(StartResponse {
            stream_timeout_sec: STREAM_TIMEOUT_SEC,
            invocation_status: Some(start_response::InvocationStatus::Executing(
                stream_buffer.freeze(),
            )),
        }));
        Ok(())
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.remote_context.operation_id = %request.operation_id,
            restate.remote_context.stream_id = %request.stream_id,
        )
    )]
    async fn send(
        &mut self,
        request: SendRequest,
        response_serializer: ResponseSerializer<SendResponse>,
    ) -> Result<(), InvocationError> {
        let status = self.load_state_or_fail(&STATUS).await?;
        let retention_period_sec = match status {
            InvocationStatus::Executing {
                stream_id,
                retention_period_sec,
                ..
            } => {
                if stream_id != request.stream_id {
                    self.reply_to_caller(response_serializer.serialize_success(SendResponse {
                        response: Some(send_response::Response::InvalidStream(())),
                    }));
                    return Ok(());
                }
                retention_period_sec
            }
            InvocationStatus::Done { .. } => {
                self.reply_to_caller(response_serializer.serialize_success(SendResponse {
                    response: Some(send_response::Response::InvocationCompleted(())),
                }));
                return Ok(());
            }
        };

        self.reset_inactivity_timer(&request.operation_id, &request.stream_id)
            .await?;

        let journal_service_id = self.journal_service_id();
        let (_, mut journal_metadata) = self
            .load_journal_metadata(&journal_service_id)
            .await?
            .ok_or_else(|| InvocationError::internal("There must be a journal at this point"))?;
        for (message_header, message) in decode_messages(request.messages)
            .map_err(|e| InvocationError::new(UserErrorCode::FailedPrecondition, e))?
        {
            self.handle_protocol_message(
                &request.operation_id,
                &request.stream_id,
                retention_period_sec,
                journal_metadata.length,
                journal_metadata.span_context.clone(),
                message_header,
                message,
            )
            .await?;
            journal_metadata.length += 1;
        }

        self.reply_to_caller(response_serializer.serialize_success(SendResponse {
            response: Some(send_response::Response::Ok(())),
        }));
        Ok(())
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.remote_context.operation_id = %request.operation_id,
            restate.remote_context.stream_id = %request.stream_id,
        )
    )]
    async fn recv(
        &mut self,
        request: RecvRequest,
        response_serializer: ResponseSerializer<RecvResponse>,
    ) -> Result<(), InvocationError> {
        let status = self.load_state_or_fail(&STATUS).await?;
        match status {
            InvocationStatus::Executing { stream_id, .. } => {
                if stream_id != request.stream_id {
                    self.reply_to_caller(response_serializer.serialize_success(RecvResponse {
                        response: Some(recv_response::Response::InvalidStream(())),
                    }));
                    return Ok(());
                }
            }
            InvocationStatus::Done { .. } => {
                self.reply_to_caller(response_serializer.serialize_success(RecvResponse {
                    response: Some(recv_response::Response::InvocationCompleted(())),
                }));
                return Ok(());
            }
        };

        self.reset_inactivity_timer(&request.operation_id, &request.stream_id)
            .await?;

        if let Some(pending_stream) = self.pop_state(&PENDING_RECV_STREAM).await? {
            self.reply_to_caller(response_serializer.serialize_success(RecvResponse {
                response: Some(recv_response::Response::Messages(pending_stream)),
            }));
        } else if let Some(service_invocation_response_sink) = self.response_sink {
            self.set_state(
                &PENDING_RECV_SINK,
                &(
                    self.full_invocation_id.clone(),
                    service_invocation_response_sink.clone(),
                ),
            )?;
        }

        Ok(())
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.remote_context.operation_id = %request.operation_id,
            restate.remote_context.stream_id = %request.stream_id,
        )
    )]
    async fn ping(
        &mut self,
        request: PingRequest,
        response_serializer: ResponseSerializer<SendResponse>,
    ) -> Result<(), InvocationError> {
        RemoteContextBuiltInService::send(
            self,
            SendRequest {
                operation_id: request.operation_id,
                stream_id: request.stream_id,
                messages: Default::default(),
            },
            response_serializer,
        )
        .await
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.remote_context.operation_id = %_request.operation_id
        )
    )]
    async fn get_result(
        &mut self,
        _request: GetResultRequest,
        response_serializer: ResponseSerializer<GetResultResponse>,
    ) -> Result<(), InvocationError> {
        match self.load_state(&STATUS).await? {
            Some(InvocationStatus::Executing { .. }) => {
                if let Some(sink) = self.response_sink {
                    let mut pending_sinks = self
                        .load_state(&PENDING_GET_RESULT_SINKS)
                        .await?
                        .unwrap_or_default();
                    pending_sinks.push((self.full_invocation_id.clone(), sink.clone()));
                    self.set_state(&PENDING_GET_RESULT_SINKS, &pending_sinks)?;
                }
            }
            Some(InvocationStatus::Done(get_result_response)) => {
                self.reply_to_caller(response_serializer.serialize_success(get_result_response));
            }
            None => {
                self.reply_to_caller(response_serializer.serialize_success(GetResultResponse {
                    response: Some(get_result_response::Response::None(())),
                    ..GetResultResponse::default()
                }));
            }
        };
        Ok(())
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.remote_context.operation_id = %_request.operation_id
        )
    )]
    async fn cleanup(
        &mut self,
        _request: CleanupRequest,
        response_serializer: ResponseSerializer<()>,
    ) -> Result<(), InvocationError> {
        self.clear_state(&STATUS);
        self.clear_state(&PENDING_RECV_STREAM);
        self.clear_state(&PENDING_RECV_SINK);
        self.clear_state(&PENDING_GET_RESULT_SINKS);

        // Drop journal
        let journal_service_id = self.journal_service_id();
        if let Some((_, journal_meta)) = self.load_journal_metadata(&journal_service_id).await? {
            self.drop_journal(journal_service_id, journal_meta.length)
        }

        self.reply_to_caller(response_serializer.serialize_success(()));
        Ok(())
    }

    #[instrument(level = "trace", skip_all)]
    async fn internal_on_completion(
        &mut self,
        request: JournalCompletionNotificationRequest,
        response_serializer: ResponseSerializer<()>,
    ) -> Result<(), InvocationError> {
        match self.load_state(&STATUS).await? {
            Some(InvocationStatus::Executing {
                invocation_uuid, ..
            }) if invocation_uuid.as_bytes() == request.invocation_uuid => {
                self.enqueue_protocol_message(ProtocolMessage::from(Completion::new(
                    request.entry_index,
                    match request.result.ok_or_else(|| {
                        InvocationError::internal("Completion notification must be non empty")
                    })? {
                        journal_completion_notification_request::Result::Empty(()) => {
                            CompletionResult::Empty
                        }
                        journal_completion_notification_request::Result::Success(s) => {
                            CompletionResult::Success(s)
                        }
                        journal_completion_notification_request::Result::Failure(failure) => {
                            CompletionResult::Failure(failure.code.into(), failure.message.into())
                        }
                    },
                )))
                .await?;
            }
            Some(InvocationStatus::Executing {
                invocation_uuid, ..
            }) => {
                debug!(
                    "Discarding response received with fid {:?} because the journal has a different invocation_uuid: {} != {}.",
                    self.full_invocation_id, invocation_uuid, InvocationUuid::from_slice(&request.invocation_uuid).unwrap()
                );
            }
            _ => {
                debug!(
                    "Discarding response received with fid {:?} because there is no journal.",
                    self.full_invocation_id
                );
            }
        }

        self.reply_to_caller(response_serializer.serialize_success(()));
        Ok(())
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.remote_context.operation_id = %request.operation_id,
            restate.remote_context.stream_id = %request.stream_id,
        )
    )]
    async fn internal_on_inactivity_timer(
        &mut self,
        request: InactivityTimeoutTimerRequest,
        response_serializer: ResponseSerializer<()>,
    ) -> Result<(), InvocationError> {
        if let Some(InvocationStatus::Executing { stream_id, .. }) =
            self.load_state(&STATUS).await?
        {
            if stream_id == request.stream_id {
                if let Some(inactivity_tracker) = self.load_state(&INACTIVITY_TRACKER).await? {
                    if inactivity_tracker.timer_index == request.inactivity_timer_index {
                        // Fail both pending get results and recv for inactivity
                        let response = ResponseResult::Failure(
                            UserErrorCode::DeadlineExceeded,
                            ByteString::from_static("Closing due to leader inactivity"),
                        );
                        for (fid, sink) in self
                            .pop_state(&PENDING_GET_RESULT_SINKS)
                            .await?
                            .unwrap_or_default()
                        {
                            trace!(
                                "Closing the previously listening client with {:?} for inactivity",
                                response
                            );
                            self.send_message(OutboxMessage::from_response_sink(
                                &fid,
                                sink,
                                response.clone(),
                            ));
                        }
                    }
                }
            }
        }

        self.reply_to_caller(response_serializer.serialize_success(()));
        Ok(())
    }

    #[instrument(level = "trace", skip_all)]
    async fn internal_on_kill(
        &mut self,
        request: KillNotificationRequest,
        response_serializer: ResponseSerializer<()>,
    ) -> Result<(), InvocationError> {
        if let Some(InvocationStatus::Executing {
            invocation_uuid,
            retention_period_sec,
            ..
        }) = self.load_state(&STATUS).await?
        {
            if invocation_uuid.as_bytes() == request.invocation_uuid {
                self.complete_invocation(
                    // It's ok to not set the operation_id here, because this is used for the CleanupRequest,
                    // and it's used only for observability purposes.
                    // The delivery of the cleanup request is hardwired with the service id, so it won't go through key extraction.
                    "",
                    retention_period_sec,
                    0,
                    KILLED_INVOCATION_ERROR.into(),
                )
                .await?;
            } else {
                trace!(
                    "Ignoring kill because invocation uuid don't match: {:?} != {:?}",
                    invocation_uuid.as_bytes(),
                    request.invocation_uuid
                )
            }
        }

        self.reply_to_caller(response_serializer.serialize_success(()));
        Ok(())
    }
}

fn decode_messages(buf: Bytes) -> Result<Vec<(MessageHeader, ProtocolMessage)>, EncodingError> {
    let mut decoder = Decoder::new(usize::MAX, None);
    decoder.push(buf);

    iter::from_fn(|| decoder.consume_next().transpose()).collect()
}

fn check_state_key(key: Bytes) -> Result<StateKey<Raw>, InvocationError> {
    if key.starts_with(b"_internal") {
        return Err(InvocationError::new(
            UserErrorCode::InvalidArgument,
            "Unexpected key {key:?}. State keys should not start with _internal",
        ));
    }
    Ok(StateKey::<Raw>::from(
        String::from_utf8(key.to_vec()).map_err(InvocationError::internal)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::services::non_deterministic::tests::TestInvocationContext;
    use futures::future::LocalBoxFuture;
    use futures::FutureExt;
    use googletest::matcher::{Matcher, MatcherResult};
    use googletest::{all, assert_that, elements_are, pat, property};
    use prost::Message;
    use restate_pb::mocks::greeter::{GreetingRequest, GreetingResponse};
    use restate_pb::mocks::GREETER_SERVICE_NAME;
    use restate_pb::restate::internal::{get_result_response, start_response, CleanupRequest};
    use restate_pb::REMOTE_CONTEXT_SERVICE_NAME;
    use restate_schema_api::discovery::{
        DiscoveredInstanceType, DiscoveredMethodMetadata, ServiceRegistrationRequest,
    };
    use restate_schema_api::endpoint::EndpointMetadata;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_test_util::matchers::*;
    use restate_test_util::{assert_eq, test};
    use restate_types::errors::InvocationErrorCode;
    use restate_types::invocation::{InvocationResponse, MaybeFullInvocationId};
    use restate_types::journal::{Entry, EntryResult, EntryType};

    const USER_STATE: StateKey<Raw> = StateKey::new_raw("my-state");

    fn encode_messages(messages: Vec<ProtocolMessage>) -> Bytes {
        let encoder = Encoder::new(PROTOCOL_VERSION);

        let mut buf = BytesMut::new();
        for msg in messages {
            trace!(restate.protocol.message = ?msg, "Sending message");
            encoder.encode_to_buf_mut(&mut buf, msg).expect(
                "Encoding messages to a BytesMut should be infallible, unless OOM is reached.",
            )
        }

        buf.freeze()
    }

    fn encode_entries(entries: Vec<Entry>) -> Bytes {
        encode_messages(
            entries
                .into_iter()
                .map(|e| ProtocolMessage::from(ProtobufRawEntryCodec::serialize(e)))
                .collect(),
        )
    }

    pub struct ProtocolMessageDecodeMatcher<InnerMatcher> {
        inner: InnerMatcher,
    }

    impl<InnerMatcher: Matcher<ActualT = Vec<ProtocolMessage>>> Matcher
        for ProtocolMessageDecodeMatcher<InnerMatcher>
    {
        type ActualT = Bytes;

        fn matches(&self, actual: &Self::ActualT) -> MatcherResult {
            if let Ok(msgs) = decode_messages(actual.clone()) {
                let messages = msgs.into_iter().map(|(_, msg)| msg).collect();
                self.inner.matches(&messages)
            } else {
                MatcherResult::NoMatch
            }
        }

        fn describe(&self, matcher_result: MatcherResult) -> String {
            match matcher_result {
                MatcherResult::Match => {
                    format!(
                        "can be decoded to protocol messages which {:?}",
                        self.inner.describe(MatcherResult::Match)
                    )
                }
                MatcherResult::NoMatch => "cannot be decoded to protocol messages".to_string(),
            }
        }
    }

    pub fn decoded_as_protocol_messages(
        inner: impl Matcher<ActualT = Vec<ProtocolMessage>>,
    ) -> impl Matcher<ActualT = Bytes> {
        ProtocolMessageDecodeMatcher { inner }
    }

    fn to_get_result_response(res: ResponseResult, expiry_time: String) -> GetResultResponse {
        GetResultResponse {
            response: Some(match res {
                ResponseResult::Success(res) => get_result_response::Response::Success(res),
                ResponseResult::Failure(code, msg) => {
                    get_result_response::Response::Failure(InvocationFailure {
                        code: code.into(),
                        message: msg.to_string(),
                    })
                }
            }),
            expiry_time,
        }
    }

    // --- Start tests

    #[test(tokio::test)]
    async fn new_invocation_start() {
        // Generate operation id and key
        let operation_id = "my-operation-id".to_string();
        let mut remote_context_service_key = Vec::new();
        prost::encoding::encode_varint(operation_id.len() as u64, &mut remote_context_service_key);
        remote_context_service_key.put_slice(operation_id.as_bytes());
        let remote_context_service_id =
            ServiceId::new(REMOTE_CONTEXT_SERVICE_NAME, remote_context_service_key);

        let mut ctx = TestInvocationContext::from_service_id(remote_context_service_id.clone());
        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.start(
                    StartRequest {
                        stream_id: "my-stream".to_string(),
                        operation_id,
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Executing {
                stream_id: eq("my-stream"),
                retention_period_sec: eq(DEFAULT_RETENTION_PERIOD_SEC)
            })
        );
        assert_that!(
            ctx.state().assert_has_journal_entry(0),
            property!(EnrichedRawEntry.ty(), eq(EntryType::PollInputStream))
        );
        assert_that!(
            effects,
            all!(
                contains(pat!(Effect::CreateJournal {
                    service_id: property!(
                        ServiceId.partition_key(),
                        eq(remote_context_service_id.partition_key())
                    )
                })),
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            StartResponse {
                                invocation_status: some(pat!(
                                    start_response::InvocationStatus::Executing(
                                        decoded_as_protocol_messages(elements_are![
                                            pat!(ProtocolMessage::Start(pat!(
                                            restate_service_protocol::pb::protocol::StartMessage {
                                                known_entries: eq(1)
                                            }
                                        ))),
                                            pat!(ProtocolMessage::UnparsedEntry(property!(
                                                PlainRawEntry.ty(),
                                                eq(EntryType::PollInputStream)
                                            )))
                                        ])
                                    )
                                ))
                            }
                        ))))
                    }
                ))))
            )
        );
    }

    #[test(tokio::test)]
    async fn new_invocation_start_with_custom_retention_period() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        let _ = ctx
            .invoke(|ctx| {
                ctx.start(
                    StartRequest {
                        stream_id: "my-stream".to_string(),
                        retention_period_sec: 1,
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Executing {
                stream_id: eq("my-stream"),
                retention_period_sec: eq(1)
            })
        );
    }

    #[test(tokio::test)]
    async fn new_invocation_start_from_a_different_client() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);

        // Start with my-stream-1
        let _ = ctx
            .invoke(|ctx| {
                ctx.start(
                    StartRequest {
                        stream_id: "my-stream-1".to_string(),
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Executing {
                stream_id: eq("my-stream-1")
            })
        );

        // Recv on my-stream-1, there should be no response right away
        let (recv_fid_stream_1, effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        stream_id: "my-stream-1".to_string(),
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            effects,
            not(contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(recv_fid_stream_1.clone()),
                }
            )))))
        );

        // Start with a different stream invalidates the previous one
        let (start_fid_stream_2, effects) = ctx
            .invoke(|ctx| {
                ctx.start(
                    StartRequest {
                        stream_id: "my-stream-2".to_string(),
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Executing {
                stream_id: eq("my-stream-2")
            })
        );
        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(recv_fid_stream_1),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        RecvResponse {
                            response: some(eq(recv_response::Response::InvalidStream(())))
                        }
                    ))))
                }
            ))))
        );
        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(start_fid_stream_2),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        StartResponse {
                            invocation_status: some(pat!(
                                start_response::InvocationStatus::Executing(anything())
                            ))
                        }
                    ))))
                }
            ))))
        );
    }

    #[test(tokio::test)]
    async fn new_invocation_start_replay_existing_journal() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        ctx.state_mut()
            .append_journal_entry(Entry::poll_input_stream(Bytes::copy_from_slice(b"123")))
            .append_journal_entry(Entry::clear_state(Bytes::copy_from_slice(b"abc")));
        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.start(
                    StartRequest {
                        stream_id: "my-stream".to_string(),
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Executing {
                stream_id: eq("my-stream")
            })
        );
        assert_that!(
            ctx.state().assert_has_journal_entries(0..2),
            elements_are![
                property!(EnrichedRawEntry.ty(), eq(EntryType::PollInputStream)),
                property!(EnrichedRawEntry.ty(), eq(EntryType::ClearState))
            ]
        );
        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        StartResponse {
                            invocation_status: some(pat!(
                                start_response::InvocationStatus::Executing(
                                    decoded_as_protocol_messages(elements_are![
                                        pat!(ProtocolMessage::Start(pat!(
                                            restate_service_protocol::pb::protocol::StartMessage {
                                                known_entries: eq(2)
                                            }
                                        ))),
                                        pat!(ProtocolMessage::UnparsedEntry(property!(
                                            PlainRawEntry.ty(),
                                            eq(EntryType::PollInputStream)
                                        ))),
                                        pat!(ProtocolMessage::UnparsedEntry(property!(
                                            PlainRawEntry.ty(),
                                            eq(EntryType::ClearState)
                                        )))
                                    ])
                                )
                            ))
                        }
                    ))))
                }
            ))))
        );
    }

    // --- Send tests

    #[test(tokio::test)]
    async fn new_invocation_send() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        assert_eq!(
            ctx.invoke(|ctx| ctx.send(
                SendRequest {
                    stream_id: "my-stream".to_string(),
                    ..Default::default()
                },
                Default::default()
            ))
            .await
            .unwrap_err()
            .code(),
            InvocationErrorCode::User(UserErrorCode::Internal)
        );
        ctx.state().assert_has_not_state(&STATUS);
    }

    #[test(tokio::test)]
    async fn send_set_state() {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;

        let _ = ctx
            .invoke(|ctx| {
                ctx.send(
                    SendRequest {
                        operation_id,
                        stream_id,
                        messages: encode_entries(vec![Entry::set_state(
                            USER_STATE.to_string(),
                            b"my-value".to_vec(),
                        )]),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_eq!(
            ctx.state().assert_has_state(&USER_STATE),
            Bytes::copy_from_slice(b"my-value")
        );
    }

    #[test(tokio::test)]
    async fn send_clear_state() {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;
        ctx.state_mut()
            .set(&USER_STATE, Bytes::copy_from_slice(b"my-value"));

        let _ = ctx
            .invoke(|ctx| {
                ctx.send(
                    SendRequest {
                        operation_id,
                        stream_id,
                        messages: encode_entries(vec![Entry::clear_state(USER_STATE.to_string())]),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        ctx.state().assert_has_not_state(&USER_STATE);
    }

    #[test(tokio::test)]
    async fn send_output() {
        let result = Bytes::copy_from_slice(b"my-output");

        let (ctx, operation_id, _, _, effects) = send_test(
            ProtobufRawEntryCodec::serialize(Entry::output_stream(EntryResult::Success(
                result.clone(),
            )))
            .into(),
        )
        .await;

        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Done(pat!(GetResultResponse {
                response: some(eq(get_result_response::Response::Success(result)))
            })))
        );
        assert_that!(
            effects,
            contains(pat!(Effect::DelayedInvoke {
                target_method: eq("Cleanup".to_string()),
                argument: protobuf_decoded(eq(CleanupRequest { operation_id }))
            }))
        );
    }

    #[test(tokio::test)]
    async fn send_output_unblocks_get_result_and_recv() {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;

        let (recv_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        // No response is expected to recv
        assert_that!(
            effects,
            not(contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(recv_fid.clone()),
                }
            )))))
        );

        let (get_result_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.get_result(
                    GetResultRequest {
                        operation_id: operation_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        // No response is expected to get_result
        assert_that!(
            effects,
            not(contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(get_result_fid.clone()),
                }
            )))))
        );

        let output = Bytes::copy_from_slice(b"my-output");
        let (send_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.send(
                    SendRequest {
                        operation_id: operation_id.clone(),
                        stream_id: "my-stream".to_string(),
                        messages: encode_entries(vec![Entry::output_stream(EntryResult::Success(
                            output.clone(),
                        ))]),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Done(pat!(GetResultResponse {
                response: some(eq(get_result_response::Response::Success(output.clone())))
            })))
        );

        // Effects should contain responses for send, recv and get_result
        assert_that!(
            effects,
            all!(
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(send_fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            SendResponse {
                                response: some(eq(send_response::Response::Ok(())))
                            }
                        ))))
                    }
                )))),
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(recv_fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            RecvResponse {
                                response: some(eq(recv_response::Response::Messages(Bytes::new())))
                            }
                        ))))
                    }
                )))),
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(get_result_fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            GetResultResponse {
                                response: some(eq(get_result_response::Response::Success(output)))
                            }
                        ))))
                    }
                ))))
            )
        );
    }

    #[test(tokio::test)]
    async fn send_when_invocation_completed() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        ctx.state_mut().set(
            &STATUS,
            InvocationStatus::Done(to_get_result_response(
                ResponseResult::Success(Bytes::new()),
                String::new(),
            )),
        );

        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.send(
                    SendRequest {
                        stream_id: "my-stream".to_string(),
                        messages: encode_entries(vec![Entry::output_stream(EntryResult::Success(
                            Bytes::new(),
                        ))]),
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        SendResponse {
                            response: some(eq(send_response::Response::InvocationCompleted(())))
                        }
                    ))))
                }
            ))))
        );
    }

    #[test(tokio::test)]
    async fn send_get_state_and_recv_result() {
        send_then_receive_test(
            ProtobufRawEntryCodec::serialize(Entry::get_state(USER_STATE.to_string(), None)).into(),
            |_, _, _| std::future::ready(()).boxed(),
            eq(ProtocolMessage::from(Completion::new(
                1,
                CompletionResult::Empty,
            ))),
        )
        .await;
    }

    #[test(tokio::test)]
    async fn send_custom_entry_and_recv_ack() {
        send_then_receive_test(
            ProtocolMessage::UnparsedEntry(PlainRawEntry::new(
                RawEntryHeader::Custom {
                    code: 0xFC00,
                    requires_ack: true,
                },
                Bytes::copy_from_slice(b"123"),
            )),
            |_, _, _| std::future::ready(()).boxed(),
            eq(ProtocolMessage::from(Completion::new(
                1,
                CompletionResult::Ack,
            ))),
        )
        .await;
    }

    #[test(tokio::test)]
    async fn send_invoke_entry_and_recv_ack() {
        let argument: Bytes = GreetingRequest {
            person: "Francesco".to_string(),
        }
        .encode_to_vec()
        .into();
        let response: Bytes = GreetingResponse {
            greeting: "Greetings Francesco!".to_string(),
        }
        .encode_to_vec()
        .into();

        let (operation_id, _, send_effects, _) = send_then_receive_test(
            ProtobufRawEntryCodec::serialize(Entry::invoke(
                InvokeRequest::new(GREETER_SERVICE_NAME, "Greet", argument.clone()),
                None,
            ))
            .into(),
            |ctx, _, _| {
                let response = response.clone();
                let (invocation_uuid, _, _) = ctx.state().assert_has_journal();
                ctx.state_mut().complete_entry(Completion::new(
                    1,
                    CompletionResult::Success(response.clone()),
                ));

                async move {
                    // Invoke internal_on_response to complete the request
                    ctx.invoke(|ctx| {
                        ctx.internal_on_completion(
                            JournalCompletionNotificationRequest {
                                entry_index: 1,
                                invocation_uuid: Bytes::copy_from_slice(invocation_uuid.as_bytes()),
                                result: Some(
                                    journal_completion_notification_request::Result::Success(
                                        response,
                                    ),
                                ),
                            },
                            Default::default(),
                        )
                    })
                    .await
                    .unwrap();
                }
                .boxed_local()
            },
            eq(ProtocolMessage::from(Completion::new(
                1,
                CompletionResult::Success(response.clone()),
            ))),
        )
        .await;

        // Make sure send effects has the outbox message to send the invocation
        assert_that!(
            send_effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::ServiceInvocation(pat!(ServiceInvocation {
                    fid: pat!(FullInvocationId {
                        service_id: pat!(ServiceId {
                            service_name: displays_as(eq(GREETER_SERVICE_NAME))
                        })
                    }),
                    method_name: displays_as(eq("Greet")),
                    argument: eq(argument),
                    response_sink: some(pat!(ServiceInvocationResponseSink::NewInvocation {
                        target: pat!(FullInvocationId {
                            service_id: pat!(ServiceId {
                                service_name: displays_as(eq(REMOTE_CONTEXT_SERVICE_NAME))
                            })
                        }),
                        method: displays_as(eq(REMOTE_CONTEXT_INTERNAL_ON_COMPLETION_METHOD_NAME)),
                        caller_context: eq(Bincode::encode(&InvokeEntryContext {
                            operation_id: operation_id.to_string(),
                            entry_index: 1,
                        })
                        .unwrap())
                    }))
                }))
            ))))
        );
    }

    #[test(tokio::test)]
    async fn send_background_invoke() {
        let argument: Bytes = GreetingRequest {
            person: "Francesco".to_string(),
        }
        .encode_to_vec()
        .into();

        let (_, _, _, _, effects) = send_test(
            ProtobufRawEntryCodec::serialize(Entry::background_invoke(
                InvokeRequest::new(GREETER_SERVICE_NAME, "Greet", argument.clone()),
                None,
            ))
            .into(),
        )
        .await;

        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::ServiceInvocation(pat!(ServiceInvocation {
                    fid: pat!(FullInvocationId {
                        service_id: pat!(ServiceId {
                            service_name: displays_as(eq(GREETER_SERVICE_NAME))
                        })
                    }),
                    method_name: displays_as(eq("Greet")),
                    argument: eq(argument),
                    response_sink: none()
                }))
            ))))
        );
    }

    #[test(tokio::test)]
    async fn send_background_invoke_with_delay() {
        let argument: Bytes = GreetingRequest {
            person: "Francesco".to_string(),
        }
        .encode_to_vec()
        .into();
        let time = MillisSinceEpoch::from(SystemTime::now() + Duration::from_secs(100));

        let (_, _, _, _, effects) = send_test(
            ProtobufRawEntryCodec::serialize(Entry::background_invoke(
                InvokeRequest::new(GREETER_SERVICE_NAME, "Greet", argument.clone()),
                Some(time),
            ))
            .into(),
        )
        .await;

        assert_that!(
            effects,
            contains(pat!(Effect::DelayedInvoke {
                target_fid: pat!(FullInvocationId {
                    service_id: pat!(ServiceId {
                        service_name: displays_as(eq(GREETER_SERVICE_NAME))
                    })
                }),
                target_method: displays_as(eq("Greet")),
                argument: eq(argument),
                response_sink: none(),
                time: eq(time),
                timer_index: eq(1)
            }))
        );
    }

    #[test(tokio::test)]
    async fn send_many_side_effects_at_once() {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;

        let (fid, send_effects) = ctx
            .invoke(|ctx| {
                ctx.send(
                    SendRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                        messages: encode_messages(vec![
                            ProtocolMessage::UnparsedEntry(PlainRawEntry::new(
                                RawEntryHeader::Custom {
                                    code: 0xFC00,
                                    requires_ack: true,
                                },
                                Bytes::copy_from_slice(b"123"),
                            )),
                            ProtocolMessage::UnparsedEntry(PlainRawEntry::new(
                                RawEntryHeader::Custom {
                                    code: 0xFC00,
                                    requires_ack: true,
                                },
                                Bytes::copy_from_slice(b"456"),
                            )),
                            ProtocolMessage::UnparsedEntry(PlainRawEntry::new(
                                RawEntryHeader::Custom {
                                    code: 0xFC00,
                                    requires_ack: true,
                                },
                                Bytes::copy_from_slice(b"789"),
                            )),
                        ]),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            send_effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        SendResponse {
                            response: some(eq(send_response::Response::Ok(())))
                        }
                    ))))
                }
            ))))
        );

        let (fid, recv_effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            recv_effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        RecvResponse {
                            response: some(pat!(recv_response::Response::Messages(
                                decoded_as_protocol_messages(elements_are![
                                    eq(ProtocolMessage::from(Completion::new(
                                        1,
                                        CompletionResult::Ack,
                                    ))),
                                    eq(ProtocolMessage::from(Completion::new(
                                        2,
                                        CompletionResult::Ack,
                                    ))),
                                    eq(ProtocolMessage::from(Completion::new(
                                        3,
                                        CompletionResult::Ack,
                                    )))
                                ])
                            )))
                        }
                    ))))
                }
            ))))
        );
    }

    #[test(tokio::test)]
    async fn send_complete_awakeable() {
        let awakeable_id = AwakeableIdentifier::new(InvocationId::mock_random(), 10);
        let entry_result = EntryResult::Success(Bytes::copy_from_slice(b"456"));

        let (_, _, _, _, effects) = send_test(
            ProtobufRawEntryCodec::serialize(Entry::complete_awakeable(
                awakeable_id.encode(),
                entry_result.clone(),
            ))
            .into(),
        )
        .await;

        let (awakeable_invocation_id, awakeable_entry_index) = awakeable_id.into_inner();

        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::ServiceResponse(pat!(InvocationResponse {
                    id: pat!(MaybeFullInvocationId::Partial(eq(awakeable_invocation_id))),
                    entry_index: eq(awakeable_entry_index),
                    result: eq(ResponseResult::from(entry_result))
                }))
            ))))
        );
    }

    // --- Recv tests

    #[test(tokio::test)]
    async fn new_invocation_recv() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        assert_eq!(
            ctx.invoke(|ctx| ctx.recv(
                RecvRequest {
                    stream_id: "my-stream".to_string(),
                    ..Default::default()
                },
                Default::default()
            ))
            .await
            .unwrap_err()
            .code(),
            InvocationErrorCode::User(UserErrorCode::Internal)
        );
        ctx.state().assert_has_not_state(&STATUS);
    }

    #[test(tokio::test)]
    async fn pending_recv_is_unblocked_on_new_completion() {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;

        let (recv_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        // No response is expected to recv
        assert_that!(
            effects,
            not(contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(recv_fid.clone()),
                }
            )))))
        );

        let (send_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.send(
                    SendRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                        messages: encode_entries(vec![Entry::get_state(
                            USER_STATE.to_string(),
                            None,
                        )]),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            effects,
            all!(
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(send_fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            SendResponse {
                                response: some(eq(send_response::Response::Ok(())))
                            }
                        ))))
                    }
                )))),
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(recv_fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            RecvResponse {
                                response: some(pat!(recv_response::Response::Messages(
                                    decoded_as_protocol_messages(elements_are![eq(
                                        ProtocolMessage::from(Completion::new(
                                            1,
                                            CompletionResult::Empty
                                        ))
                                    )])
                                )))
                            }
                        ))))
                    }
                ))))
            )
        );
    }

    #[test(tokio::test)]
    async fn recv_many_acks_at_once() {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;

        for _ in 0..3 {
            let (fid, send_effects) = ctx
                .invoke(|ctx| {
                    ctx.send(
                        SendRequest {
                            operation_id: operation_id.clone(),
                            stream_id: stream_id.clone(),
                            messages: encode_messages(vec![ProtocolMessage::UnparsedEntry(
                                PlainRawEntry::new(
                                    RawEntryHeader::Custom {
                                        code: 0xFC00,
                                        requires_ack: true,
                                    },
                                    Bytes::copy_from_slice(b"123"),
                                ),
                            )]),
                        },
                        Default::default(),
                    )
                })
                .await
                .unwrap();
            assert_that!(
                send_effects,
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            SendResponse {
                                response: some(eq(send_response::Response::Ok(())))
                            }
                        ))))
                    }
                ))))
            );
        }

        let (fid, recv_effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            recv_effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        RecvResponse {
                            response: some(pat!(recv_response::Response::Messages(
                                decoded_as_protocol_messages(elements_are![
                                    eq(ProtocolMessage::from(Completion::new(
                                        1,
                                        CompletionResult::Ack,
                                    ))),
                                    eq(ProtocolMessage::from(Completion::new(
                                        2,
                                        CompletionResult::Ack,
                                    ))),
                                    eq(ProtocolMessage::from(Completion::new(
                                        3,
                                        CompletionResult::Ack,
                                    )))
                                ])
                            )))
                        }
                    ))))
                }
            ))))
        );
    }

    #[test(tokio::test)]
    async fn recv_when_invocation_completed() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        ctx.state_mut().set(
            &STATUS,
            InvocationStatus::Done(to_get_result_response(
                ResponseResult::Success(Bytes::new()),
                String::new(),
            )),
        );

        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        stream_id: "my-stream".to_string(),
                        operation_id: "my-operation".to_string(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        RecvResponse {
                            response: some(eq(recv_response::Response::InvocationCompleted(())))
                        }
                    ))))
                }
            ))))
        );
    }

    // --- Get result tests

    #[test(tokio::test)]
    async fn get_result_unknown_invocation() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.get_result(
                    GetResultRequest {
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        GetResultResponse {
                            response: some(eq(get_result_response::Response::None(())))
                        }
                    ))))
                }
            ))))
        );
    }

    #[test(tokio::test)]
    async fn get_result_done_success_invocation() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);

        let expected_result = Bytes::copy_from_slice(b"123");
        ctx.state_mut().set(
            &STATUS,
            InvocationStatus::Done(to_get_result_response(
                ResponseResult::Success(expected_result.clone()),
                String::new(),
            )),
        );
        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.get_result(
                    GetResultRequest {
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        GetResultResponse {
                            response: some(eq(get_result_response::Response::Success(
                                expected_result
                            )))
                        }
                    ))))
                }
            ))))
        );
    }

    #[test(tokio::test)]
    async fn get_result_done_failed_invocation() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        ctx.state_mut().set(
            &STATUS,
            InvocationStatus::Done(to_get_result_response(
                ResponseResult::Failure(UserErrorCode::OutOfRange, "my-error".into()),
                String::new(),
            )),
        );
        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.get_result(
                    GetResultRequest {
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        GetResultResponse {
                            response: some(eq(get_result_response::Response::Failure(
                                get_result_response::InvocationFailure {
                                    code: UserErrorCode::OutOfRange as u32,
                                    message: "my-error".to_string()
                                }
                            )))
                        }
                    ))))
                }
            ))))
        );
    }

    // -- Cleanup

    #[test(tokio::test)]
    async fn cleanup() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        ctx.state_mut().set(
            &STATUS,
            InvocationStatus::Done(to_get_result_response(
                ResponseResult::Failure(UserErrorCode::OutOfRange, "my-error".into()),
                String::new(),
            )),
        );
        ctx.state_mut()
            .append_journal_entry(Entry::poll_input_stream(Bytes::from_static(b"123")));
        ctx.state_mut()
            .set(&USER_STATE, Bytes::copy_from_slice(b"123"));
        let _ = ctx
            .invoke(|ctx| {
                ctx.cleanup(
                    CleanupRequest {
                        operation_id: "my-operation-id".to_string(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        ctx.state().assert_has_state(&USER_STATE);
        ctx.state().assert_has_not_state(&STATUS);
        ctx.state().assert_has_no_journal();
    }

    // -- Inactivity timeout

    #[test(tokio::test)]
    async fn fire_inactivity_timeout() {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;

        // Create pending recv and pending get result
        let (recv_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            effects,
            not(contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(recv_fid.clone()),
                }
            )))))
        );
        let (get_result_fid, _) = ctx
            .invoke(|ctx| {
                ctx.get_result(
                    GetResultRequest {
                        operation_id: operation_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            effects,
            not(contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(get_result_fid.clone()),
                }
            )))))
        );

        let inactivity_tracker = ctx.state().assert_has_state(&INACTIVITY_TRACKER);

        // Fire inactivity timer
        let (_, effects) = ctx
            .invoke(|ctx| {
                ctx.internal_on_inactivity_timer(
                    InactivityTimeoutTimerRequest {
                        operation_id,
                        stream_id,
                        inactivity_timer_index: inactivity_tracker.timer_index,
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            effects,
            all!(
                not(contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(recv_fid),
                        response: pat!(ResponseResult::Failure(_, _))
                    }
                ))))),
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(get_result_fid),
                        response: pat!(ResponseResult::Failure(_, _))
                    }
                ))))
            )
        );
    }

    // -- Kill command

    #[test(tokio::test)]
    async fn kill_invocation() {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;

        // Create a pending recv
        let (recv_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            effects,
            not(contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(recv_fid.clone()),
                }
            )))))
        );

        // Create pending get_result
        let (get_result_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.get_result(
                    GetResultRequest {
                        operation_id: operation_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            effects,
            not(contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(get_result_fid.clone()),
                }
            )))))
        );

        // Get invocation_uuid
        let (invocation_uuid, _, _) = ctx.state().assert_has_journal();

        // Kill request
        let (_, effects) = ctx
            .invoke(|ctx| {
                ctx.internal_on_kill(
                    KillNotificationRequest {
                        invocation_uuid: invocation_uuid.as_bytes().to_vec().into(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        let expected_invocation_failure = InvocationFailure {
            code: UserErrorCode::from(KILLED_INVOCATION_ERROR.code()).into(),
            message: KILLED_INVOCATION_ERROR.message().to_string(),
        };
        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Done(pat!(GetResultResponse {
                response: some(eq(get_result_response::Response::Failure(
                    expected_invocation_failure.clone()
                )))
            })))
        );

        // Effects should contain responses for recv and get_result
        assert_that!(
            effects,
            all!(
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(recv_fid),
                        // No special handling for blocked recv and kill: Once receiving empty bytes,
                        // the client will go through GetResult and get the killed status.
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            RecvResponse {
                                response: some(eq(recv_response::Response::Messages(Bytes::new())))
                            }
                        ))))
                    }
                )))),
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(get_result_fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            GetResultResponse {
                                response: some(eq(get_result_response::Response::Failure(
                                    expected_invocation_failure.clone()
                                )))
                            }
                        ))))
                    }
                ))))
            )
        );

        // Subsequent get result returns the killed status
        let (get_result_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.get_result(
                    GetResultRequest {
                        operation_id: operation_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(get_result_fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        GetResultResponse {
                            response: some(eq(get_result_response::Response::Failure(
                                expected_invocation_failure.clone()
                            )))
                        }
                    ))))
                }
            ))))
        );
    }

    // -- Helpers

    fn mock_schemas() -> Schemas {
        let schemas = Schemas::default();

        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint_updates(
                        EndpointMetadata::mock_with_uri("http://localhost:8080"),
                        vec![ServiceRegistrationRequest::new(
                            GREETER_SERVICE_NAME.to_string(),
                            DiscoveredInstanceType::Unkeyed,
                            HashMap::from([(
                                "Greet".to_string(),
                                DiscoveredMethodMetadata::default(),
                            )]),
                        )],
                        restate_pb::mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas
    }

    async fn bootstrap_invocation_using_start() -> (TestInvocationContext, String, String) {
        let mut ctx =
            TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME).with_schemas(mock_schemas());
        let _ = ctx
            .invoke(|ctx| {
                ctx.start(
                    StartRequest {
                        operation_id: "my-operation-id".to_string(),
                        stream_id: "my-stream".to_string(),
                        ..Default::default()
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            ctx.state().assert_has_state(&STATUS),
            pat!(InvocationStatus::Executing {
                stream_id: eq("my-stream")
            })
        );
        ctx.state().assert_has_journal();

        (ctx, "my-operation-id".to_string(), "my-stream".to_string())
    }

    async fn send_then_receive_test<F, M>(
        send_msg: ProtocolMessage,
        action_between_send_and_recv: F,
        recv_messages_matcher: M,
    ) -> (String, String, Vec<Effect>, Vec<Effect>)
    where
        F: for<'a> FnOnce(
            &'a mut TestInvocationContext,
            &'a str,
            &'a str,
        ) -> LocalBoxFuture<'a, ()>,
        M: Matcher<ActualT = ProtocolMessage>,
    {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;

        let (fid, send_effects) = ctx
            .invoke(|ctx| {
                ctx.send(
                    SendRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                        messages: encode_messages(vec![send_msg]),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            send_effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        SendResponse {
                            response: some(eq(send_response::Response::Ok(())))
                        }
                    ))))
                }
            ))))
        );

        action_between_send_and_recv(&mut ctx, &operation_id, &stream_id).await;

        let (fid, recv_effects) = ctx
            .invoke(|ctx| {
                ctx.recv(
                    RecvRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();
        assert_that!(
            recv_effects,
            contains(pat!(Effect::OutboxMessage(pat!(
                OutboxMessage::IngressResponse {
                    full_invocation_id: eq(fid),
                    response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                        RecvResponse {
                            response: some(pat!(recv_response::Response::Messages(
                                decoded_as_protocol_messages(elements_are![recv_messages_matcher])
                            )))
                        }
                    ))))
                }
            ))))
        );

        (operation_id, stream_id, send_effects, recv_effects)
    }

    async fn send_test(
        send_msg: ProtocolMessage,
    ) -> (
        TestInvocationContext,
        String,
        String,
        FullInvocationId,
        Vec<Effect>,
    ) {
        let (mut ctx, operation_id, stream_id) = bootstrap_invocation_using_start().await;
        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.send(
                    SendRequest {
                        operation_id: operation_id.clone(),
                        stream_id: stream_id.clone(),
                        messages: encode_messages(vec![send_msg]),
                    },
                    Default::default(),
                )
            })
            .await
            .unwrap();

        (ctx, operation_id, stream_id, fid, effects)
    }
}
