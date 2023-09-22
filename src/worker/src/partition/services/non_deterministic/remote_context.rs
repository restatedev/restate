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
use prost::Message;
use restate_pb::builtin_service::ResponseSerializer;
use restate_pb::restate::internal::get_result_response::InvocationFailure;
use restate_pb::restate::internal::{
    get_result_response, recv_response, send_response, start_response, CleanupRequest,
    GetResultRequest, GetResultResponse, RecvRequest, RecvResponse, RemoteContextBuiltInService,
    SendRequest, SendResponse, ServiceInvocationSinkRequest, StartRequest, StartResponse,
};
use restate_pb::REMOTE_CONTEXT_INTERNAL_ON_RESPONSE_METHOD_NAME;
use restate_schema_api::key::KeyExtractor;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol::message::{
    Decoder, Encoder, EncodingError, MessageHeader, ProtocolMessage,
};
use restate_types::identifiers::InvocationUuid;
use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext};
use restate_types::journal::raw::{PlainRawEntry, RawEntryCodec, RawEntryHeader};
use restate_types::journal::{
    BackgroundInvokeEntry, ClearStateEntry, Entry, EntryResult, GetStateEntry, InvokeEntry,
    InvokeRequest, OutputStreamEntry, SetStateEntry,
};
use restate_types::journal::{Completion, CompletionResult};
use serde::{Deserialize, Serialize};
use std::iter;
use std::time::{Duration, SystemTime};
use tracing::{debug, instrument, trace, warn};

#[derive(Serialize, Deserialize, Debug)]
enum InvocationStatus {
    Executing {
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

// TODO we should reuse the journal storage of the partition processor
type JournalState = Vec<PlainRawEntry>;
const JOURNAL: StateKey<Bincode<JournalState>> = StateKey::new_bincode("_internal_journal");

#[derive(Serialize, Deserialize, Debug)]
struct InvokeEntryContext {
    operation_id: String,
    entry_index: EntryIndex,
}

const DEFAULT_RETENTION_PERIOD: u32 = 30 * 60;

impl<'a, State: StateReader> InvocationContext<'a, State> {
    async fn handle_protocol_message(
        &mut self,
        operation_id: &str,
        stream_id: &str,
        retention_period_sec: u32,
        journal: &mut JournalState,
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
                self.handle_entry(operation_id, retention_period_sec, journal, entry)
                    .await
            }
        }
    }

    async fn handle_entry(
        &mut self,
        operation_id: &str,
        retention_period_sec: u32,
        journal: &mut JournalState,
        mut entry: PlainRawEntry,
    ) -> Result<(), InvocationError> {
        let entry_index: EntryIndex = journal.len() as EntryIndex;
        match entry.header.clone() {
            RawEntryHeader::OutputStream => {
                let_assert!(
                    Entry::OutputStream(OutputStreamEntry { result }) =
                        ProtobufRawEntryCodec::deserialize(&entry)
                            .map_err(InvocationError::internal)?
                );

                let expiry_time =
                    SystemTime::now() + Duration::from_secs(retention_period_sec as u64);

                let get_result_response = GetResultResponse {
                    expiry_time: humantime::format_rfc3339(expiry_time).to_string(),
                    response: Some(match result {
                        EntryResult::Success(s) => get_result_response::Response::Success(s),
                        EntryResult::Failure(code, msg) => {
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
                )
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
            }
            RawEntryHeader::SetState => {
                let_assert!(
                    Entry::SetState(SetStateEntry { key, value }) =
                        ProtobufRawEntryCodec::deserialize(&entry)
                            .map_err(InvocationError::internal)?
                );
                let state_key = check_state_key(key)?;
                self.set_state(&state_key, &value)?;
            }
            RawEntryHeader::ClearState => {
                let_assert!(
                    Entry::ClearState(ClearStateEntry { key }) =
                        ProtobufRawEntryCodec::deserialize(&entry)
                            .map_err(InvocationError::internal)?
                );
                let state_key = check_state_key(key)?;
                self.clear_state(&state_key);
            }
            RawEntryHeader::Invoke { is_completed } => {
                if !is_completed {
                    let_assert!(
                        Entry::Invoke(InvokeEntry { request, .. }) =
                            ProtobufRawEntryCodec::deserialize(&entry)
                                .map_err(InvocationError::internal)?
                    );

                    let fid = self.generate_fid_from_invoke_request(&request)?;

                    self.send_message(OutboxMessage::ServiceInvocation(ServiceInvocation {
                        fid,
                        method_name: request.method_name,
                        argument: request.parameter,
                        response_sink: Some(ServiceInvocationResponseSink::NewInvocation {
                            target: FullInvocationId::with_service_id(
                                self.full_invocation_id.service_id.clone(),
                                InvocationUuid::now_v7(),
                            ),
                            method: REMOTE_CONTEXT_INTERNAL_ON_RESPONSE_METHOD_NAME.to_string(),
                            caller_context: Bincode::encode(&InvokeEntryContext {
                                operation_id: operation_id.to_string(),
                                entry_index,
                            })
                            .map_err(InvocationError::internal)?,
                        }),
                        span_context: ServiceInvocationSpanContext::empty(),
                    }));
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

                if invoke_time != 0 {
                    self.delay_invoke(
                        fid,
                        request.method_name.into(),
                        request.parameter,
                        None,
                        invoke_time.into(),
                        entry_index,
                    )
                } else {
                    self.send_message(OutboxMessage::ServiceInvocation(ServiceInvocation {
                        fid,
                        method_name: request.method_name,
                        argument: request.parameter,
                        response_sink: None,
                        span_context: ServiceInvocationSpanContext::empty(),
                    }))
                }
            }
            RawEntryHeader::Custom { requires_ack, .. } => {
                if requires_ack {
                    self.enqueue_protocol_message(ProtocolMessage::from(Completion::new(
                        entry_index,
                        CompletionResult::Ack,
                    )))
                    .await?;
                }
            }
            RawEntryHeader::PollInputStream { .. } => {
                return Err(InvocationError::new(
                    UserErrorCode::FailedPrecondition,
                    "Unexpected PollInputStream entry received",
                ))
            }
            RawEntryHeader::Sleep { .. }
            | RawEntryHeader::Awakeable { .. }
            | RawEntryHeader::CompleteAwakeable => {
                return Err(InvocationError::new(
                    UserErrorCode::Unimplemented,
                    "Unsupported entry type",
                ))
            }
        };

        journal.push(entry);
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
        if let Some((fid, recv_sink)) = self.pop_state(&PENDING_RECV_SINK).await? {
            trace!(restate.protocol.message = ?msg, "Sending message");
            self.send_message(OutboxMessage::from_response_sink(
                &fid,
                recv_sink,
                ResponseSerializer::<RecvResponse>::default().serialize_success(RecvResponse {
                    response: Some(recv_response::Response::Messages(encode_messages(vec![
                        msg,
                    ]))),
                }),
            ));
        } else {
            trace!(restate.protocol.message = ?msg, "Enqueuing message");
            // Enqueue in existing recv message stream
            let pending_recv_stream = self
                .load_state(&PENDING_RECV_STREAM)
                .await?
                .unwrap_or_default();
            let new_msg_encoded = encode_messages(vec![msg]);
            let mut new_pending_recv_stream =
                BytesMut::with_capacity(pending_recv_stream.len() + new_msg_encoded.len());
            new_pending_recv_stream.put(pending_recv_stream);
            new_pending_recv_stream.put(new_msg_encoded);
            self.set_state(&PENDING_RECV_STREAM, &new_pending_recv_stream.freeze())?;
        }
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
            }));
            return Ok(());
        }

        self.close_pending_recv(recv_response::Response::InvalidStream(()))
            .await?;

        // We're now the leading client, let's write the status
        self.set_state(
            &STATUS,
            &InvocationStatus::Executing {
                stream_id: request.stream_id,
                retention_period_sec: if request.retention_period_sec == 0 {
                    request.retention_period_sec
                } else {
                    DEFAULT_RETENTION_PERIOD
                },
            },
        )?;

        let journal = if let Some(journal) = self.load_state(&JOURNAL).await? {
            journal
        } else {
            // If there isn't any journal, let's create one
            let new_journal = vec![ProtobufRawEntryCodec::serialize_as_unary_input_entry(
                request.argument,
            )];
            self.set_state(&JOURNAL, &new_journal)?;
            new_journal
        };

        // Let's create the messages and write them to a buffer
        let mut messages = Vec::with_capacity(journal.len() + 1);
        messages.push(ProtocolMessage::new_start_message(
            Bytes::new(),              // TODO
            "unspecified".to_string(), // TODO
            journal.len() as u32,
            true, // TODO add eager state
            iter::empty(),
        ));
        for entry in journal {
            messages.push(ProtocolMessage::from(entry));
        }
        let stream_buffer = encode_messages(messages);

        self.reply_to_caller(response_serializer.serialize_success(StartResponse {
            invocation_status: Some(start_response::InvocationStatus::Executing(stream_buffer)),
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
                return Err(InvocationError::new(
                    UserErrorCode::FailedPrecondition,
                    "Invocation is done, collect the result with GetResult",
                ))
            }
        };

        let mut journal = self.load_state(&JOURNAL).await?.unwrap_or_default();
        for (message_header, message) in decode_messages(request.messages)
            .map_err(|e| InvocationError::new(UserErrorCode::FailedPrecondition, e))?
        {
            self.handle_protocol_message(
                &request.operation_id,
                &request.stream_id,
                retention_period_sec,
                &mut journal,
                message_header,
                message,
            )
            .await?;
        }
        self.set_state(&JOURNAL, &journal)?;

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
                return Err(InvocationError::new(
                    UserErrorCode::FailedPrecondition,
                    "Invocation is done, collect the result with GetResult",
                ))
            }
        };

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
        self.clear_state(&JOURNAL);
        self.clear_state(&PENDING_RECV_STREAM);
        self.clear_state(&PENDING_RECV_SINK);
        self.clear_state(&PENDING_GET_RESULT_SINKS);
        self.reply_to_caller(response_serializer.serialize_success(()));
        Ok(())
    }

    #[instrument(level = "trace", skip_all)]
    async fn internal_on_response(
        &mut self,
        request: ServiceInvocationSinkRequest,
        response_serializer: ResponseSerializer<()>,
    ) -> Result<(), InvocationError> {
        let ctx: InvokeEntryContext =
            Bincode::decode(request.caller_context.clone()).map_err(InvocationError::internal)?;

        if let Some(mut journal) = self.load_state(&JOURNAL).await? {
            if let Some(raw_entry) = journal.get_mut(ctx.entry_index as usize) {
                let completion_result = CompletionResult::from(
                    ResponseResult::try_from(request).map_err(InvocationError::internal)?,
                );
                ProtobufRawEntryCodec::write_completion(raw_entry, completion_result.clone())
                    .map_err(InvocationError::internal)?;
                self.set_state(&JOURNAL, &journal)?;
                self.enqueue_protocol_message(ProtocolMessage::from(Completion::new(
                    ctx.entry_index,
                    completion_result,
                )))
                .await?;
            } else {
                // TODO we should assign a unique invocation id for each time we see the operation id.
                // this to avoid cases where i'm executing again with the same operation id, but the invocations are different.
                // This can also address a bunch of other issues.
            }
        } else {
            debug!(
                "Discarding response received with fid {:?} because there is no journal.",
                self.full_invocation_id
            );
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

fn encode_messages(messages: Vec<ProtocolMessage>) -> Bytes {
    let encoder = Encoder::new(0);

    let mut buf = BytesMut::new();
    for msg in messages {
        trace!(restate.protocol.message = ?msg, "Sending message");
        buf.put(encoder.encode(msg))
    }

    buf.freeze()
}

fn to_get_result_response(res: ResponseResult, expiry_time: String) -> GetResultResponse {
    GetResultResponse {
        response: Some(match res {
            ResponseResult::Success(res) => get_result_response::Response::Success(res),
            ResponseResult::Failure(code, msg) => {
                get_result_response::Response::Failure(get_result_response::InvocationFailure {
                    code: code.into(),
                    message: msg.to_string(),
                })
            }
        }),
        expiry_time,
    }
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
    use restate_pb::restate::internal::{
        get_result_response, service_invocation_sink_request, start_response, CleanupRequest,
    };
    use restate_pb::REMOTE_CONTEXT_SERVICE_NAME;
    use restate_schema_api::endpoint::EndpointMetadata;
    use restate_schema_api::key::ServiceInstanceType;
    use restate_schema_impl::ServiceRegistrationRequest;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_test_util::matchers::*;
    use restate_test_util::{assert_eq, test};
    use restate_types::errors::InvocationErrorCode;
    use restate_types::journal::{Entry, EntryResult, EntryType};

    const USER_STATE: StateKey<Raw> = StateKey::new_raw("my-state");

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

    // --- Start tests

    #[test(tokio::test)]
    async fn new_invocation_start() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
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
            ctx.state().assert_has_state(&JOURNAL),
            elements_are![property!(
                PlainRawEntry.ty(),
                eq(EntryType::PollInputStream)
            )]
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
                                        pat!(ProtocolMessage::Start {
                                            inner: pat!(
                                        restate_service_protocol::pb::protocol::StartMessage {
                                            known_entries: eq(1)
                                        }
                                    )
                                        }),
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
        ctx.state_mut().set(
            &JOURNAL,
            vec![
                ProtobufRawEntryCodec::serialize_as_unary_input_entry(Bytes::copy_from_slice(
                    b"123",
                )),
                ProtobufRawEntryCodec::serialize(Entry::clear_state(Bytes::copy_from_slice(
                    b"abc",
                ))),
            ],
        );
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
            ctx.state().assert_has_state(&JOURNAL),
            elements_are![
                property!(PlainRawEntry.ty(), eq(EntryType::PollInputStream)),
                property!(PlainRawEntry.ty(), eq(EntryType::ClearState))
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
                                        pat!(ProtocolMessage::Start {
                                            inner: pat!(
                                        restate_service_protocol::pb::protocol::StartMessage {
                                            known_entries: eq(2)
                                        }
                                    )
                                        }),
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
    async fn send_when_done_is_invalid() {
        let mut ctx = TestInvocationContext::new(REMOTE_CONTEXT_SERVICE_NAME);
        ctx.state_mut().set(
            &STATUS,
            InvocationStatus::Done(to_get_result_response(
                ResponseResult::Success(Bytes::new()),
                String::new(),
            )),
        );

        let err = ctx
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
            .unwrap_err();

        assert_eq!(
            err.code(),
            InvocationErrorCode::from(UserErrorCode::FailedPrecondition)
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
            |ctx, operation_id, _| {
                let response = response.clone();

                async {
                    // Invoke internal_on_response to complete the request
                    ctx.invoke(|ctx| {
                        ctx.internal_on_response(
                            ServiceInvocationSinkRequest {
                                caller_context: Bincode::encode(&InvokeEntryContext {
                                    operation_id: operation_id.to_string(),
                                    entry_index: 1,
                                })
                                .unwrap(),
                                response: Some(service_invocation_sink_request::Response::Success(
                                    response,
                                )),
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
                        method: displays_as(eq(REMOTE_CONTEXT_INTERNAL_ON_RESPONSE_METHOD_NAME)),
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
        ctx.state_mut().set(&JOURNAL, JournalState::default());
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
        ctx.state().assert_has_not_state(&JOURNAL);
        ctx.state().assert_has_not_state(&STATUS);
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
                            ServiceInstanceType::Unkeyed,
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

        (ctx, "my-operation-id".to_string(), "my-stream".to_string())
    }

    async fn send_then_receive_test<F, M>(
        send_msg: ProtocolMessage,
        action_between_send_and_recv: F,
        matcher: M,
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
                                decoded_as_protocol_messages(elements_are![matcher])
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
