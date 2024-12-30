// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use assert2::let_assert;
use async_stream::{stream, try_stream};
use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt};
use http_body_util::{BodyStream, Either, Empty, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Request, Response};
use prost::Message;
use tracing::{debug, error};

use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol::message::{Decoder, Encoder, EncodingError, ProtocolMessage};
use restate_types::errors::codes;
use restate_types::journal::raw::{EntryHeader, PlainRawEntry, RawEntryCodecError};
use restate_types::journal::{Entry, EntryType, InputEntry};
use restate_types::service_protocol::start_message::StateEntry;
use restate_types::service_protocol::{
    self, get_state_entry_message, output_entry_message, ServiceProtocolVersion, StartMessage,
};

#[derive(Debug, thiserror::Error)]
enum FrameError {
    #[error(transparent)]
    EncodingError(EncodingError),
    #[error(transparent)]
    Hyper(hyper::Error),
    #[error("Stream ended before finished replay")]
    UnexpectedEOF,
    #[error("Journal does not contain expected messages")]
    InvalidJournal,
    #[error(transparent)]
    RawEntryCodecError(#[from] RawEntryCodecError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

pub async fn serve(
    req: Request<Incoming>,
) -> Result<
    Response<
        Either<Empty<Bytes>, StreamBody<impl Stream<Item = Result<Frame<Bytes>, Infallible>>>>,
    >,
    Infallible,
> {
    let (req_head, req_body) = req.into_parts();
    let mut split = req_head.uri.path().rsplit('/');
    let handler_name = if let Some(handler_name) = split.next() {
        handler_name
    } else {
        return Ok(Response::builder()
            .status(404)
            .body(Either::Left(Empty::new()))
            .unwrap());
    };
    if let Some("Counter") = split.next() {
    } else {
        return Ok(Response::builder()
            .status(404)
            .body(Either::Left(Empty::new()))
            .unwrap());
    };
    if let Some("invoke") = split.next() {
    } else {
        return Ok(Response::builder()
            .status(404)
            .body(Either::Left(Empty::new()))
            .unwrap());
    };

    let req_body = BodyStream::new(req_body);
    let mut decoder = Decoder::new(ServiceProtocolVersion::V1, usize::MAX, None);
    let encoder = Encoder::new(ServiceProtocolVersion::V1);

    let incoming = stream! {
        for await frame in req_body {
           match frame {
              Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        decoder.push(data);
                        loop {
                            match decoder.consume_next() {
                                Ok(Some((_header, message))) => yield Ok(message),
                                Ok(None) => {
                                    break
                                },
                                Err(err) => yield Err(FrameError::EncodingError(err)),
                            }
                        }
                 }
              },
              Err(err) => yield Err(FrameError::Hyper(err)),
           };
        }
    };

    let handler: Handler = match handler_name.parse() {
        Ok(handler) => handler,
        Err(_err) => {
            return Ok(Response::builder()
                .status(404)
                .body(Either::Left(Empty::new()))
                .unwrap());
        }
    };

    let outgoing = handler.handle(incoming).map(move |message| match message {
        Ok(message) => Ok(Frame::data(encoder.encode(message))),
        Err(err) => {
            error!("Error handling stream: {err:?}");
            Ok(Frame::data(encoder.encode(error(err))))
        }
    });

    Ok(Response::builder()
        .status(200)
        .header("content-type", "application/vnd.restate.invocation.v1")
        .body(Either::Right(StreamBody::new(outgoing)))
        .unwrap())
}

pub enum Handler {
    Get,
    Add,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid handler")]
pub struct InvalidHandler;

impl FromStr for Handler {
    type Err = InvalidHandler;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "get" => Ok(Self::Get),
            "add" => Ok(Self::Add),
            _ => Err(InvalidHandler),
        }
    }
}

impl Display for Handler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get => write!(f, "get"),
            Self::Add => write!(f, "add"),
        }
    }
}

impl Handler {
    fn handle(
        self,
        incoming: impl Stream<Item = Result<ProtocolMessage, FrameError>>,
    ) -> impl Stream<Item = Result<ProtocolMessage, FrameError>> {
        try_stream! {
            pin_mut!(incoming);
            match (incoming.next().await, incoming.next().await) {
                (Some(Ok(ProtocolMessage::Start(start_message))), Some(Ok(ProtocolMessage::UnparsedEntry(input)))) if input.ty() == EntryType::Input => {
                    let input = input.deserialize_entry_ref::<ProtobufRawEntryCodec>()?;
                    let_assert!(
                        Entry::Input(input) = input
                    );

                    let replay_count =  start_message.known_entries as usize - 1;
                    let mut replayed = Vec::with_capacity(replay_count);
                    for _ in 0..replay_count {
                        let message = incoming.next().await.ok_or(FrameError::UnexpectedEOF)??;
                        replayed.push(message);
                    }

                    debug!("Handling request to {self} with {} known entries",  start_message.known_entries);

                    match self {
                        Handler::Get => {
                            for await message in Self::handle_get(start_message, input, replayed, incoming) {
                                yield message?
                            }
                        },
                        Handler::Add => {
                            for await message in Self::handle_add(start_message, input, replayed, incoming) {
                                yield message?
                            }
                        },
                    };
                },
                _ => {Err(FrameError::InvalidJournal)?; return},
            };
        }
    }

    fn handle_get(
        start_message: StartMessage,
        _input: InputEntry,
        replayed: Vec<ProtocolMessage>,
        _incoming: impl Stream<Item = Result<ProtocolMessage, FrameError>>,
    ) -> impl Stream<Item = Result<ProtocolMessage, FrameError>> {
        try_stream! {
            let counter = read_counter(&start_message.state_map);
            match replayed.len() {
                0 => {
                    yield get_state(counter.clone());
                    yield output(counter.unwrap_or("0".into()));
                    yield end();
                },
                1 => {
                    yield output(counter.unwrap_or("0".into()));
                    yield end();
                }
                2=> {
                    yield end();
                }
                _ => {Err(FrameError::InvalidJournal)?; return},
            }
        }
    }

    fn handle_add(
        start_message: StartMessage,
        input: InputEntry,
        replayed: Vec<ProtocolMessage>,
        _incoming: impl Stream<Item = Result<ProtocolMessage, FrameError>>,
    ) -> impl Stream<Item = Result<ProtocolMessage, FrameError>> {
        try_stream! {
                let counter = read_counter(&start_message.state_map);
                match replayed.len() {
                    0 => {
                        yield get_state(counter.clone());

                        let next_value = match counter {
                            Some(ref counter) => {
                                let to_add: i32 = serde_json::from_slice(input.value.as_ref())?;
                                let current: i32 = serde_json::from_slice(counter.as_ref())?;

                                serde_json::to_vec(&(to_add + current))?.into()
                            }
                            None => input.value,
                        };

                        yield set_state(next_value.clone());
                        yield output(next_value);
                        yield end();
                    },
                    1 => {
                        let next_value = match counter {
                            Some(ref counter) => {
                                let to_add: i32 = serde_json::from_slice(input.value.as_ref())?;
                                let current: i32 = serde_json::from_slice(counter.as_ref())?;

                                serde_json::to_vec(&(to_add + current))?.into()
                            }
                            None => input.value,
                        };

                        yield set_state(next_value.clone());
                        yield output(next_value);
                        yield end();
                    }
                    2 => {
                        let set_value = match &replayed[1] {
                            ProtocolMessage::UnparsedEntry(set) if set.ty() == EntryType::SetState => {
                                let set = set.deserialize_entry_ref::<ProtobufRawEntryCodec>()?;
                                let_assert!(
                                  Entry::SetState(set) = set
                                );
                                set.value.clone()
                            },
                             _ => {Err(FrameError::InvalidJournal)?; return},
                        };
                        yield output(set_value);
                        yield end();
                    }
                    3 => {
                        yield end();
                    }
                    _ => {Err(FrameError::InvalidJournal)?; return},
                }
        }
    }
}

fn read_counter(state_map: &[StateEntry]) -> Option<Bytes> {
    let entry = state_map
        .iter()
        .find(|entry| entry.key.as_ref() == b"counter")?;
    Some(entry.value.clone())
}

fn get_state(counter: Option<Bytes>) -> ProtocolMessage {
    debug!(
        "Yielding GetStateEntryMessage with value {}",
        LossyDisplay(counter.as_deref())
    );

    ProtocolMessage::UnparsedEntry(PlainRawEntry::new(
        EntryHeader::GetState { is_completed: true },
        service_protocol::GetStateEntryMessage {
            name: String::new(),
            key: "counter".into(),
            result: Some(match counter {
                Some(ref counter) => get_state_entry_message::Result::Value(counter.clone()),
                None => get_state_entry_message::Result::Empty(service_protocol::Empty {}),
            }),
        }
        .encode_to_vec()
        .into(),
    ))
}

fn set_state(value: Bytes) -> ProtocolMessage {
    debug!(
        "Yielding SetStateEntryMessage with value {}",
        LossyDisplay(Some(&value))
    );

    ProtocolMessage::UnparsedEntry(PlainRawEntry::new(
        EntryHeader::SetState,
        service_protocol::SetStateEntryMessage {
            name: String::new(),
            key: "counter".into(),
            value: value.clone(),
        }
        .encode_to_vec()
        .into(),
    ))
}

fn output(value: Bytes) -> ProtocolMessage {
    debug!(
        "Yielding OutputEntryMessage with result {}",
        LossyDisplay(Some(&value))
    );

    ProtocolMessage::UnparsedEntry(PlainRawEntry::new(
        EntryHeader::Output,
        service_protocol::OutputEntryMessage {
            name: String::new(),
            result: Some(output_entry_message::Result::Value(value)),
        }
        .encode_to_vec()
        .into(),
    ))
}

fn end() -> ProtocolMessage {
    debug!("Yielding EndMessage");

    ProtocolMessage::End(service_protocol::EndMessage {})
}

fn error(err: FrameError) -> ProtocolMessage {
    let code = match err {
        FrameError::EncodingError(_) => codes::PROTOCOL_VIOLATION,
        FrameError::Hyper(_) => codes::INTERNAL,
        FrameError::UnexpectedEOF => codes::PROTOCOL_VIOLATION,
        FrameError::InvalidJournal => codes::JOURNAL_MISMATCH,
        FrameError::RawEntryCodecError(_) => codes::PROTOCOL_VIOLATION,
        FrameError::Serde(_) => codes::INTERNAL,
    };
    ProtocolMessage::Error(service_protocol::ErrorMessage {
        code: code.into(),
        description: err.to_string(),
        message: String::new(),
        related_entry_index: None,
        related_entry_name: None,
        related_entry_type: None,
        next_retry_delay: None,
    })
}

struct LossyDisplay<'a>(Option<&'a [u8]>);
impl<'a> Display for LossyDisplay<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(bytes) => write!(f, "{}", String::from_utf8_lossy(bytes)),
            None => write!(f, "<empty>"),
        }
    }
}
