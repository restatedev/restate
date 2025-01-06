// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::proto;
use assert2::let_assert;
use bytestring::ByteString;
use prost::Message;
use restate_types::errors::GenericError;
use restate_types::invocation::Header;
use restate_types::journal_v2::command::{
    CallCommand, CallRequest, CommandInner, CommandMetadata, InputCommand, OneWayCallCommand,
    OutputCommand, OutputResult, RunCommand, SleepCommand,
};
use restate_types::journal_v2::encoding::EncodingError;
use restate_types::journal_v2::raw::{
    RawCommand, RawCommandSpecificMetadata, RawEntry, RawEntryHeader, RawEntryInner,
    RawNotification,
};
use restate_types::journal_v2::{
    Command, CommandType, Decoder, Encoder, Entry, Failure, Notification, NotificationId,
    NotificationResult,
};
use std::fmt::Debug;

#[derive(Debug, thiserror::Error)]
#[error("missing required field {0}")]
struct MissingFieldError(&'static str);

pub struct ServiceProtocolV4Codec;

impl Encoder for ServiceProtocolV4Codec {
    fn encode_entry(entry: &Entry) -> Result<RawEntry, EncodingError> {
        let entry_inner: RawEntryInner = match entry {
            Entry::Command(Command {
                metadata,
                inner: CommandInner::Input(InputCommand { headers, payload }),
            }) => RawCommand::new(
                CommandType::Input,
                proto::InputCommandMessage {
                    headers: headers.clone().into_iter().map(Into::into).collect(),
                    value: payload.clone(),
                    name: metadata.name().to_owned(),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command {
                metadata,
                inner: CommandInner::Output(OutputCommand { result }),
            }) => RawCommand::new(
                CommandType::Output,
                proto::OutputCommandMessage {
                    name: metadata.name().to_owned(),
                    result: Some(result.clone().into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command {
                metadata,
                inner: CommandInner::Run(RunCommand { notification_idx }),
            }) => RawCommand::new(
                CommandType::Run,
                proto::RunCommandMessage {
                    result_notification_idx: *notification_idx,
                    name: metadata.name().to_owned(),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command {
                metadata,
                inner:
                    CommandInner::Sleep(SleepCommand {
                        wake_up_time,
                        notification_idx,
                    }),
            }) => RawCommand::new(
                CommandType::Sleep,
                proto::SleepCommandMessage {
                    wake_up_time: wake_up_time.as_u64(),
                    result_notification_idx: *notification_idx,
                    name: metadata.name().to_owned(),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command {
                metadata,
                inner:
                    CommandInner::Call(CallCommand {
                        request:
                            CallRequest {
                                invocation_target,
                                parameter,
                                headers,
                                idempotency_key,
                                ..
                            },
                        invocation_id_notification_idx,
                        result_notification_idx,
                    }),
            }) => RawCommand::new(
                CommandType::Call,
                proto::CallCommandMessage {
                    service_name: invocation_target.service_name().to_string(),
                    handler_name: invocation_target.handler_name().to_string(),
                    parameter: parameter.clone(),
                    headers: headers.clone().into_iter().map(Into::into).collect(),
                    key: invocation_target
                        .key()
                        .unwrap_or(&ByteString::new())
                        .to_string(),
                    idempotency_key: idempotency_key.clone().map(|s| s.to_string()),
                    name: metadata.name().to_owned(),
                    invocation_id_notification_idx: *invocation_id_notification_idx,
                    result_notification_idx: *result_notification_idx,
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command {
                metadata,
                inner:
                    CommandInner::OneWayCall(OneWayCallCommand {
                        request:
                            CallRequest {
                                invocation_target,
                                parameter,
                                headers,
                                idempotency_key,
                                ..
                            },
                        invoke_time,
                        invocation_id_notification_idx,
                    }),
            }) => RawCommand::new(
                CommandType::OneWayCall,
                proto::OneWayCallCommandMessage {
                    service_name: invocation_target.service_name().to_string(),
                    handler_name: invocation_target.handler_name().to_string(),
                    parameter: parameter.clone(),
                    invoke_time: invoke_time.as_u64(),
                    headers: headers.clone().into_iter().map(Into::into).collect(),
                    key: invocation_target
                        .key()
                        .unwrap_or(&ByteString::new())
                        .to_string(),
                    idempotency_key: idempotency_key.clone().map(|s| s.to_string()),
                    name: metadata.name().to_owned(),
                    invocation_id_notification_idx: *invocation_id_notification_idx,
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Notification(Notification { id, result }) => RawNotification::new(
                id.clone(),
                proto::NotificationMessage {
                    id: Some(proto::notification_message::Id::from(id.clone())),
                    result: Some(proto::notification_message::Result::from(result.clone())),
                }
                .encode_to_vec()
                .into(),
            )
            .into(),

            Entry::Event(e) => e.clone().into(),
        };
        Ok(RawEntry::new(RawEntryHeader::new(), entry_inner))
    }
}

impl Decoder for ServiceProtocolV4Codec {
    fn decode_entry(entry: &RawEntry) -> Result<Entry, EncodingError> {
        Ok(match &entry.inner {
            RawEntryInner::Command(cmd) => match cmd.command_type() {
                CommandType::Input => {
                    let pb = proto::InputCommandMessage::decode(&mut cmd.serialized_content())
                        .map_err(GenericError::from)?;
                    Entry::Command(Command::new(
                        CommandMetadata::new(pb.name),
                        InputCommand {
                            headers: pb.headers.into_iter().map(Into::into).collect(),
                            payload: pb.value.clone(),
                        },
                    ))
                }
                CommandType::Run => {
                    let pb = proto::RunCommandMessage::decode(&mut cmd.serialized_content())
                        .map_err(GenericError::from)?;
                    Entry::Command(Command::new(
                        CommandMetadata::new(pb.name),
                        RunCommand {
                            notification_idx: pb.result_notification_idx,
                        },
                    ))
                }
                CommandType::Sleep => {
                    let pb = proto::SleepCommandMessage::decode(&mut cmd.serialized_content())
                        .map_err(GenericError::from)?;
                    Entry::Command(Command::new(
                        CommandMetadata::new(pb.name),
                        SleepCommand {
                            wake_up_time: pb.wake_up_time.into(),
                            notification_idx: pb.result_notification_idx,
                        },
                    ))
                }
                CommandType::Call => {
                    let pb = proto::CallCommandMessage::decode(&mut cmd.serialized_content())
                        .map_err(GenericError::from)?;
                    let_assert!(
                        RawCommandSpecificMetadata::CallOrSend(metadata) =
                            cmd.command_specific_metadata()
                    );
                    Entry::Command(Command::new(
                        CommandMetadata::new(pb.name),
                        CallCommand {
                            request: CallRequest {
                                invocation_id: metadata.invocation_id,
                                invocation_target: metadata.invocation_target.clone(),
                                span_context: metadata.span_context.clone(),
                                parameter: pb.parameter,
                                headers: pb.headers.into_iter().map(Into::into).collect(),
                                idempotency_key: pb.idempotency_key.map(|s| s.into()),
                            },
                            invocation_id_notification_idx: pb.invocation_id_notification_idx,
                            result_notification_idx: pb.result_notification_idx,
                        },
                    ))
                }
                CommandType::OneWayCall => {
                    let pb = proto::OneWayCallCommandMessage::decode(&mut cmd.serialized_content())
                        .map_err(GenericError::from)?;
                    let_assert!(
                        RawCommandSpecificMetadata::CallOrSend(metadata) =
                            cmd.command_specific_metadata()
                    );
                    Entry::Command(Command::new(
                        CommandMetadata::new(pb.name),
                        OneWayCallCommand {
                            request: CallRequest {
                                invocation_id: metadata.invocation_id,
                                invocation_target: metadata.invocation_target.clone(),
                                span_context: metadata.span_context.clone(),
                                parameter: pb.parameter,
                                headers: pb.headers.into_iter().map(Into::into).collect(),
                                idempotency_key: pb.idempotency_key.map(|s| s.into()),
                            },
                            invoke_time: pb.invoke_time.into(),
                            invocation_id_notification_idx: pb.invocation_id_notification_idx,
                        },
                    ))
                }
                CommandType::Output => {
                    let pb = proto::OutputCommandMessage::decode(&mut cmd.serialized_content())
                        .map_err(GenericError::from)?;
                    Entry::Command(Command::new(
                        CommandMetadata::new(pb.name),
                        OutputCommand {
                            result: pb
                                .result
                                .ok_or_else(|| {
                                    EncodingError::from(GenericError::from(MissingFieldError(
                                        "result",
                                    )))
                                })?
                                .try_into()?,
                        },
                    ))
                }
            },
            RawEntryInner::Notification(notif) => {
                let pb = proto::NotificationMessage::decode(&mut notif.serialized_content())
                    .map_err(GenericError::from)?;
                Entry::Notification(Notification::new(
                    notif.id(),
                    pb.result
                        .ok_or_else(|| {
                            EncodingError::from(GenericError::from(MissingFieldError("result")))
                        })?
                        .try_into()?,
                ))
            }
            RawEntryInner::Event(e) => Entry::Event(e.clone()),
        })
    }
}

impl From<proto::Header> for Header {
    fn from(value: proto::Header) -> Self {
        Self {
            name: value.key.into(),
            value: value.value.into(),
        }
    }
}

impl From<Header> for proto::Header {
    fn from(value: Header) -> Self {
        Self {
            key: value.name.into(),
            value: value.value.into(),
        }
    }
}

impl From<Failure> for proto::Failure {
    fn from(value: Failure) -> Self {
        Self {
            code: value.code.into(),
            message: value.message.into(),
        }
    }
}

impl From<proto::Failure> for Failure {
    fn from(value: proto::Failure) -> Self {
        Self {
            code: value.code.into(),
            message: value.message.into(),
        }
    }
}

impl From<OutputResult> for proto::output_command_message::Result {
    fn from(value: OutputResult) -> Self {
        match value {
            OutputResult::Success(s) => Self::Value(s),
            OutputResult::Failure(f) => Self::Failure(f.into()),
        }
    }
}

impl TryFrom<proto::output_command_message::Result> for OutputResult {
    type Error = EncodingError;

    fn try_from(value: proto::output_command_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::output_command_message::Result::Value(value) => Self::Success(value),
            proto::output_command_message::Result::Failure(f) => Self::Failure(f.into()),
        })
    }
}

impl From<NotificationId> for proto::notification_message::Id {
    fn from(value: NotificationId) -> Self {
        match value {
            NotificationId::Index(idx) => Self::Idx(idx),
            NotificationId::Name(n) => Self::Name(n.into()),
        }
    }
}

impl From<NotificationResult> for proto::notification_message::Result {
    fn from(value: NotificationResult) -> Self {
        match value {
            NotificationResult::Void => Self::Void(proto::Void::default()),
            NotificationResult::Success(s) => Self::Value(proto::Value { content: s }),
            NotificationResult::Failure(f) => Self::Failure(f.into()),
            NotificationResult::InvocationId(id) => Self::InvocationId(id.into()),
            NotificationResult::StateKeys(sk) => Self::StateKeys(proto::StateKeys {
                keys: sk
                    .into_iter()
                    .map(|s| s.as_bytes().to_vec().into())
                    .collect(),
            }),
        }
    }
}

impl TryFrom<proto::notification_message::Result> for NotificationResult {
    type Error = EncodingError;

    fn try_from(value: proto::notification_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::notification_message::Result::Void(_) => Self::Void,
            proto::notification_message::Result::Value(proto::Value { content }) => {
                Self::Success(content)
            }
            proto::notification_message::Result::Failure(f) => Self::Failure(f.into()),
            proto::notification_message::Result::InvocationId(s) => Self::InvocationId(s.into()),
            proto::notification_message::Result::StateKeys(sk) => Self::StateKeys(
                sk.keys
                    .into_iter()
                    .map(|s| String::from_utf8(s.to_vec()))
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(|e| EncodingError::from(GenericError::from(e)))?,
            ),
        })
    }
}
