// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::str::FromStr;

use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
use prost::Message;

use restate_ty::identifiers::{
    AwakeableIdentifier, ExternalSignalIdentifier, IdDecodeError, IdempotencyId, InvocationId,
};
use restate_ty::invocation::ServiceId;
use restate_types::errors::{GenericError, InvocationError};
use restate_types::invocation::Header;
use restate_types::journal_v2::encoding::DecodingError;
use restate_types::journal_v2::lite::{
    AttachInvocationCommandLite, CallCommandLite, ClearAllStateCommandLite, ClearStateCommandLite,
    CompleteAwakeableCommandLite, CompleteAwakeableResultLite, CompletePromiseCommandLite,
    EntryLite, GetEagerStateCommandLite, GetEagerStateKeysCommandLite,
    GetInvocationOutputCommandLite, GetLazyStateCommandLite, GetLazyStateKeysCommandLite,
    GetPromiseCommandLite, GetStateResultLite, InputCommandLite, NotificationLite,
    NotificationResultLite, OneWayCallCommandLite, OutputCommandLite, OutputResultLite,
    PeekPromiseCommandLite, RunCommandLite, SendSignalCommandLite, SetStateCommandLite,
    SignalResultLite, SleepCommandLite,
};
use restate_types::journal_v2::raw::{
    CallOrSendMetadata, RawCommand, RawCommandSpecificMetadata, RawEntry, RawNotification,
};
use restate_types::journal_v2::*;

use crate::proto;
use crate::proto::{
    complete_awakeable_command_message, notification_template, output_command_message,
    send_signal_command_message,
};

#[derive(Debug, thiserror::Error)]
#[error("missing required field {0}")]
struct MissingFieldError(&'static str);

#[derive(Debug, thiserror::Error)]
#[error("the given awakeable id '{0}' is not valid: {1}")]
struct InvalidAwakeableId(String, #[source] IdDecodeError);

#[derive(Debug, thiserror::Error)]
#[error("bad field {0}, reason: {1}")]
struct BadFieldError(&'static str, GenericError);

pub struct ServiceProtocolV4Codec;

impl Encoder for ServiceProtocolV4Codec {
    fn encode_entry(entry: Entry) -> RawEntry {
        match entry {
            Entry::Command(Command::Input(InputCommand {
                headers,
                payload,
                name,
            })) => RawCommand::new(
                CommandType::Input,
                proto::InputCommandMessage {
                    headers: headers.into_iter().map(Into::into).collect(),
                    value: Some(proto::Value { content: payload }),
                    name: name.to_string(),
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Command(Command::Output(OutputCommand { result, name })) => RawCommand::new(
                CommandType::Output,
                proto::OutputCommandMessage {
                    name: name.to_string(),
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command::Run(RunCommand {
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::Run,
                proto::RunCommandMessage {
                    result_completion_id: completion_id,
                    name: name.to_string(),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command::Sleep(SleepCommand {
                wake_up_time,
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::Sleep,
                proto::SleepCommandMessage {
                    wake_up_time: wake_up_time.as_u64(),
                    result_completion_id: completion_id,
                    name: name.to_string(),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command::Call(CallCommand {
                request:
                    CallRequest {
                        invocation_target,
                        parameter,
                        headers,
                        idempotency_key,
                        invocation_id,
                        span_context,
                        completion_retention_duration,
                        journal_retention_duration,
                    },
                invocation_id_completion_id,
                result_completion_id,
                name,
            })) => RawCommand::new(
                CommandType::Call,
                proto::CallCommandMessage {
                    service_name: invocation_target.service_name().to_string(),
                    handler_name: invocation_target.handler_name().to_string(),
                    parameter,
                    headers: headers.into_iter().map(Into::into).collect(),
                    key: invocation_target
                        .key()
                        .unwrap_or(&ByteString::new())
                        .to_string(),
                    idempotency_key: idempotency_key.map(|s| s.to_string()),
                    name: name.to_string(),
                    invocation_id_notification_idx: invocation_id_completion_id,
                    result_completion_id,
                }
                .encode_to_vec(),
            )
            .with_command_specific_metadata(RawCommandSpecificMetadata::CallOrSend(Box::new(
                CallOrSendMetadata {
                    invocation_id,
                    invocation_target,
                    span_context,
                    completion_retention_duration,
                    journal_retention_duration,
                },
            )))
            .into(),

            Entry::Command(Command::OneWayCall(OneWayCallCommand {
                request:
                    CallRequest {
                        invocation_target,
                        parameter,
                        headers,
                        idempotency_key,
                        invocation_id,
                        span_context,
                        completion_retention_duration,
                        journal_retention_duration,
                    },
                invoke_time,
                invocation_id_completion_id,
                name,
            })) => RawCommand::new(
                CommandType::OneWayCall,
                proto::OneWayCallCommandMessage {
                    service_name: invocation_target.service_name().to_string(),
                    handler_name: invocation_target.handler_name().to_string(),
                    parameter,
                    invoke_time: invoke_time.as_u64(),
                    headers: headers.into_iter().map(Into::into).collect(),
                    key: invocation_target
                        .key()
                        .unwrap_or(&ByteString::new())
                        .to_string(),
                    idempotency_key: idempotency_key.map(|s| s.to_string()),
                    name: name.to_string(),
                    invocation_id_notification_idx: invocation_id_completion_id,
                }
                .encode_to_vec(),
            )
            .with_command_specific_metadata(RawCommandSpecificMetadata::CallOrSend(Box::new(
                CallOrSendMetadata {
                    invocation_id,
                    invocation_target,
                    span_context,
                    completion_retention_duration,
                    journal_retention_duration,
                },
            )))
            .into(),

            Entry::Command(Command::GetLazyState(GetLazyStateCommand {
                key,
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::GetLazyState,
                proto::GetLazyStateCommandMessage {
                    key: key.into_bytes(),
                    name: name.to_string(),
                    result_completion_id: completion_id,
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Command(Command::SetState(SetStateCommand { key, value, name })) => {
                RawCommand::new(
                    CommandType::SetState,
                    proto::SetStateCommandMessage {
                        key: key.into_bytes(),
                        value: Some(proto::Value { content: value }),
                        name: name.to_string(),
                    }
                    .encode_to_vec(),
                )
                .into()
            }
            Entry::Command(Command::ClearState(ClearStateCommand { key, name })) => {
                RawCommand::new(
                    CommandType::ClearState,
                    proto::ClearStateCommandMessage {
                        key: key.into_bytes(),
                        name: name.to_string(),
                    }
                    .encode_to_vec(),
                )
                .into()
            }
            Entry::Command(Command::ClearAllState(ClearAllStateCommand { name })) => {
                RawCommand::new(
                    CommandType::ClearAllState,
                    proto::ClearAllStateCommandMessage {
                        name: name.to_string(),
                    }
                    .encode_to_vec(),
                )
                .into()
            }
            Entry::Command(Command::GetLazyStateKeys(GetLazyStateKeysCommand {
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::GetLazyStateKeys,
                proto::GetLazyStateKeysCommandMessage {
                    name: name.to_string(),
                    result_completion_id: completion_id,
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Command(Command::GetEagerState(GetEagerStateCommand { key, result, name })) => {
                RawCommand::new(
                    CommandType::GetEagerState,
                    proto::GetEagerStateCommandMessage {
                        key: key.into_bytes(),
                        name: name.to_string(),
                        result: Some(result.into()),
                    }
                    .encode_to_vec(),
                )
                .into()
            }
            Entry::Command(Command::GetEagerStateKeys(GetEagerStateKeysCommand {
                state_keys,
                name,
            })) => RawCommand::new(
                CommandType::GetEagerStateKeys,
                proto::GetEagerStateKeysCommandMessage {
                    name: name.to_string(),
                    value: Some(proto::StateKeys {
                        keys: state_keys.into_iter().map(Bytes::from).collect(),
                    }),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command::GetPromise(GetPromiseCommand {
                key,
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::GetPromise,
                proto::GetPromiseCommandMessage {
                    key: key.into(),
                    name: name.into(),
                    result_completion_id: completion_id,
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Command(Command::PeekPromise(PeekPromiseCommand {
                key,
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::PeekPromise,
                proto::PeekPromiseCommandMessage {
                    key: key.into(),
                    name: name.into(),
                    result_completion_id: completion_id,
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Command(Command::CompletePromise(CompletePromiseCommand {
                key,
                value,
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::CompletePromise,
                proto::CompletePromiseCommandMessage {
                    key: key.into(),
                    name: name.into(),
                    result_completion_id: completion_id,
                    completion: Some(value.into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command::SendSignal(SendSignalCommand {
                target_invocation_id,
                signal_id,
                result,
                name,
            })) => RawCommand::new(
                CommandType::SendSignal,
                proto::SendSignalCommandMessage {
                    target_invocation_id: target_invocation_id.to_string(),
                    entry_name: name.to_string(),
                    signal_id: Some(signal_id.into()),
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Command(Command::AttachInvocation(AttachInvocationCommand {
                target,
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::AttachInvocation,
                proto::AttachInvocationCommandMessage {
                    name: name.to_string(),
                    result_completion_id: completion_id,
                    target: Some(target.into()),
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Command(Command::GetInvocationOutput(GetInvocationOutputCommand {
                target,
                completion_id,
                name,
            })) => RawCommand::new(
                CommandType::GetInvocationOutput,
                proto::GetInvocationOutputCommandMessage {
                    name: name.to_string(),
                    result_completion_id: completion_id,
                    target: Some(target.into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Command(Command::CompleteAwakeable(CompleteAwakeableCommand {
                id,
                result,
                name,
            })) => RawCommand::new(
                CommandType::CompleteAwakeable,
                proto::CompleteAwakeableCommandMessage {
                    awakeable_id: id.to_string(),
                    name: name.to_string(),
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Notification(Notification::Completion(Completion::GetLazyState(
                GetLazyStateCompletion {
                    completion_id,
                    result,
                },
            ))) => RawNotification::new(
                CompletionType::GetLazyState,
                NotificationId::CompletionId(completion_id),
                proto::GetLazyStateCompletionNotificationMessage {
                    completion_id,
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Notification(Notification::Completion(Completion::GetLazyStateKeys(
                GetLazyStateKeysCompletion {
                    completion_id,
                    state_keys,
                },
            ))) => RawNotification::new(
                CompletionType::GetLazyStateKeys,
                NotificationId::CompletionId(completion_id),
                proto::GetLazyStateKeysCompletionNotificationMessage {
                    completion_id,
                    state_keys: Some(proto::StateKeys {
                        keys: state_keys.into_iter().map(Bytes::from).collect(),
                    }),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Notification(Notification::Completion(Completion::GetPromise(
                GetPromiseCompletion {
                    completion_id,
                    result,
                },
            ))) => RawNotification::new(
                CompletionType::GetPromise,
                NotificationId::CompletionId(completion_id),
                proto::GetPromiseCompletionNotificationMessage {
                    completion_id,
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Notification(Notification::Completion(Completion::PeekPromise(
                PeekPromiseCompletion {
                    completion_id,
                    result,
                },
            ))) => RawNotification::new(
                CompletionType::PeekPromise,
                NotificationId::CompletionId(completion_id),
                proto::PeekPromiseCompletionNotificationMessage {
                    completion_id,
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Notification(Notification::Completion(Completion::CompletePromise(
                CompletePromiseCompletion {
                    completion_id,
                    result,
                },
            ))) => RawNotification::new(
                CompletionType::CompletePromise,
                NotificationId::CompletionId(completion_id),
                proto::CompletePromiseCompletionNotificationMessage {
                    completion_id,
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Notification(Notification::Completion(Completion::Sleep(SleepCompletion {
                completion_id,
            }))) => RawNotification::new(
                CompletionType::Sleep,
                NotificationId::CompletionId(completion_id),
                proto::SleepCompletionNotificationMessage {
                    completion_id,
                    void: Some(proto::Void::default()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Notification(Notification::Completion(Completion::CallInvocationId(
                CallInvocationIdCompletion {
                    completion_id,
                    invocation_id,
                },
            ))) => RawNotification::new(
                CompletionType::CallInvocationId,
                NotificationId::CompletionId(completion_id),
                proto::CallInvocationIdCompletionNotificationMessage {
                    completion_id,
                    invocation_id: invocation_id.to_string(),
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Notification(Notification::Completion(Completion::Call(CallCompletion {
                completion_id,
                result,
            }))) => RawNotification::new(
                CompletionType::Call,
                NotificationId::CompletionId(completion_id),
                proto::CallCompletionNotificationMessage {
                    completion_id,
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Notification(Notification::Completion(Completion::Run(RunCompletion {
                completion_id,
                result,
            }))) => RawNotification::new(
                CompletionType::Run,
                NotificationId::CompletionId(completion_id),
                proto::RunCompletionNotificationMessage {
                    completion_id,
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Notification(Notification::Completion(Completion::AttachInvocation(
                AttachInvocationCompletion {
                    completion_id,
                    result,
                },
            ))) => RawNotification::new(
                CompletionType::AttachInvocation,
                NotificationId::CompletionId(completion_id),
                proto::AttachInvocationCompletionNotificationMessage {
                    completion_id,
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),
            Entry::Notification(Notification::Completion(Completion::GetInvocationOutput(
                GetInvocationOutputCompletion {
                    completion_id,
                    result,
                },
            ))) => RawNotification::new(
                CompletionType::GetInvocationOutput,
                NotificationId::CompletionId(completion_id),
                proto::GetInvocationOutputCompletionNotificationMessage {
                    completion_id,
                    result: Some(result.into()),
                }
                .encode_to_vec(),
            )
            .into(),

            Entry::Notification(Notification::Signal(Signal { id, result })) => {
                RawNotification::new(
                    NotificationType::Signal,
                    id.clone().into(),
                    proto::SignalNotificationMessage {
                        signal_id: Some(match id {
                            SignalId::Index(idx) => {
                                proto::signal_notification_message::SignalId::Idx(idx)
                            }
                            SignalId::Name(n) => {
                                proto::signal_notification_message::SignalId::Name(n.into())
                            }
                        }),
                        result: Some(result.into()),
                    }
                    .encode_to_vec(),
                )
                .into()
            }
        }
    }
}

macro_rules! get_or_bail {
    ($field:ident) => {
        $field.ok_or_else(|| {
            DecodingError::from(GenericError::from(MissingFieldError(stringify!($field))))
        })?
    };
}

macro_rules! to_string_or_bail {
    ($field:ident) => {
        String::from_utf8($field.to_vec()).map_err(|e| {
            DecodingError::from(GenericError::from(BadFieldError(
                stringify!($field),
                e.into(),
            )))
        })?
    };
}

macro_rules! to_invocation_id_or_bail {
    ($field:ident) => {
        InvocationId::from_str(&$field).map_err(|e| {
            DecodingError::from(GenericError::from(BadFieldError(
                stringify!($field),
                e.into(),
            )))
        })?
    };
}

macro_rules! decode_or_bail {
    ($e:expr, $ty:ident) => {
        proto::$ty::decode(&mut $e).map_err(GenericError::from)?
    };
}

impl Decoder for ServiceProtocolV4Codec {
    fn decode_entry(entry: &RawEntry) -> Result<Entry, DecodingError> {
        Ok(match &entry {
            RawEntry::Command(cmd) => match cmd.command_type() {
                CommandType::Input => {
                    let proto::InputCommandMessage {
                        headers,
                        value,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), InputCommandMessage);
                    InputCommand {
                        headers: headers.into_iter().map(Into::into).collect(),
                        payload: get_or_bail!(value).content,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::Output => {
                    let proto::OutputCommandMessage { name, result } =
                        decode_or_bail!(cmd.serialized_content(), OutputCommandMessage);
                    OutputCommand {
                        result: get_or_bail!(result).try_into()?,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::Run => {
                    let proto::RunCommandMessage {
                        result_completion_id,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), RunCommandMessage);
                    RunCommand {
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::Sleep => {
                    let proto::SleepCommandMessage {
                        wake_up_time,
                        result_completion_id,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), SleepCommandMessage);
                    SleepCommand {
                        wake_up_time: wake_up_time.into(),
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::Call => {
                    let proto::CallCommandMessage {
                        parameter,
                        headers,
                        idempotency_key,
                        invocation_id_notification_idx,
                        result_completion_id,
                        name,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), CallCommandMessage);
                    let_assert!(
                        RawCommandSpecificMetadata::CallOrSend(metadata) =
                            cmd.command_specific_metadata()
                    );
                    CallCommand {
                        request: CallRequest {
                            invocation_id: metadata.invocation_id,
                            invocation_target: metadata.invocation_target.clone(),
                            span_context: metadata.span_context.clone(),
                            parameter,
                            headers: headers.into_iter().map(Into::into).collect(),
                            idempotency_key: idempotency_key.map(|s| s.into()),
                            completion_retention_duration: metadata.completion_retention_duration,
                            journal_retention_duration: metadata.journal_retention_duration,
                        },
                        invocation_id_completion_id: invocation_id_notification_idx,
                        result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::OneWayCall => {
                    let proto::OneWayCallCommandMessage {
                        parameter,
                        invoke_time,
                        headers,
                        idempotency_key,
                        invocation_id_notification_idx,
                        name,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), OneWayCallCommandMessage);
                    let_assert!(
                        RawCommandSpecificMetadata::CallOrSend(metadata) =
                            cmd.command_specific_metadata()
                    );
                    OneWayCallCommand {
                        request: CallRequest {
                            invocation_id: metadata.invocation_id,
                            invocation_target: metadata.invocation_target.clone(),
                            span_context: metadata.span_context.clone(),
                            parameter,
                            headers: headers.into_iter().map(Into::into).collect(),
                            idempotency_key: idempotency_key.map(|s| s.into()),
                            completion_retention_duration: metadata.completion_retention_duration,
                            journal_retention_duration: metadata.journal_retention_duration,
                        },
                        invoke_time: invoke_time.into(),
                        invocation_id_completion_id: invocation_id_notification_idx,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::GetLazyState => {
                    let proto::GetLazyStateCommandMessage {
                        key,
                        result_completion_id,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), GetLazyStateCommandMessage);
                    GetLazyStateCommand {
                        key: to_string_or_bail!(key).into(),
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::SetState => {
                    let proto::SetStateCommandMessage { key, value, name } =
                        decode_or_bail!(cmd.serialized_content(), SetStateCommandMessage);
                    SetStateCommand {
                        key: to_string_or_bail!(key).into(),
                        name: name.into(),
                        value: get_or_bail!(value).content,
                    }
                    .into()
                }
                CommandType::ClearState => {
                    let proto::ClearStateCommandMessage { key, name } =
                        decode_or_bail!(cmd.serialized_content(), ClearStateCommandMessage);
                    ClearStateCommand {
                        key: to_string_or_bail!(key).into(),
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::ClearAllState => {
                    let proto::ClearAllStateCommandMessage { name } =
                        decode_or_bail!(cmd.serialized_content(), ClearAllStateCommandMessage);
                    ClearAllStateCommand { name: name.into() }.into()
                }
                CommandType::GetLazyStateKeys => {
                    let proto::GetLazyStateKeysCommandMessage {
                        result_completion_id,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), GetLazyStateKeysCommandMessage);
                    GetLazyStateKeysCommand {
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::GetEagerState => {
                    let proto::GetEagerStateCommandMessage { key, name, result } =
                        decode_or_bail!(cmd.serialized_content(), GetEagerStateCommandMessage);
                    GetEagerStateCommand {
                        key: to_string_or_bail!(key).into(),
                        name: name.into(),
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }
                CommandType::GetEagerStateKeys => {
                    let proto::GetEagerStateKeysCommandMessage { value, name } =
                        decode_or_bail!(cmd.serialized_content(), GetEagerStateKeysCommandMessage);
                    GetEagerStateKeysCommand {
                        state_keys: get_or_bail!(value)
                            .keys
                            .into_iter()
                            .map(|state_key| Ok::<_, DecodingError>(to_string_or_bail!(state_key)))
                            .collect::<Result<Vec<_>, _>>()?,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::GetPromise => {
                    let proto::GetPromiseCommandMessage {
                        key,
                        result_completion_id,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), GetPromiseCommandMessage);
                    GetPromiseCommand {
                        key: key.into(),
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::PeekPromise => {
                    let proto::PeekPromiseCommandMessage {
                        key,
                        result_completion_id,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), PeekPromiseCommandMessage);
                    PeekPromiseCommand {
                        key: key.into(),
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::CompletePromise => {
                    let proto::CompletePromiseCommandMessage {
                        key,
                        result_completion_id,
                        name,
                        completion,
                    } = decode_or_bail!(cmd.serialized_content(), CompletePromiseCommandMessage);
                    CompletePromiseCommand {
                        key: key.into(),
                        value: get_or_bail!(completion).try_into()?,
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::SendSignal => {
                    let proto::SendSignalCommandMessage {
                        target_invocation_id,
                        entry_name,
                        signal_id,
                        result,
                    } = decode_or_bail!(cmd.serialized_content(), SendSignalCommandMessage);
                    SendSignalCommand {
                        target_invocation_id: to_invocation_id_or_bail!(target_invocation_id),
                        signal_id: get_or_bail!(signal_id).try_into()?,
                        name: entry_name.into(),
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }
                CommandType::AttachInvocation => {
                    let proto::AttachInvocationCommandMessage {
                        result_completion_id,
                        name,
                        target,
                    } = decode_or_bail!(cmd.serialized_content(), AttachInvocationCommandMessage);
                    AttachInvocationCommand {
                        target: get_or_bail!(target).try_into()?,
                        name: name.into(),
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::GetInvocationOutput => {
                    let proto::GetInvocationOutputCommandMessage {
                        result_completion_id,
                        name,
                        target,
                    } = decode_or_bail!(
                        cmd.serialized_content(),
                        GetInvocationOutputCommandMessage
                    );
                    GetInvocationOutputCommand {
                        target: get_or_bail!(target).try_into()?,
                        name: name.into(),
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::CompleteAwakeable => {
                    let proto::CompleteAwakeableCommandMessage {
                        awakeable_id,
                        name,
                        result,
                    } = decode_or_bail!(cmd.serialized_content(), CompleteAwakeableCommandMessage);
                    CompleteAwakeableCommand {
                        id: parse_complete_awakeable_id(awakeable_id)?,
                        result: get_or_bail!(result).try_into()?,
                        name: name.into(),
                    }
                    .into()
                }
            },

            RawEntry::Notification(notif) => match notif.ty() {
                NotificationType::Completion(CompletionType::GetLazyState) => {
                    let proto::GetLazyStateCompletionNotificationMessage {
                        completion_id,
                        result,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        GetLazyStateCompletionNotificationMessage
                    );
                    GetLazyStateCompletion {
                        completion_id,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }
                NotificationType::Completion(CompletionType::GetLazyStateKeys) => {
                    let proto::GetLazyStateKeysCompletionNotificationMessage {
                        completion_id,
                        state_keys,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        GetLazyStateKeysCompletionNotificationMessage
                    );
                    GetLazyStateKeysCompletion {
                        completion_id,
                        state_keys: get_or_bail!(state_keys)
                            .keys
                            .into_iter()
                            .map(|state_key| Ok::<_, DecodingError>(to_string_or_bail!(state_key)))
                            .collect::<Result<Vec<_>, _>>()?,
                    }
                    .into()
                }

                NotificationType::Completion(CompletionType::GetPromise) => {
                    let proto::GetPromiseCompletionNotificationMessage {
                        completion_id,
                        result,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        GetPromiseCompletionNotificationMessage
                    );
                    GetPromiseCompletion {
                        completion_id,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }
                NotificationType::Completion(CompletionType::PeekPromise) => {
                    let proto::PeekPromiseCompletionNotificationMessage {
                        completion_id,
                        result,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        PeekPromiseCompletionNotificationMessage
                    );
                    PeekPromiseCompletion {
                        completion_id,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }
                NotificationType::Completion(CompletionType::CompletePromise) => {
                    let proto::CompletePromiseCompletionNotificationMessage {
                        completion_id,
                        result,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        CompletePromiseCompletionNotificationMessage
                    );
                    CompletePromiseCompletion {
                        completion_id,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }

                NotificationType::Completion(CompletionType::Sleep) => {
                    let proto::SleepCompletionNotificationMessage { completion_id, .. } = decode_or_bail!(
                        notif.serialized_content(),
                        SleepCompletionNotificationMessage
                    );
                    SleepCompletion { completion_id }.into()
                }

                NotificationType::Completion(CompletionType::CallInvocationId) => {
                    let proto::CallInvocationIdCompletionNotificationMessage {
                        completion_id,
                        invocation_id,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        CallInvocationIdCompletionNotificationMessage
                    );
                    CallInvocationIdCompletion {
                        completion_id,
                        invocation_id: to_invocation_id_or_bail!(invocation_id),
                    }
                    .into()
                }
                NotificationType::Completion(CompletionType::Call) => {
                    let proto::CallCompletionNotificationMessage {
                        completion_id,
                        result,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        CallCompletionNotificationMessage
                    );
                    CallCompletion {
                        completion_id,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }

                NotificationType::Completion(CompletionType::Run) => {
                    let proto::RunCompletionNotificationMessage {
                        completion_id,
                        result,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        RunCompletionNotificationMessage
                    );
                    RunCompletion {
                        completion_id,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }

                NotificationType::Completion(CompletionType::AttachInvocation) => {
                    let proto::AttachInvocationCompletionNotificationMessage {
                        completion_id,
                        result,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        AttachInvocationCompletionNotificationMessage
                    );
                    AttachInvocationCompletion {
                        completion_id,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }
                NotificationType::Completion(CompletionType::GetInvocationOutput) => {
                    let proto::GetInvocationOutputCompletionNotificationMessage {
                        completion_id,
                        result,
                    } = decode_or_bail!(
                        notif.serialized_content(),
                        GetInvocationOutputCompletionNotificationMessage
                    );
                    GetInvocationOutputCompletion {
                        completion_id,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }

                NotificationType::Signal => {
                    let proto::SignalNotificationMessage { signal_id, result } =
                        decode_or_bail!(notif.serialized_content(), SignalNotificationMessage);
                    Signal {
                        id: get_or_bail!(signal_id).try_into()?,
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }
            },
        })
    }

    fn decode_entry_lite(entry: &RawEntry) -> Result<EntryLite, DecodingError> {
        Ok(match &entry {
            RawEntry::Command(cmd) => match cmd.command_type() {
                CommandType::Input => InputCommandLite {}.into(),
                CommandType::Output => {
                    let proto::OutputCommandMessage { result, .. } =
                        decode_or_bail!(cmd.serialized_content(), OutputCommandMessage);
                    OutputCommandLite {
                        result: match get_or_bail!(result) {
                            output_command_message::Result::Value(_) => OutputResultLite::Success,
                            output_command_message::Result::Failure(_) => OutputResultLite::Failure,
                        },
                    }
                    .into()
                }
                CommandType::Run => {
                    let proto::RunCommandMessage {
                        result_completion_id,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), RunCommandMessage);
                    RunCommandLite {
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::Sleep => {
                    let proto::SleepCommandMessage {
                        wake_up_time,
                        result_completion_id,
                        name,
                    } = decode_or_bail!(cmd.serialized_content(), SleepCommandMessage);
                    SleepCommandLite {
                        wake_up_time: wake_up_time.into(),
                        completion_id: result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::Call => {
                    let proto::CallCommandMessage {
                        invocation_id_notification_idx,
                        result_completion_id,
                        name,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), CallCommandMessage);
                    let_assert!(
                        RawCommandSpecificMetadata::CallOrSend(metadata) =
                            cmd.command_specific_metadata()
                    );
                    CallCommandLite {
                        invocation_id: metadata.invocation_id,
                        invocation_target: metadata.invocation_target.clone(),
                        invocation_id_completion_id: invocation_id_notification_idx,
                        result_completion_id,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::OneWayCall => {
                    let proto::OneWayCallCommandMessage {
                        invoke_time,
                        invocation_id_notification_idx,
                        name,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), OneWayCallCommandMessage);
                    let_assert!(
                        RawCommandSpecificMetadata::CallOrSend(metadata) =
                            cmd.command_specific_metadata()
                    );
                    OneWayCallCommandLite {
                        invocation_id: metadata.invocation_id,
                        invocation_target: metadata.invocation_target.clone(),
                        invoke_time: invoke_time.into(),
                        invocation_id_completion_id: invocation_id_notification_idx,
                        name: name.into(),
                    }
                    .into()
                }
                CommandType::GetLazyState => {
                    let proto::GetLazyStateCommandMessage {
                        key,
                        result_completion_id,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), GetLazyStateCommandMessage);
                    GetLazyStateCommandLite {
                        key: to_string_or_bail!(key).into(),
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::SetState => {
                    let proto::SetStateCommandMessage { key, .. } =
                        decode_or_bail!(cmd.serialized_content(), SetStateCommandMessage);
                    SetStateCommandLite {
                        key: to_string_or_bail!(key).into(),
                    }
                    .into()
                }
                CommandType::ClearState => {
                    let proto::ClearStateCommandMessage { key, .. } =
                        decode_or_bail!(cmd.serialized_content(), ClearStateCommandMessage);
                    ClearStateCommandLite {
                        key: to_string_or_bail!(key).into(),
                    }
                    .into()
                }
                CommandType::ClearAllState => ClearAllStateCommandLite {}.into(),
                CommandType::GetLazyStateKeys => {
                    let proto::GetLazyStateKeysCommandMessage {
                        result_completion_id,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), GetLazyStateKeysCommandMessage);
                    GetLazyStateKeysCommandLite {
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::GetEagerState => {
                    let proto::GetEagerStateCommandMessage { key, result, .. } =
                        decode_or_bail!(cmd.serialized_content(), GetEagerStateCommandMessage);
                    GetEagerStateCommandLite {
                        key: to_string_or_bail!(key).into(),
                        result: get_or_bail!(result).try_into()?,
                    }
                    .into()
                }
                CommandType::GetEagerStateKeys => GetEagerStateKeysCommandLite {}.into(),
                CommandType::GetPromise => {
                    let proto::GetPromiseCommandMessage {
                        key,
                        result_completion_id,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), GetPromiseCommandMessage);
                    GetPromiseCommandLite {
                        key: key.into(),
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::PeekPromise => {
                    let proto::PeekPromiseCommandMessage {
                        key,
                        result_completion_id,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), PeekPromiseCommandMessage);
                    PeekPromiseCommandLite {
                        key: key.into(),
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::CompletePromise => {
                    let proto::CompletePromiseCommandMessage {
                        key,
                        result_completion_id,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), CompletePromiseCommandMessage);
                    CompletePromiseCommandLite {
                        key: key.into(),
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::SendSignal => {
                    let proto::SendSignalCommandMessage {
                        target_invocation_id,
                        signal_id,
                        result,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), SendSignalCommandMessage);
                    SendSignalCommandLite {
                        target_invocation_id: to_invocation_id_or_bail!(target_invocation_id),
                        signal_id: get_or_bail!(signal_id).try_into()?,
                        result: match get_or_bail!(result) {
                            send_signal_command_message::Result::Void(_) => SignalResultLite::Void,
                            send_signal_command_message::Result::Value(_) => {
                                SignalResultLite::Success
                            }
                            send_signal_command_message::Result::Failure(_) => {
                                SignalResultLite::Failure
                            }
                        },
                    }
                    .into()
                }
                CommandType::AttachInvocation => {
                    let proto::AttachInvocationCommandMessage {
                        result_completion_id,
                        target,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), AttachInvocationCommandMessage);
                    AttachInvocationCommandLite {
                        target: get_or_bail!(target).try_into()?,
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::GetInvocationOutput => {
                    let proto::GetInvocationOutputCommandMessage {
                        result_completion_id,
                        target,
                        ..
                    } = decode_or_bail!(
                        cmd.serialized_content(),
                        GetInvocationOutputCommandMessage
                    );
                    GetInvocationOutputCommandLite {
                        target: get_or_bail!(target).try_into()?,
                        completion_id: result_completion_id,
                    }
                    .into()
                }
                CommandType::CompleteAwakeable => {
                    let proto::CompleteAwakeableCommandMessage {
                        awakeable_id,
                        result,
                        ..
                    } = decode_or_bail!(cmd.serialized_content(), CompleteAwakeableCommandMessage);
                    CompleteAwakeableCommandLite {
                        id: parse_complete_awakeable_id(awakeable_id)?,
                        result: match get_or_bail!(result) {
                            complete_awakeable_command_message::Result::Value(_) => {
                                CompleteAwakeableResultLite::Success
                            }
                            complete_awakeable_command_message::Result::Failure(_) => {
                                CompleteAwakeableResultLite::Failure
                            }
                        },
                    }
                    .into()
                }
            },

            RawEntry::Notification(notif) => {
                let proto::NotificationTemplate { result, .. } =
                    decode_or_bail!(notif.serialized_content(), NotificationTemplate);

                EntryLite::Notification(NotificationLite {
                    ty: notif.ty(),
                    id: notif.id(),
                    result: match get_or_bail!(result) {
                        notification_template::Result::Void(_) => NotificationResultLite::Void,
                        notification_template::Result::Value(_) => NotificationResultLite::Success,
                        notification_template::Result::Failure(_) => {
                            NotificationResultLite::Failure
                        }
                        notification_template::Result::InvocationId(_) => {
                            NotificationResultLite::InvocationId
                        }
                        notification_template::Result::StateKeys(_) => {
                            NotificationResultLite::StateKeys
                        }
                    },
                })
            }
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
            metadata: value
                .metadata
                .into_iter()
                .map(|FailureMetadata { key, value }| proto::FailureMetadata {
                    key: key.into(),
                    value: value.into(),
                })
                .collect(),
        }
    }
}

impl From<proto::Failure> for Failure {
    fn from(value: proto::Failure) -> Self {
        Self {
            code: value.code.into(),
            message: value.message.into(),
            metadata: value
                .metadata
                .into_iter()
                .map(|proto::FailureMetadata { key, value }| FailureMetadata {
                    key: key.into(),
                    value: value.into(),
                })
                .collect(),
        }
    }
}

impl From<OutputResult> for proto::output_command_message::Result {
    fn from(value: OutputResult) -> Self {
        match value {
            OutputResult::Success(content) => Self::Value(proto::Value { content }),
            OutputResult::Failure(f) => Self::Failure(f.into()),
        }
    }
}

impl TryFrom<proto::output_command_message::Result> for OutputResult {
    type Error = DecodingError;

    fn try_from(value: proto::output_command_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::output_command_message::Result::Value(value) => Self::Success(value.content),
            proto::output_command_message::Result::Failure(f) => Self::Failure(f.into()),
        })
    }
}

impl From<GetStateResult> for proto::get_eager_state_command_message::Result {
    fn from(value: GetStateResult) -> Self {
        match value {
            GetStateResult::Void => Self::Void(proto::Void::default()),
            GetStateResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl From<GetStateResult> for proto::get_lazy_state_completion_notification_message::Result {
    fn from(value: GetStateResult) -> Self {
        match value {
            GetStateResult::Void => Self::Void(proto::Void::default()),
            GetStateResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::get_eager_state_command_message::Result> for GetStateResult {
    type Error = DecodingError;

    fn try_from(
        value: proto::get_eager_state_command_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::get_eager_state_command_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::get_eager_state_command_message::Result::Void(_) => Self::Void,
        })
    }
}

impl TryFrom<proto::get_eager_state_command_message::Result> for GetStateResultLite {
    type Error = DecodingError;

    fn try_from(
        value: proto::get_eager_state_command_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::get_eager_state_command_message::Result::Value(_) => Self::Success,
            proto::get_eager_state_command_message::Result::Void(_) => Self::Void,
        })
    }
}

impl TryFrom<proto::get_lazy_state_completion_notification_message::Result> for GetStateResult {
    type Error = DecodingError;

    fn try_from(
        value: proto::get_lazy_state_completion_notification_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::get_lazy_state_completion_notification_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::get_lazy_state_completion_notification_message::Result::Void(_) => Self::Void,
        })
    }
}

impl From<CompletePromiseValue> for proto::complete_promise_command_message::Completion {
    fn from(value: CompletePromiseValue) -> Self {
        match value {
            CompletePromiseValue::Success(s) => Self::CompletionValue(proto::Value { content: s }),
            CompletePromiseValue::Failure(f) => Self::CompletionFailure(f.into()),
        }
    }
}

impl TryFrom<proto::complete_promise_command_message::Completion> for CompletePromiseValue {
    type Error = DecodingError;

    fn try_from(
        value: proto::complete_promise_command_message::Completion,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::complete_promise_command_message::Completion::CompletionValue(value) => {
                Self::Success(value.content)
            }
            proto::complete_promise_command_message::Completion::CompletionFailure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

impl From<SignalId> for proto::send_signal_command_message::SignalId {
    fn from(value: SignalId) -> Self {
        match value {
            SignalId::Index(i) => Self::Idx(i),
            SignalId::Name(n) => Self::Name(n.into()),
        }
    }
}

impl TryFrom<proto::send_signal_command_message::SignalId> for SignalId {
    type Error = DecodingError;

    fn try_from(value: proto::send_signal_command_message::SignalId) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::send_signal_command_message::SignalId::Idx(value) => Self::Index(value),
            proto::send_signal_command_message::SignalId::Name(f) => Self::Name(f.into()),
        })
    }
}

impl TryFrom<proto::signal_notification_message::SignalId> for SignalId {
    type Error = DecodingError;

    fn try_from(value: proto::signal_notification_message::SignalId) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::signal_notification_message::SignalId::Idx(value) => Self::Index(value),
            proto::signal_notification_message::SignalId::Name(f) => Self::Name(f.into()),
        })
    }
}

impl From<SignalResult> for proto::send_signal_command_message::Result {
    fn from(value: SignalResult) -> Self {
        match value {
            SignalResult::Void => Self::Void(proto::Void::default()),
            SignalResult::Success(s) => Self::Value(proto::Value { content: s }),
            SignalResult::Failure(f) => Self::Failure(f.into()),
        }
    }
}

impl TryFrom<proto::send_signal_command_message::Result> for SignalResult {
    type Error = DecodingError;

    fn try_from(value: proto::send_signal_command_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::send_signal_command_message::Result::Void(_) => Self::Void,
            proto::send_signal_command_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::send_signal_command_message::Result::Failure(f) => Self::Failure(f.into()),
        })
    }
}

impl From<AttachInvocationTarget> for proto::attach_invocation_command_message::Target {
    fn from(value: AttachInvocationTarget) -> Self {
        match value {
            AttachInvocationTarget::InvocationId(id) => Self::InvocationId(id.to_string()),
            AttachInvocationTarget::IdempotentRequest(id) => {
                Self::IdempotentRequestTarget(proto::IdempotentRequestTarget {
                    service_name: id.service_name.into(),
                    service_key: id.service_key.map(Into::into),
                    handler_name: id.service_handler.into(),
                    idempotency_key: id.idempotency_key.into(),
                })
            }
            AttachInvocationTarget::Workflow(id) => Self::WorkflowTarget(proto::WorkflowTarget {
                workflow_name: id.service_name.into(),
                workflow_key: id.key.into(),
            }),
        }
    }
}

impl TryFrom<proto::attach_invocation_command_message::Target> for AttachInvocationTarget {
    type Error = DecodingError;

    fn try_from(
        value: proto::attach_invocation_command_message::Target,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::attach_invocation_command_message::Target::InvocationId(invocation_id) => {
                Self::InvocationId(to_invocation_id_or_bail!(invocation_id))
            }
            proto::attach_invocation_command_message::Target::IdempotentRequestTarget(
                idempotent_request,
            ) => Self::IdempotentRequest(IdempotencyId::new(
                idempotent_request.service_name.into(),
                idempotent_request.service_key.map(Into::into),
                idempotent_request.handler_name.into(),
                idempotent_request.idempotency_key.into(),
            )),
            proto::attach_invocation_command_message::Target::WorkflowTarget(workflow_target) => {
                Self::Workflow(ServiceId::new(
                    workflow_target.workflow_name,
                    workflow_target.workflow_key,
                ))
            }
        })
    }
}

impl From<AttachInvocationTarget> for proto::get_invocation_output_command_message::Target {
    fn from(value: AttachInvocationTarget) -> Self {
        match value {
            AttachInvocationTarget::InvocationId(id) => Self::InvocationId(id.to_string()),
            AttachInvocationTarget::IdempotentRequest(id) => {
                Self::IdempotentRequestTarget(proto::IdempotentRequestTarget {
                    service_name: id.service_name.into(),
                    service_key: id.service_key.map(Into::into),
                    handler_name: id.service_handler.into(),
                    idempotency_key: id.idempotency_key.into(),
                })
            }
            AttachInvocationTarget::Workflow(id) => Self::WorkflowTarget(proto::WorkflowTarget {
                workflow_name: id.service_name.into(),
                workflow_key: id.key.into(),
            }),
        }
    }
}

impl TryFrom<proto::get_invocation_output_command_message::Target> for AttachInvocationTarget {
    type Error = DecodingError;

    fn try_from(
        value: proto::get_invocation_output_command_message::Target,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::get_invocation_output_command_message::Target::InvocationId(invocation_id) => {
                Self::InvocationId(to_invocation_id_or_bail!(invocation_id))
            }
            proto::get_invocation_output_command_message::Target::IdempotentRequestTarget(
                idempotent_request,
            ) => Self::IdempotentRequest(IdempotencyId::new(
                idempotent_request.service_name.into(),
                idempotent_request.service_key.map(Into::into),
                idempotent_request.handler_name.into(),
                idempotent_request.idempotency_key.into(),
            )),
            proto::get_invocation_output_command_message::Target::WorkflowTarget(
                workflow_target,
            ) => Self::Workflow(ServiceId::new(
                workflow_target.workflow_name,
                workflow_target.workflow_key,
            )),
        })
    }
}

impl From<GetPromiseResult> for proto::get_promise_completion_notification_message::Result {
    fn from(value: GetPromiseResult) -> Self {
        match value {
            GetPromiseResult::Failure(f) => Self::Failure(f.into()),
            GetPromiseResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::get_promise_completion_notification_message::Result> for GetPromiseResult {
    type Error = DecodingError;

    fn try_from(
        value: proto::get_promise_completion_notification_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::get_promise_completion_notification_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::get_promise_completion_notification_message::Result::Failure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

impl From<PeekPromiseResult> for proto::peek_promise_completion_notification_message::Result {
    fn from(value: PeekPromiseResult) -> Self {
        match value {
            PeekPromiseResult::Void => Self::Void(proto::Void::default()),
            PeekPromiseResult::Failure(f) => Self::Failure(f.into()),
            PeekPromiseResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::peek_promise_completion_notification_message::Result> for PeekPromiseResult {
    type Error = DecodingError;

    fn try_from(
        value: proto::peek_promise_completion_notification_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::peek_promise_completion_notification_message::Result::Void(_) => Self::Void,
            proto::peek_promise_completion_notification_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::peek_promise_completion_notification_message::Result::Failure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

impl From<CompletePromiseResult>
    for proto::complete_promise_completion_notification_message::Result
{
    fn from(value: CompletePromiseResult) -> Self {
        match value {
            CompletePromiseResult::Failure(f) => Self::Failure(f.into()),
            CompletePromiseResult::Void => Self::Void(proto::Void::default()),
        }
    }
}

impl TryFrom<proto::complete_promise_completion_notification_message::Result>
    for CompletePromiseResult
{
    type Error = DecodingError;

    fn try_from(
        value: proto::complete_promise_completion_notification_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::complete_promise_completion_notification_message::Result::Void(_) => Self::Void,
            proto::complete_promise_completion_notification_message::Result::Failure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

impl From<CallResult> for proto::call_completion_notification_message::Result {
    fn from(value: CallResult) -> Self {
        match value {
            CallResult::Failure(f) => Self::Failure(f.into()),
            CallResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::call_completion_notification_message::Result> for CallResult {
    type Error = DecodingError;

    fn try_from(
        value: proto::call_completion_notification_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::call_completion_notification_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::call_completion_notification_message::Result::Failure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

impl From<RunResult> for proto::run_completion_notification_message::Result {
    fn from(value: RunResult) -> Self {
        match value {
            RunResult::Failure(f) => Self::Failure(f.into()),
            RunResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::run_completion_notification_message::Result> for RunResult {
    type Error = DecodingError;

    fn try_from(
        value: proto::run_completion_notification_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::run_completion_notification_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::run_completion_notification_message::Result::Failure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

impl From<AttachInvocationResult>
    for proto::attach_invocation_completion_notification_message::Result
{
    fn from(value: AttachInvocationResult) -> Self {
        match value {
            AttachInvocationResult::Failure(f) => Self::Failure(f.into()),
            AttachInvocationResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::attach_invocation_completion_notification_message::Result>
    for AttachInvocationResult
{
    type Error = DecodingError;

    fn try_from(
        value: proto::attach_invocation_completion_notification_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::attach_invocation_completion_notification_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::attach_invocation_completion_notification_message::Result::Failure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

impl From<GetInvocationOutputResult>
    for proto::get_invocation_output_completion_notification_message::Result
{
    fn from(value: GetInvocationOutputResult) -> Self {
        match value {
            GetInvocationOutputResult::Void => Self::Void(proto::Void::default()),
            GetInvocationOutputResult::Failure(f) => Self::Failure(f.into()),
            GetInvocationOutputResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::get_invocation_output_completion_notification_message::Result>
    for GetInvocationOutputResult
{
    type Error = DecodingError;

    fn try_from(
        value: proto::get_invocation_output_completion_notification_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::get_invocation_output_completion_notification_message::Result::Void(_) => {
                Self::Void
            }
            proto::get_invocation_output_completion_notification_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::get_invocation_output_completion_notification_message::Result::Failure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

impl From<SignalResult> for proto::signal_notification_message::Result {
    fn from(value: SignalResult) -> Self {
        match value {
            SignalResult::Void => Self::Void(proto::Void::default()),
            SignalResult::Failure(f) => Self::Failure(f.into()),
            SignalResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::signal_notification_message::Result> for SignalResult {
    type Error = DecodingError;

    fn try_from(value: proto::signal_notification_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::signal_notification_message::Result::Void(_) => Self::Void,
            proto::signal_notification_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::signal_notification_message::Result::Failure(f) => Self::Failure(f.into()),
        })
    }
}

impl From<CompleteAwakeableResult> for proto::complete_awakeable_command_message::Result {
    fn from(value: CompleteAwakeableResult) -> Self {
        match value {
            CompleteAwakeableResult::Failure(f) => Self::Failure(f.into()),
            CompleteAwakeableResult::Success(s) => Self::Value(proto::Value { content: s }),
        }
    }
}

impl TryFrom<proto::complete_awakeable_command_message::Result> for CompleteAwakeableResult {
    type Error = DecodingError;

    fn try_from(
        value: proto::complete_awakeable_command_message::Result,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::complete_awakeable_command_message::Result::Value(value) => {
                Self::Success(value.content)
            }
            proto::complete_awakeable_command_message::Result::Failure(f) => {
                Self::Failure(f.into())
            }
        })
    }
}

fn parse_complete_awakeable_id(awakeable_id: String) -> Result<CompleteAwakeableId, DecodingError> {
    if let Ok(signal_id) = ExternalSignalIdentifier::from_str(&awakeable_id) {
        Ok(CompleteAwakeableId::New(signal_id))
    } else {
        Ok(CompleteAwakeableId::Old(
            AwakeableIdentifier::from_str(&awakeable_id).map_err(|e| {
                DecodingError::from(GenericError::from(InvalidAwakeableId(awakeable_id, e)))
            })?,
        ))
    }
}

impl From<proto::ErrorMessage> for InvocationError {
    fn from(value: proto::ErrorMessage) -> Self {
        if value.stacktrace.is_empty() {
            InvocationError::new(value.code, value.message)
        } else {
            InvocationError::new(value.code, value.message).with_stacktrace(value.stacktrace)
        }
    }
}
