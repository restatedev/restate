// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::journal_v2::raw::RawEvent;
use crate::journal_v2::{Event, EventType};
use bytes::Bytes;
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub(super) enum EventDecodingError {
    #[error("decoding error: {0:?}")]
    Protobuf(#[from] prost::DecodeError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub(super) fn decode(ty: EventType, value: Bytes) -> Result<Event, EventDecodingError> {
    match ty {
        EventType::TransientError => Ok(Event::TransientError(
            pb::TransientErrorEvent::decode(value)?.try_into()?,
        )),
        EventType::Unknown => Ok(Event::Unknown),
    }
}

pub(super) fn encode(event: Event) -> RawEvent {
    match event {
        Event::TransientError(e) => RawEvent::new(
            EventType::TransientError,
            pb::TransientErrorEvent::from(e).encode_to_vec().into(),
        ),
        Event::Unknown => RawEvent::unknown(),
    }
}

mod pb {
    use crate::errors::InvocationErrorCode;
    use crate::journal_v2;
    use crate::journal_v2::event;
    use std::num::NonZeroU32;

    include!(concat!(env!("OUT_DIR"), "/restate.journal.events.rs"));

    impl From<event::TransientErrorEvent> for TransientErrorEvent {
        fn from(
            event::TransientErrorEvent {
                error_code,
                error_message,
                error_stacktrace,
                restate_doc_error_code,
                related_command_index,
                related_command_name,
                related_command_type,
                count,
            }: event::TransientErrorEvent,
        ) -> Self {
            TransientErrorEvent {
                error_code: error_code.into(),
                error_message,
                error_stacktrace,
                restate_doc_error_code,
                related_command_index,
                related_command_name,
                related_command_type: related_command_type
                    .map(|ct| transient_error_event::CommandType::from(ct).into()),
                count: count.into(),
            }
        }
    }

    impl TryFrom<TransientErrorEvent> for event::TransientErrorEvent {
        type Error = anyhow::Error;

        fn try_from(
            TransientErrorEvent {
                error_code,
                error_message,
                error_stacktrace,
                restate_doc_error_code,
                related_command_index,
                related_command_name,
                related_command_type,
                count,
            }: TransientErrorEvent,
        ) -> Result<Self, Self::Error> {
            Ok(event::TransientErrorEvent {
                error_code: InvocationErrorCode::new(error_code as u16),
                error_message,
                error_stacktrace,
                restate_doc_error_code,
                related_command_index,
                related_command_name,
                related_command_type: related_command_type
                    .map(|ct| {
                        transient_error_event::CommandType::try_from(ct)
                            .unwrap_or(transient_error_event::CommandType::Unknown)
                            .try_into()
                    })
                    .transpose()?,
                count: NonZeroU32::new(count)
                    .ok_or_else(|| anyhow::anyhow!("count out of bounds"))?,
            })
        }
    }

    impl From<journal_v2::CommandType> for transient_error_event::CommandType {
        fn from(value: journal_v2::CommandType) -> Self {
            match value {
                journal_v2::CommandType::Input => Self::Input,
                journal_v2::CommandType::Output => Self::Output,
                journal_v2::CommandType::GetLazyState => Self::GetLazyState,
                journal_v2::CommandType::SetState => Self::SetState,
                journal_v2::CommandType::ClearState => Self::ClearState,
                journal_v2::CommandType::ClearAllState => Self::ClearAllState,
                journal_v2::CommandType::GetLazyStateKeys => Self::GetLazyStateKeys,
                journal_v2::CommandType::GetEagerState => Self::GetEagerState,
                journal_v2::CommandType::GetEagerStateKeys => Self::GetEagerStateKeys,
                journal_v2::CommandType::GetPromise => Self::GetPromise,
                journal_v2::CommandType::PeekPromise => Self::PeekPromise,
                journal_v2::CommandType::CompletePromise => Self::CompletePromise,
                journal_v2::CommandType::Sleep => Self::Sleep,
                journal_v2::CommandType::Call => Self::Call,
                journal_v2::CommandType::OneWayCall => Self::OneWayCall,
                journal_v2::CommandType::SendSignal => Self::SendSignal,
                journal_v2::CommandType::Run => Self::Run,
                journal_v2::CommandType::AttachInvocation => Self::AttachInvocation,
                journal_v2::CommandType::GetInvocationOutput => Self::GetInvocationOutput,
                journal_v2::CommandType::CompleteAwakeable => Self::CompleteAwakeable,
            }
        }
    }

    impl TryFrom<transient_error_event::CommandType> for journal_v2::CommandType {
        type Error = anyhow::Error;

        fn try_from(value: transient_error_event::CommandType) -> Result<Self, Self::Error> {
            match value {
                transient_error_event::CommandType::Unknown => {
                    Err(anyhow::anyhow!("unknown command type"))
                }
                transient_error_event::CommandType::Input => Ok(Self::Input),
                transient_error_event::CommandType::Output => Ok(Self::Output),
                transient_error_event::CommandType::GetLazyState => Ok(Self::GetLazyState),
                transient_error_event::CommandType::SetState => Ok(Self::SetState),
                transient_error_event::CommandType::ClearState => Ok(Self::ClearState),
                transient_error_event::CommandType::ClearAllState => Ok(Self::ClearAllState),
                transient_error_event::CommandType::GetLazyStateKeys => Ok(Self::GetLazyStateKeys),
                transient_error_event::CommandType::GetEagerState => Ok(Self::GetEagerState),
                transient_error_event::CommandType::GetEagerStateKeys => {
                    Ok(Self::GetEagerStateKeys)
                }
                transient_error_event::CommandType::GetPromise => Ok(Self::GetPromise),
                transient_error_event::CommandType::PeekPromise => Ok(Self::PeekPromise),
                transient_error_event::CommandType::CompletePromise => Ok(Self::CompletePromise),
                transient_error_event::CommandType::Sleep => Ok(Self::Sleep),
                transient_error_event::CommandType::Call => Ok(Self::Call),
                transient_error_event::CommandType::OneWayCall => Ok(Self::OneWayCall),
                transient_error_event::CommandType::SendSignal => Ok(Self::SendSignal),
                transient_error_event::CommandType::Run => Ok(Self::Run),
                transient_error_event::CommandType::AttachInvocation => Ok(Self::AttachInvocation),
                transient_error_event::CommandType::GetInvocationOutput => {
                    Ok(Self::GetInvocationOutput)
                }
                transient_error_event::CommandType::CompleteAwakeable => {
                    Ok(Self::CompleteAwakeable)
                }
            }
        }
    }
}
