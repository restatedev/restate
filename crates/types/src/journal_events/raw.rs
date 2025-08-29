// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::GenericError;
use crate::journal_events::{Event, EventType};
use anyhow::Context;
use bytes::Bytes;
use prost::Message;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawEvent {
    ty: EventType,
    deduplication_hash: Option<Bytes>,
    value: Bytes,
}

impl RawEvent {
    pub fn new(ty: EventType, value: Bytes) -> Self {
        RawEvent {
            ty,
            deduplication_hash: None,
            value,
        }
    }

    pub fn unknown() -> Self {
        RawEvent {
            ty: EventType::Unknown,
            deduplication_hash: None,
            value: Bytes::default(),
        }
    }

    /// See [self.set_deduplication_hash].
    pub fn deduplication_hash(&self) -> Option<&Bytes> {
        self.deduplication_hash.as_ref()
    }

    /// When setting the deduplication hash, the Partition processor will try to deduplicate this event with the last event in the journal (if present) by matching the deduplication_hash.
    ///
    /// When unset, no deduplication will happen and the event is stored as is.
    pub fn set_deduplication_hash(&mut self, hash: impl Into<Bytes>) {
        self.deduplication_hash = Some(hash.into());
    }

    pub fn ty(&self) -> EventType {
        self.ty
    }

    pub fn into_inner(self) -> (EventType, Option<Bytes>, Bytes) {
        (self.ty, self.deduplication_hash, self.value)
    }

    pub fn into_event_or_unknown(self) -> Event {
        Event::try_from(self).unwrap_or(Event::Unknown)
    }
}

// The conversion RawEvent <-> Event is defined at this level directly.

impl TryFrom<RawEvent> for Event {
    type Error = GenericError;

    fn try_from(value: RawEvent) -> Result<Self, Self::Error> {
        decode(value.ty, value.value)
            .context("error when decoding event")
            .map_err(Into::into)
    }
}

impl From<Event> for RawEvent {
    fn from(value: Event) -> Self {
        encode(value)
    }
}

#[derive(Debug, thiserror::Error)]
enum EventDecodingError {
    #[error("decoding error: {0:?}")]
    Protobuf(#[from] prost::DecodeError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

fn decode(ty: EventType, value: Bytes) -> Result<Event, EventDecodingError> {
    match ty {
        EventType::TransientError => Ok(Event::TransientError(
            pb::TransientErrorEvent::decode(value)?.try_into()?,
        )),
        EventType::Paused => Ok(Event::Paused(pb::PausedEvent::decode(value)?.try_into()?)),
        EventType::Unknown => Ok(Event::Unknown),
    }
}

fn encode(event: Event) -> RawEvent {
    match event {
        Event::TransientError(e) => RawEvent::new(
            EventType::TransientError,
            pb::TransientErrorEvent::from(e).encode_to_vec().into(),
        ),
        Event::Paused(e) => RawEvent::new(
            EventType::Paused,
            pb::PausedEvent::from(e).encode_to_vec().into(),
        ),
        Event::Unknown => RawEvent::unknown(),
    }
}

mod pb {
    use crate::errors::InvocationErrorCode;
    use crate::journal_events as event;
    use crate::journal_v2;
    use anyhow::Context;

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
                            .context("Unrecognized command type")
                            .map(Into::into)
                    })
                    .transpose()?,
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

    impl From<transient_error_event::CommandType> for journal_v2::CommandType {
        fn from(value: transient_error_event::CommandType) -> Self {
            match value {
                transient_error_event::CommandType::Input => Self::Input,
                transient_error_event::CommandType::Output => Self::Output,
                transient_error_event::CommandType::GetLazyState => Self::GetLazyState,
                transient_error_event::CommandType::SetState => Self::SetState,
                transient_error_event::CommandType::ClearState => Self::ClearState,
                transient_error_event::CommandType::ClearAllState => Self::ClearAllState,
                transient_error_event::CommandType::GetLazyStateKeys => Self::GetLazyStateKeys,
                transient_error_event::CommandType::GetEagerState => Self::GetEagerState,
                transient_error_event::CommandType::GetEagerStateKeys => Self::GetEagerStateKeys,
                transient_error_event::CommandType::GetPromise => Self::GetPromise,
                transient_error_event::CommandType::PeekPromise => Self::PeekPromise,
                transient_error_event::CommandType::CompletePromise => Self::CompletePromise,
                transient_error_event::CommandType::Sleep => Self::Sleep,
                transient_error_event::CommandType::Call => Self::Call,
                transient_error_event::CommandType::OneWayCall => Self::OneWayCall,
                transient_error_event::CommandType::SendSignal => Self::SendSignal,
                transient_error_event::CommandType::Run => Self::Run,
                transient_error_event::CommandType::AttachInvocation => Self::AttachInvocation,
                transient_error_event::CommandType::GetInvocationOutput => {
                    Self::GetInvocationOutput
                }
                transient_error_event::CommandType::CompleteAwakeable => Self::CompleteAwakeable,
            }
        }
    }

    impl From<event::PausedEvent> for PausedEvent {
        fn from(event::PausedEvent { last_failure }: event::PausedEvent) -> Self {
            PausedEvent {
                last_failure: last_failure.map(Into::into),
            }
        }
    }

    impl TryFrom<PausedEvent> for event::PausedEvent {
        type Error = anyhow::Error;

        fn try_from(PausedEvent { last_failure }: PausedEvent) -> Result<Self, Self::Error> {
            Ok(event::PausedEvent {
                last_failure: last_failure.map(|f| f.try_into()).transpose()?,
            })
        }
    }
}
