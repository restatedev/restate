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
                related_command_type: related_command_type.map(|ct| ct.to_string()),
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
                        ct.parse::<journal_v2::CommandType>()
                            .map_err(|e| anyhow::anyhow!("unrecognized command type: {}", e))
                    })
                    .transpose()?,
                count: NonZeroU32::new(count)
                    .ok_or_else(|| anyhow::anyhow!("count out of bounds"))?,
            })
        }
    }
}
