// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::InvocationErrorCode;
use crate::journal_v2::raw::{RawEntry, RawEvent, TryFromEntry, TryFromEntryError};
use crate::journal_v2::{CommandIndex, CommandType, Encoder, Entry, EntryMetadata, EntryType};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use strum::EnumString;

#[derive(Debug, Clone, PartialEq, Eq, EnumString, strum::Display)]
pub enum EventType {
    // TODO(slinkydeveloper) I would love to wire these up,
    //  but ATM this is not possible b/c we miss https://github.com/restatedev/restate/issues/2765
    // Suspend,
    // Resume,
    TransientError,
    #[strum(default)]
    Generic(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransientErrorEvent {
    pub error_code: InvocationErrorCode,
    pub error_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_stacktrace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restate_doc_error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_index: Option<CommandIndex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_type: Option<CommandType>,

    // Count of the same transient error
    pub count: NonZeroU32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    // TODO(slinkydeveloper) I would love to wire these up,
    //  but ATM this is not possible b/c we miss https://github.com/restatedev/restate/issues/2765
    // Suspend {
    //     waiting_for_notification_ids: Vec<NotificationId>,
    // },
    // Resume {},
    TransientError(TransientErrorEvent),
    /// This is used when it's not possible to parse in this Restate version the event.
    Generic(RawEvent),
}

impl EntryMetadata for Event {
    fn ty(&self) -> EntryType {
        EntryType::Event
    }
}

impl Event {
    pub fn encode<E: Encoder>(&self) -> RawEntry {
        E::encode_entry(&Entry::Event(self.clone()))
    }
}

impl TryFromEntry for Event {
    fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
        match entry {
            Entry::Event(e) => Ok(e),
            e => Err(TryFromEntryError {
                expected: EntryType::Event,
                actual: e.ty(),
            }),
        }
    }
}
