// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains the data model of journal events.

use crate::errors::InvocationErrorCode;
use crate::journal_v2::CommandType;
use serde::{Deserialize, Serialize};
use strum::EnumString;

pub mod raw;

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    EnumString,
    strum::Display,
    Serialize,
    Deserialize,
    strum::FromRepr,
)]
#[repr(u8)]
pub enum EventType {
    Unknown = 0,
    TransientError = 1,
    Paused = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, derive_more::From)]
#[serde(tag = "ty")]
pub enum Event {
    TransientError(TransientErrorEvent),
    Paused(PausedEvent),
    /// This is used when it's not possible to parse in this Restate version the event.
    Unknown,
}

// --- The individual events

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransientErrorEvent {
    pub error_code: InvocationErrorCode,
    pub error_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_stacktrace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restate_doc_error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_index: Option<crate::journal_v2::CommandIndex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_type: Option<CommandType>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PausedEvent {
    /// This is potentially empty if the service was manually paused
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<TransientErrorEvent>,
}
