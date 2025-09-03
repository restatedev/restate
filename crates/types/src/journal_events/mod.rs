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
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use strum::EnumString;

pub mod raw;

#[derive(Debug, Copy, Clone, PartialEq, Eq, EnumString, strum::Display, Serialize, Deserialize)]
pub enum EventType {
    TransientError,
    Paused,
    Unknown,
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

impl TransientErrorEvent {
    pub fn deduplication_hash(&self) -> Bytes {
        const HASH_SEPARATOR: u8 = 0x2c;

        // WARNING: Changing this code will result in having duplicated events between restate versions.
        let mut hasher = Sha256::new();
        hasher.update(b"tee");
        hasher.update([HASH_SEPARATOR]);
        hasher.update(u16::from(self.error_code).to_le_bytes());
        hasher.update([HASH_SEPARATOR]);
        hasher.update(&self.error_message);
        hasher.update([HASH_SEPARATOR]);
        if let Some(error_code) = &self.restate_doc_error_code {
            hasher.update(error_code);
        } else {
            hasher.update(b"-");
        }
        hasher.update([HASH_SEPARATOR]);
        if let Some(command_index) = &self.related_command_index {
            hasher.update(command_index.to_le_bytes());
        } else {
            hasher.update(b"-");
        }
        hasher.update([HASH_SEPARATOR]);
        if let Some(command_name) = &self.related_command_name {
            hasher.update(command_name);
        } else {
            hasher.update(b"-");
        }
        hasher.update([HASH_SEPARATOR]);
        if let Some(command_type) = &self.related_command_type {
            let str: &'static str = command_type.into();
            hasher.update(str);
        } else {
            hasher.update(b"-");
        }
        let result = hasher.finalize();

        result.to_vec().into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PausedEvent {
    /// This is potentially empty if the service was manually paused
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<TransientErrorEvent>,
}
