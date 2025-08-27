// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains the data model of the Journal.
//!
//! Within the runtime, Journal entries are represented in different ways in different stages/components:
//!
//! * In the invoker, and in the interaction with the SDK, entries are represented in the service-protocol specific format. See crate `restate_service_protocol` for more details.
//! * When the invoker interacts with the FSM, entries are represented using [`RawEntry`].
//! * When the FSM processes entries, it interacts with [`Entry`].
//! * When observing the journal (e.g. in Datafusion), typically [`Entry`] is used as well.
//!
//! The API to access the entry content is optimized for the 3 common access patterns, used in invoker, FSM and Datafusion:
//!
//! 1. [`RawEntry`] -> Deserialized specific command type. This is used primarily within the FSM to act on commands. This conversion requires a [`Decoder`] trait implementation, provided by the `restate_service_protocol` crate.
//! 2. [`RawEntry`] -> [`Entry`]. This is used in Datafusion and other observability APIs. As above, this conversion requires a [`Decoder`].
//! 3. [`RawEntry`] only. This is used in the Invoker when preparing service protocol messages.

use std::fmt;

use bytestring::ByteString;
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};

pub mod command;
pub mod encoding;
pub mod lite;
pub mod notification;
pub mod raw;
mod types;

pub use command::*;
pub use encoding::*;
pub use notification::*;
pub use types::*;

// -- Various alias types for Ids

pub type EntryIndex = u32;
pub type CommandIndex = u32;
pub type CompletionId = u32;
pub type SignalIndex = u32;
pub type SignalName = ByteString;

// -- Entry metadata

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, strum::EnumIs, strum::EnumTryAs, derive_more::From,
)]
pub enum EntryType {
    Command(CommandType),
    Notification(NotificationType),
}

impl fmt::Display for EntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntryType::Command(cmd) => {
                write!(f, "Command: ")?;
                fmt::Display::fmt(cmd, f)
            }
            EntryType::Notification(notif) => {
                write!(f, "Notification: ")?;
                fmt::Display::fmt(notif, f)
            }
        }
    }
}

impl From<EntryType> for &'static str {
    fn from(val: EntryType) -> Self {
        match val {
            EntryType::Command(cmd_type) => cmd_type.into(),
            EntryType::Notification(notif_type) => match notif_type {
                NotificationType::Completion(completion_type) => completion_type.into(),
                NotificationType::Signal => "Signal",
            },
            EntryType::Event => "Event",
        }
    }
}

use crate::journal_v2::lite::*;
use crate::journal_v2::raw::*;

#[enum_dispatch]
pub trait EntryMetadata {
    fn ty(&self) -> EntryType;
}

/// Root enum representing a decoded entry.
#[enum_dispatch(EntryMetadata)]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    /* The serialize trait is used in Datafusion to propagate entries to the UI and CLI */
    Serialize,
    /* The deserialize trait is used only by CLI */ Deserialize,
)]
// todo: fix this and box the large variant (Command is 416 bytes)
#[allow(clippy::large_enum_variant)]
pub enum Entry {
    Command(Command),
    Notification(Notification),
}

impl Entry {
    pub fn encode<E: Encoder>(self) -> RawEntry {
        E::encode_entry(self)
    }
}
