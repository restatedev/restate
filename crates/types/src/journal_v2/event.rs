// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::journal_v2::raw::{TryFromEntry, TryFromEntryError};
use crate::journal_v2::{Entry, EntryMetadata, EntryType, NotificationId};
use bytestring::ByteString;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use strum::EnumString;

#[derive(Debug, Clone, PartialEq, Eq, EnumString, strum::Display)]
pub enum EventType {
    Suspend,
    Resume,
    TransientError,
    #[strum(default)]
    Generic(String),
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    Suspend {
        waiting_for_notification_ids: Vec<NotificationId>,
    },
    Resume {},
    TransientError {},
    Generic {
        ty: String,
        metadata: HashMap<String, ByteString>,
    },
}

impl EntryMetadata for Event {
    fn ty(&self) -> EntryType {
        EntryType::Event
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
