// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::InvocationId;
use crate::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use crate::journal_v2::encoding::DecodingError;
use crate::journal_v2::{
    CommandType, Decoder, Entry, EntryMetadata, EntryType, Event, EventType, NotificationId,
    NotificationType,
};
use crate::time::MillisSinceEpoch;
use bytes::Bytes;
use bytestring::ByteString;
use enum_dispatch::enum_dispatch;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[error(
    "Unexpected mapping, expecting entry {expected:?} but was {actual:?}. This might be a symptom of data corruption."
)]
pub struct TryFromEntryError {
    pub(crate) expected: EntryType,
    pub(crate) actual: EntryType,
}

/// Specialized trait for TryFrom<Entry>, used to narrow the command type to the one we're interested into. It's very much like downcast ref in a way.
pub trait TryFromEntry: Sized {
    fn try_from(entry: Entry) -> Result<Self, TryFromEntryError>;
}

impl TryFromEntry for Entry {
    fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
        Ok(entry)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RawEntryError {
    #[error(transparent)]
    Decoder(#[from] DecodingError),
    #[error(transparent)]
    TryFromEntry(#[from] TryFromEntryError),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawEntryHeader {
    /// **NOTE: This is currently not agreed among leaders and followers!**
    pub append_time: MillisSinceEpoch,
}

impl RawEntryHeader {
    pub fn new() -> Self {
        Self {
            append_time: MillisSinceEpoch::now(),
        }
    }
}

impl Default for RawEntryHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Container of the raw entry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawEntry {
    header: RawEntryHeader,
    pub inner: RawEntryInner,
}

impl RawEntry {
    pub fn new(header: RawEntryHeader, inner: impl Into<RawEntryInner>) -> Self {
        Self {
            header,
            inner: inner.into(),
        }
    }

    pub fn header(&self) -> &RawEntryHeader {
        &self.header
    }
}

impl RawEntry {
    pub fn decode<D: Decoder, T: TryFromEntry>(&self) -> Result<T, RawEntryError> {
        Ok(<T as TryFromEntry>::try_from(D::decode_entry(self)?)?)
    }
}

impl EntryMetadata for RawEntry {
    fn ty(&self) -> EntryType {
        self.inner.ty()
    }
}

#[enum_dispatch(EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, strum::EnumTryAs)]
pub enum RawEntryInner {
    Command(RawCommand),
    Notification(RawNotification),
    Event(RawEvent),
}

// -- Raw command

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawCommand {
    ty: CommandType,
    command_specific_metadata: RawCommandSpecificMetadata,
    serialized_content: Bytes,
}

impl RawCommand {
    pub fn new(ty: CommandType, serialized_content: impl Into<Bytes>) -> Self {
        Self {
            ty,
            command_specific_metadata: RawCommandSpecificMetadata::None,
            serialized_content: serialized_content.into(),
        }
    }

    pub fn with_command_specific_metadata(
        mut self,
        command_specific_metadata: RawCommandSpecificMetadata,
    ) -> Self {
        self.command_specific_metadata = command_specific_metadata;
        self
    }

    pub fn command_type(&self) -> CommandType {
        self.ty
    }

    pub fn command_specific_metadata(&self) -> &RawCommandSpecificMetadata {
        &self.command_specific_metadata
    }

    pub fn serialized_content(&self) -> Bytes {
        self.serialized_content.clone()
    }

    pub fn decode<D: Decoder, T: TryFromEntry>(&self) -> Result<T, RawEntryError> {
        Ok(<T as TryFromEntry>::try_from(D::decode_entry(
            &RawEntry::new(RawEntryHeader::new(), RawEntryInner::Command(self.clone())),
        )?)?)
    }
}

impl EntryMetadata for RawCommand {
    fn ty(&self) -> EntryType {
        EntryType::Command(self.ty)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CallOrSendMetadata {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub span_context: ServiceInvocationSpanContext,
    pub completion_retention_duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RawCommandSpecificMetadata {
    CallOrSend(CallOrSendMetadata),
    None,
}

// -- Raw notification

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawNotification {
    ty: NotificationType,
    id: NotificationId,
    serialized_content: Bytes,
}

impl RawNotification {
    pub fn new(
        ty: impl Into<NotificationType>,
        id: NotificationId,
        serialized_content: impl Into<Bytes>,
    ) -> Self {
        Self {
            ty: ty.into(),
            id,
            serialized_content: serialized_content.into(),
        }
    }

    pub fn ty(&self) -> NotificationType {
        self.ty
    }

    pub fn id(&self) -> NotificationId {
        self.id.clone()
    }

    pub fn serialized_content(&self) -> Bytes {
        self.serialized_content.clone()
    }

    pub fn decode<D: Decoder, T: TryFromEntry>(&self) -> Result<T, RawEntryError> {
        Ok(<T as TryFromEntry>::try_from(D::decode_entry(
            &RawEntry::new(
                RawEntryHeader::new(),
                RawEntryInner::Notification(self.clone()),
            ),
        )?)?)
    }
}

impl EntryMetadata for RawNotification {
    fn ty(&self) -> EntryType {
        EntryType::Notification(self.ty)
    }
}

// -- Raw event

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawEvent {
    ty: String,
    metadata: HashMap<String, ByteString>,
}

impl RawEvent {
    pub fn new<T: IntoIterator<Item = (String, String)>>(
        event_type: EventType,
        metadata: T,
    ) -> Self {
        RawEvent {
            ty: event_type.to_string(),
            metadata: HashMap::from_iter(metadata.into_iter().map(|(k, v)| (k, v.into()))),
        }
    }

    pub fn into_inner(self) -> (String, HashMap<String, ByteString>) {
        (self.ty, self.metadata)
    }
}

impl EntryMetadata for RawEvent {
    fn ty(&self) -> EntryType {
        EntryType::Event
    }
}

// The conversion RawEvent <-> Event is defined at this level directly.

impl From<RawEvent> for Event {
    fn from(value: RawEvent) -> Self {
        let Ok(entry_type) = value.ty.parse::<EventType>() else {
            return Event::Generic {
                ty: value.ty,
                metadata: value.metadata,
            };
        };

        match entry_type {
            EventType::Suspend => {
                let Ok(waiting_for_notification_ids) = serde_json::from_str(
                    value
                        .metadata
                        .get("waiting_for_notification_ids")
                        .map(|s| s.as_ref())
                        .unwrap_or_default(),
                ) else {
                    return Event::Generic {
                        ty: value.ty,
                        metadata: value.metadata,
                    };
                };
                Event::Suspend {
                    waiting_for_notification_ids,
                }
            }
            EventType::Resume => Event::Resume {},
            EventType::TransientError => Event::TransientError {},
            EventType::Generic(_) => Event::Generic {
                ty: value.ty,
                metadata: value.metadata,
            },
        }
    }
}

impl From<Event> for RawEvent {
    fn from(value: Event) -> Self {
        match value {
            Event::Suspend {
                waiting_for_notification_ids,
            } => RawEvent::new(
                EventType::Suspend,
                [(
                    "waiting_for_notification_ids".to_owned(),
                    serde_json::to_string(&waiting_for_notification_ids).unwrap(),
                )],
            ),
            Event::Resume {} => RawEvent::new(EventType::Resume, []),
            Event::TransientError {} => RawEvent::new(EventType::TransientError, []),
            Event::Generic { ty, metadata } => RawEvent { ty, metadata },
        }
    }
}
