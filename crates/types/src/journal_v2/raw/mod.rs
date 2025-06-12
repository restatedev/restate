// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use enum_dispatch::enum_dispatch;

use crate::errors::GenericError;
use crate::identifiers::InvocationId;
use crate::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use crate::journal_v2::encoding::DecodingError;
use crate::journal_v2::{
    CommandType, Decoder, Entry, EntryMetadata, EntryType, Event, EventType, NotificationId,
    NotificationType,
};
use crate::time::MillisSinceEpoch;

mod events;

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
    pub command_specific_metadata: RawCommandSpecificMetadata,
    pub serialized_content: Bytes,
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
    pub journal_retention_duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RawCommandSpecificMetadata {
    CallOrSend(Box<CallOrSendMetadata>),
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

    pub fn event_type(&self) -> EventType {
        self.ty
    }

    pub fn into_inner(self) -> (EventType, Option<Bytes>, Bytes) {
        (self.ty, self.deduplication_hash, self.value)
    }
}

impl EntryMetadata for RawEvent {
    fn ty(&self) -> EntryType {
        EntryType::Event
    }
}

// The conversion RawEvent <-> Event is defined at this level directly.

impl TryFrom<RawEvent> for Event {
    type Error = GenericError;

    fn try_from(value: RawEvent) -> Result<Self, Self::Error> {
        events::decode(value.ty, value.value)
            .context("error when decoding event")
            .map_err(Into::into)
    }
}

impl From<Event> for RawEvent {
    fn from(value: Event) -> Self {
        events::encode(value)
    }
}
