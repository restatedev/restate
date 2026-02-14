// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use bytes::Bytes;
use enum_dispatch::enum_dispatch;

use restate_serde_util::ByteCount;

use crate::identifiers::InvocationId;
use crate::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use crate::journal_v2::encoding::DecodingError;
use crate::journal_v2::{
    CommandType, Decoder, Entry, EntryMetadata, EntryType, NotificationId, NotificationType,
};

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

/// The serialized journal entries with some additional metadata to discriminate them.
#[enum_dispatch(EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, strum::EnumTryAs)]
pub enum RawEntry {
    Command(RawCommand),
    Notification(RawNotification),
}

impl RawEntry {
    pub fn decode<D: Decoder, T: TryFromEntry>(&self) -> Result<T, RawEntryError> {
        Ok(<T as TryFromEntry>::try_from(D::decode_entry(self)?)?)
    }
}

// -- Raw command

#[derive(derive_more::Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawCommand {
    ty: CommandType,
    pub command_specific_metadata: RawCommandSpecificMetadata,
    #[debug("Bytes({})", ByteCount::from(serialized_content.len()))]
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

    pub fn into_serialized_content(self) -> Bytes {
        self.serialized_content
    }

    pub fn decode<D: Decoder, T: TryFromEntry>(&self) -> Result<T, RawEntryError> {
        Ok(<T as TryFromEntry>::try_from(D::decode_entry(
            &RawEntry::Command(self.clone()),
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
    // Since v1.4.0. Messages older than v1.4.0 will have zero retention as that
    // matches the default behaviour of <= 1.3.x.
    #[serde(default)]
    pub journal_retention_duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RawCommandSpecificMetadata {
    CallOrSend(Box<CallOrSendMetadata>),
    None,
}

// -- Raw notification

#[derive(derive_more::Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawNotification {
    ty: NotificationType,
    id: NotificationId,
    #[debug("Bytes({})", ByteCount::from(serialized_content.len()))]
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

    pub fn into_serialized_content(self) -> Bytes {
        self.serialized_content
    }

    pub fn decode<D: Decoder, T: TryFromEntry>(&self) -> Result<T, RawEntryError> {
        Ok(<T as TryFromEntry>::try_from(D::decode_entry(
            &RawEntry::Notification(self.clone()),
        )?)?)
    }
}

impl EntryMetadata for RawNotification {
    fn ty(&self) -> EntryType {
        EntryType::Notification(self.ty)
    }
}
