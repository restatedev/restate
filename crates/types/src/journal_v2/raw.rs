use crate::identifiers::InvocationId;
use crate::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use crate::journal_v2::encoding::EncodingError;
use crate::journal_v2::{
    CommandType, Decoder, Entry, EntryMetadata, EntryType, Event, NotificationId,
};
use crate::time::MillisSinceEpoch;
use bytes::Bytes;

#[derive(Debug, thiserror::Error)]
#[error("Unexpected mapping, expecting entry {expected:?} but was {actual:?}. This might be a symptom of data corruption.")]
pub struct TryFromEntryError {
    pub(crate) expected: EntryType,
    pub(crate) actual: EntryType,
}

/// Specialized trait for TryFrom<Entry>, used to narrow the command type to the one we're interested into. It's very much like downcast ref in a way.
pub trait TryFromEntry: Sized {
    fn try_from(entry: Entry) -> Result<Self, TryFromEntryError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RawEntryError {
    #[error(transparent)]
    Decoder(#[from] EncodingError),
    #[error(transparent)]
    TryFromEntry(#[from] TryFromEntryError),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawEntryHeader {
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
    pub fn deserialize_to<D: Decoder, T: TryFromEntry>(&self) -> Result<T, RawEntryError> {
        Ok(<T as TryFromEntry>::try_from(D::decode_entry(self)?)?)
    }
}

impl EntryMetadata for RawEntry {
    fn ty(&self) -> EntryType {
        self.inner.ty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[enum_delegate::implement(EntryMetadata)]
#[derive(strum::EnumTryAs)]
pub enum RawEntryInner {
    Command(RawCommand),
    Notification(RawNotification),
    Event(Event),
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
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RawCommandSpecificMetadata {
    CallOrSend(CallOrSendMetadata),
    None,
}

// -- Raw notification

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawNotification {
    id: NotificationId,
    serialized_content: Bytes,
}

impl RawNotification {
    pub fn new(id: NotificationId, serialized_content: Bytes) -> Self {
        Self {
            id,
            serialized_content,
        }
    }

    pub fn id(&self) -> NotificationId {
        self.id.clone()
    }

    pub fn serialized_content(&self) -> Bytes {
        self.serialized_content.clone()
    }
}

impl EntryMetadata for RawNotification {
    fn ty(&self) -> EntryType {
        EntryType::Notification
    }
}
