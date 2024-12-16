use crate::errors::{InvocationError, InvocationErrorCode};
use crate::journal_v2::raw::{TryFromEntry, TryFromEntryError};
use crate::journal_v2::{Entry, EntryMetadata, EntryType, NotificationIndex, NotificationName};
use bytes::Bytes;
use bytestring::ByteString;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum NotificationId {
    Signal(NotificationIndex),
    NamedSignal(NotificationName),
}

impl NotificationId {
    pub fn for_index(id: NotificationIndex) -> Self {
        Self::Signal(id)
    }

    pub fn for_name(id: NotificationName) -> Self {
        Self::NamedSignal(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub id: NotificationId,
    result: NotificationResult,
}

impl EntryMetadata for Notification {
    fn ty(&self) -> EntryType {
        EntryType::Notification
    }
}

impl TryFromEntry for Notification {
    fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
        match entry {
            Entry::Notification(e) => Ok(e),
            e => Err(TryFromEntryError {
                expected: EntryType::Notification,
                actual: e.ty(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationResult {
    Void,
    Success(Bytes),
    Failure(Failure),

    // Special results for certain commands
    InvocationId(ByteString),
    StateKeys(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    code: InvocationErrorCode,
    message: ByteString,
}

impl From<InvocationError> for Failure {
    fn from(value: InvocationError) -> Self {
        Failure {
            code: value.code(),
            message: value.message().into(),
        }
    }
}
