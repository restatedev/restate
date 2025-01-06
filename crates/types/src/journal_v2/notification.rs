use crate::errors::{InvocationError, InvocationErrorCode};
use crate::journal_v2::raw::{TryFromEntry, TryFromEntryError};
use crate::journal_v2::{Entry, EntryMetadata, EntryType, NotificationIndex, NotificationName};
use bytes::Bytes;
use bytestring::ByteString;
use std::fmt;

#[repr(i64)]
pub enum BuiltInSignals {
    Cancel = -1,
}

/// See [`Notification`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum NotificationId {
    Index(NotificationIndex),
    Name(NotificationName),
}

impl NotificationId {
    pub fn for_index(id: NotificationIndex) -> Self {
        Self::Index(id)
    }

    pub fn for_name(id: NotificationName) -> Self {
        Self::Name(id)
    }
}

impl fmt::Display for NotificationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationId::Index(idx) => write!(f, "{idx}"),
            NotificationId::Name(name) => write!(f, "{name}"),
        }
    }
}

/// Notifications are split in two categories:
///
/// * Command completions. These always have a corresponding Command in the journal **before** this notification entry. The identifier is a positive `NotificationIndex`.
/// * A signal result. Signals are split in 3 categories:
///     * Built-in signals. The identifier is a negative `NotificationIndex` from -1 to -15 (included).
///     * Unnamed signals: The identifier is a negative `NotificationIndex` from -16 below.
///     * Named signals: The identifier is a `NotificationName`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub id: NotificationId,
    pub result: NotificationResult,
}

impl Notification {
    pub fn new(id: NotificationId, result: NotificationResult) -> Self {
        Self { id, result }
    }
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
    pub code: InvocationErrorCode,
    pub message: ByteString,
}

impl From<InvocationError> for Failure {
    fn from(value: InvocationError) -> Self {
        Failure {
            code: value.code(),
            message: value.message().into(),
        }
    }
}
