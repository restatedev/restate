use crate::journal_v2::raw::{TryFromEntry, TryFromEntryError};
use crate::journal_v2::{Entry, EntryMetadata, EntryType};
use bytestring::ByteString;
use std::collections::HashMap;
use strum::EnumString;

#[derive(
    Debug, Clone, PartialEq, Eq, EnumString, strum::Display, serde::Serialize, serde::Deserialize,
)]
pub enum EventType {
    Lifecycle,
    #[strum(default)]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub ty: EventType,
    pub metadata: HashMap<String, ByteString>,
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
