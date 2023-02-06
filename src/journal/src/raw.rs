use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntryHeader {
    pub ty: EntryType,

    // --- Flags
    /// This flag represents whether the entry is completed or not.
    /// This is always [`Some`] if the entry type is a [`CompletableEntry`],
    /// and always [`None`] if the entry type is not a [`CompletableEntry`].
    pub completed_flag: Option<bool>,
}

/// This struct represents a serialized journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntry {
    pub header: RawEntryHeader,
    pub entry: Bytes,
}

impl RawEntry {
    pub fn new(header: RawEntryHeader, entry: Bytes) -> Self {
        Self { header, entry }
    }

    pub fn entry_type(&self) -> EntryType {
        self.header.ty
    }

    pub fn into_inner(self) -> (RawEntryHeader, Bytes) {
        (self.header, self.entry)
    }
}

pub trait RawEntryCodec {
    type Error;

    fn deserialize(entry: &RawEntry) -> Result<Entry, Self::Error>;

    fn write_completion(
        entry: &mut RawEntry,
        completion_result: CompletionResult,
    ) -> Result<(), Self::Error>;
}
