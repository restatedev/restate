use crate::{GetFuture, GetStream, PutFuture};
use restate_types::identifiers::{EntryIndex, ServiceId};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::CompletionResult;

/// Different types of journal entries persisted by the runtime
#[derive(Debug)]
pub enum JournalEntry {
    Entry(EnrichedRawEntry),
    Completion(CompletionResult),
}

pub trait JournalTable {
    fn put_journal_entry(
        &mut self,
        service_id: &ServiceId,
        journal_index: u32,
        journal_entry: JournalEntry,
    ) -> PutFuture;

    fn get_journal_entry(
        &mut self,
        service_id: &ServiceId,
        journal_index: u32,
    ) -> GetFuture<Option<JournalEntry>>;

    fn get_journal(
        &mut self,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> GetStream<JournalEntry>;

    fn delete_journal(&mut self, service_id: &ServiceId, journal_length: EntryIndex) -> PutFuture;
}
