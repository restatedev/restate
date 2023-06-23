use restate_common::errors::InvocationError;
use restate_common::types::{EnrichedRawEntry, EntryIndex, ServiceInvocationId};
use std::collections::HashSet;

#[derive(Debug)]
pub struct Effect {
    pub service_invocation_id: ServiceInvocationId,
    pub kind: EffectKind,
}

#[derive(Debug)]
pub enum EffectKind {
    JournalEntry {
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
    },
    Suspended {
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed(InvocationError),
}
