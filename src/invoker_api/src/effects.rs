use restate_types::errors::InvocationError;
use restate_types::identifiers::ServiceInvocationId;
use restate_types::identifiers::{EndpointId, EntryIndex};
use restate_types::journal::enriched::EnrichedRawEntry;
use std::collections::HashSet;

#[derive(Debug)]
pub struct Effect {
    pub service_invocation_id: ServiceInvocationId,
    pub kind: EffectKind,
}

#[derive(Debug)]
pub enum EffectKind {
    /// This is sent before any new entry is created by the invoker. This won't be sent if the endpoint_id is already set.
    SelectedEndpoint(EndpointId),
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
