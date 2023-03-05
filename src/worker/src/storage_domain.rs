// types from the state machine

use common::types::{EntryIndex, InvocationId, ServiceInvocation};
use std::collections::HashSet;

pub type InboxEntry = (u64, ServiceInvocation);

pub struct JournalStatus {
    pub length: EntryIndex,
}

// types from partition.rs

#[derive(Debug, PartialEq)]
pub(crate) enum InvocationStatus {
    Invoked(InvocationId),
    Suspended {
        invocation_id: InvocationId,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    Free,
}
