use crate::{GetFuture, GetStream, PutFuture};
use restate_common::types::{MessageIndex, ServiceId, ServiceInvocation};

/// Entry of the inbox
#[derive(Debug, Clone, PartialEq)]
pub struct InboxEntry {
    pub inbox_sequence_number: MessageIndex,
    pub service_invocation: ServiceInvocation,
}

impl InboxEntry {
    pub fn new(inbox_sequence_number: MessageIndex, service_invocation: ServiceInvocation) -> Self {
        Self {
            inbox_sequence_number,
            service_invocation,
        }
    }
}

pub trait InboxTable {
    fn put_invocation(&mut self, service_id: &ServiceId, inbox_entry: InboxEntry) -> PutFuture;

    fn delete_invocation(&mut self, service_id: &ServiceId, sequence_number: u64) -> PutFuture;

    fn peek_inbox(&mut self, service_id: &ServiceId) -> GetFuture<Option<InboxEntry>>;

    fn inbox(&mut self, service_id: &ServiceId) -> GetStream<InboxEntry>;
}
