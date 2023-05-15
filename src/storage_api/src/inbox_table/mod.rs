use crate::{GetFuture, GetStream, PutFuture};
use restate_common::types::{InboxEntry, ServiceId};

pub trait InboxTable {
    fn put_invocation(&mut self, service_id: &ServiceId, inbox_entry: InboxEntry) -> PutFuture;

    fn delete_invocation(&mut self, service_id: &ServiceId, sequence_number: u64) -> PutFuture;

    fn peek_inbox(&mut self, service_id: &ServiceId) -> GetFuture<Option<InboxEntry>>;

    fn inbox(&mut self, service_id: &ServiceId) -> GetStream<InboxEntry>;
}
