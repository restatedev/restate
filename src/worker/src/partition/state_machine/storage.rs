use crate::partition::InvocationStatus;
use common::types::{ServiceId, ServiceInvocation, ServiceInvocationId};
use futures::{stream, Stream};
use journal::JournalRevision;
use storage_api::StorageReader;

pub(super) struct StorageReaderHelper {}

pub(super) struct JournalStatus {
    pub(super) revision: JournalRevision,
    pub(super) length: u32,
}

impl StorageReaderHelper {
    pub(super) fn new<S: StorageReader>(_storage: &S) -> Self {
        StorageReaderHelper {}
    }

    pub(super) fn get_invocation_status(&self, service_id: &ServiceId) -> InvocationStatus {
        InvocationStatus::Free
    }

    pub(super) fn peek_inbox(&self, service_id: &ServiceId) -> Option<(u64, ServiceInvocation)> {
        None
    }

    pub(super) fn get_journal_status(&self, service_id: &ServiceId) -> JournalStatus {
        JournalStatus {
            revision: 0,
            length: 0,
        }
    }

    pub fn scan_invoked_invocations(&self) -> impl Stream<Item = ServiceInvocationId> {
        stream::empty()
    }
}
