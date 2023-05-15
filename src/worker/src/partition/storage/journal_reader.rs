use futures::future::BoxFuture;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use restate_common::types::{
    EnrichedRawEntry, InvocationStatus, JournalEntry, JournalMetadata, ServiceInvocationId,
};
use restate_journal::raw::PlainRawEntry;
use restate_storage_api::journal_table::JournalTable;
use restate_storage_api::status_table::StatusTable;
use restate_storage_api::Transaction;
use std::vec::IntoIter;

#[derive(Debug, thiserror::Error)]
pub enum JournalReaderError {
    #[error("Not invoked")]
    NotInvoked,
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

#[derive(Debug, Clone)]
pub(crate) struct JournalReader<Storage>(Storage);

impl<Storage> JournalReader<Storage> {
    pub(crate) fn new(storage: Storage) -> Self {
        JournalReader(storage)
    }
}

impl<Storage> restate_invoker::JournalReader for JournalReader<Storage>
where
    Storage: restate_storage_api::Storage,
{
    type JournalStream = stream::Iter<IntoIter<PlainRawEntry>>;
    type Error = JournalReaderError;
    type Future<'a> = BoxFuture<'a, Result<(JournalMetadata, Self::JournalStream), Self::Error>> where Self: 'a;

    fn read_journal<'a>(&'a self, sid: &'a ServiceInvocationId) -> Self::Future<'_> {
        let mut transaction = self.0.transaction();

        async move {
            let invocation_status = transaction.get_invocation_status(&sid.service_id).await?;

            if let Some(InvocationStatus::Invoked(invoked_status)) = invocation_status {
                let journal_metadata = invoked_status.journal_metadata;
                let journal_stream = transaction
                    .get_journal(&sid.service_id, journal_metadata.length)
                    .map(|entry| {
                        entry
                            .map_err(JournalReaderError::Storage)
                            .map(|journal_entry| match journal_entry {
                                JournalEntry::Entry(EnrichedRawEntry { header, entry }) => {
                                    PlainRawEntry::new(header.into(), entry)
                                }
                                JournalEntry::Completion(_) => {
                                    panic!("should only read entries when reading the journal")
                                }
                            })
                    })
                    // TODO: Update invoker to maintain transaction while reading the journal stream: See https://github.com/restatedev/restate/issues/275
                    // collecting the stream because we cannot keep the transaction open
                    .try_collect::<Vec<_>>()
                    .await?;

                transaction.commit().await?;

                Ok((journal_metadata, stream::iter(journal_stream)))
            } else {
                Err(JournalReaderError::NotInvoked)
            }
        }
        .boxed()
    }
}
