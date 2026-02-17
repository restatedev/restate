// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod capacity;
mod effects;
pub mod entry_enricher;
mod handle;
pub mod invocation_reader;
pub mod status_handle;

pub use effects::*;
pub use entry_enricher::EntryEnricher;
pub use handle::*;
pub use invocation_reader::JournalMetadata;
pub use status_handle::{InvocationErrorReport, InvocationStatusReport, StatusHandle};

#[cfg(any(test, feature = "test-util"))]
pub mod test_util {
    use super::*;
    use crate::invocation_reader::{
        EagerState, InvocationReader, InvocationReaderTransaction, JournalEntry,
    };
    use bytes::Bytes;
    use restate_errors::NotRunningError;
    use restate_futures_util::concurrency::Permit;
    use restate_types::identifiers::{
        EntryIndex, InvocationId, PartitionKey, PartitionLeaderEpoch, ServiceId,
    };
    use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};

    use restate_types::time::MillisSinceEpoch;
    use restate_types::vqueue::VQueueId;
    use std::convert::Infallible;
    use std::marker::PhantomData;
    use std::ops::RangeInclusive;
    use tokio::sync::mpsc::Sender;

    #[derive(Debug, Clone)]
    pub struct EmptyStorageReader;

    impl InvocationReader for EmptyStorageReader {
        type Transaction<'a> = EmptyStorageReaderTransaction;
        type Error = Infallible;

        fn transaction(&mut self) -> Self::Transaction<'_> {
            EmptyStorageReaderTransaction
        }

        async fn read_journal_entry(
            &mut self,
            _invocation_id: &InvocationId,
            _entry_index: EntryIndex,
            _using_journal_table_v2: bool,
        ) -> Result<Option<JournalEntry>, Infallible> {
            Ok(None)
        }
    }

    pub struct EmptyStorageReaderTransaction;

    impl InvocationReaderTransaction for EmptyStorageReaderTransaction {
        type JournalStream<'a> = futures::stream::Empty<Result<JournalEntry, Self::Error>>;
        type StateStream<'a> = futures::stream::Empty<Result<(Bytes, Bytes), Self::Error>>;
        type Error = Infallible;

        async fn read_journal_metadata(
            &mut self,
            _invocation_id: &InvocationId,
        ) -> Result<Option<JournalMetadata>, Self::Error> {
            Ok(Some(JournalMetadata::new(
                0,
                ServiceInvocationSpanContext::empty(),
                None,
                MillisSinceEpoch::UNIX_EPOCH,
                0,
                true,
            )))
        }

        fn read_journal(
            &self,
            _invocation_id: &InvocationId,
            _length: EntryIndex,
            _using_journal_table_v2: bool,
        ) -> Result<Self::JournalStream<'_>, Self::Error> {
            Ok(futures::stream::empty())
        }

        fn read_state(
            &self,
            _service_id: &ServiceId,
        ) -> Result<EagerState<Self::StateStream<'_>>, Self::Error> {
            Ok(EagerState::new_complete(futures::stream::empty()))
        }
    }

    #[derive(Debug)]
    pub struct MockInvokerHandle<SR> {
        phantom_data: PhantomData<SR>,
    }

    impl<SR> Default for MockInvokerHandle<SR> {
        fn default() -> Self {
            Self {
                phantom_data: PhantomData,
            }
        }
    }

    impl<SR: Send> InvokerHandle<SR> for MockInvokerHandle<SR> {
        fn invoke(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _invocation_target: InvocationTarget,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn vqueue_invoke(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _qid: VQueueId,
            _permit: Permit,
            _invocation_id: InvocationId,
            _invocation_target: InvocationTarget,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn notify_completion(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _entry_index: EntryIndex,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn notify_notification(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _entry_index: EntryIndex,
            _notification_id: restate_types::journal_v2::NotificationId,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn notify_stored_command_ack(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _entry_index: EntryIndex,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn abort_all_partition(
            &mut self,
            _partition: PartitionLeaderEpoch,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn abort_invocation(
            &mut self,
            _partition_leader_epoch: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn retry_invocation_now(
            &mut self,
            _partition_leader_epoch: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn pause_invocation(
            &mut self,
            _partition_leader_epoch: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn register_partition(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _partition_key_range: RangeInclusive<PartitionKey>,
            _storage_reader: SR,
            _sender: Sender<Box<Effect>>,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }
    }
}
