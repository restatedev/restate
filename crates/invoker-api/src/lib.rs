// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    use restate_types::identifiers::{
        EntryIndex, InvocationId, PartitionKey, PartitionLeaderEpoch, ServiceId,
    };
    use restate_types::invocation::{
        InvocationEpoch, InvocationTarget, ServiceInvocationSpanContext,
    };
    use restate_types::journal::Completion;
    use restate_types::journal_v2::raw::RawNotification;
    use restate_types::time::MillisSinceEpoch;
    use std::convert::Infallible;
    use std::iter::empty;
    use std::marker::PhantomData;
    use std::ops::RangeInclusive;
    use tokio::sync::mpsc::Sender;

    #[derive(Debug, Clone)]
    pub struct EmptyStorageReader;

    impl InvocationReader for EmptyStorageReader {
        type Transaction<'a> = EmptyStorageReaderTransaction;

        fn transaction(&mut self) -> Self::Transaction<'_> {
            EmptyStorageReaderTransaction
        }
    }

    pub struct EmptyStorageReaderTransaction;

    impl InvocationReaderTransaction for EmptyStorageReaderTransaction {
        type JournalStream = futures::stream::Empty<JournalEntry>;
        type StateIter = std::iter::Empty<(Bytes, Bytes)>;
        type Error = Infallible;

        async fn read_journal(
            &mut self,
            _fid: &InvocationId,
        ) -> Result<Option<(JournalMetadata, Self::JournalStream)>, Self::Error> {
            Ok(Some((
                JournalMetadata::new(
                    0,
                    ServiceInvocationSpanContext::empty(),
                    None,
                    0,
                    MillisSinceEpoch::UNIX_EPOCH,
                    0,
                ),
                futures::stream::empty(),
            )))
        }

        async fn read_state(
            &mut self,
            _service_id: &ServiceId,
        ) -> Result<EagerState<Self::StateIter>, Self::Error> {
            Ok(EagerState::new_complete(empty()))
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
            _invocation_epoch: InvocationEpoch,
            _invocation_target: InvocationTarget,
            _journal: InvokeInputJournal,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn notify_completion(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _completion: Completion,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn notify_notification(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _invocation_epoch: InvocationEpoch,
            _notification: RawNotification,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn notify_stored_command_ack(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _invocation_epoch: InvocationEpoch,
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
            _invocation_epoch: InvocationEpoch,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        fn retry_invocation_now(
            &mut self,
            _partition_leader_epoch: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _invocation_epoch: InvocationEpoch,
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
