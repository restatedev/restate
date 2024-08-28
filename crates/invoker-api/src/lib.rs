// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
pub mod journal_reader;
pub mod state_reader;
pub mod status_handle;

pub use effects::*;
pub use entry_enricher::EntryEnricher;
pub use handle::*;
pub use journal_reader::{JournalMetadata, JournalReader};
pub use state_reader::{EagerState, StateReader};
pub use status_handle::{InvocationErrorReport, InvocationStatusReport, StatusHandle};

#[cfg(any(test, feature = "test-util"))]
pub mod test_util {
    use super::*;
    use bytes::Bytes;
    use restate_errors::NotRunningError;
    use restate_types::identifiers::{
        EntryIndex, InvocationId, PartitionKey, PartitionLeaderEpoch, ServiceId,
    };
    use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
    use restate_types::journal::raw::PlainRawEntry;
    use restate_types::journal::Completion;
    use restate_types::time::MillisSinceEpoch;
    use std::convert::Infallible;
    use std::iter::empty;
    use std::marker::PhantomData;
    use std::ops::RangeInclusive;
    use tokio::sync::mpsc::Sender;

    #[derive(Debug, Clone)]
    pub struct EmptyStorageReader;

    impl JournalReader for EmptyStorageReader {
        type JournalStream = futures::stream::Empty<PlainRawEntry>;
        type Error = Infallible;

        async fn read_journal<'a>(
            &'a mut self,
            _sid: &'a InvocationId,
        ) -> Result<(JournalMetadata, Self::JournalStream), Self::Error> {
            Ok((
                JournalMetadata::new(
                    0,
                    ServiceInvocationSpanContext::empty(),
                    None,
                    MillisSinceEpoch::UNIX_EPOCH,
                ),
                futures::stream::empty(),
            ))
        }
    }

    impl StateReader for EmptyStorageReader {
        type StateIter = std::iter::Empty<(Bytes, Bytes)>;
        type Error = Infallible;

        async fn read_state<'a>(
            &'a mut self,
            _service_id: &'a ServiceId,
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
        async fn invoke(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _invocation_target: InvocationTarget,
            _journal: InvokeInputJournal,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        async fn notify_completion(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _completion: Completion,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        async fn notify_stored_entry_ack(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _entry_index: EntryIndex,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        async fn abort_all_partition(
            &mut self,
            _partition: PartitionLeaderEpoch,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        async fn abort_invocation(
            &mut self,
            _partition_leader_epoch: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }

        async fn register_partition(
            &mut self,
            _partition: PartitionLeaderEpoch,
            _partition_key_range: RangeInclusive<PartitionKey>,
            _storage_reader: SR,
            _sender: Sender<Effect>,
        ) -> Result<(), NotRunningError> {
            Ok(())
        }
    }
}
