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
    use restate_types::identifiers::{InvocationId, ServiceId};
    use restate_types::invocation::ServiceInvocationSpanContext;
    use restate_types::journal::raw::PlainRawEntry;
    use std::convert::Infallible;
    use std::iter::empty;

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
                JournalMetadata::new(0, ServiceInvocationSpanContext::empty(), None),
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
}
