// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::Stream;
use restate_types::identifiers::FullInvocationId;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::JournalMetadata;
use std::future::Future;

pub trait JournalReader {
    type JournalStream: Stream<Item = PlainRawEntry>;
    type Error: std::error::Error + Send + Sync + 'static;
    type Future<'a>: Future<Output = Result<(JournalMetadata, Self::JournalStream), Self::Error>>
        + Send
    where
        Self: 'a;

    fn read_journal<'a>(&'a self, fid: &'a FullInvocationId) -> Self::Future<'_>;
}

#[cfg(any(test, feature = "mocks"))]
pub mod mocks {
    use super::*;
    use restate_types::invocation::ServiceInvocationSpanContext;
    use std::convert::Infallible;

    #[derive(Debug, Clone)]
    pub struct EmptyJournalReader;

    impl JournalReader for EmptyJournalReader {
        type JournalStream = futures::stream::Empty<PlainRawEntry>;
        type Error = Infallible;
        type Future<'a> = futures::future::Ready<Result<(JournalMetadata, Self::JournalStream), Self::Error>> where
            Self: 'a;

        fn read_journal<'a>(&'a self, _sid: &'a FullInvocationId) -> Self::Future<'_> {
            futures::future::ready(Ok((
                JournalMetadata::new("test", ServiceInvocationSpanContext::empty(), 0),
                futures::stream::empty(),
            )))
        }
    }
}
