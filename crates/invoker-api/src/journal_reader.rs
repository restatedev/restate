// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;
use futures::Stream;
use restate_types::identifiers::{DeploymentId, FullInvocationId};
use restate_types::invocation::ServiceInvocationSpanContext;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::EntryIndex;
use std::future::Future;

/// Metadata associated with a journal
#[derive(Debug, Clone, PartialEq)]
pub struct JournalMetadata {
    pub length: EntryIndex,
    pub span_context: ServiceInvocationSpanContext,
    pub method: ByteString,
    pub deployment_id: Option<DeploymentId>,
}

impl JournalMetadata {
    pub fn new(
        length: EntryIndex,
        span_context: ServiceInvocationSpanContext,
        method: ByteString,
        deployment_id: Option<DeploymentId>,
    ) -> Self {
        Self {
            deployment_id,
            method,
            span_context,
            length,
        }
    }
}

pub trait JournalReader {
    type JournalStream: Stream<Item = PlainRawEntry>;
    type Error: std::error::Error + Send + Sync + 'static;

    fn read_journal<'a>(
        &'a mut self,
        fid: &'a FullInvocationId,
    ) -> impl Future<Output = Result<(JournalMetadata, Self::JournalStream), Self::Error>> + Send;
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

        async fn read_journal<'a>(
            &'a mut self,
            _sid: &'a FullInvocationId,
        ) -> Result<(JournalMetadata, Self::JournalStream), Self::Error> {
            Ok((
                JournalMetadata::new(
                    0,
                    ServiceInvocationSpanContext::empty(),
                    "test".into(),
                    None,
                ),
                futures::stream::empty(),
            ))
        }
    }
}
