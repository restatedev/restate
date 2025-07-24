// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};
use std::task::Poll;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};

use restate_types::logs::{KeyFilter, LogletOffset, Record, SequenceNumber, TailState};

use crate::LogEntry;
use crate::Result;
use crate::loglet::{
    FindTailOptions, Loglet, LogletCommit, LogletReadStream, OperationError,
    SendableLogletReadStream,
};

static SEALED_LOGLET: LazyLock<Arc<SealedLoglet>> = LazyLock::new(|| Arc::new(SealedLoglet));

#[derive(derive_more::Debug)]
pub struct SealedLoglet;

impl SealedLoglet {
    pub fn get() -> Arc<dyn Loglet> {
        SEALED_LOGLET.clone() as Arc<dyn Loglet>
    }
}

struct SealedLogletReadStream;

impl LogletReadStream for SealedLogletReadStream {
    /// Current read pointer. This points to the next offset to be read.
    fn read_pointer(&self) -> LogletOffset {
        LogletOffset::OLDEST
    }
    /// Returns true if the stream is terminated.
    fn is_terminated(&self) -> bool {
        false
    }
}

impl Stream for SealedLogletReadStream {
    type Item = Result<LogEntry<LogletOffset>, OperationError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

#[async_trait]
impl Loglet for SealedLoglet {
    fn debug_str(&self) -> Cow<'static, str> {
        Cow::from("sealed")
    }

    async fn create_read_stream(
        self: Arc<Self>,
        _filter: KeyFilter,
        _from: LogletOffset,
        _to: Option<LogletOffset>,
    ) -> Result<SendableLogletReadStream, OperationError> {
        Ok(Box::pin(SealedLogletReadStream))
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<LogletOffset>> {
        Box::pin(
            stream::once(async { TailState::Open(LogletOffset::OLDEST) })
                // The stream must continue to be pending. If the stream is terminated
                // bifrost will consider the system to be shutting down.
                .chain(stream::pending()),
        )
    }

    async fn enqueue_batch(
        &self,
        _payloads: Arc<[Record]>,
    ) -> Result<LogletCommit, OperationError> {
        Ok(LogletCommit::reconfiguration_needed(
            "a sealed loglet cannot be appended to",
        ))
    }

    async fn find_tail(
        &self,
        _: FindTailOptions,
    ) -> Result<TailState<LogletOffset>, OperationError> {
        // Important: Returns `Sealed` because we want to allow seal() and reconfiguration
        // to complete without special handling.
        //
        // On the other hand, the read stream will observe that the tail is Open because we report
        // that in the `watch_tail()` stream. The reader will hop off this stream once the log
        // chain is updated and a new segment replaces this one (if any).
        //
        // The read stream is not expected to seal the chain until it observes a "partial seal". A
        // partial seal happens when the loglet is reporting sealed but the chain is still open.
        // For that reason, this (sealed) loglet always returns TailState::Open to read streams.
        //
        // The benefit is that the read stream will move to AwaitingReconfiguration only when real
        // loglets start reporting that they are sealing. That state can then have a timeout
        // to trigger a seal() operation if the chain was not sealed in a timely manner.
        Ok(TailState::Sealed(LogletOffset::OLDEST))
    }

    /// Find the head (oldest) record in the loglet.
    async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError> {
        Ok(None)
    }

    async fn trim(&self, _new_trim_point: LogletOffset) -> Result<(), OperationError> {
        Ok(())
    }

    async fn seal(&self) -> Result<(), OperationError> {
        Ok(())
    }
}
