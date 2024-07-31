// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{hash_map, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::ready;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info};

use restate_core::ShutdownError;
use restate_types::logs::metadata::{LogletParams, ProviderKind, SegmentIndex};
use restate_types::logs::{LogId, SequenceNumber};

use crate::loglet::util::TailOffsetWatch;
use crate::loglet::{
    AppendError, Loglet, LogletBase, LogletOffset, LogletProvider, LogletProviderFactory,
    LogletReadStream, OperationError, SendableLogletReadStream,
};
use crate::Result;
use crate::{LogRecord, TailState};

#[derive(Default)]
pub struct Factory {
    init_delay: Option<Duration>,
}

impl Factory {
    #[cfg(test)]
    pub fn with_init_delay(init_delay: Duration) -> Self {
        Self {
            init_delay: Some(init_delay),
        }
    }
}

#[async_trait]
impl LogletProviderFactory for Factory {
    fn kind(&self) -> ProviderKind {
        ProviderKind::InMemory
    }

    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider>, OperationError> {
        Ok(Arc::new(MemoryLogletProvider {
            store: Default::default(),
            init_delay: self.init_delay.unwrap_or_default(),
        }))
    }
}

#[derive(Default)]
struct MemoryLogletProvider {
    store: AsyncMutex<HashMap<(LogId, SegmentIndex), Arc<MemoryLoglet>>>,
    init_delay: Duration,
}

#[async_trait]
impl LogletProvider for MemoryLogletProvider {
    async fn get_loglet(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        params: &LogletParams,
    ) -> Result<Arc<dyn Loglet>> {
        let mut guard = self.store.lock().await;

        let loglet = match guard.entry((log_id, segment_index)) {
            hash_map::Entry::Vacant(entry) => {
                if !self.init_delay.is_zero() {
                    // Artificial delay to simulate slow loglet creation
                    info!(
                        "Simulating slow loglet creation, delaying for {:?}",
                        self.init_delay
                    );
                    tokio::time::sleep(self.init_delay).await;
                }

                // Create loglet
                let loglet = entry.insert(MemoryLoglet::new(params.clone()));
                Arc::clone(loglet)
            }
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet as Arc<dyn Loglet>)
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        info!("Shutting down in-memory loglet provider");
        Ok(())
    }
}

#[derive(Debug)]
pub struct MemoryLoglet {
    // We treat params as an opaque identifier for the underlying loglet.
    params: LogletParams,
    log: Mutex<Vec<Bytes>>,
    // internal offset _before_ the loglet head. Loglet head is trim_point_offset.next()
    trim_point_offset: AtomicU64,
    last_committed_offset: AtomicU64,
    sealed: AtomicBool,
    // watches the tail state of ths loglet
    tail_watch: TailOffsetWatch,
}

impl MemoryLoglet {
    pub fn new(params: LogletParams) -> Arc<Self> {
        Arc::new(Self {
            params,
            log: Mutex::new(Vec::new()),
            // Trim point is 0 initially
            trim_point_offset: AtomicU64::new(0),
            last_committed_offset: AtomicU64::new(0),
            sealed: AtomicBool::new(false),
            tail_watch: TailOffsetWatch::new(TailState::new(false, LogletOffset::OLDEST)),
        })
    }

    fn index_to_offset(&self, index: usize) -> LogletOffset {
        let offset = self.trim_point_offset.load(Ordering::Relaxed);
        LogletOffset::from(offset + 1 + index as u64)
    }

    fn saturating_offset_to_index(&self, offset: LogletOffset) -> usize {
        let trim_point = self.trim_point_offset.load(Ordering::Relaxed);
        (offset.0.saturating_sub(trim_point) - 1) as usize
    }

    fn advance_commit_offset(&self, offset: LogletOffset) {
        self.last_committed_offset
            .fetch_max(offset.into(), Ordering::Relaxed);
        self.notify_readers();
    }

    fn notify_readers(&self) {
        let release_pointer = LogletOffset(self.last_committed_offset.load(Ordering::Relaxed));
        // Note: We always notify with false here and the watcher will ignore it if it has observed
        // a previous seal.
        self.tail_watch.notify(false, release_pointer.next());
    }

    fn read_from(
        &self,
        from_offset: LogletOffset,
    ) -> Result<Option<LogRecord<LogletOffset, Bytes>>, OperationError> {
        let guard = self.log.lock().unwrap();
        let trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));
        let head_offset = trim_point.next();
        // Are we reading behind the loglet head?
        if from_offset < head_offset {
            return Ok(Some(LogRecord::new_trim_gap(from_offset, trim_point)));
        }

        // are we reading after commit offset?
        let commit_offset = LogletOffset(self.last_committed_offset.load(Ordering::Relaxed));
        if from_offset > commit_offset {
            Ok(None)
        } else {
            let index = self.saturating_offset_to_index(from_offset);
            Ok(Some(LogRecord::new_data(
                from_offset,
                guard.get(index).expect("reading untrimmed data").clone(),
            )))
        }
    }
}

struct MemoryReadStream {
    loglet: Arc<MemoryLoglet>,
    /// The next offset to read from
    read_pointer: LogletOffset,
    tail_watch: BoxStream<'static, TailState<LogletOffset>>,
    /// stop when read_pointer is at or beyond this offset
    last_known_tail: LogletOffset,
    /// Last offset to read before terminating the stream. None means "tailing" reader.
    read_to: Option<LogletOffset>,
    terminated: bool,
}

impl MemoryReadStream {
    async fn create(
        loglet: Arc<MemoryLoglet>,
        from_offset: LogletOffset,
        to: Option<LogletOffset>,
    ) -> Self {
        let mut tail_watch = loglet.watch_tail();
        let last_known_tail = tail_watch
            .next()
            .await
            .expect("loglet watch returns tail pointer")
            .offset();

        Self {
            loglet,
            read_pointer: from_offset,
            tail_watch,
            last_known_tail,
            read_to: to,
            terminated: false,
        }
    }
}

impl LogletReadStream<LogletOffset> for MemoryReadStream {
    /// Current read pointer. This points to the next offset to be read.
    fn read_pointer(&self) -> LogletOffset {
        self.read_pointer
    }
    /// Returns true if the stream is terminated.
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

impl Stream for MemoryReadStream {
    type Item = Result<LogRecord<LogletOffset, Bytes>, OperationError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        let next_offset = self.read_pointer;

        loop {
            // We have reached the limit we are allowed to read
            if self.read_to.is_some_and(|read_to| next_offset > read_to) {
                self.terminated = true;
                return Poll::Ready(None);
            }

            // Are we reading after commit offset?
            // We are at tail. We need to wait until new records have been released.
            if next_offset >= self.last_known_tail {
                match ready!(self.tail_watch.poll_next_unpin(cx)) {
                    Some(tail_state) => {
                        self.last_known_tail = tail_state.offset();
                        continue;
                    }
                    None => {
                        // system shutdown. Or that the loglet has been unexpectedly shutdown.
                        self.terminated = true;
                        return Poll::Ready(Some(Err(OperationError::Shutdown(ShutdownError))));
                    }
                }
            }

            // tail has been updated.
            let last_known_tail = self.last_known_tail;

            // assert that we are behind tail
            assert!(last_known_tail > next_offset);

            // Trim point is the the slot **before** the first readable record (if it exists)
            // trim point might have been updated since last time.
            let trim_point = LogletOffset(self.loglet.trim_point_offset.load(Ordering::Relaxed));
            let head_offset = trim_point.next();

            // Are we reading behind the loglet head? -> TrimGap
            assert!(next_offset > LogletOffset::from(0));
            if next_offset < head_offset {
                let trim_gap = LogRecord::new_trim_gap(next_offset, trim_point);
                // next record should be beyond at the head
                self.read_pointer = head_offset;
                return Poll::Ready(Some(Ok(trim_gap)));
            }

            let next_record = self
                .loglet
                .read_from(self.read_pointer)?
                .expect("read_from reads after commit offset");
            if next_record.record.is_trim_gap() {
                self.read_pointer = next_record.record.try_as_trim_gap_ref().unwrap().to.next();
            } else {
                self.read_pointer = next_record.offset.next();
            }
            return Poll::Ready(Some(Ok(next_record)));
        }
    }
}

#[async_trait]
impl LogletBase for MemoryLoglet {
    type Offset = LogletOffset;

    async fn create_read_stream(
        self: Arc<Self>,
        from: Self::Offset,
        to: Option<Self::Offset>,
    ) -> Result<SendableLogletReadStream<Self::Offset>> {
        Ok(Box::pin(MemoryReadStream::create(self, from, to).await))
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<Self::Offset>> {
        Box::pin(self.tail_watch.to_stream())
    }

    async fn append(&self, payload: Bytes) -> Result<LogletOffset, AppendError> {
        let mut log = self.log.lock().unwrap();
        if self.sealed.load(Ordering::Relaxed) {
            return Err(AppendError::Sealed);
        }
        let offset = self.index_to_offset(log.len());
        debug!(
            "Appending record to in-memory loglet {:?} at offset {}",
            self.params, offset,
        );
        log.push(payload);
        // mark as committed immediately.
        let offset = LogletOffset(self.last_committed_offset.load(Ordering::Relaxed)).next();
        self.advance_commit_offset(offset);
        Ok(offset)
    }

    async fn append_batch(&self, payloads: &[Bytes]) -> Result<LogletOffset, AppendError> {
        let mut log = self.log.lock().unwrap();
        if self.sealed.load(Ordering::Relaxed) {
            return Err(AppendError::Sealed);
        }
        let offset = LogletOffset(self.last_committed_offset.load(Ordering::Relaxed)).next();
        let first_offset = offset;
        let num_payloads = payloads.len();
        for payload in payloads {
            debug!(
                "Appending record to in-memory loglet {:?} at offset {}",
                self.params, offset,
            );
            log.push(payload.clone());
        }
        // mark as committed immediately.
        self.advance_commit_offset(first_offset + num_payloads);
        Ok(first_offset)
    }

    async fn find_tail(&self) -> Result<TailState<LogletOffset>, OperationError> {
        let _guard = self.log.lock().unwrap();
        let committed = LogletOffset(self.last_committed_offset.load(Ordering::Relaxed)).next();
        let sealed = self.sealed.load(Ordering::Relaxed);
        Ok(if sealed {
            TailState::Sealed(committed)
        } else {
            TailState::Open(committed)
        })
    }

    /// Find the head (oldest) record in the loglet.
    async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError> {
        let _guard = self.log.lock().unwrap();
        let current_trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point == LogletOffset::INVALID {
            Ok(None)
        } else {
            Ok(Some(current_trim_point))
        }
    }

    async fn trim(&self, new_trim_point: Self::Offset) -> Result<(), OperationError> {
        let mut log = self.log.lock().unwrap();
        let actual_trim_point = new_trim_point.min(LogletOffset(
            self.last_committed_offset.load(Ordering::Relaxed),
        ));

        let current_trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point >= actual_trim_point {
            return Ok(());
        }

        let trim_point_index = self.saturating_offset_to_index(actual_trim_point);
        self.trim_point_offset
            .store(actual_trim_point.0, Ordering::Relaxed);
        log.drain(0..=trim_point_index);

        Ok(())
    }

    async fn seal(&self) -> Result<(), OperationError> {
        // Ensures no in-flight operations are taking place.
        let _guard = self.log.lock().unwrap();
        self.sealed.store(true, Ordering::Relaxed);
        self.tail_watch.notify_seal();
        Ok(())
    }

    async fn read(
        &self,
        from: LogletOffset,
    ) -> Result<LogRecord<Self::Offset, Bytes>, OperationError> {
        loop {
            let next_record = self.read_from(from)?;
            if let Some(next_record) = next_record {
                break Ok(next_record);
            }
            // Wait and respond when available.
            self.tail_watch.wait_for(from).await?;
        }
    }

    async fn read_opt(
        &self,
        after: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset, Bytes>>, OperationError> {
        self.read_from(after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! run_test {
        ($test:ident) => {
            paste::paste! {
                #[tokio::test(start_paused = true)]
                async fn [<memory_loglet_  $test>]() -> googletest::Result<()> {
                    let loglet = MemoryLoglet::new(LogletParams::from("112".to_string()));
                    crate::loglet::loglet_tests::$test(loglet).await
                }
            }
        };
    }

    run_test!(gapless_loglet_smoke_test);
    run_test!(single_loglet_readstream);
    run_test!(single_loglet_readstream_with_trims);
    run_test!(append_after_seal);
    run_test!(append_after_seal_concurrent);
    run_test!(seal_empty);
}
