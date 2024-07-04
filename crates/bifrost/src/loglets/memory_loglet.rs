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
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use pin_project::pin_project;
use restate_core::ShutdownError;
use tokio::sync::Mutex as AsyncMutex;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tracing::{debug, info};

use restate_types::logs::metadata::{LogletParams, ProviderKind};
use restate_types::logs::SequenceNumber;

use crate::loglet::{Loglet, LogletBase, LogletOffset, LogletReadStream, SendableLogletReadStream};
use crate::{Error, LogRecord, LogletProvider, TailState};
use crate::{ProviderError, Result};

use super::util::OffsetWatch;

#[derive(Default)]
pub struct Factory {
    init_delay: Option<Duration>,
}

impl Factory {
    pub fn with_init_delay(init_delay: Duration) -> Self {
        Self {
            init_delay: Some(init_delay),
        }
    }
}

#[async_trait]
impl crate::LogletProviderFactory for Factory {
    fn kind(&self) -> ProviderKind {
        ProviderKind::InMemory
    }

    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider>, ProviderError> {
        Ok(Arc::new(MemoryLogletProvider {
            store: Default::default(),
            init_delay: self.init_delay.unwrap_or_default(),
        }))
    }
}

#[derive(Default)]
struct MemoryLogletProvider {
    store: AsyncMutex<HashMap<LogletParams, Arc<MemoryLoglet>>>,
    init_delay: Duration,
}

#[async_trait]
impl LogletProvider for MemoryLogletProvider {
    async fn get_loglet(&self, params: &LogletParams) -> Result<Arc<dyn Loglet>> {
        let mut guard = self.store.lock().await;

        let loglet = match guard.entry(params.clone()) {
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

    async fn shutdown(&self) -> Result<(), ProviderError> {
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
    release_watch: OffsetWatch,
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
            release_watch: OffsetWatch::new(LogletOffset::INVALID),
        })
    }

    fn index_to_offset(&self, index: usize) -> LogletOffset {
        let offset = self.trim_point_offset.load(Ordering::Acquire);
        LogletOffset::from(offset + 1 + index as u64)
    }

    fn saturating_offset_to_index(&self, offset: LogletOffset) -> usize {
        let trim_point = self.trim_point_offset.load(Ordering::Acquire);
        (offset.0.saturating_sub(trim_point) - 1) as usize
    }

    pub fn advance_commit_offset(&self, offset: LogletOffset) {
        self.last_committed_offset
            .fetch_max(offset.into(), Ordering::Release);
        self.notify_readers();
    }

    fn notify_readers(&self) {
        let release_pointer = LogletOffset(self.last_committed_offset.load(Ordering::Relaxed));
        self.release_watch.notify(release_pointer);
    }

    fn read_from(
        &self,
        from_offset: LogletOffset,
    ) -> Result<Option<LogRecord<LogletOffset, Bytes>>> {
        let guard = self.log.lock().unwrap();
        let trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Acquire));
        let head_offset = trim_point.next();
        // Are we reading behind the loglet head?
        if from_offset < head_offset {
            return Ok(Some(LogRecord::new_trim_gap(from_offset, trim_point)));
        }

        // are we reading after commit offset?
        let commit_offset = LogletOffset(self.last_committed_offset.load(Ordering::Acquire));
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

#[pin_project]
struct MemoryReadStream {
    loglet: Arc<MemoryLoglet>,
    /// The next offset to read from
    read_pointer: LogletOffset,
    #[pin]
    release_watch: WatchStream<LogletOffset>,
    // how far we are allowed to read in the loglet
    release_pointer: LogletOffset,
    #[pin]
    terminated: bool,
}

impl MemoryReadStream {
    async fn create(loglet: Arc<MemoryLoglet>, from_offset: LogletOffset) -> Self {
        let mut release_watch = loglet.release_watch.to_stream();
        let release_pointer = release_watch
            .next()
            .await
            .expect("loglet watch returns release pointer");

        Self {
            loglet,
            read_pointer: from_offset,
            release_watch,
            release_pointer,
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
    type Item = Result<LogRecord<LogletOffset, Bytes>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        let next_offset = self.read_pointer;
        loop {
            let mut this = self.as_mut().project();

            // Are we reading after commit offset?
            // We are at tail. We need to wait until new records have been released.
            if next_offset > *this.release_pointer {
                let updated_release_pointer = match this.release_watch.poll_next(cx) {
                    Poll::Ready(t) => t,
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                };

                match updated_release_pointer {
                    Some(updated_release_pointer) => {
                        *this.release_pointer = updated_release_pointer;
                        continue;
                    }
                    None => {
                        // system shutdown. Or that the loglet has been unexpectedly shutdown.
                        this.terminated.set(true);
                        return Poll::Ready(Some(Err(Error::Shutdown(ShutdownError))));
                    }
                }
            }

            // release_pointer has been updated.
            let release_pointer = *this.release_pointer;

            // assert that we are newer
            assert!(release_pointer >= next_offset);

            // Trim point is the the slot **before** the first readable record (if it exists)
            // trim point might have been updated since last time.
            let trim_point = LogletOffset(this.loglet.trim_point_offset.load(Ordering::Relaxed));
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
    ) -> Result<SendableLogletReadStream<Self::Offset>> {
        Ok(Box::pin(MemoryReadStream::create(self, from).await))
    }

    async fn append(&self, payload: Bytes) -> Result<LogletOffset> {
        let mut log = self.log.lock().unwrap();
        let offset = self.index_to_offset(log.len());
        debug!(
            "Appending record to in-memory loglet {:?} at offset {}",
            self.params, offset,
        );
        log.push(payload);
        // mark as committed immediately.
        let offset = LogletOffset(self.last_committed_offset.load(Ordering::Acquire)).next();
        self.advance_commit_offset(offset);
        Ok(offset)
    }

    async fn append_batch(&self, payloads: &[Bytes]) -> Result<LogletOffset> {
        let mut log = self.log.lock().unwrap();
        let offset = LogletOffset(self.last_committed_offset.load(Ordering::Acquire)).next();
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

    async fn find_tail(&self) -> Result<TailState<LogletOffset>> {
        let committed = LogletOffset(self.last_committed_offset.load(Ordering::Acquire)).next();
        let sealed = self.sealed.load(Ordering::Acquire);
        Ok(if sealed {
            TailState::Sealed(committed)
        } else {
            TailState::Open(committed)
        })
    }

    /// Find the head (oldest) record in the loglet.
    async fn get_trim_point(&self) -> Result<Option<LogletOffset>> {
        let current_trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point == LogletOffset::INVALID {
            Ok(None)
        } else {
            Ok(Some(current_trim_point))
        }
    }

    async fn trim(&self, new_trim_point: Self::Offset) -> Result<()> {
        let actual_trim_point = new_trim_point.min(LogletOffset(
            self.last_committed_offset.load(Ordering::Relaxed),
        ));

        let mut log = self.log.lock().unwrap();

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

    async fn read_next_single(&self, from: LogletOffset) -> Result<LogRecord<Self::Offset, Bytes>> {
        loop {
            let next_record = self.read_from(from)?;
            if let Some(next_record) = next_record {
                break Ok(next_record);
            }
            // Wait and respond when available.
            self.release_watch.wait_for(from).await?;
        }
    }

    async fn read_next_single_opt(
        &self,
        after: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset, Bytes>>> {
        self.read_from(after)
    }
}

#[cfg(test)]
mod tests {
    use crate::loglet_tests::*;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn memory_loglet_smoke_test() -> googletest::Result<()> {
        let loglet = MemoryLoglet::new(LogletParams::from("112".to_string()));
        gapless_loglet_smoke_test(loglet).await
    }

    #[tokio::test(start_paused = true)]
    async fn memory_loglet_readstream_test() -> googletest::Result<()> {
        let loglet = MemoryLoglet::new(LogletParams::from("112".to_string()));
        single_loglet_readstream_test(loglet).await
    }

    #[tokio::test(start_paused = true)]
    async fn memory_loglet_readstream_test_with_trims() -> googletest::Result<()> {
        let loglet = MemoryLoglet::new(LogletParams::from("112".to_string()));
        single_loglet_readstream_test_with_trims(loglet).await
    }
}
