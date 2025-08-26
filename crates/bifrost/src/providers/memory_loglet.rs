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
use std::collections::{HashMap, hash_map};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::task::ready;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info};

use restate_core::ShutdownError;
use restate_types::logs::metadata::{
    Chain, LogletParams, ProviderConfiguration, ProviderKind, SegmentIndex,
};
use restate_types::logs::{
    KeyFilter, LogId, LogletId, LogletOffset, MatchKeyQuery, Record, SequenceNumber,
    TailOffsetWatch, TailState,
};

use crate::LogEntry;
use crate::Result;
use crate::loglet::{
    FindTailOptions, Loglet, LogletCommit, LogletProvider, LogletProviderFactory, LogletReadStream,
    OperationError, SendableLogletReadStream,
};

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
                let raw: u64 = params
                    .parse()
                    .expect("memory loglets are configured just with u64 loglet ids");
                let loglet = entry.insert(MemoryLoglet::new(LogletId::new_unchecked(raw)));
                Arc::clone(loglet)
            }
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet as Arc<dyn Loglet>)
    }

    fn propose_new_loglet_params(
        &self,
        log_id: LogId,
        chain: Option<&Chain>,
        _defaults: &ProviderConfiguration,
    ) -> Result<LogletParams, OperationError> {
        let new_segment_index = chain
            .map(|c| c.tail_index().next())
            .unwrap_or(SegmentIndex::OLDEST);
        let id = LogletId::new(log_id, new_segment_index);
        Ok(LogletParams::from(u64::from(id).to_string()))
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        info!("Shutting down in-memory loglet provider");
        Ok(())
    }
}

#[derive(derive_more::Debug)]
pub struct MemoryLoglet {
    loglet_id: LogletId,
    #[debug(skip)]
    log: Mutex<Vec<Record>>,
    // internal offset _before_ the loglet head. Loglet head is trim_point_offset.next()
    trim_point_offset: AtomicU32,
    last_committed_offset: AtomicU32,
    sealed: AtomicBool,
    // watches the tail state of this loglet
    #[debug(skip)]
    tail_watch: TailOffsetWatch,
}

impl MemoryLoglet {
    pub fn new(loglet_id: LogletId) -> Arc<Self> {
        Arc::new(Self {
            loglet_id,
            log: Mutex::new(Vec::new()),
            // Trim point is 0 initially
            trim_point_offset: AtomicU32::new(0),
            last_committed_offset: AtomicU32::new(0),
            sealed: AtomicBool::new(false),
            tail_watch: TailOffsetWatch::new(TailState::new(false, LogletOffset::OLDEST)),
        })
    }

    fn saturating_offset_to_index(&self, offset: LogletOffset) -> usize {
        let trim_point = self.trim_point_offset.load(Ordering::Relaxed);
        (offset.saturating_sub(trim_point) - 1) as usize
    }

    fn advance_commit_offset(&self, offset: LogletOffset) {
        self.last_committed_offset
            .fetch_max(offset.into(), Ordering::Relaxed);
        self.notify_readers();
    }

    fn notify_readers(&self) {
        let release_pointer = LogletOffset::new(self.last_committed_offset.load(Ordering::Relaxed));
        // Note: We always notify with false here and the watcher will ignore it if it has observed
        // a previous seal.
        self.tail_watch.notify(false, release_pointer.next());
    }

    fn read_from(
        &self,
        from_offset: LogletOffset,
    ) -> Result<Option<LogEntry<LogletOffset>>, OperationError> {
        let guard = self.log.lock().unwrap();
        let trim_point = LogletOffset::new(self.trim_point_offset.load(Ordering::Relaxed));
        let head_offset = trim_point.next();
        // Are we reading behind the loglet head?
        if from_offset < head_offset {
            return Ok(Some(LogEntry::new_trim_gap(from_offset, trim_point)));
        }

        // are we reading after commit offset?
        let commit_offset = LogletOffset::new(self.last_committed_offset.load(Ordering::Relaxed));
        if from_offset > commit_offset {
            Ok(None)
        } else {
            let index = self.saturating_offset_to_index(from_offset);
            let record = guard.get(index).expect("reading untrimmed data").clone();
            Ok(Some(LogEntry::new_data(from_offset, record)))
        }
    }
}

struct MemoryReadStream {
    loglet: Arc<MemoryLoglet>,
    /// Chooses which records to read/return
    filter: KeyFilter,
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
        filter: KeyFilter,
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
            filter,
            read_pointer: from_offset,
            tail_watch,
            last_known_tail,
            read_to: to,
            terminated: false,
        }
    }
}

impl LogletReadStream for MemoryReadStream {
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
    type Item = Result<LogEntry<LogletOffset>, OperationError>;

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
            let trim_point =
                LogletOffset::new(self.loglet.trim_point_offset.load(Ordering::Relaxed));
            let head_offset = trim_point.next();

            // Are we reading behind the loglet head? -> TrimGap
            assert!(next_offset > LogletOffset::from(0));
            if next_offset < head_offset {
                let trim_gap = LogEntry::new_trim_gap(next_offset, trim_point);
                // next record should be beyond at the head
                self.read_pointer = head_offset;
                return Poll::Ready(Some(Ok(trim_gap)));
            }

            let next_record = self
                .loglet
                .read_from(self.read_pointer)?
                .expect("read_from reads after commit offset");

            self.read_pointer = next_record
                .trim_gap_to_sequence_number()
                .unwrap_or(next_record.sequence_number())
                .next();

            // If this is a filtered record, skip it.
            if let Some(data_record) = next_record.as_record()
                && !data_record.matches_key_query(&self.filter)
            {
                // read_pointer is already advanced, just don't return the
                // record and fast-forward.
                continue;
            }

            return Poll::Ready(Some(Ok(next_record)));
        }
    }
}

#[async_trait]
impl Loglet for MemoryLoglet {
    fn debug_str(&self) -> Cow<'static, str> {
        Cow::from(format!("in-memory/{}", self.loglet_id))
    }

    async fn create_read_stream(
        self: Arc<Self>,
        filter: KeyFilter,
        from: LogletOffset,
        to: Option<LogletOffset>,
    ) -> Result<SendableLogletReadStream, OperationError> {
        Ok(Box::pin(
            MemoryReadStream::create(self, filter, from, to).await,
        ))
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<LogletOffset>> {
        Box::pin(self.tail_watch.to_stream())
    }

    async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, OperationError> {
        let mut log = self.log.lock().unwrap();
        if self.sealed.load(Ordering::Relaxed) {
            return Ok(LogletCommit::sealed());
        }
        let mut last_committed_offset =
            LogletOffset::new(self.last_committed_offset.load(Ordering::Relaxed));
        log.reserve(payloads.len());
        for payload in payloads.iter() {
            last_committed_offset = last_committed_offset.next();
            debug!(
                "Appending record to in-memory loglet {} at offset {}",
                self.loglet_id, last_committed_offset
            );
            log.push(payload.clone());
        }
        // mark as committed immediately.
        self.advance_commit_offset(last_committed_offset);
        Ok(LogletCommit::resolved(last_committed_offset))
    }

    async fn find_tail(
        &self,
        _: FindTailOptions,
    ) -> Result<TailState<LogletOffset>, OperationError> {
        let _guard = self.log.lock().unwrap();
        let committed =
            LogletOffset::new(self.last_committed_offset.load(Ordering::Relaxed)).next();
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
        let current_trim_point = LogletOffset::new(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point == LogletOffset::INVALID {
            Ok(None)
        } else {
            Ok(Some(current_trim_point))
        }
    }

    async fn trim(&self, new_trim_point: LogletOffset) -> Result<(), OperationError> {
        let mut log = self.log.lock().unwrap();
        let actual_trim_point = new_trim_point.min(LogletOffset::new(
            self.last_committed_offset.load(Ordering::Relaxed),
        ));

        let current_trim_point = LogletOffset::new(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point >= actual_trim_point {
            return Ok(());
        }

        let trim_point_index = self.saturating_offset_to_index(actual_trim_point);
        self.trim_point_offset
            .store(*actual_trim_point, Ordering::Relaxed);
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_core::TestCoreEnvBuilder;

    macro_rules! run_test {
        ($test:ident) => {
            paste::paste! {
                #[restate_core::test(start_paused = true)]
                async fn [<memory_loglet_  $test>]() -> googletest::Result<()> {
                    let _node_env = TestCoreEnvBuilder::with_incoming_only_connector()
                        .set_provider_kind(ProviderKind::InMemory)
                        .build()
                        .await;
                    let loglet = MemoryLoglet::new(LogletId::new_unchecked(112));
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
