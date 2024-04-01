// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::{hash_map, BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use restate_types::logs::metadata::LogletParams;
use restate_types::logs::{Payload, SequenceNumber};
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info};

use crate::loglet::{Loglet, LogletBase, LogletOffset, LogletProvider};
use crate::LogRecord;
use crate::{Error, ProviderError};

#[derive(Default)]
pub struct MemoryLogletProvider {
    store: AsyncMutex<HashMap<LogletParams, Arc<MemoryLoglet>>>,
    init_delay: Option<Duration>,
}

#[allow(dead_code)]
impl MemoryLogletProvider {
    pub fn new() -> Result<Arc<Self>, ProviderError> {
        Ok(Arc::default())
    }

    pub fn with_init_delay(init_delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            init_delay: Some(init_delay),
            ..Default::default()
        })
    }
}

#[async_trait]
impl LogletProvider for MemoryLogletProvider {
    async fn get_loglet(&self, params: &LogletParams) -> Result<std::sync::Arc<dyn Loglet>, Error> {
        let mut guard = self.store.lock().await;

        let loglet = match guard.entry(params.clone()) {
            hash_map::Entry::Vacant(entry) => {
                if let Some(init_delay) = self.init_delay {
                    // Artificial delay to simulate slow loglet creation
                    info!(
                        "Simulating slow loglet creation, delaying for {:?}",
                        init_delay
                    );
                    tokio::time::sleep(init_delay).await;
                }

                // Create loglet
                let loglet = entry.insert(MemoryLoglet::new(params.clone()));
                Arc::clone(loglet)
            }
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet as Arc<dyn Loglet>)
    }

    fn start(&self) -> Result<(), ProviderError> {
        info!("Starting in-memory loglet provider");
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ProviderError> {
        info!("Shutting down in-memory loglet provider");
        Ok(())
    }
}

#[derive(Debug)]
struct OffsetWatcher {
    offset: LogletOffset,
    channel: Sender<()>,
}

impl PartialEq for OffsetWatcher {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl Eq for OffsetWatcher {}

impl PartialOrd for OffsetWatcher {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.offset.cmp(&other.offset))
    }
}

impl Ord for OffsetWatcher {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}

pub struct MemoryLoglet {
    // We treat params as an opaque identifier for the underlying loglet.
    params: LogletParams,
    log: Mutex<Vec<Payload>>,
    // internal offset of the first record (or slot available)
    trim_point_offset: AtomicU64,
    last_committed_offset: AtomicU64,
    // reversed comparator. The watcher with the lowest offset ranks
    // higher in the binary heap.
    watchers: Mutex<BinaryHeap<Reverse<OffsetWatcher>>>,
}

impl MemoryLoglet {
    pub fn new(params: LogletParams) -> Arc<Self> {
        Arc::new(Self {
            params,
            log: Mutex::new(Vec::new()),
            // Trim point is 0 initially
            trim_point_offset: AtomicU64::new(0),
            last_committed_offset: AtomicU64::new(0),
            watchers: Mutex::new(BinaryHeap::new()),
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
            .store(offset.0, Ordering::Release);
        self.notify_watchers();
    }

    pub fn watch_for_offset(&self, offset: LogletOffset) -> Receiver<()> {
        let mut watchers = self.watchers.lock().unwrap();
        let (snd, rcv) = tokio::sync::oneshot::channel();
        watchers.push(Reverse(OffsetWatcher {
            offset,
            channel: snd,
        }));
        rcv
    }

    pub fn notify_watchers(&self) {
        // it's safe to not lock the logs mutex because commit offset increases monotonically.
        let committed = LogletOffset(self.last_committed_offset.load(Ordering::Acquire));
        let mut watchers = self.watchers.lock().unwrap();
        // remove all watchers with offset <= committed and notify them
        while let Some(Reverse(watcher)) = watchers.peek() {
            if watcher.offset <= committed {
                let Reverse(watcher) = watchers.pop().expect("watcher is present");
                let _ = watcher.channel.send(());
            } else {
                break;
            }
        }
    }

    fn read_after(&self, after: LogletOffset) -> Result<Option<LogRecord<LogletOffset>>, Error> {
        let guard = self.log.lock().unwrap();
        let trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Acquire));
        // are we reading after before the trim point? Note that if trim_point == after then we
        // don't return a trim gap, the next record is potentially a data record.
        if trim_point > after {
            return Ok(Some(LogRecord::new_trim_gap(after.next(), trim_point)));
        }

        let from_offset = after.next();
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

#[async_trait]
impl LogletBase for MemoryLoglet {
    type Offset = LogletOffset;

    async fn append(&self, payload: Payload) -> Result<LogletOffset, Error> {
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

    async fn find_tail(&self) -> Result<Option<LogletOffset>, Error> {
        let log = self.log.lock().unwrap();
        if log.is_empty() {
            Ok(None)
        } else {
            let committed = LogletOffset(self.last_committed_offset.load(Ordering::Acquire));
            Ok(Some(committed))
        }
    }

    /// Find the head (oldest) record in the loglet.
    async fn get_trim_point(&self) -> Result<LogletOffset, Error> {
        Ok(LogletOffset(self.trim_point_offset.load(Ordering::Acquire)))
    }

    async fn read_next_single(
        &self,
        after: LogletOffset,
    ) -> Result<LogRecord<Self::Offset>, Error> {
        loop {
            let next_record = self.read_after(after)?;
            if let Some(next_record) = next_record {
                break Ok(next_record);
            } else {
                // Wait and respond when available.
                let receiver = self.watch_for_offset(after.next());
                receiver.await.unwrap();
                continue;
            }
        }
    }

    async fn read_next_single_opt(
        &self,
        after: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset>>, Error> {
        self.read_after(after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use restate_test_util::let_assert;
    use tokio::task::JoinHandle;
    use tracing_test::traced_test;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_memory_loglet() -> Result<()> {
        let loglet = MemoryLoglet::new(LogletParams::from("112".to_string()));

        assert_eq!(LogletOffset::INVALID, loglet.get_trim_point().await?);
        assert_eq!(None, loglet.find_tail().await?);

        // Append 1
        let offset = loglet.append(Payload::from("record1")).await?;
        assert_eq!(LogletOffset::OLDEST, offset);
        assert_eq!(LogletOffset::INVALID, loglet.get_trim_point().await?);
        assert_eq!(Some(LogletOffset::OLDEST), loglet.find_tail().await?);

        // Append 2
        let offset = loglet.append(Payload::from("record2")).await?;
        assert_eq!(LogletOffset(2), offset);
        assert_eq!(LogletOffset::INVALID, loglet.get_trim_point().await?);
        assert_eq!(Some(LogletOffset(2)), loglet.find_tail().await?);

        // Append 3
        let offset = loglet.append(Payload::from("record3")).await?;
        assert_eq!(LogletOffset(3), offset);
        assert_eq!(LogletOffset::INVALID, loglet.get_trim_point().await?);
        assert_eq!(Some(LogletOffset(3)), loglet.find_tail().await?);

        // read record 1 (reading next after INVALID)
        let_assert!(Some(log_record) = loglet.read_next_single_opt(LogletOffset::INVALID).await?);
        let LogRecord { offset, record } = log_record;
        assert_eq!(offset, loglet.get_trim_point().await?.next());
        assert_eq!(LogletOffset::OLDEST, offset);
        assert!(record.is_data());
        assert_eq!(Payload::from("record1"), record.into_payload_unchecked());

        // read record 2 (reading next after OLDEST)
        let LogRecord { offset, record } = loglet.read_next_single(offset).await?;
        assert_eq!(LogletOffset(2), offset);
        assert_eq!(Payload::from("record2"), record.into_payload_unchecked());

        // read record 3
        let LogRecord { offset, record } = loglet.read_next_single(offset).await?;
        assert_eq!(LogletOffset(3), offset);
        assert_eq!(Payload::from("record3"), record.into_payload_unchecked());

        // read from the future returns None
        assert!(loglet
            .read_next_single_opt(LogletOffset(5))
            .await?
            .is_none());

        let handle1: JoinHandle<Result<()>> = tokio::spawn({
            let loglet = loglet.clone();
            async move {
                // read future record 4
                let LogRecord { offset, record } = loglet.read_next_single(LogletOffset(3)).await?;
                assert_eq!(LogletOffset(4), offset);
                assert_eq!(Payload::from("record4"), record.into_payload_unchecked());
                Ok(())
            }
        });

        // Waiting for 10
        let handle2: JoinHandle<Result<()>> = tokio::spawn({
            let loglet = loglet.clone();
            async move {
                // read future record 10
                let LogRecord { offset, record } = loglet.read_next_single(LogletOffset(9)).await?;
                assert_eq!(LogletOffset(10), offset);
                assert_eq!(Payload::from("record10"), record.into_payload_unchecked());
                Ok(())
            }
        });

        // Giving a chance to other tasks to work.
        tokio::task::yield_now().await;
        assert!(!handle1.is_finished());

        // Append 4
        let offset = loglet.append(Payload::from("record4")).await?;
        assert_eq!(LogletOffset(4), offset);
        assert_eq!(LogletOffset::INVALID, loglet.get_trim_point().await?);
        assert_eq!(Some(LogletOffset(4)), loglet.find_tail().await?);

        assert!(handle1.await.unwrap().is_ok());

        tokio::task::yield_now().await;
        // Only handle1 should have finished work.
        assert!(!handle2.is_finished());

        // test timeout future items
        let start = tokio::time::Instant::now();
        let res = tokio::time::timeout(Duration::from_secs(10), handle2).await;

        // We have timedout waiting.
        assert!(res.is_err());
        assert_eq!(Duration::from_secs(10), start.elapsed());
        // Tail didn't change.
        assert_eq!(Some(LogletOffset(4)), loglet.find_tail().await?);

        Ok(())
    }
}
