// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod keys;
mod log_state;
mod log_store;
mod log_store_writer;
mod options;
mod provider;
mod utils;

use async_trait::async_trait;
use bytes::Bytes;
pub use log_store::LogStoreError;
pub use options::Options;
pub use provider::LocalLogletProvider;
use restate_core::ShutdownError;
use restate_types::logs::{Payload, SequenceNumber};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use std::sync::atomic::{AtomicU64, Ordering};

use crate::loglet::{LogletBase, LogletOffset};
use crate::{Error, LogRecord, SealReason};

use self::keys::RecordKey;
use self::log_store::RocksDbLogStore;
use self::log_store_writer::RocksDbLogWriterHandle;
use self::utils::OffsetWatch;

#[derive(Debug)]
pub struct LocalLoglet {
    log_id: u64,
    log_store: RocksDbLogStore,
    log_writer: RocksDbLogWriterHandle,
    // internal offset of the first record (or slot available)
    trim_point_offset: AtomicU64,
    // In local loglet, the release point == the last committed offset
    last_committed_offset: AtomicU64,
    next_write_offset: Mutex<LogletOffset>,
    #[allow(dead_code)]
    seal: Option<SealReason>,
    release_watch: OffsetWatch,
}

impl LocalLoglet {
    pub async fn create(
        log_id: u64,
        log_store: RocksDbLogStore,
        log_writer: RocksDbLogWriterHandle,
    ) -> Result<Self, Error> {
        // Fetch the log metadata from the store
        let log_state = log_store.get_log_state(log_id)?;
        let log_state = log_state.unwrap_or_default();

        let trim_point_offset = AtomicU64::new(log_state.trim_point);
        // In local loglet, the release point == the last committed offset
        let last_committed_offset = AtomicU64::new(log_state.release_pointer);
        let next_write_offset_raw = log_state.release_pointer + 1;
        let next_write_offset = Mutex::new(LogletOffset::from(next_write_offset_raw));
        let release_pointer = LogletOffset::from(log_state.release_pointer);
        let seal = log_state.seal;
        let loglet = Self {
            log_id,
            log_store,
            log_writer,
            trim_point_offset,
            next_write_offset,
            last_committed_offset,
            seal,
            release_watch: OffsetWatch::new(release_pointer),
        };
        debug!(
            log_id = log_id,
            release_pointer = %release_pointer,
            next_offset = next_write_offset_raw,
            "Local loglet started"
        );

        Ok(loglet)
    }

    #[inline]
    fn notify_readers(&self) {
        let release_pointer = LogletOffset(self.last_committed_offset.load(Ordering::Relaxed));
        self.release_watch.notify(release_pointer);
    }

    fn read_after(&self, after: LogletOffset) -> Result<Option<LogRecord<LogletOffset>>, Error> {
        let trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));
        // Are we reading after before the trim point? Note that if `trim_point` == `after`
        // then we don't return a trim gap, because the next record is potentially a data
        // record.
        if trim_point > after {
            return Ok(Some(LogRecord::new_trim_gap(after.next(), trim_point)));
        }

        let from_offset = after.next();
        // Are we reading after commit offset?
        let commit_offset = LogletOffset(self.last_committed_offset.load(Ordering::Relaxed));
        if from_offset > commit_offset {
            Ok(None)
        } else {
            let key = RecordKey::new(self.log_id, from_offset);
            let data_cf = self.log_store.data_cf();
            let mut read_opts = rocksdb::ReadOptions::default();
            read_opts.set_iterate_upper_bound(RecordKey::upper_bound(self.log_id).to_bytes());

            let mut iter = self.log_store.db().iterator_cf_opt(
                data_cf,
                read_opts,
                rocksdb::IteratorMode::From(&key.to_bytes(), rocksdb::Direction::Forward),
            );
            let record = iter.next().transpose().map_err(LogStoreError::Rocksdb)?;
            let Some(record) = record else {
                return Ok(None);
            };

            let (key, data) = record;
            let key = RecordKey::from_slice(&key);
            // Defensive, the upper_bound set on the iterator should prevent this.
            if key.log_id != self.log_id {
                warn!(
                    log_id = self.log_id,
                    "read_after moved to the adjacent log {}, that should not happen.\
                    This is harmless but needs to be investigated!",
                    key.log_id,
                );
                return Ok(None);
            }
            let data = Bytes::from(data);
            Ok(Some(LogRecord::new_data(key.offset, Payload::from(data))))
        }
    }
}

#[async_trait]
impl LogletBase for LocalLoglet {
    type Offset = LogletOffset;
    async fn append(&self, payload: Payload) -> Result<LogletOffset, Error> {
        // We hold the lock to ensure that offsets are enqueued in the order of
        // their offsets in the logstore writer. This means that acknowledgements
        // that an offset N from the writer imply that all previous offsets have
        // been durably committed, therefore, such offsets can be released to readers.
        let (receiver, offset) = {
            let mut next_offset_guard = self.next_write_offset.lock().await;
            // lock acquired
            let offset = next_offset_guard.next();
            let receiver = self
                .log_writer
                .enqueue_put_record(
                    self.log_id,
                    offset,
                    payload.into(),
                    true, /* release_immediately */
                )
                .await?;
            *next_offset_guard = offset;
            (receiver, offset)
            // lock dropped
        };

        let _ = receiver.await.unwrap_or_else(|_| {
            warn!("Unsure if the local loglet record was written, the ack channel was dropped");
            Err(Error::Shutdown(ShutdownError))
        })?;

        self.last_committed_offset
            .fetch_max(offset.into(), Ordering::Relaxed);
        self.notify_readers();
        Ok(offset)
    }

    async fn find_tail(&self) -> Result<Option<LogletOffset>, Error> {
        let last_committed = LogletOffset::from(self.last_committed_offset.load(Ordering::Relaxed));
        if last_committed == LogletOffset::INVALID {
            Ok(None)
        } else {
            Ok(Some(last_committed))
        }
    }

    async fn get_trim_point(&self) -> Result<Self::Offset, Error> {
        Ok(LogletOffset(self.trim_point_offset.load(Ordering::Relaxed)))
    }

    async fn read_next_single(
        &self,
        after: Self::Offset,
    ) -> Result<LogRecord<Self::Offset>, Error> {
        loop {
            let next_record = self.read_after(after)?;
            if let Some(next_record) = next_record {
                break Ok(next_record);
            }
            // Wait and respond when available.
            self.release_watch.wait_for(after.next()).await?;
        }
    }

    async fn read_next_single_opt(
        &self,
        after: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset>>, Error> {
        self.read_after(after)
    }
}
