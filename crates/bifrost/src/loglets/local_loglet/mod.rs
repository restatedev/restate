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
mod metric_definitions;
mod provider;
mod read_stream;

use async_trait::async_trait;
use bytes::Bytes;
pub use log_store::LogStoreError;
use metrics::{counter, histogram, Histogram};
pub use provider::Factory;
use restate_core::ShutdownError;
use restate_types::logs::SequenceNumber;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::loglet::{LogletBase, LogletOffset, SendableLogletReadStream};
use crate::loglets::local_loglet::metric_definitions::{
    BIFROST_LOCAL_TRIM, BIFROST_LOCAL_TRIM_LENGTH,
};
use crate::{Error, LogRecord, Result, SealReason, TailState};

use self::keys::RecordKey;
use self::log_store::RocksDbLogStore;
use self::log_store_writer::RocksDbLogWriterHandle;
use self::metric_definitions::{BIFROST_LOCAL_APPEND, BIFROST_LOCAL_APPEND_DURATION};
use self::read_stream::LocalLogletReadStream;
use super::util::OffsetWatch;

struct LocalLoglet {
    log_id: u64,
    log_store: RocksDbLogStore,
    log_writer: RocksDbLogWriterHandle,
    // internal offset _before_ the loglet head. Loglet head is trim_point_offset.next()
    trim_point_offset: AtomicU64,
    // used to order concurrent trim operations :-(
    trim_point_lock: Mutex<()>,
    // In local loglet, the release point == the last committed offset
    last_committed_offset: AtomicU64,
    next_write_offset: Mutex<LogletOffset>,
    #[allow(dead_code)]
    seal: Option<SealReason>,
    release_watch: OffsetWatch,
    append_latency: Histogram,
}

impl std::fmt::Debug for LocalLoglet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalLoglet")
            .field("log_id", &self.log_id)
            .field("trim_point_offset", &self.trim_point_offset)
            .field("last_committed_offset", &self.last_committed_offset)
            .field("next_write_offset", &self.next_write_offset)
            .field("seal", &self.seal)
            .finish()
    }
}

impl LocalLoglet {
    pub async fn create(
        log_id: u64,
        log_store: RocksDbLogStore,
        log_writer: RocksDbLogWriterHandle,
    ) -> Result<Self> {
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
        let append_latency = histogram!(BIFROST_LOCAL_APPEND_DURATION);
        let loglet = Self {
            log_id,
            log_store,
            log_writer,
            trim_point_offset,
            trim_point_lock: Mutex::new(()),
            next_write_offset,
            last_committed_offset,
            seal,
            release_watch: OffsetWatch::new(release_pointer),
            append_latency,
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

    fn read_from(
        &self,
        from_offset: LogletOffset,
    ) -> Result<Option<LogRecord<LogletOffset, Bytes>>> {
        debug_assert_ne!(LogletOffset::INVALID, from_offset);
        let trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));
        let head_offset = trim_point.next();
        // Are we reading behind the loglet head?
        if from_offset < head_offset {
            return Ok(Some(LogRecord::new_trim_gap(from_offset, trim_point)));
        }

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
                &data_cf,
                read_opts,
                rocksdb::IteratorMode::From(&key.to_bytes(), rocksdb::Direction::Forward),
            );
            let record = iter.next().transpose().map_err(LogStoreError::Rocksdb)?;
            let Some(record) = record else {
                let trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));
                // we might not have been able to read the next record because of a concurrent trim operation
                return if trim_point >= from_offset {
                    Ok(Some(LogRecord::new_trim_gap(from_offset, trim_point)))
                } else {
                    Ok(None)
                };
            };

            let (key, data) = record;
            let key = RecordKey::from_slice(&key);
            // Defensive, the upper_bound set on the iterator should prevent this.
            if key.log_id != self.log_id {
                warn!(
                    log_id = self.log_id,
                    "read_from moved to the adjacent log {}, that should not happen.\
                    This is harmless but needs to be investigated!",
                    key.log_id,
                );
                return Ok(None);
            }
            // Next record isn't what we expected to read. Issue a trim gap to fast-forward just
            // before the next real record.
            if key.offset != from_offset {
                return Ok(Some(LogRecord::new_trim_gap(
                    from_offset,
                    key.offset.prev(),
                )));
            }
            let data = Bytes::from(data);
            Ok(Some(LogRecord::new_data(key.offset, data)))
        }
    }
}

#[async_trait]
impl LogletBase for LocalLoglet {
    type Offset = LogletOffset;

    async fn create_read_stream(
        self: Arc<Self>,
        from: Self::Offset,
    ) -> Result<SendableLogletReadStream<Self::Offset>> {
        Ok(Box::pin(LocalLogletReadStream::create(self, from).await?))
    }

    async fn append(&self, payload: Bytes) -> Result<LogletOffset> {
        counter!(BIFROST_LOCAL_APPEND).increment(1);
        let start_time = std::time::Instant::now();
        // We hold the lock to ensure that offsets are enqueued in the order of
        // their offsets in the logstore writer. This means that acknowledgements
        // that an offset N from the writer imply that all previous offsets have
        // been durably committed, therefore, such offsets can be released to readers.
        let (receiver, offset) = {
            let mut next_offset_guard = self.next_write_offset.lock().await;
            // lock acquired
            let offset = *next_offset_guard;
            let receiver = self
                .log_writer
                .enqueue_put_record(self.log_id, offset, payload)
                .await?;
            // next offset points to the next available slot.
            *next_offset_guard = offset.next();
            (receiver, offset)
            // lock dropped
        };

        debug!("Written entry to {offset:?}");

        let _ = receiver.await.unwrap_or_else(|_| {
            warn!("Unsure if the local loglet record was written, the ack channel was dropped");
            Err(Error::Shutdown(ShutdownError))
        })?;

        self.last_committed_offset
            .fetch_max(offset.into(), Ordering::Relaxed);
        self.notify_readers();
        self.append_latency.record(start_time.elapsed());
        Ok(offset)
    }

    async fn append_batch(&self, payloads: &[Bytes]) -> Result<LogletOffset> {
        let num_payloads = payloads.len();
        counter!(BIFROST_LOCAL_APPEND).increment(num_payloads as u64);
        let start_time = std::time::Instant::now();
        // We hold the lock to ensure that offsets are enqueued in the order of
        // their offsets in the logstore writer. This means that acknowledgements
        // that an offset N from the writer imply that all previous offsets have
        // been durably committed, therefore, such offsets can be released to readers.
        let (receiver, offset) = {
            let mut next_offset_guard = self.next_write_offset.lock().await;
            let offset = *next_offset_guard;
            // lock acquired
            let receiver = self
                .log_writer
                .enqueue_put_records(self.log_id, *next_offset_guard, payloads)
                .await?;
            // next offset points to the next available slot.
            *next_offset_guard = offset + num_payloads;
            (receiver, next_offset_guard.prev())
            // lock dropped
        };

        let _ = receiver.await.unwrap_or_else(|_| {
            warn!("Unsure if the local loglet record was written, the ack channel was dropped");
            Err(Error::Shutdown(ShutdownError))
        })?;

        self.last_committed_offset
            .fetch_max(offset.into(), Ordering::Relaxed);
        self.notify_readers();
        self.append_latency.record(start_time.elapsed());
        Ok(offset)
    }

    async fn find_tail(&self) -> Result<TailState<LogletOffset>> {
        let last_committed =
            LogletOffset::from(self.last_committed_offset.load(Ordering::Relaxed)).next();
        Ok(if self.seal.is_some() {
            TailState::Sealed(last_committed)
        } else {
            TailState::Open(last_committed)
        })
    }

    async fn get_trim_point(&self) -> Result<Option<Self::Offset>> {
        let current_trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point == LogletOffset::INVALID {
            Ok(None)
        } else {
            Ok(Some(current_trim_point))
        }
    }

    /// Trim the log to the minimum of new_trim_point and last_committed_offset
    /// new_trim_point is inclusive (will be trimmed)
    async fn trim(&self, new_trim_point: Self::Offset) -> Result<(), Error> {
        let effective_trim_point = new_trim_point.min(LogletOffset(
            self.last_committed_offset.load(Ordering::Relaxed),
        ));

        // the lock is needed to prevent concurrent trim operations from over taking each other :-(
        // The problem is that the LogStoreWriter is not the component holding the ground truth
        // but the LocalLoglet is. The bad thing that could happen is that we might not trim earlier
        // parts in case two trim operations get reordered and we crash before applying the second.
        let _trim_point_lock_guard = self.trim_point_lock.lock().await;

        let current_trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point >= effective_trim_point {
            // nothing to do since we have already trimmed beyond new_trim_point
            return Ok(());
        }

        counter!(BIFROST_LOCAL_TRIM).increment(1);

        // no compare & swap operation is needed because we are operating under the trim point lock
        self.trim_point_offset
            .store(effective_trim_point.0, Ordering::Relaxed);

        self.log_writer
            .enqueue_trim(self.log_id, current_trim_point, effective_trim_point)
            .await?;

        histogram!(BIFROST_LOCAL_TRIM_LENGTH).record(
            u32::try_from(effective_trim_point.0 - current_trim_point.0).unwrap_or(u32::MAX),
        );

        Ok(())
    }

    async fn read_next_single(&self, from: Self::Offset) -> Result<LogRecord<Self::Offset, Bytes>> {
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
        from: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset, Bytes>>> {
        self.read_from(from)
    }
}

#[cfg(test)]
mod tests {
    use restate_core::TestCoreEnvBuilder;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::live::Live;
    use restate_types::logs::metadata::{LogletParams, ProviderKind};

    use crate::loglet_tests::*;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn local_loglet_smoke_test() -> googletest::Result<()> {
        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        node_env
            .tc
            .run_in_scope("test", None, async {
                let config = Live::from_value(Configuration::default());
                RocksDbManager::init(config.clone().map(|c| &c.common));
                let params = LogletParams::from("42".to_string());

                let log_store = RocksDbLogStore::create(
                    &config.pinned().bifrost.local,
                    config.clone().map(|c| &c.bifrost.local.rocksdb).boxed(),
                )
                .await?;

                let log_writer = log_store
                    .create_writer()
                    .start(config.clone().map(|c| &c.bifrost.local).boxed())?;

                let loglet = Arc::new(
                    LocalLoglet::create(
                        params
                            .id()
                            .parse()
                            .expect("loglet params can be converted into u64"),
                        log_store,
                        log_writer,
                    )
                    .await?,
                );

                gapless_loglet_smoke_test(loglet).await?;
                Ok(())
            })
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn local_loglet_readstream_test() -> googletest::Result<()> {
        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        node_env
            .tc
            .run_in_scope("test", None, async {
                let config = Live::from_value(Configuration::default());
                RocksDbManager::init(config.clone().map(|c| &c.common));
                let params = LogletParams::from("42".to_string());

                let log_store = RocksDbLogStore::create(
                    &config.pinned().bifrost.local,
                    config.clone().map(|c| &c.bifrost.local.rocksdb).boxed(),
                )
                .await?;

                let log_writer = log_store
                    .create_writer()
                    .start(config.clone().map(|c| &c.bifrost.local).boxed())?;

                let loglet = Arc::new(
                    LocalLoglet::create(
                        params
                            .id()
                            .parse()
                            .expect("loglet params can be converted into u64"),
                        log_store,
                        log_writer,
                    )
                    .await?,
                );

                single_loglet_readstream_test(loglet).await?;
                Ok(())
            })
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn local_loglet_readstream_test_with_trims() -> googletest::Result<()> {
        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        node_env
            .tc
            .run_in_scope("test", None, async {
                let config = Live::from_value(Configuration::default());
                RocksDbManager::init(config.clone().map(|c| &c.common));
                let params = LogletParams::from("99".to_string());

                let log_store = RocksDbLogStore::create(
                    &config.pinned().bifrost.local,
                    config.clone().map(|c| &c.bifrost.local.rocksdb).boxed(),
                )
                .await?;

                let log_writer = log_store
                    .create_writer()
                    .start(config.clone().map(|c| &c.bifrost.local).boxed())?;

                let loglet = Arc::new(
                    LocalLoglet::create(
                        params
                            .id()
                            .parse()
                            .expect("loglet params can be converted into u64"),
                        log_store,
                        log_writer,
                    )
                    .await?,
                );

                single_loglet_readstream_test_with_trims(loglet).await?;
                Ok(())
            })
            .await
    }
}
