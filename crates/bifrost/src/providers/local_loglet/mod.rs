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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use metrics::{counter, histogram, Histogram};
use tokio::sync::Mutex;
use tracing::{debug, warn};

pub use self::provider::Factory;

use self::log_store::LogStoreError;
use self::log_store::RocksDbLogStore;
use self::log_store_writer::RocksDbLogWriterHandle;
use self::metric_definitions::{BIFROST_LOCAL_APPEND, BIFROST_LOCAL_APPEND_DURATION};
use self::read_stream::LocalLogletReadStream;
use crate::loglet::util::TailOffsetWatch;
use crate::loglet::{
    AppendError, LogletBase, LogletOffset, OperationError, SendableLogletReadStream,
};
use crate::providers::local_loglet::metric_definitions::{
    BIFROST_LOCAL_TRIM, BIFROST_LOCAL_TRIM_LENGTH,
};
use crate::{ErasedInputRecord, Result, TailState};
use restate_core::ShutdownError;
use restate_types::logs::{KeyFilter, SequenceNumber};

struct LocalLoglet {
    loglet_id: u64,
    log_store: RocksDbLogStore,
    log_writer: RocksDbLogWriterHandle,
    // internal offset _before_ the loglet head. Loglet head is trim_point_offset.next()
    trim_point_offset: AtomicU64,
    // used to order concurrent trim operations :-(
    trim_point_lock: Mutex<()>,
    // In local loglet, the release point == the last committed offset
    last_committed_offset: AtomicU64,
    next_write_offset: Mutex<LogletOffset>,
    sealed: AtomicBool,
    // watches the tail state of this loglet
    tail_watch: TailOffsetWatch,
    append_latency: Histogram,
}

impl std::fmt::Debug for LocalLoglet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalLoglet")
            .field("loglet_id", &self.loglet_id)
            .field("trim_point_offset", &self.trim_point_offset)
            .field("last_committed_offset", &self.last_committed_offset)
            .field("next_write_offset", &self.next_write_offset)
            .field("sealed", &self.sealed)
            .finish()
    }
}

impl LocalLoglet {
    pub fn create(
        loglet_id: u64,
        log_store: RocksDbLogStore,
        log_writer: RocksDbLogWriterHandle,
    ) -> Result<Self, OperationError> {
        // Fetch the log metadata from the store
        let log_state = log_store
            .get_log_state(loglet_id)
            .map_err(OperationError::other)?;
        let log_state = log_state.unwrap_or_default();

        let trim_point_offset = AtomicU64::new(log_state.trim_point);
        // In local loglet, the release point == the last committed offset
        let last_committed_offset = AtomicU64::new(log_state.release_pointer);
        let next_write_offset_raw = log_state.release_pointer + 1;
        let next_write_offset = Mutex::new(LogletOffset::from(next_write_offset_raw));
        let release_pointer = LogletOffset::from(log_state.release_pointer);
        let sealed = AtomicBool::new(log_state.seal);
        let append_latency = histogram!(BIFROST_LOCAL_APPEND_DURATION);
        let loglet = Self {
            loglet_id,
            log_store,
            log_writer,
            trim_point_offset,
            trim_point_lock: Mutex::new(()),
            next_write_offset,
            last_committed_offset,
            sealed,
            tail_watch: TailOffsetWatch::new(TailState::new(
                log_state.seal,
                release_pointer.next(),
            )),
            append_latency,
        };
        debug!(
            loglet_id = loglet_id,
            release_pointer = %release_pointer,
            next_offset = next_write_offset_raw,
            "Local loglet started"
        );

        Ok(loglet)
    }

    #[inline]
    fn notify_readers(&self, sealed: bool, release_pointer: LogletOffset) {
        // tail is beyond the release pointer
        self.tail_watch.notify(sealed, release_pointer.next());
    }
}

#[async_trait]
impl LogletBase for LocalLoglet {
    type Offset = LogletOffset;

    async fn create_read_stream(
        self: Arc<Self>,
        filter: KeyFilter,
        from: Self::Offset,
        to: Option<Self::Offset>,
    ) -> Result<SendableLogletReadStream<Self::Offset>, OperationError> {
        Ok(Box::pin(
            LocalLogletReadStream::create(self, filter, from, to).await?,
        ))
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<Self::Offset>> {
        Box::pin(self.tail_watch.to_stream())
    }

    async fn append_batch(
        &self,
        payloads: Arc<[ErasedInputRecord]>,
    ) -> Result<LogletOffset, AppendError> {
        // An initial check if we are sealed or not, we are not worried about accepting an
        // append while sealing is taking place. We only care about *not* acknowledging
        // it if we lost the race and the seal was completed while waiting on this append.
        if self.sealed.load(Ordering::Relaxed) {
            return Err(AppendError::Sealed);
        }

        let num_payloads = payloads.len();
        counter!(BIFROST_LOCAL_APPEND).increment(num_payloads as u64);
        let start_time = std::time::Instant::now();
        // We hold the lock to ensure that offsets are enqueued in the order of
        // their offsets in the logstore writer. This means that acknowledgements
        // that an offset N from the writer imply that all previous offsets have
        // been durably committed, therefore, such offsets can be released to readers.
        let (receiver, offset) = {
            let mut next_offset_guard = self.next_write_offset.lock().await;
            // lock acquired
            let receiver = self
                .log_writer
                .enqueue_put_records(self.loglet_id, *next_offset_guard, payloads)
                .await?;
            // next offset points to the next available slot.
            *next_offset_guard = *next_offset_guard + num_payloads;
            (receiver, next_offset_guard.prev())
            // lock dropped
        };

        let _ = receiver.await.unwrap_or_else(|_| {
            warn!("Unsure if the local loglet record was written, the ack channel was dropped");
            Err(ShutdownError.into())
        })?;

        // AcqRel to ensure that the offset is visible to other threads and to synchronize sealed
        // with find_tail.
        let release_pointer = LogletOffset::from(
            self.last_committed_offset
                .fetch_max(offset.into(), Ordering::AcqRel)
                .max(offset.into()),
        );
        let is_sealed = self.sealed.load(Ordering::Relaxed);
        self.notify_readers(is_sealed, release_pointer);
        // Ensure that we don't acknowledge the append (even that it has happened) if the loglet
        // has been sealed already.
        if is_sealed {
            return Err(AppendError::Sealed);
        }
        self.append_latency.record(start_time.elapsed());
        Ok(offset)
    }

    async fn find_tail(&self) -> Result<TailState<LogletOffset>, OperationError> {
        // `fetch_add(0)` with Release ordering to enforce using last_committed_offset as a memory
        // barrier and synchronization point with other threads.
        let last_committed =
            LogletOffset::from(self.last_committed_offset.fetch_add(0, Ordering::Release)).next();
        Ok(if self.sealed.load(Ordering::Relaxed) {
            TailState::Sealed(last_committed)
        } else {
            TailState::Open(last_committed)
        })
    }

    async fn get_trim_point(&self) -> Result<Option<Self::Offset>, OperationError> {
        let current_trim_point = LogletOffset(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point == LogletOffset::INVALID {
            Ok(None)
        } else {
            Ok(Some(current_trim_point))
        }
    }

    /// Trim the log to the minimum of new_trim_point and last_committed_offset
    /// new_trim_point is inclusive (will be trimmed)
    async fn trim(&self, new_trim_point: Self::Offset) -> Result<(), OperationError> {
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
            .enqueue_trim(self.loglet_id, current_trim_point, effective_trim_point)
            .await?;

        histogram!(BIFROST_LOCAL_TRIM_LENGTH).record(
            u32::try_from(effective_trim_point.0 - current_trim_point.0).unwrap_or(u32::MAX),
        );

        Ok(())
    }

    async fn seal(&self) -> Result<(), OperationError> {
        if self.sealed.load(Ordering::Relaxed) {
            return Ok(());
        }
        let receiver = self.log_writer.enqueue_seal(self.loglet_id).await?;
        let _ = receiver.await.unwrap_or_else(|_| {
            warn!("Unsure if the local loglet record was sealed, the ack channel was dropped");
            Err(ShutdownError.into())
        })?;
        self.sealed.store(true, Ordering::Relaxed);
        self.tail_watch.notify_seal();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use restate_core::TestCoreEnvBuilder;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::live::Live;
    use restate_types::logs::metadata::{LogletParams, ProviderKind};

    use crate::loglet::Loglet;

    use super::*;

    macro_rules! run_test {
        ($test:ident) => {
            paste::paste! {
                #[tokio::test(start_paused = true)]
                async fn [<local_loglet_  $test>]() -> googletest::Result<()> {
                    run_in_test_env(crate::loglet::loglet_tests::$test).await
                }
            }
        };
    }

    async fn run_in_test_env<F, O>(mut future: F) -> googletest::Result<()>
    where
        F: FnMut(Arc<dyn Loglet>) -> O,
        O: std::future::Future<Output = googletest::Result<()>>,
    {
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

                let loglet = Arc::new(LocalLoglet::create(
                    params
                        .as_str()
                        .parse()
                        .expect("loglet params can be converted into u64"),
                    log_store,
                    log_writer,
                )?);

                future(loglet).await
            })
            .await?;
        node_env.tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    run_test!(gapless_loglet_smoke_test);
    run_test!(single_loglet_readstream);
    run_test!(single_loglet_readstream_with_trims);
    run_test!(append_after_seal);
    run_test!(seal_empty);

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_loglet_append_after_seal_concurrent() -> googletest::Result<()> {
        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        node_env
            .tc
            .run_in_scope("test", None, async {
                let config = Live::from_value(Configuration::default());
                RocksDbManager::init(config.clone().map(|c| &c.common));

                let log_store = RocksDbLogStore::create(
                    &config.pinned().bifrost.local,
                    config.clone().map(|c| &c.bifrost.local.rocksdb).boxed(),
                )
                .await?;

                let log_writer = log_store
                    .create_writer()
                    .start(config.clone().map(|c| &c.bifrost.local).boxed())?;

                // Run the test 10 times
                for i in 1..=10 {
                    let loglet = Arc::new(LocalLoglet::create(
                        i,
                        log_store.clone(),
                        log_writer.clone(),
                    )?);
                    crate::loglet::loglet_tests::append_after_seal_concurrent(loglet).await?;
                }

                googletest::Result::Ok(())
            })
            .await?;
        node_env.tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
