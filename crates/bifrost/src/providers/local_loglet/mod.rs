// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
mod record_format;

pub use self::provider::Factory;

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use async_trait::async_trait;
use futures::stream::BoxStream;
use metrics::{Histogram, counter, histogram};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use restate_core::ShutdownError;
use restate_types::logs::TailOffsetWatch;
use restate_types::logs::{KeyFilter, LogletId, LogletOffset, Record, SequenceNumber, TailState};

use self::log_store::LogStoreError;
use self::log_store::RocksDbLogStore;
use self::log_store_writer::RocksDbLogWriterHandle;
use self::metric_definitions::{BIFROST_LOCAL_APPEND, BIFROST_LOCAL_APPEND_DURATION};
use self::read_stream::LocalLogletReadStream;
use crate::Result;
use crate::loglet::{
    FindTailOptions, Loglet, LogletCommit, OperationError, SendableLogletReadStream,
};
use crate::providers::local_loglet::metric_definitions::{
    BIFROST_LOCAL_TRIM, BIFROST_LOCAL_TRIM_LENGTH,
};

#[derive(derive_more::Debug)]
struct LocalLoglet {
    loglet_id: u64,
    #[debug(skip)]
    log_store: RocksDbLogStore,
    #[debug(skip)]
    log_writer: RocksDbLogWriterHandle,
    // internal offset _before_ the loglet head. Loglet head is trim_point_offset.next()
    trim_point_offset: AtomicU32,
    // used to order concurrent trim operations :-(
    #[debug(skip)]
    trim_point_lock: Mutex<()>,
    // In local loglet, the release point == the last committed offset
    last_committed_offset: AtomicU32,
    #[debug(skip)]
    next_write_offset: Mutex<LogletOffset>,
    sealed: AtomicBool,
    // watches the tail state of this loglet
    #[debug(skip)]
    tail_watch: TailOffsetWatch,
    #[debug(skip)]
    append_latency: Histogram,
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

        let trim_point_offset = AtomicU32::new(log_state.trim_point);
        // In local loglet, the release point == the last committed offset
        let last_committed_offset = AtomicU32::new(log_state.release_pointer);
        let next_write_offset_raw = log_state.release_pointer + 1;
        let next_write_offset = Mutex::new(LogletOffset::from(next_write_offset_raw));
        let release_pointer = LogletOffset::from(log_state.release_pointer);
        let sealed = AtomicBool::new(log_state.sealed);
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
                log_state.sealed,
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
    fn notify_readers(&self, release_pointer: LogletOffset) {
        // The tail is beyond the release pointer; only `seal` seals the watch.
        self.tail_watch.notify_offset_update(release_pointer.next());
    }
}

#[async_trait]
impl Loglet for LocalLoglet {
    fn id(&self) -> Option<LogletId> {
        Some(LogletId::from(self.loglet_id))
    }

    fn debug_str(&self) -> Cow<'static, str> {
        Cow::from(format!("local/{}", LogletId::from(self.loglet_id)))
    }

    async fn create_read_stream(
        self: Arc<Self>,
        filter: KeyFilter,
        from: LogletOffset,
        to: Option<LogletOffset>,
    ) -> Result<SendableLogletReadStream, OperationError> {
        Ok(Box::pin(
            LocalLogletReadStream::create(self, filter, from, to).await?,
        ))
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<LogletOffset>> {
        Box::pin(self.tail_watch.to_stream())
    }

    async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, OperationError> {
        // NOTE: This implementation doesn't perform pipelined writes yet. This will block the caller
        // while the underlying write is in progress and only return the Commit future as resolved.
        // This is temporary until pipelined writes are fully supported.

        // An initial check if we are sealed or not, we are not worried about accepting an
        // append while sealing is taking place. We only care about *not* acknowledging
        // it if we lost the race and the seal was completed while waiting on this append.
        if self.sealed.load(Ordering::Relaxed) {
            return Ok(LogletCommit::sealed());
        }

        // Do not allow more than 65k records in a single batch!
        assert!(payloads.len() <= u16::MAX as usize);
        let num_payloads = payloads.len() as u32;

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

        receiver.await.unwrap_or_else(|_| {
            warn!("Unsure if the local loglet record was written, the ack channel was dropped");
            Err(ShutdownError.into())
        })?;

        // Publish the offset, to `find_tail` and to the tail watch, before reading `sealed`.
        // An append acknowledged below thus read `sealed` as false before `seal` set it, and a
        // tail that reads `sealed` as true covers this offset (see `find_tail`).
        let release_pointer = LogletOffset::from(
            self.last_committed_offset
                .fetch_max(offset.into(), Ordering::SeqCst)
                .max(offset.into()),
        );
        self.notify_readers(release_pointer);
        // Ensure that we don't acknowledge the append (even that it has happened) if the loglet
        // has been sealed already.
        if self.sealed.load(Ordering::SeqCst) {
            return Ok(LogletCommit::sealed());
        }
        self.append_latency.record(start_time.elapsed());
        Ok(LogletCommit::resolved(offset))
    }

    async fn find_tail(
        &self,
        _: FindTailOptions,
    ) -> Result<TailState<LogletOffset>, OperationError> {
        // `sealed` first, then the offset, both SeqCst like the writes in `enqueue_batch` and
        // `seal`: if `sealed` reads true, every append that read it as false, and so was
        // acknowledged, raised the offset before this read. A sealed tail thus covers every
        // acknowledged append.
        let sealed = self.sealed.load(Ordering::SeqCst);
        let last_committed =
            LogletOffset::from(self.last_committed_offset.load(Ordering::SeqCst)).next();
        Ok(if sealed {
            TailState::Sealed(last_committed)
        } else {
            TailState::Open(last_committed)
        })
    }

    async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError> {
        let current_trim_point = LogletOffset::new(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point == LogletOffset::INVALID {
            Ok(None)
        } else {
            Ok(Some(current_trim_point))
        }
    }

    /// Trim the log to the minimum of new_trim_point and last_committed_offset
    /// new_trim_point is inclusive (will be trimmed)
    async fn trim(&self, new_trim_point: LogletOffset) -> Result<(), OperationError> {
        let effective_trim_point = new_trim_point.min(LogletOffset::new(
            self.last_committed_offset.load(Ordering::Relaxed),
        ));

        // the lock is needed to prevent concurrent trim operations from over taking each other :-(
        // The problem is that the LogStoreWriter is not the component holding the ground truth
        // but the LocalLoglet is. The bad thing that could happen is that we might not trim earlier
        // parts in case two trim operations get reordered and we crash before applying the second.
        let _trim_point_lock_guard = self.trim_point_lock.lock().await;

        let current_trim_point = LogletOffset::new(self.trim_point_offset.load(Ordering::Relaxed));

        if current_trim_point >= effective_trim_point {
            // nothing to do since we have already trimmed beyond new_trim_point
            return Ok(());
        }

        counter!(BIFROST_LOCAL_TRIM).increment(1);

        // no compare & swap operation is needed because we are operating under the trim point lock
        self.trim_point_offset
            .store(*effective_trim_point, Ordering::Relaxed);

        self.log_writer
            .enqueue_trim(self.loglet_id, current_trim_point, effective_trim_point)
            .await?;

        histogram!(BIFROST_LOCAL_TRIM_LENGTH).record(*effective_trim_point - *current_trim_point);

        debug!(
            loglet_id = self.loglet_id,
            ?current_trim_point,
            ?effective_trim_point,
            "Loglet trim operation enqueued"
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
        self.sealed.store(true, Ordering::SeqCst);
        self.tail_watch.notify_seal();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::TryStreamExt;
    use googletest::prelude::{elements_are, eq, err, pat};
    use googletest::{IntoTestResult, assert_that};
    use test_log::test;

    use restate_core::{TaskCenter, TestCoreEnvBuilder};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{Configuration, LocalLogletOptionsBuilder};
    use restate_types::errors::MaybeRetryableError;
    use restate_types::live::{Live, LiveLoadExt};
    use restate_types::logs::Keys;
    use restate_types::logs::metadata::{LogletParams, ProviderKind};

    use super::*;
    use crate::loglet::Loglet;

    macro_rules! run_test {
        ($test:ident) => {
            paste::paste! {
                #[restate_core::test(start_paused = true)]
                async fn [<local_loglet_  $test>]() -> googletest::Result<()> {
                    let loglet = create_loglet().await.into_test_result()?;
                    crate::loglet::loglet_tests::$test(loglet).await?;
                    Ok(())
                }
            }
        };
    }

    async fn create_loglet() -> anyhow::Result<Arc<LocalLoglet>> {
        let _node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let config = Live::from_value(Configuration::default());
        RocksDbManager::init();
        let params = LogletParams::from("42".to_string());

        let local_loglet_config = config.map(|config| &config.bifrost.local);
        let log_store = RocksDbLogStore::create(local_loglet_config.clone()).await?;

        let log_writer = log_store.create_writer().start(local_loglet_config)?;

        let loglet = Arc::new(LocalLoglet::create(
            params
                .parse()
                .expect("loglet params can be converted into u64"),
            log_store,
            log_writer,
        )?);

        Ok(loglet)
    }

    run_test!(gapless_loglet_smoke_test);
    run_test!(single_loglet_readstream);
    run_test!(single_loglet_readstream_with_trims);
    run_test!(append_after_seal);
    run_test!(seal_empty);

    #[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_loglet_append_after_seal_concurrent() -> googletest::Result<()> {
        let _node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let config = Live::from_value(Configuration::default());
        RocksDbManager::init();

        let local_loglet_config = config.map(|config| &config.bifrost.local);
        let log_store = RocksDbLogStore::create(local_loglet_config.clone()).await?;

        let log_writer = log_store.create_writer().start(local_loglet_config)?;

        // Run the test 10 times
        for i in 1..=10 {
            let loglet = Arc::new(LocalLoglet::create(
                i,
                log_store.clone(),
                log_writer.clone(),
            )?);
            crate::loglet::loglet_tests::append_after_seal_concurrent(loglet).await?;
        }

        Ok(())
    }

    /// A failed RocksDB write fails its append, drops already-queued and subsequent writes across
    /// all loglets sharing the writer (avoiding offset holes), triggers node shutdown, and leaves
    /// durable state at the last committed record for restart recovery.
    #[test(restate_core::test)]
    async fn append_is_not_acked_when_write_fails() -> googletest::Result<()> {
        let _node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;
        RocksDbManager::init();

        // Batch size of 1 ensures the second concurrent append queues into the next batch rather
        // than coalescing into the failing write batch.
        let mut config = Configuration::default();
        config.bifrost.local = LocalLogletOptionsBuilder::default()
            .writer_batch_commit_count(1)
            .build()?;
        let local_loglet_config = Live::from_value(config).map(|config| &config.bifrost.local);

        let log_store = RocksDbLogStore::create(local_loglet_config.clone()).await?;
        let writer = log_store.create_writer();
        let fail_next_commit = writer.fail_next_commit();
        let log_writer = writer.start(local_loglet_config)?;
        let loglet = Arc::new(LocalLoglet::create(
            42,
            log_store.clone(),
            log_writer.clone(),
        )?);
        let other_loglet = LocalLoglet::create(43, log_store.clone(), log_writer.clone())?;

        // Baseline: a healthy append commits at offset 1 and moves the tail to 2.
        let first_offset = loglet
            .enqueue_batch(vec![Record::from("first")].into())
            .await?
            .await?;
        assert_that!(first_offset, eq(LogletOffset::OLDEST));
        assert_that!(
            loglet.find_tail(FindTailOptions::default()).await?,
            eq(TailState::Open(LogletOffset::from(2)))
        );

        // One-shot write failure: the failing append returns a terminal error, and an append
        // already queued behind it fails with Shutdown instead of committing at offset 3.
        fail_next_commit.store(true, Ordering::Release);
        let (lost, queued) = tokio::join!(
            loglet.enqueue_batch(vec![Record::from("lost")].into()),
            loglet.enqueue_batch(vec![Record::from("queued")].into()),
        );
        let lost_err = lost
            .map(|_| ())
            .expect_err("failed RocksDB write must not be acknowledged");
        assert!(
            !lost_err.retryable(),
            "write error should be terminal: {lost_err}"
        );
        assert_that!(lost_err, pat!(OperationError::Other(_)));
        assert_that!(queued.map(|_| ()), err(pat!(OperationError::Shutdown(_))));
        assert!(
            !fail_next_commit.load(Ordering::Acquire),
            "one-shot failure was consumed so RocksDB would accept subsequent writes"
        );

        // Subsequent operations fail with Shutdown even though RocksDB would now accept writes —
        // both on this loglet and on other loglets sharing the writer.
        assert_that!(
            loglet
                .enqueue_batch(vec![Record::from("after-lost")].into())
                .await
                .map(|_| ()),
            err(pat!(OperationError::Shutdown(_)))
        );
        assert_that!(
            other_loglet
                .enqueue_batch(vec![Record::from("other")].into())
                .await
                .map(|_| ()),
            err(pat!(OperationError::Shutdown(_)))
        );
        assert_that!(loglet.seal().await, err(pat!(OperationError::Shutdown(_))));

        // In-memory tail and readable records cover only the committed record.
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_that!(tail, eq(TailState::Open(LogletOffset::from(2))));
        let read_records: Vec<_> = loglet
            .clone()
            .create_read_stream(
                KeyFilter::Any,
                LogletOffset::OLDEST,
                Some(tail.offset().prev()),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|entry| (entry.sequence_number(), entry.decode_unchecked::<String>()))
            .collect();
        assert_that!(
            read_records,
            elements_are![eq((LogletOffset::OLDEST, "first".to_owned()))]
        );

        // The writer task failure triggers node shutdown.
        tokio::time::timeout(
            Duration::from_secs(5),
            TaskCenter::current().shutdown_token().cancelled(),
        )
        .await
        .expect("writer failure should trigger node shutdown");

        // Reopening from RocksDB recovers the last durable offset (1), so after a restart writing
        // resumes at offset 2 with neither a hole nor reuse of an acknowledged offset.
        let reopened = LocalLoglet::create(42, log_store, log_writer)?;
        assert_that!(
            reopened.find_tail(FindTailOptions::default()).await?,
            eq(TailState::Open(LogletOffset::from(2)))
        );
        assert_that!(
            *reopened.next_write_offset.lock().await,
            eq(LogletOffset::from(2))
        );

        Ok(())
    }

    #[test(restate_core::test)]
    async fn read_stream_with_filters() -> googletest::Result<()> {
        let loglet = create_loglet().await.into_test_result()?;
        let batch: Arc<[Record]> = vec![
            ("record-1", Keys::Single(1)).into(),
            ("record-2", Keys::Single(2)).into(),
            ("record-3", Keys::Single(1)).into(),
        ]
        .into();
        let offset = loglet.enqueue_batch(batch).await?.await?;

        let key_filter = KeyFilter::Include(1);
        let read_stream = loglet
            .create_read_stream(key_filter, LogletOffset::OLDEST, Some(offset))
            .await?;

        let records: Vec<_> = read_stream
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|log_entry| {
                (
                    log_entry.sequence_number(),
                    log_entry.decode_unchecked::<String>(),
                )
            })
            .collect();

        assert_that!(
            records,
            elements_are![
                eq((LogletOffset::from(1), "record-1".to_owned())),
                eq((LogletOffset::from(3), "record-3".to_owned()))
            ]
        );

        Ok(())
    }
}
