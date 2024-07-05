// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use bytes::BytesMut;
use enum_map::EnumMap;

use smallvec::SmallVec;
use tracing::instrument;

use restate_core::{Metadata, MetadataKind};
use restate_types::logs::metadata::{ProviderKind, Segment};
use restate_types::logs::{LogId, Lsn, Payload, SequenceNumber};
use restate_types::storage::StorageCodec;
use restate_types::Version;

use crate::loglet::{LogletBase, LogletWrapper};
use crate::watchdog::WatchdogSender;
use crate::{
    Error, FindTailAttributes, LogReadStream, LogRecord, LogletProvider, Result, TailState,
    SMALL_BATCH_THRESHOLD_COUNT,
};

/// Bifrost is Restate's durable interconnect system
///
/// Bifrost is a mutable-friendly handle to access the system. You don't need
/// to wrap this in an Arc or a lock, pass it around by reference or by a clone.
/// Bifrost handle is relatively cheap to clone.
#[derive(Clone)]
pub struct Bifrost {
    inner: Arc<BifrostInner>,
    metadata: Metadata,
}

impl Bifrost {
    pub(crate) fn new(inner: Arc<BifrostInner>, metadata: Metadata) -> Self {
        Self { inner, metadata }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn init_in_memory(metadata: Metadata) -> Self {
        use crate::loglets::memory_loglet;

        Self::init_with_factory(metadata, memory_loglet::Factory::default()).await
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn init_local(metadata: Metadata) -> Self {
        use restate_types::config::Configuration;

        use crate::BifrostService;

        let config = Configuration::updateable();
        let bifrost_svc =
            BifrostService::new(restate_core::task_center(), metadata).enable_local_loglet(&config);
        let bifrost = bifrost_svc.handle();

        // start bifrost service in the background
        bifrost_svc
            .start()
            .await
            .expect("in memory loglet must start");
        bifrost
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn init_with_factory(
        metadata: Metadata,
        factory: impl crate::LogletProviderFactory,
    ) -> Self {
        use crate::BifrostService;

        let bifrost_svc =
            BifrostService::new(restate_core::task_center(), metadata).with_factory(factory);
        let bifrost = bifrost_svc.handle();

        // start bifrost service in the background
        bifrost_svc
            .start()
            .await
            .expect("in memory loglet must start");
        bifrost
    }

    /// Appends a single record to a log. The log id must exist, otherwise the
    /// operation fails with [`Error::UnknownLogId`]
    #[instrument(level = "debug", skip(self, payload), err)]
    pub async fn append(&self, log_id: LogId, payload: Payload) -> Result<Lsn> {
        self.inner.append(log_id, payload).await
    }

    /// Appends a batch of records to a log. The log id must exist, otherwise the
    /// operation fails with [`Error::UnknownLogId`]. The returned Lsn is the Lsn of the first
    /// record in this batch. This will only return after all records have been stored.
    #[instrument(level = "debug", skip(self, payloads), err)]
    pub async fn append_batch(&self, log_id: LogId, payloads: &[Payload]) -> Result<Lsn> {
        self.inner.append_batch(log_id, payloads).await
    }

    /// Read the next record from the LSN provided. The `from` indicates the LSN where we will
    /// start reading from. This means that the record returned will have a LSN that is equal or greater than
    /// `from`. If no records are committed yet at this LSN, this read operation will "wait"
    /// for such records to appear.
    pub async fn read_next_single(&self, log_id: LogId, from: Lsn) -> Result<LogRecord> {
        self.inner.read_next_single(log_id, from).await
    }

    /// Read the next record from the LSN provided. The `from` indicates the LSN where we will
    /// start reading from. This means that the record returned will have a LSN that is equal or greater than
    /// `from`. If no records are committed yet at this LSN, this read operation will "wait"
    /// for such records to appear.
    pub async fn read_next_single_opt(
        &self,
        log_id: LogId,
        from: Lsn,
    ) -> Result<Option<LogRecord>> {
        self.inner.read_next_single_opt(log_id, from).await
    }

    /// Create a read stream. `end_lsn` is inclusive. Pass [[`Lsn::Max`]] for a tailing stream. Use
    /// Lsn::OLDEST in `from` to read from the start (head) of the log.
    ///
    /// When using this in conjunction with `find_tail()` or `get_trim_point()`, note that
    /// you'll need to offset start and end to ensure you are reading within the bounds of the log.
    ///
    /// For instance, if you intend to read all records from a log
    ///
    /// ```no_run
    /// let reader = bifrost.create_reader(
    ///        log_id,
    ///        bifrost.get_trim_point(log_id).await.next(),
    ///        bifrost.find_tail(log_id).await().offset().prev(),
    ///     ).await;
    /// ```
    pub async fn create_reader(
        &self,
        log_id: LogId,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Result<LogReadStream> {
        LogReadStream::create(self.inner.clone(), log_id, start_lsn, end_lsn).await
    }

    /// The tail is *the first unwritten LSN* in the log
    ///
    /// Finds the first available LSN after the durable tail of the log.
    ///
    /// If the log is empty, it return TailState::Open(Lsn::OLDEST).
    /// This should never return Err(Error::LogSealed). Sealed state is represented as
    /// TailState::Sealed(..)
    pub async fn find_tail(
        &self,
        log_id: LogId,
        attributes: FindTailAttributes,
    ) -> Result<TailState> {
        Ok(self.inner.find_tail(log_id, attributes).await?.1)
    }

    /// The lsn of the slot **before** the first readable record (if it exists), or the offset
    /// before the next slot that will be written to. Another way to think about `get_trim_point`
    /// is that it points to the last lsn that was trimmed/deleted.
    ///
    /// Returns Lsn::INVALID if the log was never trimmed
    pub async fn get_trim_point(&self, log_id: LogId) -> Result<Lsn, Error> {
        self.inner.get_trim_point(log_id).await
    }

    /// Trim the log prefix up to and including the `trim_point`.
    /// Set `before_lsn` to the value returned from `find_tail()` or `Lsn::MAX` to
    /// trim all records of the log.
    ///
    /// Note that trim does not promise that the log will be trimmed immediately and atomically,
    /// but they are promise to trim prefixes, earlier segments in the log will be trimmed
    /// before later segments and the log will not be in an inconsistent state at any point.
    #[instrument(level = "debug", skip(self), err)]
    pub async fn trim(&self, log_id: LogId, trim_point: Lsn) -> Result<(), Error> {
        self.inner.trim(log_id, trim_point).await
    }

    /// The version of the currently loaded logs metadata
    pub fn version(&self) -> Version {
        self.metadata.logs_version()
    }

    #[cfg(test)]
    pub fn inner(&self) -> Arc<BifrostInner> {
        self.inner.clone()
    }

    /// Read a full log with the given id. To be used only in tests!!!
    #[cfg(any(test, feature = "test-util"))]
    pub async fn read_all(&self, log_id: LogId) -> Result<Vec<LogRecord>> {
        use futures::TryStreamExt;

        self.inner.fail_if_shutting_down()?;

        let current_tail = self
            .find_tail(log_id, FindTailAttributes::default())
            .await?;

        if current_tail.offset() <= Lsn::OLDEST {
            return Ok(Vec::default());
        }

        let reader = self
            .create_reader(log_id, Lsn::OLDEST, current_tail.offset().prev())
            .await?;
        reader.try_collect().await
    }
}

// compile-time check
static_assertions::assert_impl_all!(Bifrost: Send, Sync, Clone);

// Locks in this data-structure are held for very short time and should never be
// held across an async boundary.
pub struct BifrostInner {
    metadata: Metadata,
    #[allow(unused)]
    watchdog: WatchdogSender,
    // Initialized after BifrostService::start completes.
    pub(crate) providers: OnceLock<EnumMap<ProviderKind, Option<Arc<dyn LogletProvider>>>>,
    shutting_down: AtomicBool,
}

impl BifrostInner {
    pub fn new(metadata: Metadata, watchdog: WatchdogSender) -> Self {
        Self {
            metadata,
            watchdog,
            providers: Default::default(),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Indicates that an ongoing shutdown/drain is in progress. New writes and
    /// reads will be rejected during shutdown, but in-flight operations are
    /// allowed to complete.
    pub fn set_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
    }

    /// Appends a single record to a log. The log id must exist, otherwise the
    /// operation fails with [`Error::UnknownLogId`]
    pub async fn append(&self, log_id: LogId, payload: Payload) -> Result<Lsn> {
        self.fail_if_shutting_down()?;
        let loglet = self.writeable_loglet(log_id).await?;
        let mut buf = BytesMut::default();
        StorageCodec::encode(payload, &mut buf).expect("serialization to bifrost is infallible");
        loglet.append(buf.freeze()).await
    }

    pub async fn append_batch(&self, log_id: LogId, payloads: &[Payload]) -> Result<Lsn> {
        let loglet = self.writeable_loglet(log_id).await?;
        let raw_payloads: SmallVec<[_; SMALL_BATCH_THRESHOLD_COUNT]> = payloads
            .iter()
            .map(|payload| {
                let mut buf = BytesMut::new();
                StorageCodec::encode(payload, &mut buf)
                    .expect("serialization to bifrost is infallible");
                buf.freeze()
            })
            .collect();
        loglet.append_batch(&raw_payloads).await
    }

    pub async fn read_next_single(&self, log_id: LogId, after: Lsn) -> Result<LogRecord> {
        self.fail_if_shutting_down()?;

        let loglet = self.find_loglet_for_lsn(log_id, after.next()).await?;
        Ok(loglet
            .read_next_single(after)
            .await?
            .decode()
            .expect("decoding a bifrost envelope succeeds"))
    }

    pub async fn read_next_single_opt(
        &self,
        log_id: LogId,
        from: Lsn,
    ) -> Result<Option<LogRecord>> {
        self.fail_if_shutting_down()?;

        let loglet = self.find_loglet_for_lsn(log_id, from).await?;
        Ok(loglet.read_next_single_opt(from).await?.map(|record| {
            record
                .decode()
                .expect("decoding a bifrost envelope succeeds")
        }))
    }

    pub async fn find_tail(
        &self,
        log_id: LogId,
        _attributes: FindTailAttributes,
    ) -> Result<(LogletWrapper, TailState)> {
        self.fail_if_shutting_down()?;
        let loglet = self.writeable_loglet(log_id).await?;
        let tail = loglet.find_tail().await?;
        Ok((loglet, tail))
    }

    async fn get_trim_point(&self, log_id: LogId) -> Result<Lsn, Error> {
        self.fail_if_shutting_down()?;

        let logs = self.metadata.logs().ok_or(Error::UnknownLogId(log_id))?;
        let log_chain = logs.logs.get(&log_id).ok_or(Error::UnknownLogId(log_id))?;

        let mut trim_point = None;

        // Iterate over the chain until we find the first missing trim point, return value before
        // todo: maybe update configuration to remember trim point for the whole chain
        // todo: support multiple segments.
        // todo: dispatch loglet deletion in the background when entire segments are trimmed
        for segment in log_chain.iter() {
            let loglet = self.get_loglet(&segment).await?;
            let loglet_specific_trim_point = loglet.get_trim_point().await?;

            // if a loglet has no trim point, then all subsequent loglets should also not contain a trim point
            if loglet_specific_trim_point.is_none() {
                break;
            }

            trim_point = loglet_specific_trim_point;
        }

        Ok(trim_point.unwrap_or(Lsn::INVALID))
    }

    async fn trim(&self, log_id: LogId, trim_point: Lsn) -> Result<(), Error> {
        self.fail_if_shutting_down()?;

        let logs = self.metadata.logs().ok_or(Error::UnknownLogId(log_id))?;
        let log_chain = logs.logs.get(&log_id).ok_or(Error::UnknownLogId(log_id))?;

        for segment in log_chain.iter() {
            let loglet = self.get_loglet(&segment).await?;

            if loglet.base_lsn >= trim_point {
                break;
            }

            loglet.trim(trim_point).await?;
        }
        // todo: Update logs configuration to remove sealed and empty loglets
        Ok(())
    }

    #[inline]
    fn fail_if_shutting_down(&self) -> Result<()> {
        if self.shutting_down.load(Ordering::Relaxed) {
            Err(Error::Shutdown(restate_core::ShutdownError))
        } else {
            Ok(())
        }
    }

    /// Immediately fetch new metadata from metadata store.
    pub async fn sync_metadata(&self) -> Result<()> {
        self.fail_if_shutting_down()?;
        self.metadata
            .sync(MetadataKind::Logs)
            .await
            .map_err(Arc::new)?;
        Ok(())
    }

    // --- Helper functions --- //
    /// Get the provider for a given kind. A provider must be enabled and BifrostService **must**
    /// be started before calling this.
    fn provider_for(&self, kind: ProviderKind) -> Result<&Arc<dyn LogletProvider>> {
        let providers = self
            .providers
            .get()
            .expect("BifrostService must be started prior to using Bifrost");

        providers[kind]
            .as_ref()
            .ok_or_else(|| Error::Disabled(kind.to_string()))
    }

    async fn writeable_loglet(&self, log_id: LogId) -> Result<LogletWrapper> {
        let tail_segment = self
            .metadata
            .logs()
            .and_then(|logs| logs.tail_segment(log_id))
            .ok_or(Error::UnknownLogId(log_id))?;
        self.get_loglet(&tail_segment).await
    }

    pub(crate) async fn find_loglet_for_lsn(
        &self,
        log_id: LogId,
        lsn: Lsn,
    ) -> Result<LogletWrapper> {
        let segment = self
            .metadata
            .logs()
            .and_then(|logs| logs.find_segment_for_lsn(log_id, lsn))
            .ok_or(Error::UnknownLogId(log_id))?;
        self.get_loglet(&segment).await
    }

    async fn get_loglet(&self, segment: &Segment) -> Result<LogletWrapper, Error> {
        let provider = self.provider_for(segment.config.kind)?;
        let loglet = provider.get_loglet(&segment.config.params).await?;
        Ok(LogletWrapper::new(segment.base_lsn, loglet))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    use crate::loglets::memory_loglet::{self};
    use googletest::prelude::*;

    use crate::{Record, TrimGap};
    use restate_core::{metadata, TestCoreEnv};
    use restate_core::{task_center, TestCoreEnvBuilder};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::CommonOptions;
    use restate_types::live::Constant;
    use restate_types::logs::SequenceNumber;
    use restate_types::partition_table::FixedPartitionTable;
    use test_log::test;
    use tracing::info;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_append_smoke() -> googletest::Result<()> {
        let num_partitions = 5;
        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .with_partition_table(FixedPartitionTable::new(Version::MIN, num_partitions))
            .build()
            .await;
        let tc = node_env.tc;
        tc.run_in_scope("test", None, async {
            let bifrost = Bifrost::init_in_memory(metadata()).await;

            let clean_bifrost_clone = bifrost.clone();

            let mut max_lsn = Lsn::INVALID;
            for i in 1..=5 {
                // Append a record to memory
                let lsn = bifrost.append(LogId::from(0), Payload::default()).await?;
                info!(%lsn, "Appended record to log");
                assert_eq!(Lsn::from(i), lsn);
                max_lsn = lsn;
            }

            // Append to a log that doesn't exist.
            let invalid_log = LogId::from(num_partitions + 1);
            let resp = bifrost.append(invalid_log, Payload::default()).await;

            assert_that!(resp, pat!(Err(pat!(Error::UnknownLogId(eq(invalid_log))))));

            // use a cloned bifrost.
            let cloned_bifrost = bifrost.clone();
            for _ in 1..=5 {
                // Append a record to memory
                let lsn = cloned_bifrost
                    .append(LogId::from(0), Payload::default())
                    .await?;
                info!(%lsn, "Appended record to log");
                assert_eq!(max_lsn + Lsn::from(1), lsn);
                max_lsn = lsn;
            }

            // Ensure original clone writes to the same underlying loglet.
            let lsn = clean_bifrost_clone
                .append(LogId::from(0), Payload::default())
                .await?;
            assert_eq!(max_lsn + Lsn::from(1), lsn);
            max_lsn = lsn;

            // Writes to a another log doesn't impact existing
            let lsn = bifrost.append(LogId::from(3), Payload::default()).await?;
            assert_eq!(Lsn::from(1), lsn);

            let lsn = bifrost.append(LogId::from(0), Payload::default()).await?;
            assert_eq!(max_lsn + Lsn::from(1), lsn);
            max_lsn = lsn;

            let tail = bifrost
                .find_tail(LogId::from(0), FindTailAttributes::default())
                .await?;
            assert_eq!(max_lsn.next(), tail.offset());

            // Initiate shutdown
            task_center().shutdown_node("completed", 0).await;
            // appends cannot succeed after shutdown
            let res = bifrost.append(LogId::from(0), Payload::default()).await;
            assert!(matches!(res, Err(Error::Shutdown(_))));
            // Validate the watchdog has called the provider::start() function.
            assert!(logs_contain("Shutting down in-memory loglet provider"));
            assert!(logs_contain("Bifrost watchdog shutdown complete"));
            Ok(())
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn test_lazy_initialization() -> googletest::Result<()> {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let tc = node_env.tc;
        tc.run_in_scope("test", None, async {
            let delay = Duration::from_secs(5);
            // This memory provider adds a delay to its loglet initialization, we want
            // to ensure that appends do not fail while waiting for the loglet;
            let factory = memory_loglet::Factory::with_init_delay(delay);
            let bifrost = Bifrost::init_with_factory(metadata(), factory).await;

            let start = tokio::time::Instant::now();
            let lsn = bifrost.append(LogId::from(0), Payload::default()).await?;
            assert_eq!(Lsn::from(1), lsn);
            // The append was properly delayed
            assert_eq!(delay, start.elapsed());
            Ok(())
        })
        .await
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn trim_log_smoke_test() -> googletest::Result<()> {
        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;
        node_env
            .tc
            .run_in_scope("test", None, async {
                RocksDbManager::init(Constant::new(CommonOptions::default()));

                let log_id = LogId::from(0);
                let bifrost = Bifrost::init_local(metadata()).await;

                assert_eq!(
                    Lsn::OLDEST,
                    bifrost
                        .find_tail(log_id, FindTailAttributes::default())
                        .await?
                        .offset()
                );

                assert_eq!(Lsn::INVALID, bifrost.get_trim_point(log_id).await?);

                // append 10 records
                for _ in 1..=10 {
                    bifrost.append(log_id, Payload::default()).await?;
                }

                bifrost.trim(log_id, Lsn::from(5)).await?;

                let tail = bifrost
                    .find_tail(log_id, FindTailAttributes::default())
                    .await?;
                assert_eq!(tail.offset(), Lsn::from(11));
                assert!(!tail.is_sealed());
                assert_eq!(Lsn::from(5), bifrost.get_trim_point(log_id).await?);

                // 5 itself is trimmed
                for lsn in 1..=5 {
                    let record = bifrost.read_next_single_opt(log_id, Lsn::from(lsn)).await?;
                    assert_that!(
                        record,
                        pat!(Some(pat!(LogRecord {
                            offset: eq(Lsn::from(lsn)),
                            record: pat!(Record::TrimGap(pat!(TrimGap {
                                to: eq(Lsn::from(5)),
                            })))
                        })))
                    )
                }

                for lsn in 6..=10 {
                    let record = bifrost.read_next_single_opt(log_id, Lsn::from(lsn)).await?;
                    assert_that!(
                        record,
                        pat!(Some(pat!(LogRecord {
                            offset: eq(Lsn::from(lsn)),
                            record: pat!(Record::Data(_))
                        })))
                    );
                }

                // trimming beyond the release point will fall back to the release point
                bifrost.trim(log_id, Lsn::MAX).await?;

                assert_eq!(
                    Lsn::from(11),
                    bifrost
                        .find_tail(log_id, FindTailAttributes::default())
                        .await?
                        .offset()
                );
                let new_trim_point = bifrost.get_trim_point(log_id).await?;
                assert_eq!(Lsn::from(10), new_trim_point);

                let record = bifrost
                    .read_next_single_opt(log_id, Lsn::from(10))
                    .await?
                    .unwrap();
                assert!(record.record.is_trim_gap());
                assert_eq!(Lsn::from(10), record.record.try_as_trim_gap().unwrap().to);

                // Add 10 more records
                for _ in 0..10 {
                    bifrost.append(log_id, Payload::default()).await?;
                }

                for lsn in 11..20 {
                    let record = bifrost.read_next_single_opt(log_id, Lsn::from(lsn)).await?;
                    assert_that!(
                        record,
                        pat!(Some(pat!(LogRecord {
                            offset: eq(Lsn::from(lsn)),
                            record: pat!(Record::Data(_))
                        })))
                    );
                }

                Ok(())
            })
            .await
    }
}
