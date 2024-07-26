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
use tracing::{info, instrument};

use restate_core::{Metadata, MetadataKind, TargetVersion};
use restate_types::config::Configuration;
use restate_types::logs::metadata::{MaybeSegment, ProviderKind, Segment};
use restate_types::logs::{LogId, Lsn, Payload, SequenceNumber};
use restate_types::storage::StorageCodec;
use restate_types::Version;

use crate::loglet::{AppendError, LogletBase, LogletProvider};
use crate::loglet_wrapper::LogletWrapper;
use crate::watchdog::WatchdogSender;
use crate::{
    Error, FindTailAttributes, LogReadStream, LogRecord, Result, TailState,
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
}

impl Bifrost {
    pub(crate) fn new(inner: Arc<BifrostInner>) -> Self {
        Self { inner }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn init_in_memory(metadata: Metadata) -> Self {
        use crate::providers::memory_loglet;

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
        factory: impl crate::loglet::LogletProviderFactory,
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
    /// `from`. If no records are committed yet at this LSN, this read operation will return
    /// `None`.
    pub async fn read(&self, log_id: LogId, from: Lsn) -> Result<Option<LogRecord>> {
        self.inner.read(log_id, from).await
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
    ///     );
    /// ```
    pub fn create_reader(
        &self,
        log_id: LogId,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Result<LogReadStream> {
        LogReadStream::create(self.inner.clone(), log_id, start_lsn, end_lsn)
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
    /// Set `trim_point` to the value returned from `find_tail()` or `Lsn::MAX` to
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
        self.inner.metadata.logs_version()
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

        let reader = self.create_reader(log_id, Lsn::OLDEST, current_tail.offset().prev())?;
        reader.try_collect().await
    }
}

// compile-time check
static_assertions::assert_impl_all!(Bifrost: Send, Sync, Clone);

// Locks in this data-structure are held for very short time and should never be
// held across an async boundary.
pub struct BifrostInner {
    pub(crate) metadata: Metadata,
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

        let res = loglet.append(buf.freeze()).await;
        // todo: Handle retries, segment seals and other recoverable errors.
        res.map_err(|e| match e {
            AppendError::Sealed => todo!(),
            AppendError::Shutdown(e) => Error::Shutdown(e),
            AppendError::Other(e) => Error::LogletError(e),
        })
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
        let res = loglet.append_batch(&raw_payloads).await;
        // todo: Handle retries, segment seals and other recoverable errors.
        res.map_err(|e| match e {
            AppendError::Sealed => todo!(),
            AppendError::Shutdown(e) => Error::Shutdown(e),
            AppendError::Other(e) => Error::LogletError(e),
        })
    }

    pub async fn read(&self, log_id: LogId, from: Lsn) -> Result<Option<LogRecord>> {
        self.fail_if_shutting_down()?;
        // Accidental reads from Lsn::INVALID are reset to Lsn::OLDEST
        let from = std::cmp::max(Lsn::OLDEST, from);

        let mut retry = Configuration::pinned()
            .bifrost
            .read_retry_policy
            .clone()
            .into_iter();

        let mut attempts = 0;
        let (known_tail, loglet) = loop {
            attempts += 1;
            let loglet = self.find_loglet_for_lsn(log_id, from).await?;
            // When reading from an open loglet segment, we need to make sure we need to be careful of
            // reading when the loglet is sealed (metadata is not updated yet, or reconfiguration is in
            // progress). If we observed that the loglet is sealed, we must wait until the tail is
            // determined and a new loglet segment is appended.
            match loglet.tail_lsn {
                Some(known_tail) => break (known_tail, loglet),
                None => {
                    // optimization to avoid find_tail if we know it's safe to perform this read.
                    if let Some(known_unsealed_tail) = loglet.last_known_unsealed_tail() {
                        if known_unsealed_tail > from {
                            // safe to proceed without find_tail.
                            break (known_unsealed_tail, loglet);
                        }
                    }

                    let tail = loglet.find_tail().await?;
                    match tail {
                        TailState::Open(tail) => {
                            break (tail, loglet);
                        }
                        TailState::Sealed(_) => {
                            // There we go. We need to wait for someone to finish the
                            // reconfiguration.
                            info!("Loglet is sealed, waiting for tail to be determined");
                            let Some(sleep_dur) = retry.next() else {
                                // exhausted retries
                                info!(
                                    loglet = ?loglet,
                                    "read() failed after {} attempts. Loglet reconfiguration was not completed on time",
                                    attempts
                                );
                                return Err(Error::ReadFailureDuringReconfiguration(log_id, from));
                            };
                            tokio::time::sleep(sleep_dur).await;
                            self.sync_metadata().await?;
                            continue;
                        }
                    }
                }
            }
        };

        // Are we reading at or beyond the established tail?
        if known_tail > from {
            Ok(loglet.read_opt(from).await?.map(|record| {
                record
                    .decode()
                    .expect("decoding a bifrost envelope succeeds")
            }))
        } else {
            Ok(None)
        }
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

        let log_metadata = self.metadata.logs();

        let log_chain = log_metadata
            .chain(&log_id)
            .ok_or(Error::UnknownLogId(log_id))?;

        let mut trim_point = None;

        // Iterate over the chain until we find the first missing trim point, return value before
        // todo: maybe update configuration to remember trim point for the whole chain
        // todo: support multiple segments.
        // todo: dispatch loglet deletion in the background when entire segments are trimmed
        for segment in log_chain.iter() {
            let loglet = self.get_loglet(log_id, segment).await?;
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

        let log_metadata = self.metadata.logs();

        let log_chain = log_metadata
            .chain(&log_id)
            .ok_or(Error::UnknownLogId(log_id))?;

        for segment in log_chain.iter() {
            let loglet = self.get_loglet(log_id, segment).await?;

            if loglet.base_lsn > trim_point {
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
            .sync(MetadataKind::Logs, TargetVersion::Latest)
            .await?;
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
        let log_metadata = self.metadata.logs();
        let tail_segment = log_metadata
            .chain(&log_id)
            .ok_or(Error::UnknownLogId(log_id))?
            .tail();
        self.get_loglet(log_id, tail_segment).await
    }

    pub(crate) async fn find_loglet_for_lsn(
        &self,
        log_id: LogId,
        lsn: Lsn,
    ) -> Result<LogletWrapper> {
        let log_metadata = self.metadata.logs();
        let maybe_segment = log_metadata
            .chain(&log_id)
            .ok_or(Error::UnknownLogId(log_id))?
            .find_segment_for_lsn(lsn);
        match maybe_segment {
            MaybeSegment::Some(segment) => self.get_loglet(log_id, segment).await,
            // todo: handle trimmed segments
            MaybeSegment::Trim { .. } => todo!("trimmed segments is not supported yet"),
        }
    }

    async fn get_loglet(
        &self,
        log_id: LogId,
        segment: Segment<'_>,
    ) -> Result<LogletWrapper, Error> {
        let provider = self.provider_for(segment.config.kind)?;
        let loglet = provider
            .get_loglet(log_id, segment.index(), &segment.config.params)
            .await?;
        Ok(LogletWrapper::new(
            segment.index(),
            segment.base_lsn,
            segment.tail_lsn,
            loglet,
        ))
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;

    use super::*;

    use crate::providers::memory_loglet::{self};
    use bytes::Bytes;
    use googletest::prelude::*;
    use restate_types::logs::metadata::new_single_node_loglet_params;
    use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
    use restate_types::Versioned;

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
                    let record = bifrost.read(log_id, Lsn::from(lsn)).await?;
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
                    let record = bifrost.read(log_id, Lsn::from(lsn)).await?;
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

                let record = bifrost.read(log_id, Lsn::from(10)).await?.unwrap();
                assert!(record.record.is_trim_gap());
                assert_eq!(Lsn::from(10), record.record.try_as_trim_gap().unwrap().to);

                // Add 10 more records
                for _ in 0..10 {
                    bifrost.append(log_id, Payload::default()).await?;
                }

                for lsn in 11..20 {
                    let record = bifrost.read(log_id, Lsn::from(lsn)).await?;
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

    #[tokio::test(start_paused = true)]
    async fn test_read_across_segments() -> googletest::Result<()> {
        const LOG_ID: LogId = LogId::new(0);
        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .with_partition_table(FixedPartitionTable::new(Version::MIN, 1))
            .build()
            .await;
        let tc = node_env.tc;
        tc.run_in_scope("test", None, async {
            let bifrost = Bifrost::init_in_memory(metadata()).await;

            // Lsns [1..5]
            for i in 1..=5 {
                // Append a record to memory
                let lsn = bifrost
                    .append(LOG_ID, Payload::new(format!("segment-1-{i}")))
                    .await?;
                assert_eq!(Lsn::from(i), lsn);
            }

            // not sealed, tail is what we expect
            assert_that!(
                bifrost
                    .find_tail(LOG_ID, FindTailAttributes::default())
                    .await?,
                pat!(TailState::Open(eq(Lsn::new(6))))
            );

            let segment_1 = bifrost
                .inner()
                .find_loglet_for_lsn(LOG_ID, Lsn::OLDEST)
                .await?;

            // seal the segment
            segment_1.seal().await?;

            // sealed, tail is what we expect
            assert_that!(
                bifrost
                    .find_tail(LOG_ID, FindTailAttributes::default())
                    .await?,
                pat!(TailState::Sealed(eq(Lsn::new(6))))
            );

            println!("attempting to read during reconfiguration");
            // attempting to read from bifrost will result in a timeout since metadata sees this as an open
            // segment but the segment itself is sealed. This means reconfiguration is in-progress
            // and we can't confidently read records.
            assert_that!(
                bifrost.read(LOG_ID, Lsn::new(2)).await,
                err(pat!(Error::ReadFailureDuringReconfiguration(
                    eq(LOG_ID),
                    eq(Lsn::new(2)),
                )))
            );

            let old_version = bifrost.inner().metadata.logs_version();

            let mut builder = bifrost.inner().metadata.logs().clone().into_builder();
            let mut chain_builder = builder.chain(&LOG_ID).unwrap();
            assert_eq!(1, chain_builder.num_segments());
            let new_segment_params = new_single_node_loglet_params(ProviderKind::InMemory);
            // deliberately skips Lsn::from(6) to create a zombie record in segment 1. Segment 1 now has 4 records.
            chain_builder.append_segment(
                Lsn::new(5),
                ProviderKind::InMemory,
                new_segment_params,
            )?;

            let new_metadata = builder.build();
            let new_version = new_metadata.version();
            assert_eq!(new_version, old_version.next());
            node_env
                .metadata_store_client
                .put(
                    BIFROST_CONFIG_KEY.clone(),
                    new_metadata,
                    restate_metadata_store::Precondition::MatchesVersion(old_version),
                )
                .await?;

            // make sure we have updated metadata.
            metadata()
                .sync(MetadataKind::Logs, TargetVersion::Latest)
                .await?;
            assert_eq!(new_version, bifrost.inner().metadata.logs_version());

            {
                // validate that the stored metadata matches our expectations.
                let new_metadata = bifrost.inner().metadata.logs().clone();
                let chain_builder = new_metadata.chain(&LOG_ID).unwrap();
                assert_eq!(2, chain_builder.num_segments());
            }

            // find_tail() on the underlying loglet returns (6) but for bifrost it should be (5) after
            // the new segment was created at tail of the chain with base_lsn=5
            assert_that!(
                bifrost
                    .find_tail(LOG_ID, FindTailAttributes::default())
                    .await?,
                pat!(TailState::Open(eq(Lsn::new(5))))
            );

            // appends should go to the new segment
            // Lsns [5..7]
            for i in 5..=7 {
                // Append a record to memory
                let lsn = bifrost
                    .append(LOG_ID, Payload::new(format!("segment-2-{i}")))
                    .await?;
                assert_eq!(Lsn::from(i), lsn);
            }

            // tail is now 8 and open.
            assert_that!(
                bifrost
                    .find_tail(LOG_ID, FindTailAttributes::default())
                    .await?,
                pat!(TailState::Open(eq(Lsn::new(8))))
            );

            // validating that segment 1 is still sealed and has its own tail at Lsn (6)
            assert_that!(
                segment_1.find_tail().await?,
                pat!(TailState::Sealed(eq(Lsn::new(6))))
            );

            let segment_2 = bifrost
                .inner()
                .find_loglet_for_lsn(LOG_ID, Lsn::new(5))
                .await?;

            assert_ne!(segment_1, segment_2);

            // segment 2 is open and at 8 as previously validated through bifrost interface
            assert_that!(
                segment_2.find_tail().await?,
                pat!(TailState::Open(eq(Lsn::new(8))))
            );

            // Reading the log. (OLDEST)
            let LogRecord { offset, record } = bifrost.read(LOG_ID, Lsn::OLDEST).await?.unwrap();
            assert_eq!(Lsn::from(1), offset);
            assert!(record.is_data());
            assert_eq!(
                &Bytes::from_static(b"segment-1-1"),
                record.payload().unwrap().body(),
            );

            let LogRecord { offset, record } = bifrost.read(LOG_ID, Lsn::new(2)).await?.unwrap();
            assert_eq!(Lsn::from(2), offset);
            assert!(record.is_data());
            assert_eq!(
                &Bytes::from_static(b"segment-1-2"),
                record.payload().unwrap().body(),
            );

            // border of segment 1
            let LogRecord { offset, record } = bifrost.read(LOG_ID, Lsn::new(4)).await?.unwrap();
            assert_eq!(Lsn::from(4), offset);
            assert!(record.is_data());
            assert_eq!(
                &Bytes::from_static(b"segment-1-4"),
                record.payload().unwrap().body(),
            );

            // start of segment 2
            let LogRecord { offset, record } = bifrost.read(LOG_ID, Lsn::new(5)).await?.unwrap();
            assert_eq!(Lsn::from(5), offset);
            assert!(record.is_data());
            assert_eq!(
                &Bytes::from_static(b"segment-2-5"),
                record.payload().unwrap().body(),
            );

            // last record
            let LogRecord { offset, record } = bifrost.read(LOG_ID, Lsn::new(7)).await?.unwrap();
            assert_eq!(Lsn::from(7), offset);
            assert!(record.is_data());
            assert_eq!(
                &Bytes::from_static(b"segment-2-7"),
                record.payload().unwrap().body(),
            );

            // 8 doesn't exist yet.
            assert_eq!(None, bifrost.read(LOG_ID, Lsn::new(8)).await?);

            Ok(())
        })
        .await
    }
}
