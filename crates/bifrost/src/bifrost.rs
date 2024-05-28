// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Remove after fleshing the code out.
#![allow(dead_code)]

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use enum_map::EnumMap;
use once_cell::sync::OnceCell;
use smallvec::SmallVec;
use tracing::{error, instrument};

use restate_core::{metadata, Metadata, MetadataKind};
use restate_types::logs::metadata::ProviderKind;
use restate_types::logs::{LogId, Lsn, Payload, SequenceNumber};
use restate_types::storage::StorageCodec;
use restate_types::Version;

use crate::loglet::{LogletBase, LogletProvider, LogletWrapper};
use crate::watchdog::{WatchdogCommand, WatchdogSender};
use crate::{
    Error, FindTailAttributes, LogReadStream, LogRecord, Result, SMALL_BATCH_THRESHOLD_COUNT,
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
    pub async fn init() -> Self {
        use crate::BifrostService;

        let metadata = metadata();
        let bifrost_svc = BifrostService::new(metadata);
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
    pub async fn append(&mut self, log_id: LogId, payload: Payload) -> Result<Lsn> {
        self.inner.append(log_id, payload).await
    }

    /// Appends a batch of records to a log. The log id must exist, otherwise the
    /// operation fails with [`Error::UnknownLogId`]. The returned Lsn is the Lsn of the first
    /// record in this batch. This will only return after all records have been stored.
    #[instrument(level = "debug", skip(self, payloads), err)]
    pub async fn append_batch(&mut self, log_id: LogId, payloads: &[Payload]) -> Result<Lsn> {
        self.inner.append_batch(log_id, payloads).await
    }

    /// Read the next record after the LSN provided. The `start` indicates the LSN where we will
    /// read after. This means that the record returned will have a LSN strictly greater than
    /// `after`. If no records are committed yet after this LSN, this read operation will "wait"
    /// for such records to appear.
    pub async fn read_next_single(&self, log_id: LogId, after: Lsn) -> Result<LogRecord> {
        self.inner.read_next_single(log_id, after).await
    }

    /// Read the next record after the LSN provided. The `start` indicates the LSN where we will
    /// read after. This means that the record returned will have a LSN strictly greater than
    /// `after`. If no records are committed after the LSN, this read operation will return None.
    pub async fn read_next_single_opt(
        &self,
        log_id: LogId,
        after: Lsn,
    ) -> Result<Option<LogRecord>> {
        self.inner.read_next_single_opt(log_id, after).await
    }

    /// Create a read stream. Until is inclusive. Pass [[`Lsn::Max`]] for a tailing stream. Use
    /// Lsn::INVALID in _after_ to read from the start (head) of the log.
    pub async fn create_reader(
        &self,
        log_id: LogId,
        after: Lsn,
        until: Lsn,
    ) -> Result<LogReadStream> {
        LogReadStream::create(self.inner.clone(), log_id, after, until).await
    }

    /// Finds the current readable tail LSN of a log.
    /// Returns `None` if there are no readable records in the log (e.g. trimmed or empty)
    pub async fn find_tail(
        &self,
        log_id: LogId,
        attributes: FindTailAttributes,
    ) -> Result<Option<Lsn>> {
        Ok(self.inner.find_tail(log_id, attributes).await?.1)
    }

    /// The version of the currently loaded logs metadata
    pub fn version(&self) -> Version {
        metadata().logs_version()
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
        let Some(current_tail) = current_tail else {
            return Ok(Vec::default());
        };

        let reader = self
            .create_reader(log_id, Lsn::INVALID, current_tail)
            .await?;
        Ok(reader.try_collect().await?)
    }
}

// compile-time check
static_assertions::assert_impl_all!(Bifrost: Send, Sync, Clone);

// Locks in this data-structure are held for very short time and should never be
// held across an async boundary.
pub struct BifrostInner {
    metadata: Metadata,
    watchdog: WatchdogSender,
    providers: EnumMap<ProviderKind, OnceCell<Arc<dyn LogletProvider>>>,
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
        after: Lsn,
    ) -> Result<Option<LogRecord>> {
        self.fail_if_shutting_down()?;

        let loglet = self.find_loglet_for_lsn(log_id, after.next()).await?;
        Ok(loglet.read_next_single_opt(after).await?.map(|record| {
            record
                .decode()
                .expect("decoding a bifrost envelope succeeds")
        }))
    }

    pub async fn find_tail(
        &self,
        log_id: LogId,
        _attributes: FindTailAttributes,
    ) -> Result<(LogletWrapper, Option<Lsn>)> {
        self.fail_if_shutting_down()?;
        let loglet = self.writeable_loglet(log_id).await?;
        let tail = loglet.find_tail().await?;
        Ok((loglet, tail))
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

    /// Get the provider for a given kind. If the provider is not yet initialized, it will be
    /// lazily initialized by the watchdog.
    fn provider_for(&self, kind: ProviderKind) -> &dyn LogletProvider {
        self.providers[kind]
            .get_or_init(|| {
                let provider =
                    crate::loglet::create_provider(kind).expect("provider is able to get created");
                if let Err(e) = provider.start() {
                    error!("Failed to start loglet provider {}: {}", kind, e);
                    // todo: Handle provider errors by a graceful system shutdown
                    // task_center().shutdown_node("Bifrost loglet provider startup error", 1);
                    panic!("Failed to start loglet provider {}: {}", kind, e);
                }
                // tell watchdog about it.
                let _ = self
                    .watchdog
                    .send(WatchdogCommand::WatchProvider(provider.clone()));
                provider
            })
            .deref()
    }

    /// Injects a provider for testing purposes. The call is responsible for starting the provider
    /// and that it's monitored by watchdog if necessary.
    /// This will only work if the provider was never accessed by bifrost before this call.
    #[cfg(test)]
    #[track_caller]
    fn inject_provider(&self, kind: ProviderKind, provider: Arc<dyn LogletProvider>) {
        assert!(self.providers[kind].try_insert(provider).is_ok());
    }

    async fn writeable_loglet(&self, log_id: LogId) -> Result<LogletWrapper> {
        let tail_segment = self
            .metadata
            .logs()
            .and_then(|logs| logs.tail_segment(log_id))
            .ok_or(Error::UnknownLogId(log_id))?;
        let provider = self.provider_for(tail_segment.config.kind);
        let loglet = provider.get_loglet(&tail_segment.config.params).await?;

        Ok(LogletWrapper::new(tail_segment.base_lsn, loglet))
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
        let provider = self.provider_for(segment.config.kind);
        let loglet = provider.get_loglet(&segment.config.params).await?;

        Ok(LogletWrapper::new(segment.base_lsn, loglet))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    use crate::loglets::memory_loglet::MemoryLogletProvider;
    use googletest::prelude::*;

    use restate_core::TestCoreEnv;
    use restate_core::{task_center, TestCoreEnvBuilder};
    use restate_types::logs::SequenceNumber;
    use restate_types::partition_table::FixedPartitionTable;
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
            let mut bifrost = Bifrost::init().await;

            let mut clean_bifrost_clone = bifrost.clone();

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
            let mut cloned_bifrost = bifrost.clone();
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
            assert_eq!(max_lsn, tail.unwrap());

            // Initiate shutdown
            task_center().shutdown_node("completed", 0).await;
            // appends cannot succeed after shutdown
            let res = bifrost.append(LogId::from(0), Payload::default()).await;
            assert!(matches!(res, Err(Error::Shutdown(_))));
            // Validate the watchdog has called the provider::start() function.
            assert!(logs_contain("Starting in-memory loglet provider"));
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
            let memory_provider = MemoryLogletProvider::with_init_delay(delay);

            let mut bifrost = Bifrost::init().await;

            // Inject out preconfigured memory provider
            bifrost
                .inner()
                .inject_provider(ProviderKind::InMemory, memory_provider);

            let start = tokio::time::Instant::now();
            let lsn = bifrost.append(LogId::from(0), Payload::default()).await?;
            assert_eq!(Lsn::from(1), lsn);
            // The append was properly delayed
            assert_eq!(delay, start.elapsed());
            Ok(())
        })
        .await
    }
}
