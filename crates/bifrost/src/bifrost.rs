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
use std::sync::{Arc, Mutex};

use enum_map::EnumMap;
use once_cell::sync::OnceCell;
#[cfg(any(test, feature = "memory_loglet"))]
use tokio::task::JoinHandle;

use restate_types::logs::{LogId, LogsVersion, Lsn, Payload, SequenceNumber};

use crate::loglet::{LogletBase, LogletProvider, LogletWrapper, ProviderKind};
use crate::metadata::Logs;
use crate::options::Options;
use crate::watchdog::{WatchdogCommand, WatchdogSender};
use crate::{create_static_metadata, Error, FindTailAttributes, LogReadStream, LogRecord};

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

    #[cfg(any(test, feature = "memory_loglet"))]
    pub async fn new_in_memory(
        num_logs: u64,
        shutdown_watch: drain::Watch,
    ) -> (Self, JoinHandle<Result<(), Error>>) {
        let bifrost_svc = Options::memory().build(num_logs);
        let bifrost = bifrost_svc.handle();

        // start bifrost service in the background
        (
            bifrost,
            bifrost_svc
                .start(shutdown_watch)
                .await
                .expect("in memory loglet must start"),
        )
    }

    /// Appends a single record to a log. The log id must exist, otherwise the
    /// operation fails with [`Error::UnknownLogId`]
    pub async fn append(&mut self, log_id: LogId, payload: Payload) -> Result<Lsn, Error> {
        self.inner.append(log_id, payload).await
    }

    /// Read the next record after the LSN provided. The `start` indicates the LSN where we will
    /// read after. This means that the record returned will have a LSN strictly greater than
    /// `after`. If no records are committed yet after this LSN, this read operation will "wait"
    /// for such records to appear.
    pub async fn read_next_single(&self, log_id: LogId, after: Lsn) -> Result<LogRecord, Error> {
        self.inner.read_next_single(log_id, after).await
    }

    /// Read the next record after the LSN provided. The `start` indicates the LSN where we will
    /// read after. This means that the record returned will have a LSN strictly greater than
    /// `after`. If no records are committed after the LSN, this read operation will return None.
    pub async fn read_next_single_opt(
        &self,
        log_id: LogId,
        after: Lsn,
    ) -> Result<Option<LogRecord>, Error> {
        self.inner.read_next_single_opt(log_id, after).await
    }

    pub fn create_reader(&self, log_id: LogId, after: Lsn) -> LogReadStream {
        LogReadStream::new(self.inner.clone(), log_id, after)
    }

    /// Finds the current readable tail LSN of a log.
    /// Returns `None` if there are no readable records in the log (e.g. trimmed or empty)
    pub async fn find_tail(
        &self,
        log_id: LogId,
        attributes: FindTailAttributes,
    ) -> Result<Option<Lsn>, Error> {
        self.inner.find_tail(log_id, attributes).await
    }

    /// The version of the currently loaded metadata
    pub fn metadata_version(&self) -> LogsVersion {
        self.inner.log_metadata.lock().unwrap().version
    }

    #[cfg(test)]
    pub fn inner(&self) -> Arc<BifrostInner> {
        self.inner.clone()
    }
}

// compile-time check
static_assertions::assert_impl_all!(Bifrost: Send, Sync, Clone);

// Locks in this data-structure are held for very short time and should never be
// held across an async boundary.
pub struct BifrostInner {
    opts: Options,
    num_partitions: u64,
    watchdog: WatchdogSender,
    log_metadata: Mutex<Logs>,
    providers: EnumMap<ProviderKind, OnceCell<Arc<dyn LogletProvider>>>,
    shutting_down: AtomicBool,
}

impl BifrostInner {
    pub fn new(opts: Options, watchdog: WatchdogSender, num_partitions: u64) -> Self {
        Self {
            opts,
            num_partitions,
            watchdog,
            log_metadata: Mutex::new(Logs::empty()),
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
    pub async fn append(&self, log_id: LogId, payload: Payload) -> Result<Lsn, Error> {
        self.fail_if_shutting_down()?;
        let loglet = self.writeable_loglet(log_id).await?;
        loglet.append(payload).await
    }

    pub async fn read_next_single(&self, log_id: LogId, after: Lsn) -> Result<LogRecord, Error> {
        self.fail_if_shutting_down()?;

        let loglet = self.find_loglet_for_lsn(log_id, after.next()).await?;
        loglet.read_next_single(after).await
    }

    pub async fn read_next_single_opt(
        &self,
        log_id: LogId,
        after: Lsn,
    ) -> Result<Option<LogRecord>, Error> {
        self.fail_if_shutting_down()?;

        let loglet = self.find_loglet_for_lsn(log_id, after.next()).await?;
        loglet.read_next_single_opt(after).await
    }

    pub async fn find_tail(
        &self,
        log_id: LogId,
        _attributes: FindTailAttributes,
    ) -> Result<Option<Lsn>, Error> {
        self.fail_if_shutting_down()?;
        let loglet = self.writeable_loglet(log_id).await?;
        loglet.find_tail().await
    }

    #[inline]
    fn fail_if_shutting_down(&self) -> Result<(), Error> {
        if self.shutting_down.load(Ordering::Relaxed) {
            Err(Error::Shutdown)
        } else {
            Ok(())
        }
    }

    /// Immediately fetch new metadata from metadata store and update the local copy
    ///
    /// NOTE: The current version assumes static metadata map!
    pub async fn sync_metadata(&self) -> Result<(), Error> {
        self.fail_if_shutting_down()?;

        let logs = create_static_metadata(&self.opts, self.num_partitions);
        let mut guard = self.log_metadata.lock().unwrap();
        if logs.version > guard.version {
            *guard = logs;
        }
        Ok(())
    }

    // --- Helper functions --- //

    /// Get the provider for a given kind. If the provider is not yet initialized, it will be
    /// lazily initialized by the watchdog.
    fn provider_for(&self, kind: ProviderKind) -> &dyn LogletProvider {
        self.providers[kind]
            .get_or_init(|| {
                let provider = crate::loglet::create_provider(kind, &self.opts);
                // tell watchdog about it.
                let _ = self
                    .watchdog
                    .send(WatchdogCommand::StartProvider(provider.clone()));
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

    async fn writeable_loglet(&self, log_id: LogId) -> Result<LogletWrapper, Error> {
        // Locks the logs mutex.
        let tail_segment = self
            .log_metadata
            .lock()
            .unwrap()
            .tail_segment(log_id)
            .ok_or(Error::UnknownLogId(log_id))?;
        // Logs lock released here.
        let provider = self.provider_for(tail_segment.config.kind);
        let loglet = provider.get_loglet(&tail_segment.config.params).await?;

        Ok(LogletWrapper::new(tail_segment.base_lsn, loglet))
    }

    async fn find_loglet_for_lsn(&self, log_id: LogId, lsn: Lsn) -> Result<LogletWrapper, Error> {
        // Locks the logs mutex.
        let segment = self
            .log_metadata
            .lock()
            .unwrap()
            .find_segment_for_lsn(log_id, lsn)
            .ok_or(Error::UnknownLogId(log_id))?;

        // Logs lock released here.
        let provider = self.provider_for(segment.config.kind);
        let loglet = provider.get_loglet(&segment.config.params).await?;

        Ok(LogletWrapper::new(segment.base_lsn, loglet))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    use crate::loglet::ProviderKind;
    use crate::loglets::memory_loglet::MemoryLogletProvider;
    use googletest::prelude::*;

    use restate_types::logs::SequenceNumber;
    use tracing::info;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_append_smoke() -> Result<()> {
        // start a simple bifrost service with 5 logs.
        let num_partitions = 5;
        let (shutdown_signal, shutdown_watch) = drain::channel();
        let (mut bifrost, svc_handle) =
            Bifrost::new_in_memory(num_partitions, shutdown_watch).await;

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
        shutdown_signal.drain().await;

        assert!(svc_handle.is_finished());

        // appends cannot succeed after shutdown
        let res = bifrost.append(LogId::from(0), Payload::default()).await;
        assert!(matches!(res, Err(Error::Shutdown)));
        // Validate the watchdog has called the provider::start() function.
        assert!(logs_contain("Starting in-memory loglet provider"));
        assert!(logs_contain("Shutting down in-memory loglet provider"));
        assert!(logs_contain("Bifrost watchdog shutdown complete"));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_lazy_initialization() -> Result<()> {
        let delay = Duration::from_secs(5);
        let (shutdown_signal, shutdown_watch) = drain::channel();
        let num_partitions = 5;
        // This memory provider adds a delay to its loglet initialization, we want
        // to ensure that appends do not fail while waiting for the loglet;
        let memory_provider = MemoryLogletProvider::with_init_delay(delay);

        let bifrost_opts = Options {
            default_provider: ProviderKind::Memory,
            ..Options::default()
        };
        let bifrost_svc = bifrost_opts.build(num_partitions);
        let mut bifrost = bifrost_svc.handle();

        // Inject out preconfigured memory provider
        bifrost
            .inner()
            .inject_provider(ProviderKind::Memory, memory_provider);

        // start bifrost service in the background
        let svc_handle = bifrost_svc.start(shutdown_watch).await?;

        let start = tokio::time::Instant::now();
        let lsn = bifrost.append(LogId::from(0), Payload::default()).await?;
        assert_eq!(Lsn::from(1), lsn);
        // The append was properly delayed
        assert_eq!(delay, start.elapsed());

        // Initiate shutdown
        shutdown_signal.drain().await;
        assert!(svc_handle.is_finished());
        Ok(())
    }
}
