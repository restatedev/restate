// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use enum_map::EnumMap;
use tokio::time::Instant;
use tracing::debug;
use tracing::{info, instrument, warn};

#[cfg(all(any(test, feature = "test-util"), feature = "local-loglet"))]
use restate_types::live::LiveLoadExt;

#[cfg(any(test, feature = "test-util"))]
use restate_core::MetadataWriter;
use restate_core::my_node_id;
use restate_core::{Metadata, ShutdownError};
use restate_types::config::Configuration;
use restate_types::logs::metadata::SealMetadata;
use restate_types::logs::metadata::{LogletParams, Logs, SegmentIndex};
use restate_types::logs::metadata::{MaybeSegment, ProviderKind, Segment};
use restate_types::logs::{KeyFilter, LogId, Lsn, SequenceNumber, TailState};
use restate_types::storage::StorageEncode;

use crate::appender::Appender;
use crate::background_appender::BackgroundAppender;
use crate::log_chain_writer::LogChainCommand;
use crate::loglet::{FindTailOptions, LogletProvider, OperationError};
use crate::loglet_wrapper::LogletWrapper;
use crate::sealed_loglet::SealedLoglet;
use crate::watchdog::{WatchdogCommand, WatchdogSender};
use crate::{BifrostAdmin, Error, InputRecord, LogReadStream, Result};

/// The strategy to use when bifrost fails to append or when it observes
/// a sealed loglet while it's tailing a log.
///
/// Please keep this enum ordered, i.e. anything > Allowed should still mean allowed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Ord, derive_more::Display)]
pub enum ErrorRecoveryStrategy {
    /// Do not extend the chain, wait indefinitely instead until the error disappears.
    Wait = 1,
    /// Extend the chain only running out of patience, others might be better suited to reconfigure
    /// the chain, but when desperate, we are allowed to seal and extend.
    #[default]
    ExtendChainAllowed,
    /// Eagerly extend the chain by creating a new loglet and appending to it.
    ExtendChainPreferred,
}

/// Bifrost is Restate's durable interconnect system
///
/// Bifrost is a mutable-friendly handle to access the system. You don't need
/// to wrap this in an Arc or a lock, pass it around by reference or by a clone.
/// Bifrost handle is relatively cheap to clone.
#[derive(Clone)]
pub struct Bifrost {
    pub(crate) inner: Arc<BifrostInner>,
}

impl Bifrost {
    pub(crate) fn new(inner: Arc<BifrostInner>) -> Self {
        Self { inner }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn init_in_memory(metadata_writer: MetadataWriter) -> Self {
        use crate::providers::memory_loglet;

        Self::init_with_factory(metadata_writer, memory_loglet::Factory::default()).await
    }

    #[cfg(all(any(test, feature = "test-util"), feature = "local-loglet"))]
    pub async fn init_local(metadata_writer: MetadataWriter) -> Self {
        use restate_types::config::Configuration;

        use crate::BifrostService;

        let config = Configuration::live();
        let bifrost_svc = BifrostService::new(metadata_writer)
            .enable_local_loglet(config.map(|config| &config.bifrost.local).boxed());
        let bifrost = bifrost_svc.handle();

        // start bifrost service in the background
        bifrost_svc.start().await.expect("local loglet must start");
        bifrost
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn init_with_factory(
        metadata_writer: MetadataWriter,
        factory: impl crate::loglet::LogletProviderFactory,
    ) -> Self {
        use crate::BifrostService;

        let bifrost_svc = BifrostService::new(metadata_writer).with_factory(factory);
        let bifrost = bifrost_svc.handle();

        // start bifrost service in the background
        bifrost_svc
            .start()
            .await
            .expect("in memory loglet must start");
        bifrost
    }

    /// Admin operations of bifrost
    pub fn admin(&self) -> BifrostAdmin<'_> {
        BifrostAdmin::new(&self.inner)
    }

    /// Appends a single record to a log. The log id must exist, otherwise the
    /// operation fails with [`Error::UnknownLogId`]
    ///
    /// It's recommended to use the [`Appender`] interface. Use [`Self::create_appender`]
    /// and reuse this appender to create a sequential write stream to the virtual log.
    #[instrument(
        level="trace",
        skip(self, body),
        fields(
            otel.name = "Bifrost: append",
        )
    )]
    pub async fn append<T: StorageEncode>(
        &self,
        log_id: LogId,
        error_recovery_strategy: ErrorRecoveryStrategy,
        body: impl Into<InputRecord<T>>,
    ) -> Result<Lsn> {
        self.inner.fail_if_shutting_down()?;
        self.inner
            .append(log_id, error_recovery_strategy, body)
            .await
    }

    /// Read the next record from the LSN provided. The `from` indicates the LSN where we will
    /// start reading from. This means that the record returned will have a LSN that is equal
    /// or greater than `from`. If no records are committed yet at this LSN, this read operation
    /// will immediately return `None`.
    ///
    /// It's recommended to use the [`LogReadStream`] interface. Use [`Self::create_reader`]
    /// and reuse this read stream if you want to read more than one record.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn read(&self, log_id: LogId, from: Lsn) -> Result<Option<crate::LogEntry>> {
        self.inner.fail_if_shutting_down()?;
        self.inner.read(log_id, from).await
    }

    /// Create a read stream. `end_lsn` is inclusive. Pass [[`Lsn::Max`]] for a tailing stream. Use
    /// Lsn::OLDEST in `from` to read from the start (head) of the log.
    ///
    /// When using this in conjunction with `find_tail()` or `get_trim_point()`, note that
    /// you'll need to offset start and end to ensure you are reading within the bounds of the log.
    ///
    /// For instance, if you intend to read all records from a log you can construct a reader as
    /// follows:
    ///
    /// ```no_run
    /// use restate_bifrost::{Bifrost, LogReadStream};
    /// use restate_bifrost::loglet::FindTailOptions;
    /// use restate_types::logs::{KeyFilter, LogId, SequenceNumber};
    ///
    /// async fn reader(bifrost: &Bifrost, log_id: LogId) -> LogReadStream {
    ///     bifrost.create_reader(
    ///        log_id,
    ///        KeyFilter::Any,
    ///        bifrost.get_trim_point(log_id).await.unwrap(),
    ///        bifrost.find_tail(log_id, FindTailOptions::default()).await.unwrap().offset().prev(),
    ///     ).unwrap()
    /// }
    /// ```
    pub fn create_reader(
        &self,
        log_id: LogId,
        filter: KeyFilter,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Result<LogReadStream> {
        self.inner.fail_if_shutting_down()?;
        LogReadStream::create(self.inner.clone(), log_id, filter, start_lsn, end_lsn)
    }

    /// The best way to write to Bifrost is to hold on to an [`Appender`] and reuse it across
    /// calls, this allows internal caching of recently accessed loglets and recycling write
    /// buffers.
    pub fn create_appender<T: StorageEncode>(
        &self,
        log_id: LogId,
        error_recovery_strategy: ErrorRecoveryStrategy,
    ) -> Result<Appender<T>> {
        self.inner.fail_if_shutting_down()?;
        self.inner.check_log_id(log_id)?;
        Ok(Appender::new(
            log_id,
            error_recovery_strategy,
            self.inner.clone(),
        ))
    }

    pub fn create_background_appender<T: StorageEncode>(
        &self,
        log_id: LogId,
        error_recovery_strategy: ErrorRecoveryStrategy,
        queue_capacity: usize,
        max_batch_size: usize,
    ) -> Result<BackgroundAppender<T>> {
        Ok(BackgroundAppender::new(
            self.create_appender(log_id, error_recovery_strategy)?,
            queue_capacity,
            max_batch_size,
        ))
    }

    /// The tail is *the first unwritten LSN* in the log
    ///
    /// Finds the first available LSN after the durable tail of the log.
    ///
    /// If the log is empty, it returns TailState::Open(Lsn::OLDEST).
    /// This should never return Err(Error::LogSealed). Sealed state is represented as
    /// TailState::Sealed(..)
    pub async fn find_tail(&self, log_id: LogId, opts: FindTailOptions) -> Result<TailState> {
        self.inner.fail_if_shutting_down()?;
        Ok(self.inner.find_tail(log_id, opts).await?.1)
    }

    // Get the loglet currently serving the tail of the chain, for use in integration tests.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn find_tail_loglet(&self, log_id: LogId) -> Result<Arc<dyn crate::loglet::Loglet>> {
        self.inner.fail_if_shutting_down()?;
        Ok(self
            .inner
            .find_tail(log_id, FindTailOptions::default())
            .await?
            .0
            .inner()
            .clone())
    }

    /// The lsn of the slot **before** the first readable record (if it exists), or the offset
    /// before the next slot that will be written to. Another way to think about `get_trim_point`
    /// is that it points to the last lsn that was trimmed/deleted.
    ///
    /// Returns Lsn::INVALID if the log was never trimmed
    pub async fn get_trim_point(&self, log_id: LogId) -> Result<Lsn, Error> {
        self.inner.fail_if_shutting_down()?;
        self.inner.get_trim_point(log_id).await
    }

    /// Read a full log with the given id. To be used only in tests!!!
    #[cfg(any(test, feature = "test-util"))]
    pub async fn read_all(&self, log_id: LogId) -> Result<Vec<crate::LogEntry>> {
        use futures::TryStreamExt;

        self.inner.fail_if_shutting_down()?;

        let current_tail = self.find_tail(log_id, FindTailOptions::default()).await?;

        if current_tail.offset() <= Lsn::OLDEST {
            return Ok(Vec::default());
        }

        let reader = self.create_reader(
            log_id,
            KeyFilter::Any,
            Lsn::OLDEST,
            current_tail.offset().prev(),
        )?;
        reader.try_collect().await
    }
}

// compile-time check
static_assertions::assert_impl_all!(Bifrost: Send, Sync, Clone);

// Locks in this data-structure are held for very short time and should never be
// held across an async boundary.
pub struct BifrostInner {
    watchdog: WatchdogSender,
    // Initialized after BifrostService::start completes.
    pub(crate) providers: OnceLock<EnumMap<ProviderKind, Option<Arc<dyn LogletProvider>>>>,
    shutting_down: AtomicBool,
}

impl BifrostInner {
    pub fn new(watchdog: WatchdogSender) -> Self {
        Self {
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
    pub async fn append<T: StorageEncode>(
        self: &Arc<Self>,
        log_id: LogId,
        error_recovery_strategy: ErrorRecoveryStrategy,
        record: impl Into<InputRecord<T>>,
    ) -> Result<Lsn> {
        Appender::<T>::new(log_id, error_recovery_strategy, Arc::clone(self))
            .append(record)
            .await
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn read(
        self: &Arc<Self>,
        log_id: LogId,
        from: Lsn,
    ) -> Result<Option<crate::LogEntry>> {
        use futures::StreamExt;
        let (_, tail_state) = self.find_tail(log_id, FindTailOptions::default()).await?;
        if from >= tail_state.offset() {
            // Can't use this function to read future records.
            return Ok(None);
        }
        // Create a terminating (non-tailing) read-stream that stops before reaching the tail.
        let mut stream = LogReadStream::create(
            Arc::clone(self),
            log_id,
            KeyFilter::Any,
            from,
            tail_state.offset().prev(),
        )?;
        // stream dropped after delivering the next record
        stream.next().await.transpose()
    }

    pub async fn find_tail(
        self: &Arc<Self>,
        log_id: LogId,
        opts: FindTailOptions,
    ) -> Result<(LogletWrapper, TailState)> {
        let start = Instant::now();
        // uses the same retry policy as reads to not add too many configuration keys
        let mut error_logged = false;
        let mut retry_iter = Configuration::pinned()
            .bifrost
            .read_retry_policy
            .clone()
            .into_iter();
        // Design Notes:
        // - If the loglet is being sealed (TailState::Sealed). We should not report this tail value
        // unless (a) the chain is sealed at this tail value, or (b) higher segments exist.
        // - If the segment is open and the loglet reports open. It's safe to return the tail
        // value.
        let mut metadata = Metadata::with_current(|m| m.updateable_logs_metadata());
        loop {
            let logs = metadata.live_load();
            let loglet = self.tail_loglet_from_metadata(logs, log_id).await?;

            match loglet.find_tail(opts).await {
                Ok(tail) => {
                    if error_logged {
                        info!(
                            %log_id,
                            "Found the log tail after {} attempts, time spent is {:?}",
                             retry_iter.attempts(),
                             start.elapsed()
                        );
                    }

                    if tail.is_sealed()
                        && matches!(opts, FindTailOptions::ConsistentRead)
                        && !logs.chain(&log_id).expect("log must exist").is_sealed()
                    {
                        debug!(%log_id, "Loglet {} is sealed but the chain is not. Sealing the chain", loglet.debug_str());
                        // loglet is sealed but chain is not sealed yet.
                        // let's seal the chain.
                        match BifrostAdmin::new(self)
                            .seal(
                                log_id,
                                loglet.segment_index(),
                                SealMetadata::new("find-tail", my_node_id()),
                            )
                            .await
                        {
                            Ok(lsn) => {
                                info!(%log_id, "Chain is sealed at lsn={lsn}, will attempt finding the tail again");
                                continue;
                            }
                            Err(err) => {
                                // retry with exponential backoff
                                info!(%log_id, ?err, attempts = retry_iter.attempts(), "Failed to seal the chain");
                                error_logged = true;
                                let Some(sleep_dur) = retry_iter.next() else {
                                    // retries exhausted
                                    return Err(err);
                                };
                                tokio::time::sleep(sleep_dur).await;
                                continue;
                            }
                        }
                    }
                    return Ok((loglet, tail));
                }
                Err(err @ OperationError::Shutdown(_)) => {
                    return Err(err.into());
                }
                Err(OperationError::Other(err)) if !err.retryable() => {
                    return Err(err.into());
                }
                // retryable errors
                Err(OperationError::Other(err)) => {
                    // retry with exponential backoff
                    let Some(sleep_dur) = retry_iter.next() else {
                        // retries exhausted
                        return Err(err.into());
                    };
                    if retry_iter.attempts() > retry_iter.max_attempts() / 2 {
                        warn!(
                            %log_id,
                            attempts = retry_iter.attempts(),
                            retry_after = ?sleep_dur,
                                "Cannot find the tail of the log, will retry. err={}",
                            err
                        );
                        error_logged = true;
                    }
                    tokio::time::sleep(sleep_dur).await;
                }
            }
        }
    }

    pub async fn get_trim_point(&self, log_id: LogId) -> Result<Lsn, Error> {
        let log_metadata = Metadata::with_current(|m| m.logs_ref());

        let log_chain = log_metadata
            .chain(&log_id)
            .ok_or(Error::UnknownLogId(log_id))?;

        let mut trim_point = None;

        // Iterate over the chain until we find the first missing trim point, return value before
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

    pub async fn trim(&self, log_id: LogId, trim_point: Lsn) -> Result<(), Error> {
        let log_metadata = Metadata::with_current(|m| m.logs_ref());

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
        // todo: maybe update configuration to remember trim point for the whole chain
        // it's okay if the watchdog is dead
        let _ = self.watchdog.send(WatchdogCommand::LogTrimmed {
            log_id,
            requested_trim_point: trim_point,
        });
        Ok(())
    }

    #[inline]
    pub(crate) fn fail_if_shutting_down(&self) -> Result<()> {
        if self.shutting_down.load(Ordering::Relaxed) {
            Err(Error::Shutdown(restate_core::ShutdownError))
        } else {
            Ok(())
        }
    }

    /// Acquire a token to tell bifrost that this node is the intended primary writer for this log.
    ///
    /// The preference can be kept for as long as the token is not dropped. Multiple tokens can be
    /// taken for the same log. Preference is only lost after the last token is dropped.
    pub fn acquire_preference_token(&self, log_id: LogId) -> PreferenceToken {
        PreferenceToken::new(self.watchdog.clone(), log_id)
    }

    /// Adds a new log if it doesn't exist.
    ///
    /// Idempotent operation; ignores the input provider/params if the log already exists.
    pub async fn add_log(
        &self,
        log_id: LogId,
        provider: ProviderKind,
        params: LogletParams,
    ) -> std::result::Result<(), Error> {
        let (response_rx, cmd) = LogChainCommand::add_log(log_id, provider, params);
        let _ = self.watchdog.send(WatchdogCommand::ChainCommand(cmd));

        response_rx.await.map_err(|_| ShutdownError)?
    }

    /// Extend the given log chain with the provided segment definition (provider, params, base_lsn).
    /// This only works if the current last segment has the same segment index as
    /// `last_segment_index`. Otherwise, the operation will fail.
    pub async fn extend_log_chain(
        &self,
        log_id: LogId,
        last_segment_index: SegmentIndex,
        base_lsn: Lsn,
        provider: ProviderKind,
        params: LogletParams,
    ) -> std::result::Result<(), Error> {
        let (response_rx, cmd) =
            LogChainCommand::extend(log_id, last_segment_index, base_lsn, provider, params);
        let _ = self.watchdog.send(WatchdogCommand::ChainCommand(cmd));

        response_rx.await.map_err(|_| ShutdownError)?
    }

    /// Seals the given log chain by writing a sealed loglet marker as the last segment.
    /// This only works if the current last segment has the same segment index as
    /// `last_segment_index`. Otherwise, the operation will fail.
    ///
    /// Note that if the segment is already sealed, the `tail_lsn` will be ignored and the returned
    /// tail_lsn should be the authoritative value.
    pub async fn seal_log_chain(
        &self,
        log_id: LogId,
        last_segment_index: SegmentIndex,
        tail_lsn: Lsn,
        metadata: SealMetadata,
    ) -> std::result::Result<Lsn, Error> {
        let (response_rx, cmd) =
            LogChainCommand::seal_chain(log_id, last_segment_index, tail_lsn, metadata);
        let _ = self.watchdog.send(WatchdogCommand::ChainCommand(cmd));

        response_rx.await.map_err(|_| ShutdownError)?
    }

    // --- Helper functions --- //
    /// Get the provider for a given kind. A provider must be enabled and BifrostService **must**
    /// be started before calling this.
    #[track_caller]
    pub(crate) fn provider_for(&self, kind: ProviderKind) -> Result<&Arc<dyn LogletProvider>> {
        let providers = self
            .providers
            .get()
            .expect("BifrostService must be started prior to using Bifrost");

        providers[kind]
            .as_ref()
            .ok_or_else(|| Error::Disabled(kind.to_string()))
    }

    /// Checks if the log_id exists and that the provider is not disabled (can be created).
    pub(crate) fn check_log_id(&self, log_id: LogId) -> Result<(), Error> {
        let logs = Metadata::with_current(|metadata| metadata.logs_ref());
        let chain = logs.chain(&log_id).ok_or(Error::UnknownLogId(log_id))?;

        let kind = chain.tail().config.kind;
        if !kind.is_seal_marker() {
            let _ = self.provider_for(kind.try_into().unwrap())?;
        }

        Ok(())
    }

    pub async fn tail_loglet(&self, log_id: LogId) -> Result<LogletWrapper> {
        let log_metadata = Metadata::with_current(|metadata| metadata.logs_ref());
        let tail_segment = log_metadata
            .chain(&log_id)
            .ok_or(Error::UnknownLogId(log_id))?
            .tail();
        self.get_loglet(log_id, tail_segment).await
    }

    pub async fn tail_loglet_from_metadata(
        &self,
        log_metadata: &Logs,
        log_id: LogId,
    ) -> Result<LogletWrapper> {
        let tail_segment = log_metadata
            .chain(&log_id)
            .ok_or(Error::UnknownLogId(log_id))?
            .tail();
        self.get_loglet(log_id, tail_segment).await
    }

    pub async fn find_loglet_for_lsn(&self, log_id: LogId, lsn: Lsn) -> Result<MaybeLoglet> {
        let log_metadata = Metadata::with_current(|metadata| metadata.logs_ref());
        let maybe_segment = log_metadata
            .chain(&log_id)
            .ok_or(Error::UnknownLogId(log_id))?
            .find_segment_for_lsn(lsn);
        match maybe_segment {
            MaybeSegment::Some(segment) => {
                Ok(MaybeLoglet::Some(self.get_loglet(log_id, segment).await?))
            }
            MaybeSegment::Trim { next_base_lsn } => Ok(MaybeLoglet::Trim { next_base_lsn }),
        }
    }

    pub async fn get_loglet(
        &self,
        log_id: LogId,
        segment: Segment<'_>,
    ) -> Result<LogletWrapper, Error> {
        let loglet = if segment.config.kind.is_seal_marker() {
            SealedLoglet::get()
        } else {
            let provider = self.provider_for(
                segment
                    .config
                    .kind
                    .try_into()
                    .expect("non-special provider"),
            )?;
            provider
                .get_loglet(log_id, segment.index(), &segment.config.params)
                .await?
        };

        Ok(LogletWrapper::new(
            segment.index(),
            segment.base_lsn,
            segment.tail_lsn,
            segment.config.clone(),
            loglet,
        ))
    }
}

/// Result of a lookup by lsn in the chain
#[derive(Debug)]
pub enum MaybeLoglet {
    /// Segment is not found in the chain.
    Some(LogletWrapper),
    /// No loglet was found, the log is trimmed until (at least) the next_base_lsn. When
    /// generating trim gaps, this value should be considered exclusive (next_base_lsn doesn't
    /// necessarily point to a gap)
    Trim { next_base_lsn: Lsn },
}

impl MaybeLoglet {
    #[allow(dead_code)]
    pub fn unwrap(self) -> LogletWrapper {
        match self {
            MaybeLoglet::Some(loglet) => loglet,
            MaybeLoglet::Trim { next_base_lsn } => {
                panic!("Expected Loglet, found Trim segment with next_base_lsn={next_base_lsn}")
            }
        }
    }
}

/// A token to tell bifrost that this node is the intended primary writer for this log.
///
/// The preference can be kept for as long as the token is not dropped. Multiple tokens can be
/// taken for the same log. Preference is only lost after the last token is dropped.
///
/// Note that multiple nodes can take preference for the same log, depending on the default
/// provider, those nodes might compete in optimizing the loglet locality differently, causing
/// bouncing or ping-pongs.
pub struct PreferenceToken {
    log_id: LogId,
    sender: WatchdogSender,
}

impl PreferenceToken {
    fn new(sender: WatchdogSender, log_id: LogId) -> Self {
        // notify the watchdog that we are preferred
        let _ = sender.send(WatchdogCommand::PreferenceAcquire(log_id));
        Self { log_id, sender }
    }
}

impl Clone for PreferenceToken {
    fn clone(&self) -> Self {
        // notify the watchdog that we are still preferred
        let _ = self
            .sender
            .send(WatchdogCommand::PreferenceAcquire(self.log_id));
        Self {
            log_id: self.log_id,
            sender: self.sender.clone(),
        }
    }
}

impl Drop for PreferenceToken {
    fn drop(&mut self) {
        // notify the watchdog that we are no longer preferred
        let _ = self
            .sender
            .send(WatchdogCommand::PreferenceRelease(self.log_id));
    }
}

#[cfg(all(test, feature = "local-loglet"))]
mod tests {
    use super::*;

    use std::num::NonZeroUsize;
    use std::sync::atomic::AtomicUsize;

    use futures::StreamExt;
    use googletest::prelude::*;
    use test_log::test;
    use tokio::time::Duration;
    use tracing::info;
    use tracing_test::traced_test;

    use restate_core::TestCoreEnvBuilder;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnv};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::set_current_config;
    use restate_types::logs::SequenceNumber;
    use restate_types::logs::metadata::{SegmentIndex, new_single_node_loglet_params};
    use restate_types::metadata::Precondition;
    use restate_types::partition_table::PartitionTable;
    use restate_types::{Version, Versioned};

    use crate::error::EnqueueError;
    use crate::providers::memory_loglet::{self};

    // Helper to create a small byte count for testing
    fn small_byte_limit(bytes: usize) -> restate_serde_util::NonZeroByteCount {
        restate_serde_util::NonZeroByteCount::new(NonZeroUsize::new(bytes).unwrap())
    }

    #[restate_core::test]
    #[traced_test]
    async fn test_append_smoke() -> googletest::Result<()> {
        let num_partitions = 5;
        let env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_partition_table(PartitionTable::with_equally_sized_partitions(
                Version::MIN,
                num_partitions,
            ))
            .build()
            .await;

        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;

        let clean_bifrost_clone = bifrost.clone();

        let mut appender_0 = bifrost.create_appender(LogId::new(0), ErrorRecoveryStrategy::Wait)?;
        let mut appender_3 = bifrost.create_appender(LogId::new(3), ErrorRecoveryStrategy::Wait)?;
        let mut max_lsn = Lsn::INVALID;
        for i in 1..=5 {
            // Append a record to memory
            let lsn = appender_0.append("").await?;
            info!(%lsn, "Appended record to log");
            assert_eq!(Lsn::from(i), lsn);
            max_lsn = lsn;
        }

        // Append to a log that doesn't exist.
        let invalid_log = LogId::from(num_partitions + 1);
        let resp = bifrost.create_appender::<String>(invalid_log, ErrorRecoveryStrategy::Wait);

        assert_that!(resp, pat!(Err(pat!(Error::UnknownLogId(eq(invalid_log))))));

        // use a cloned bifrost.
        let cloned_bifrost = bifrost.clone();
        let mut second_appender_0 =
            cloned_bifrost.create_appender(LogId::new(0), ErrorRecoveryStrategy::Wait)?;
        for _ in 1..=5 {
            // Append a record to memory
            let lsn = second_appender_0.append("").await?;
            info!(%lsn, "Appended record to log");
            assert_eq!(max_lsn + Lsn::from(1), lsn);
            max_lsn = lsn;
        }

        // Ensure original clone writes to the same underlying loglet.
        let lsn = clean_bifrost_clone
            .create_appender(LogId::new(0), ErrorRecoveryStrategy::Wait)?
            .append("")
            .await?;
        assert_eq!(max_lsn + Lsn::from(1), lsn);
        max_lsn = lsn;

        // Writes to another log don't impact original log
        let lsn = appender_3.append("").await?;
        assert_eq!(Lsn::from(1), lsn);

        let lsn = appender_0.append("").await?;
        assert_eq!(max_lsn + Lsn::from(1), lsn);
        max_lsn = lsn;

        let tail = bifrost
            .find_tail(LogId::new(0), FindTailOptions::default())
            .await?;
        assert_eq!(max_lsn.next(), tail.offset());

        // Initiate shutdown
        TaskCenter::current().shutdown_node("completed", 0).await;
        // appends cannot succeed after shutdown
        let res = appender_0.append("").await;
        assert!(matches!(res, Err(Error::Shutdown(_))));
        // Validate the watchdog has called the provider::start() function.
        assert!(logs_contain("Shutting down in-memory loglet provider"));
        assert!(logs_contain("Bifrost watchdog shutdown complete"));
        Ok(())
    }

    #[restate_core::test(start_paused = true)]
    async fn test_lazy_initialization() -> googletest::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let delay = Duration::from_secs(5);
        // This memory provider adds a delay to its loglet initialization, we want
        // to ensure that appends do not fail while waiting for the loglet;
        let factory = memory_loglet::Factory::with_init_delay(delay);
        let bifrost = Bifrost::init_with_factory(env.metadata_writer, factory).await;

        let start = tokio::time::Instant::now();
        let lsn = bifrost
            .create_appender(LogId::new(0), ErrorRecoveryStrategy::Wait)?
            .append("")
            .await?;
        assert_eq!(Lsn::from(1), lsn);
        // The append was properly delayed
        assert_eq!(delay, start.elapsed());
        Ok(())
    }

    #[test(restate_core::test(flavor = "multi_thread", worker_threads = 2))]
    async fn trim_log_smoke_test() -> googletest::Result<()> {
        const LOG_ID: LogId = LogId::new(0);
        let node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;
        RocksDbManager::init();

        let bifrost = Bifrost::init_local(node_env.metadata_writer).await;

        assert_eq!(
            Lsn::OLDEST,
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?
                .offset()
        );

        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;
        // append 10 records
        for _ in 1..=10 {
            appender.append("").await?;
        }

        bifrost.admin().trim(LOG_ID, Lsn::from(5)).await?;

        let tail = bifrost
            .find_tail(LOG_ID, FindTailOptions::default())
            .await?;
        assert_eq!(tail.offset(), Lsn::from(11));
        assert!(!tail.is_sealed());
        assert_eq!(Lsn::from(5), bifrost.get_trim_point(LOG_ID).await?);

        // 5 itself is trimmed
        for lsn in 1..=5 {
            let record = bifrost.read(LOG_ID, Lsn::from(lsn)).await?.unwrap();

            assert_that!(record.sequence_number(), eq(Lsn::new(lsn)));
            assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(5))));
        }

        for lsn in 6..=10 {
            let record = bifrost.read(LOG_ID, Lsn::from(lsn)).await?.unwrap();
            assert_that!(record.sequence_number(), eq(Lsn::new(lsn)));
            assert!(record.is_data_record());
        }

        // trimming beyond the release point will fall back to the release point
        bifrost.admin().trim(LOG_ID, Lsn::MAX).await?;

        assert_eq!(
            Lsn::from(11),
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?
                .offset()
        );
        let new_trim_point = bifrost.get_trim_point(LOG_ID).await?;
        assert_eq!(Lsn::from(10), new_trim_point);

        let record = bifrost.read(LOG_ID, Lsn::from(10)).await?.unwrap();
        assert!(record.is_trim_gap());
        assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(10))));

        // Add 10 more records
        for _ in 0..10 {
            appender.append("").await?;
        }

        for lsn in 11..20 {
            let record = bifrost.read(LOG_ID, Lsn::from(lsn)).await?.unwrap();
            assert_that!(record.sequence_number(), eq(Lsn::new(lsn)));
            assert!(record.is_data_record());
        }

        Ok(())
    }

    #[restate_core::test(start_paused = true)]
    async fn test_read_across_segments() -> googletest::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_partition_table(PartitionTable::with_equally_sized_partitions(
                Version::MIN,
                1,
            ))
            .build()
            .await;

        let bifrost = Bifrost::init_in_memory(node_env.metadata_writer.clone()).await;

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;
        // Lsns [1..5]
        for i in 1..=5 {
            // Append a record to memory
            let lsn = appender.append(format!("segment-1-{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        // not sealed, tail is what we expect
        assert_that!(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?,
            pat!(TailState::Open(eq(Lsn::new(6))))
        );

        let segment_1 = bifrost
            .inner
            .find_loglet_for_lsn(LOG_ID, Lsn::OLDEST)
            .await?
            .unwrap();

        // seal the segment (simulating partial seal where the loglet is sealed but the chain is
        // not)
        segment_1.seal().await?;

        // loglet's tail is sealed
        assert_that!(
            segment_1.find_tail(FindTailOptions::default()).await?,
            pat!(TailState::Sealed(_))
        );

        let read_one = async |log_id: LogId, from: Lsn, to: Lsn| {
            let mut stream = bifrost.create_reader(log_id, KeyFilter::Any, from, to)?;
            // stream dropped after delivering the next record
            stream.next().await.transpose()
        };

        println!("attempting to read during reconfiguration");
        // attempting to read from bifrost will result in a timeout since metadata sees this as an open
        // segment but the loglet itself is sealed. This means reconfiguration is in-progress
        // and we can't confidently read records.
        assert!(
            tokio::time::timeout(
                Duration::from_secs(2),
                read_one(LOG_ID, Lsn::new(2), Lsn::new(3))
            )
            .await
            .is_err()
        );

        println!("finding tail to trigger chain sealing");
        // sealed tail is what we expect
        // finding the tail on bifrost will seal the chain since no reconfiguration is in progress.
        assert_that!(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?,
            pat!(TailState::Sealed(eq(Lsn::new(6))))
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(2),
                read_one(LOG_ID, Lsn::new(4), Lsn::new(5))
            )
            .await
            .is_ok()
        );

        let metadata = Metadata::current();
        let old_version = metadata.logs_version();

        let mut builder = metadata.logs_ref().clone().try_into_builder().unwrap();
        let mut chain_builder = builder.chain(LOG_ID).unwrap();
        assert_eq!(2, chain_builder.num_segments());
        let new_segment_params = new_single_node_loglet_params(ProviderKind::InMemory);

        assert!(
            // seal marker is at lsn=6, cannot append to any other tail
            chain_builder
                .append_segment(
                    Lsn::new(5),
                    ProviderKind::InMemory,
                    new_segment_params.clone()
                )
                .is_err()
        );
        assert!(
            chain_builder
                .append_segment(
                    Lsn::new(7),
                    ProviderKind::InMemory,
                    new_segment_params.clone()
                )
                .is_err()
        );

        chain_builder.append_segment(Lsn::new(6), ProviderKind::InMemory, new_segment_params)?;

        // chain is still 2 because the seal marker has been replaced with the new segment.
        assert_eq!(2, chain_builder.num_segments());

        let new_metadata = builder.build();
        let new_version = new_metadata.version();
        assert_eq!(new_version, old_version.next());
        node_env
            .metadata_writer
            .global_metadata()
            .put(
                new_metadata.into(),
                Precondition::MatchesVersion(old_version),
            )
            .await?;

        assert_eq!(new_version, metadata.logs_version());

        {
            // validate that the stored metadata matches our expectations.
            let new_metadata = metadata.logs_ref().clone();
            let chain_builder = new_metadata.chain(&LOG_ID).unwrap();
            assert_eq!(2, chain_builder.num_segments());
        }

        assert_that!(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?,
            pat!(TailState::Open(eq(Lsn::new(6))))
        );

        // appends should go to the new segment
        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;
        // Lsns [6..8]
        for i in 6..=8 {
            // Append a record to memory
            let lsn = appender.append(format!("segment-2-{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        // tail is now 9 and open.
        assert_that!(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?,
            pat!(TailState::Open(eq(Lsn::new(9))))
        );

        let segment_2 = bifrost
            .inner
            .find_loglet_for_lsn(LOG_ID, Lsn::new(6))
            .await?
            .unwrap();

        assert_ne!(segment_1, segment_2);

        // Reading the log. (OLDEST)
        let record = bifrost.read(LOG_ID, Lsn::OLDEST).await?.unwrap();
        assert_that!(record.sequence_number(), eq(Lsn::new(1)));
        assert!(record.is_data_record());
        assert_that!(
            record.decode_unchecked::<String>(),
            eq("segment-1-1".to_owned())
        );

        let record = bifrost.read(LOG_ID, Lsn::new(2)).await?.unwrap();
        assert_that!(record.sequence_number(), eq(Lsn::new(2)));
        assert!(record.is_data_record());
        assert_that!(
            record.decode_unchecked::<String>(),
            eq("segment-1-2".to_owned())
        );

        // border of segment 1
        let record = bifrost.read(LOG_ID, Lsn::new(5)).await?.unwrap();
        assert_that!(record.sequence_number(), eq(Lsn::new(5)));
        assert!(record.is_data_record());
        assert_that!(
            record.decode_unchecked::<String>(),
            eq("segment-1-5".to_owned())
        );

        // start of segment 2
        let record = bifrost.read(LOG_ID, Lsn::new(6)).await?.unwrap();
        assert_that!(record.sequence_number(), eq(Lsn::new(6)));
        assert!(record.is_data_record());
        assert_that!(
            record.decode_unchecked::<String>(),
            eq("segment-2-6".to_owned())
        );

        // last record
        let record = bifrost.read(LOG_ID, Lsn::new(8)).await?.unwrap();
        assert_that!(record.sequence_number(), eq(Lsn::new(8)));
        assert!(record.is_data_record());
        assert_that!(
            record.decode_unchecked::<String>(),
            eq("segment-2-8".to_owned())
        );

        // 9 doesn't exist yet.
        assert!(bifrost.read(LOG_ID, Lsn::new(9)).await?.is_none());

        Ok(())
    }

    #[restate_core::test(start_paused = true)]
    #[traced_test]
    async fn test_appends_correctly_handle_reconfiguration() -> googletest::Result<()> {
        const LOG_ID: LogId = LogId::new(0);
        let node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_partition_table(PartitionTable::with_equally_sized_partitions(
                Version::MIN,
                1,
            ))
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;
        RocksDbManager::init();
        let bifrost = Bifrost::init_local(node_env.metadata_writer).await;

        // create an appender
        let stop_signal = Arc::new(AtomicBool::default());
        let append_counter = Arc::new(AtomicUsize::new(0));
        let _ = TaskCenter::spawn(TaskKind::TestRunner, "append-records", {
            let append_counter = append_counter.clone();
            let stop_signal = stop_signal.clone();
            let bifrost = bifrost.clone();
            let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;
            async move {
                let mut i = 0;
                while !stop_signal.load(Ordering::Relaxed) {
                    i += 1;
                    if i % 2 == 0 {
                        // append individual record
                        let lsn = appender.append(format!("record{i}")).await?;
                        println!("Appended {lsn}");
                    } else {
                        // append batch
                        let mut payloads = Vec::with_capacity(10);
                        for j in 1..=10 {
                            payloads.push(format!("record-in-batch{i}-{j}"));
                        }
                        let lsn = appender.append_batch(payloads).await?;
                        println!("Appended batch {lsn}");
                    }
                    append_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                println!("Appender terminated");
                Ok(())
            }
        })?;

        let mut append_counter_before_seal;
        loop {
            append_counter_before_seal = append_counter.load(Ordering::Relaxed);
            if append_counter_before_seal < 100 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            } else {
                break;
            }
        }

        // seal and don't extend the chain.
        let _ = bifrost
            .admin()
            .seal(LOG_ID, SegmentIndex::from(0), SealMetadata::default())
            .await?;

        // appends should stall!
        tokio::time::sleep(Duration::from_millis(100)).await;
        let append_counter_during_seal = append_counter.load(Ordering::Relaxed);
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let counter_now = append_counter.load(Ordering::Relaxed);
            assert_that!(counter_now, eq(append_counter_during_seal));
            println!("Appends are stalling, counter={counter_now}");
        }

        for i in 1..=5 {
            let last_segment = bifrost.inner.tail_loglet(LOG_ID).await?.segment_index();
            // allow appender to run a little.
            tokio::time::sleep(Duration::from_millis(500)).await;
            // seal the loglet and extend with an in-memory one
            let new_segment_params = new_single_node_loglet_params(ProviderKind::Local);
            bifrost
                .admin()
                .seal_and_extend_chain(
                    LOG_ID,
                    None,
                    Version::MIN,
                    ProviderKind::Local,
                    new_segment_params,
                )
                .await?;
            println!("Seal {i}");
            assert_that!(
                bifrost.inner.tail_loglet(LOG_ID).await?.segment_index(),
                gt(last_segment)
            );
        }

        // make sure that appends are still happening.
        let mut append_counter_after_seal = append_counter.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_that!(append_counter_after_seal, gt(append_counter_before_seal));
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let counter_now = append_counter.load(Ordering::Relaxed);
            assert_that!(counter_now, gt(append_counter_after_seal));
            append_counter_after_seal = counter_now;
        }

        // questionable.
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[restate_core::test]
    async fn test_append_record_too_large() -> googletest::Result<()> {
        // Set up a configuration with a small record size limit.
        // Note: The estimated_encode_size for a typed record (e.g., String) is ~2KB constant,
        // plus overhead for Keys and NanosSinceEpoch. We set the limit to 1KB to ensure
        // the check triggers.
        let mut config = restate_types::config::Configuration::default();
        config.networking.message_size_limit = small_byte_limit(1024); // 1KB limit
        let config = config.apply_cascading_values();
        set_current_config(config);

        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;

        // Get the configured record size limit
        let record_size_limit = restate_types::config::Configuration::pinned()
            .bifrost
            .record_size_limit();
        assert_eq!(record_size_limit.get(), 1024);

        // Any record will have an estimated size of ~2KB+ due to the constant estimate
        // for PolyBytes::Typed, which exceeds our 1KB limit
        let payload = "test";

        let appender = bifrost.create_appender(LogId::new(0), ErrorRecoveryStrategy::Wait)?;

        // Verify the appender has the correct limit
        assert_eq!(appender.record_size_limit().get(), 1024);

        // Attempting to append should fail with RecordTooLarge
        let mut appender = appender;
        let result = appender.append(payload).await;

        assert_that!(
            result,
            pat!(Err(pat!(Error::BatchTooLarge {
                batch_size_bytes: gt(1024),
                limit: eq(record_size_limit),
            })))
        );

        Ok(())
    }

    #[restate_core::test]
    async fn test_background_appender_record_too_large() -> googletest::Result<()> {
        // Set up configuration with a small record size limit (100 bytes).
        // Note: The estimated_encode_size for any typed record is ~2KB constant,
        // so even "small" strings will exceed the 100 byte limit.
        let mut config = restate_types::config::Configuration::default();
        config.networking.message_size_limit = small_byte_limit(100);
        // Apply cascading values to propagate the networking limit to bifrost
        let config = config.apply_cascading_values();
        set_current_config(config);

        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;

        let background_appender: crate::BackgroundAppender<String> = bifrost
            .create_background_appender(LogId::new(0), ErrorRecoveryStrategy::Wait, 10, 10)?;

        let mut handle = background_appender.start("test-appender")?;
        let sender = handle.sender();

        // A string with 100 bytes
        let payload = String::from_utf8(vec![b't'; 100]).unwrap();

        // try_enqueue should fail with RecordTooLarge
        let result = sender.try_enqueue(payload.clone());
        assert_that!(
            result,
            pat!(Err(pat!(EnqueueError::RecordTooLarge {
                record_size: gt(100),
                limit: eq(NonZeroUsize::new(100).unwrap()),
            })))
        );

        // enqueue (async) should also fail with RecordTooLarge
        let result = sender.enqueue(payload.clone()).await;
        assert_that!(
            result,
            pat!(Err(pat!(EnqueueError::RecordTooLarge {
                record_size: gt(100),
                limit: eq(NonZeroUsize::new(100).unwrap()),
            })))
        );

        // try_enqueue_with_notification should also fail
        let result = sender.try_enqueue_with_notification(payload.clone());
        assert!(matches!(
            result,
            Err(EnqueueError::RecordTooLarge {
                record_size,
                limit,
            }) if record_size > 100 && limit.get() == 100
        ));

        // Drain the appender (nothing should have been enqueued)
        handle.drain().await?;

        Ok(())
    }

    #[restate_core::test]
    async fn test_background_appender_record_within_limit() -> googletest::Result<()> {
        // Set up configuration with a large enough record size limit (10KB) to allow records
        let mut config = restate_types::config::Configuration::default();
        config.networking.message_size_limit = small_byte_limit(10 * 1024); // 10KB
        let config = config.apply_cascading_values();
        set_current_config(config);

        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;

        let background_appender: crate::BackgroundAppender<String> = bifrost
            .create_background_appender(LogId::new(0), ErrorRecoveryStrategy::Wait, 10, 10)?;

        let mut handle = background_appender.start("test-appender")?;
        let sender = handle.sender();

        // With a 10KB limit, the ~2KB estimated record should succeed
        let payload = "test".to_string();
        sender.enqueue(payload).await?;

        // Drain and wait for commit
        handle.drain().await?;

        Ok(())
    }

    /// Test that records enqueued rapidly are properly committed.
    #[restate_core::test]
    async fn test_background_appender_rapid_enqueue() -> googletest::Result<()> {
        let mut config = restate_types::config::Configuration::default();
        config.networking.message_size_limit = small_byte_limit(50 * 1024); // 50KB
        let config = config.apply_cascading_values();
        set_current_config(config);

        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;

        let background_appender: crate::BackgroundAppender<String> = bifrost
            .create_background_appender(LogId::new(0), ErrorRecoveryStrategy::Wait, 1000, 100)?;

        let mut handle = background_appender.start("test-appender")?;
        let sender = handle.sender();

        // Rapidly enqueue many records using try_enqueue (non-blocking)
        let mut enqueued = 0;
        for i in 0..100 {
            match sender.try_enqueue(format!("rapid-record-{i}")) {
                Ok(()) => enqueued += 1,
                Err(EnqueueError::Full(_)) => {
                    // Queue is full, use async enqueue
                    sender.enqueue(format!("rapid-record-{i}")).await?;
                    enqueued += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }

        assert_that!(enqueued, eq(100));

        // Wait for all to be committed
        let token = sender.notify_committed().await?;
        token.await?;

        handle.drain().await?;

        // Verify tail advanced correctly (100 records)
        let tail = bifrost
            .find_tail(LogId::new(0), FindTailOptions::default())
            .await?;
        assert_that!(tail.offset(), eq(Lsn::from(101u64)));

        Ok(())
    }
}
