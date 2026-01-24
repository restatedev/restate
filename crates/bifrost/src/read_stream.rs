// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::task::ready;
use std::time::Duration;

use futures::Stream;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::stream::FusedStream;
use pin_project::pin_project;

use restate_core::Metadata;
use restate_core::MetadataKind;
use restate_core::ShutdownError;
use restate_core::my_node_id;
use restate_types::Version;
use restate_types::Versioned;
use restate_types::live::Live;
use restate_types::logs::KeyFilter;
use restate_types::logs::MatchKeyQuery;
use restate_types::logs::SequenceNumber;
use restate_types::logs::TailState;
use restate_types::logs::metadata::Chain;
use restate_types::logs::metadata::InternalKind;
use restate_types::logs::metadata::Logs;
use restate_types::logs::metadata::MaybeSegment;
use restate_types::logs::metadata::SealMetadata;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, Lsn};
use restate_types::retries::with_jitter;
use tokio::time::Sleep;
use tracing::debug;
use tracing::trace;
use tracing::warn;

use crate::BifrostAdmin;
use crate::Error;
use crate::LogEntry;
use crate::Result;
use crate::bifrost::BifrostInner;
use crate::bifrost::MaybeLoglet;
use crate::error::AdminError;
use crate::loglet::OperationError;
use crate::loglet_wrapper::LogletReadStreamWrapper;

/// A read stream reads from the virtual log. The stream provides a unified view over
/// the virtual log addressing space in the face of seals, reconfiguration, and trims.
///
// The use of [pin_project] is not strictly necessary but it's left to allow future
// substream implementations to be !Unpin without changing the read_stream.
#[must_use = "streams do nothing unless polled"]
#[pin_project(project = ReadStreamProj)]
pub struct LogReadStream {
    log_id: LogId,
    /// Chooses which records to read/return.
    filter: KeyFilter,
    /// inclusive max LSN to read to
    end_lsn: Lsn,
    /// Represents the next record to read.
    ///
    ///  This is akin to the lsn that can be passed to `read(from)` to read the
    ///  next record in the log.
    read_pointer: Lsn,
    #[pin]
    log_metadata: Live<Logs>,
    latest_checked_version: Version,
    #[pin]
    next_log_metadata_watch_fut: Option<BoxFuture<'static, Result<Version, ShutdownError>>>,
    #[pin]
    state: State,
    /// Current substream we are reading from
    #[pin]
    substream: Option<LogletReadStreamWrapper>,
    // IMPORTANT: Do not re-order this field. `inner` must be dropped last. This allows
    // `state` to reference its lifetime as 'static.
    bifrost_inner: Arc<BifrostInner>,
}

/// The state machine encodes the necessary state changes in the read stream.
/// The order of variants roughly reflects the order of transitions in a typical case.
///
#[pin_project(project = StateProj)]
enum State {
    /// Initial state of the stream. No work has been done at this point
    New,
    /// Stream is waiting for bifrost to get a loglet that maps to the `read_pointer`
    FindingLoglet {
        /// The future to continue finding the loglet instance via Bifrost
        #[pin]
        find_loglet_fut: BoxFuture<'static, Result<MaybeLoglet>>,
    },
    /// Waiting for the loglet read stream (substream) to be initialized
    CreatingSubstream {
        /// Future to continue creating the substream
        #[pin]
        create_stream_fut: BoxFuture<'static, Result<LogletReadStreamWrapper, OperationError>>,
    },
    /// Reading records from `substream`
    Reading {
        /// The tail LSN which is safe to use when the loglet is unsealed
        safe_known_tail: Option<Lsn>,
        #[pin]
        tail_watch: Option<BoxStream<'static, TailState>>,
    },
    /// Chain reconfiguration has been detected, we'll update our view of the chain.
    AwaitingReconfiguration,
    /// Waiting for the tail LSN of the substream's loglet to be determined (sealing in-progress).
    /// We land on this state when we observe a "Sealed" signal from an open segment after we
    /// reached the safe known tail for that loglet. We can continue reading only after the chain
    /// has been sealed by a marker or with a new installed chain.
    AwaitingOrSealChain {
        timeout: Pin<Box<Sleep>>,
    },
    /// Performing a chain seal operation
    SealingChain {
        #[pin]
        seal_chain_fut: BoxFuture<'static, ()>,
    },
    Terminated,
}

impl State {
    fn awaiting_or_seal_chain() -> Self {
        Self::AwaitingOrSealChain {
            // Questionable whether making this value configurable adds value or not.
            timeout: Box::pin(tokio::time::sleep(with_jitter(Duration::from_secs(5), 0.5))),
        }
    }

    fn finding_loglet(bifrost: &'static BifrostInner, log_id: LogId, read_pointer: Lsn) -> Self {
        Self::FindingLoglet {
            find_loglet_fut: Box::pin(bifrost.find_loglet_for_lsn(log_id, read_pointer)),
        }
    }

    fn reading_to_known_tail(tail_lsn: Lsn) -> Self {
        Self::Reading {
            safe_known_tail: Some(tail_lsn),
            tail_watch: None,
        }
    }
}

impl LogReadStream {
    pub(crate) fn create(
        bifrost_inner: Arc<BifrostInner>,
        log_id: LogId,
        filter: KeyFilter,
        start_lsn: Lsn,
        // Inclusive. Use [`Lsn::MAX`] for a tailing stream.
        // Once reached, the stream terminates.
        end_lsn: Lsn,
    ) -> Result<Self> {
        // Accidental reads from Lsn::INVALID are reset to Lsn::OLDEST
        let start_lsn = std::cmp::max(Lsn::OLDEST, start_lsn);
        let log_metadata = Metadata::with_current(|m| m.updateable_logs_metadata());
        Ok(Self {
            bifrost_inner,
            log_id,
            filter,
            read_pointer: start_lsn,
            end_lsn,
            latest_checked_version: Version::INVALID,
            next_log_metadata_watch_fut: None,
            log_metadata,
            substream: None,
            state: State::New,
        })
    }

    /// Current read pointer. This is the next (possible) record to be read.
    pub fn read_pointer(&self) -> Lsn {
        self.read_pointer
    }

    /// Inclusive max LSN to read to
    pub fn end_lsn(&self) -> Lsn {
        self.end_lsn
    }

    /// The read pointer points to the next LSN will be attempted on the next
    /// `poll_next()`.
    fn calculate_read_pointer(record: &LogEntry) -> Lsn {
        // On trim gaps, we fast-forward the read pointer beyond the end of the gap. We do
        // this after delivering a TrimGap record. This means that the next read operation
        // skips over the boundary of the gap.
        record
            .trim_gap_to_sequence_number()
            .unwrap_or(record.sequence_number())
            .next()
    }

    pub fn safe_known_tail(&self) -> Option<Lsn> {
        match self.state {
            State::Reading {
                safe_known_tail, ..
            } => safe_known_tail,
            _ => None,
        }
    }
}

impl FusedStream for LogReadStream {
    fn is_terminated(&self) -> bool {
        matches!(self.state, State::Terminated)
    }
}

/// Read the next record from the log at the current read pointer. The stream will yield
/// after the record is available to read, this will async-block indefinitely if no records are
/// ever written and released at the read pointer.
impl Stream for LogReadStream {
    type Item = Result<LogEntry>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // # Safety
        // BifrostInner is dropped last, we can safely lift it's lifetime to 'static as
        // long as we don't leak this externally. External users should not see any `'static`
        // lifetime as a result.
        let bifrost_inner = unsafe { &*Arc::as_ptr(&self.bifrost_inner) };

        let mut this = self.as_mut().project();
        loop {
            let state = this.state.as_mut().project();
            // We have reached the end of the stream.
            if *this.read_pointer == Lsn::MAX || *this.read_pointer > *this.end_lsn {
                this.state.set(State::Terminated);
                return Poll::Ready(None);
            }

            let logs = this.log_metadata.live_load();

            let Some(chain) = logs.chain(this.log_id) else {
                this.substream.set(None);
                this.state.set(State::Terminated);
                return Poll::Ready(Some(Err(Error::UnknownLogId(*this.log_id))));
            };

            if *this.latest_checked_version < logs.version() {
                // we are checking this version now.
                *this.latest_checked_version = logs.version();
                let next_version = logs.version().next();
                let metadata_watch_fut = Box::pin(async move {
                    Metadata::current()
                        .wait_for_version(MetadataKind::Logs, next_version)
                        .await
                });
                this.next_log_metadata_watch_fut
                    .set(Some(metadata_watch_fut));
            }

            match state {
                StateProj::New => {
                    let find_loglet_fut = Box::pin(
                        bifrost_inner.find_loglet_for_lsn(*this.log_id, *this.read_pointer),
                    );
                    // => Find Loglet
                    this.state.set(State::FindingLoglet { find_loglet_fut });
                }

                // Finding a loglet and creating the loglet instance through the provider
                StateProj::FindingLoglet { find_loglet_fut } => {
                    let loglet = match ready!(find_loglet_fut.poll(cx)) {
                        Ok(MaybeLoglet::Some(loglet)) => loglet,
                        Ok(MaybeLoglet::Trim { next_base_lsn }) => {
                            // deliver trim gap and advance read pointer.
                            let record = deliver_trim_gap(&mut this, next_base_lsn, bifrost_inner);
                            return Poll::Ready(Some(Ok(record)));
                        }
                        Err(e) => {
                            this.state.set(State::Terminated);
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    // create sub-stream to read from this loglet.
                    let create_stream_fut = Box::pin(
                        loglet.create_read_stream(this.filter.clone(), *this.read_pointer),
                    );
                    // => Create Substream
                    this.state
                        .set(State::CreatingSubstream { create_stream_fut });
                }

                // Creating a new substream
                StateProj::CreatingSubstream { create_stream_fut } => {
                    let substream = match ready!(create_stream_fut.poll(cx)) {
                        Ok(substream) => substream,
                        Err(e) => {
                            this.state.set(State::Terminated);
                            return Poll::Ready(Some(Err(e.into())));
                        }
                    };
                    let safe_known_tail = substream.tail_lsn();
                    // If the substream's tail is unknown, we will need to watch the tail updates.
                    let tail_watch = if safe_known_tail.is_none() {
                        Some(substream.loglet().watch_tail().boxed())
                    } else {
                        None
                    };
                    // => Start Reading
                    trace!(
                        log_id = %this.log_id,
                        "Advancing the chain, now reading at segment={}",
                        substream.loglet().segment_index()
                    );
                    this.substream.set(Some(substream));
                    this.state.set(State::Reading {
                        safe_known_tail,
                        tail_watch,
                    });
                }

                // Reading from the current substream
                StateProj::Reading {
                    safe_known_tail,
                    tail_watch,
                } => {
                    // Continue driving the substream
                    //
                    // This depends on whether we know its tail (if sealed), or if the value of
                    // `safe_known_tail` is higher than the `read_pointer` of the substream.
                    let Some(substream) = this.substream.as_mut().as_pin_mut() else {
                        panic!("substream must be set at this point");
                    };

                    // If the loglet's `tail_lsn` is known, this is the tail we should always respect.
                    match substream.tail_lsn() {
                        // Next LSN is beyond the boundaries of this substream
                        Some(tail) if *this.read_pointer >= tail => {
                            // Switch loglets.
                            let find_loglet_fut = Box::pin(
                                bifrost_inner.find_loglet_for_lsn(*this.log_id, *this.read_pointer),
                            );
                            // => Find the next loglet. We know we _probably_ have one, otherwise
                            // `stream_tail_lsn` wouldn't have been set.
                            this.substream.set(None);
                            this.state.set(State::FindingLoglet { find_loglet_fut });
                            continue;
                        }
                        // Unsealed loglet, we can only read as far as the safe unsealed tail.
                        None => {
                            if safe_known_tail.is_none()
                                || safe_known_tail
                                    .is_some_and(|known_tail| *this.read_pointer >= known_tail)
                            {
                                // We check if there was a change in the log chain that caused a
                                // new segment to be created after this one. If this is true, we
                                // can short-circuit the wait for the seal signal and use that as
                                // our tail_lsn. We do this because `TailState::Open` won't always
                                // guarantee open-ness but `TailState::Sealed` is a guaranteed
                                // signal.
                                if chain.tail_index() > substream.loglet().segment_index() {
                                    // a new segment must have been added, time to use it for tail.
                                    this.state.set(State::AwaitingReconfiguration);
                                    continue;
                                }
                                // Wait for tail update...
                                let Some(tail_watch) = tail_watch.as_pin_mut() else {
                                    panic!("tail_watch must be set on non-sealed read streams");
                                };

                                // If the loglet is being sealed, we must wait for reconfiguration to complete.
                                let maybe_tail = tail_watch.poll_next(cx);
                                match maybe_tail {
                                    Poll::Pending => {
                                        // we also want to wait for tail segment changes
                                        if let Some(watch_fut) =
                                            this.next_log_metadata_watch_fut.as_mut().as_pin_mut()
                                        {
                                            // The issue is that we might get the new segment while the registered
                                            // waker is bound to tail_watch. In that case, the tail watch will not
                                            // be awaken because tail can stall at the last observed state indefinitely.
                                            // Therefore, we'll not get a chance to re-check the tail segment
                                            // unless something else wakes us up.
                                            let _ = ready!(watch_fut.poll(cx))?;
                                            continue;
                                        } else {
                                            unreachable!(
                                                "metadata_watch must be set on non-sealed read streams"
                                            );
                                        }
                                    }
                                    Poll::Ready(None) => {
                                        // Shutdown....
                                        this.substream.set(None);
                                        this.state.set(State::Terminated);
                                        return Poll::Ready(Some(Err(ShutdownError.into())));
                                    }
                                    Poll::Ready(Some(TailState::Open(tail))) => {
                                        // Safe to consider this as a tail.
                                        *safe_known_tail = Some(tail);
                                    }
                                    Poll::Ready(Some(TailState::Sealed(_))) => {
                                        // Wait for reconfiguration to complete.
                                        //
                                        // Note that we don't reset the substream here because
                                        // reconfiguration might bring us back to Reading on the
                                        // same substream, we don't want to lose the resources
                                        // allocated by underlying the stream.
                                        this.state.set(State::awaiting_or_seal_chain());
                                        continue;
                                    }
                                }
                            }
                        }
                        // We are well within the bounds of this loglet. Continue reading.
                        Some(_) => { /* fall-through */ }
                    }
                    let maybe_record = substream.poll_next(cx);
                    match maybe_record {
                        Poll::Pending => {
                            // we also want to wait for tail segment changes so we get woken up if
                            // the segment was sealed even if the tail watch didn't report so.
                            if let Some(watch_fut) =
                                this.next_log_metadata_watch_fut.as_mut().as_pin_mut()
                            {
                                let _ = ready!(watch_fut.poll(cx))?;
                                continue;
                            } else {
                                unreachable!(
                                    "metadata_watch must be set on non-sealed read streams"
                                );
                            }
                        }
                        Poll::Ready(Some(Ok(record))) => {
                            let new_pointer = Self::calculate_read_pointer(&record);
                            debug_assert!(new_pointer > *this.read_pointer);
                            *this.read_pointer = new_pointer;
                            // If this record was supposed to be filtered but it was not filtered
                            // by the loglet itself, we skip it ourselves and advance the
                            // read_pointer.
                            if let Some(data_record) = record.as_record()
                                && !data_record.matches_key_query(this.filter)
                            {
                                // read_pointer is already advanced, just don't return the
                                // record and fast-forward.
                                continue;
                            }

                            return Poll::Ready(Some(Ok(record)));
                        }
                        // The assumption here is that underlying stream won't move its read
                        // pointer on error.
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
                        Poll::Ready(None) => {
                            // We should, almost never, reach this.
                            this.substream.set(None);
                            this.state.set(State::Terminated);
                            return Poll::Ready(None);
                        }
                    }
                }

                // Waiting for the substream's loglet to be sealed
                StateProj::AwaitingReconfiguration => {
                    let Some(mut substream) = this.substream.as_mut().as_pin_mut() else {
                        panic!("substream must be set at this point");
                    };

                    match check_chain(
                        chain,
                        *this.read_pointer,
                        substream.loglet().segment_index(),
                        substream.loglet().config.kind,
                    ) {
                        Decision::NewSegment => {
                            this.substream.set(None);
                            this.state.set(State::finding_loglet(
                                bifrost_inner,
                                *this.log_id,
                                *this.read_pointer,
                            ));
                            continue;
                        }
                        Decision::TailLsnSet { sealed_tail } => {
                            substream.set_tail_lsn(sealed_tail);
                            // go back to reading.
                            this.state.set(State::reading_to_known_tail(sealed_tail));
                            continue;
                        }
                        Decision::Trim { next_base_lsn } => {
                            // Deliver the trim gap
                            return Poll::Ready(Some(Ok(deliver_trim_gap(
                                &mut this,
                                next_base_lsn,
                                bifrost_inner,
                            ))));
                        }
                        Decision::NoChange => {
                            // Reconfiguration still ongoing, keep waiting.
                            // No hope at this metadata version, wait for the next update.
                            let watch_fut = this
                                .next_log_metadata_watch_fut
                                .as_mut()
                                .as_pin_mut()
                                .expect("metadata_watch must be set on non-sealed read streams");
                            let _ = ready!(watch_fut.poll(cx))?;
                            continue;
                        }
                    }
                }

                // Waiting for the current segment to be sealed (partial seal detected)
                StateProj::AwaitingOrSealChain { timeout } => {
                    let Some(mut substream) = this.substream.as_mut().as_pin_mut() else {
                        panic!("substream must be set at this point");
                    };

                    match check_chain(
                        chain,
                        *this.read_pointer,
                        substream.loglet().segment_index(),
                        substream.loglet().config.kind,
                    ) {
                        Decision::NewSegment => {
                            this.substream.set(None);
                            this.state.set(State::finding_loglet(
                                bifrost_inner,
                                *this.log_id,
                                *this.read_pointer,
                            ));
                            continue;
                        }
                        Decision::TailLsnSet { sealed_tail } => {
                            substream.set_tail_lsn(sealed_tail);
                            // go back to reading.
                            this.state.set(State::reading_to_known_tail(sealed_tail));
                            continue;
                        }
                        Decision::Trim { next_base_lsn } => {
                            // Deliver the trim gap
                            return Poll::Ready(Some(Ok(deliver_trim_gap(
                                &mut this,
                                next_base_lsn,
                                bifrost_inner,
                            ))));
                        }

                        Decision::NoChange => {}
                    }

                    // Check if we ran out of patience.
                    if let Poll::Ready(()) = timeout.as_mut().poll(cx) {
                        // we timed out, taking matters into our own hands and sealing the chain.
                        let seal_chain_fut = Box::pin(seal_chain(
                            bifrost_inner,
                            *this.log_id,
                            substream.loglet().segment_index(),
                        ));
                        this.state.set(State::SealingChain { seal_chain_fut });
                        continue;
                    }
                    // No hope at this metadata version, wait for the next update.
                    let watch_fut = this
                        .next_log_metadata_watch_fut
                        .as_mut()
                        .as_pin_mut()
                        .expect("metadata_watch must be set on non-sealed read streams");
                    let _ = ready!(watch_fut.poll(cx))?;
                    continue;
                }

                // Actively sealing the chain to restore read availability
                StateProj::SealingChain { seal_chain_fut } => {
                    // In this state, we are trying to recover from a partially sealed chain (sealed loglet
                    // in an open segment). We have already waited for a grace period before
                    // moving to this state. The goal is to finish the seal.
                    ready!(seal_chain_fut.poll(cx));
                    // Note that if the seal operation failed, awaiting-or-seal-chain
                    // will bring us back here again after the grace period.
                    this.state.set(State::awaiting_or_seal_chain());
                    continue;
                }

                StateProj::Terminated => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

// NOTE: This function assumes that chain sealing is supported/enabled.
async fn seal_chain(bifrost: &BifrostInner, log_id: LogId, segment_index: SegmentIndex) {
    match BifrostAdmin::new(bifrost)
        .seal(
            log_id,
            segment_index,
            SealMetadata::new("read-stream", my_node_id()),
        )
        .await
    {
        Ok(lsn) => {
            debug!(%log_id, %segment_index, "Reader sealed the chain at lsn={lsn}, will resume reading");
        }
        Err(Error::AdminError(AdminError::ChainSealIncomplete)) => {
            warn!(%log_id, %segment_index, "Reader failed to seal the chain (seal incomplete)")
        }
        Err(Error::AdminError(AdminError::SegmentMismatch { expected, found })) => {
            trace!(%log_id, %segment_index, "Reader didn't seal the chain, the tail segment has changed from {expected} to {found}");
        }
        Err(err) => {
            warn!(%log_id, %segment_index, %err, "Reader failed to seal the chain")
        }
    }
}

enum Decision {
    NoChange,
    NewSegment,
    TailLsnSet { sealed_tail: Lsn },
    Trim { next_base_lsn: Lsn },
}

fn check_chain(
    chain: &Chain,
    read_pointer: Lsn,
    current_segment_index: SegmentIndex,
    current_provider: InternalKind,
) -> Decision {
    // todo: the chain is "permanently sealed", what to do?
    match chain.find_segment_for_lsn(read_pointer) {
        MaybeSegment::Some(segment) => {
            // This is a different segment now, we need to recreate the substream.
            // This could mean that this is a new segment replacing the existing
            // one (if it was an empty sealed loglet) or that the loglet has been
            // sealed and the read_pointer points to the next segment. In all
            // cases, we want to get the right loglet.
            if segment.index() != current_segment_index || segment.config.kind != current_provider {
                Decision::NewSegment
            } else if let Some(sealed_tail) = segment.tail_lsn {
                Decision::TailLsnSet { sealed_tail }
            } else {
                Decision::NoChange
            }
        }
        // Oh, we have a prefix trim, deliver the trim-gap and fast-forward.
        MaybeSegment::Trim { next_base_lsn } => Decision::Trim { next_base_lsn },
    }
}

fn deliver_trim_gap(
    this: &mut ReadStreamProj,
    next_base_lsn: Lsn,
    bifrost_inner: &'static BifrostInner,
) -> LogEntry {
    let read_pointer = *this.read_pointer;
    let record = LogEntry::new_trim_gap(read_pointer, next_base_lsn.prev());
    // fast-forward.
    *this.read_pointer = next_base_lsn;
    let find_loglet_fut =
        Box::pin(bifrost_inner.find_loglet_for_lsn(*this.log_id, *this.read_pointer));
    // => Find Loglet
    this.substream.set(None);
    this.state.set(State::FindingLoglet { find_loglet_fut });
    record
}

#[cfg(all(test, feature = "local-loglet"))]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicUsize;

    use googletest::prelude::*;
    use tokio_stream::StreamExt;
    use tracing::info;
    use tracing_test::traced_test;

    use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_rocksdb::RocksDbManager;
    use restate_types::Versioned;
    use restate_types::config::LocalLogletOptions;
    use restate_types::live::{Constant, LiveLoadExt};
    use restate_types::logs::metadata::{ProviderKind, new_single_node_loglet_params};
    use restate_types::logs::{KeyFilter, SequenceNumber};
    use restate_types::metadata::Precondition;

    use crate::loglet::FindTailOptions;
    use crate::{BifrostService, ErrorRecoveryStrategy};

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    #[traced_test]
    async fn test_readstream_one_loglet() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let read_from = Lsn::from(6);

        let config = Constant::new(LocalLogletOptions::default()).boxed();
        RocksDbManager::init();

        let svc = BifrostService::new(env.metadata_writer).enable_local_loglet(config);
        let bifrost = svc.handle();
        svc.start().await.expect("loglet must start");

        let mut reader = bifrost.create_reader(LOG_ID, KeyFilter::Any, read_from, Lsn::MAX)?;
        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;

        let tail = bifrost
            .find_tail(LOG_ID, FindTailOptions::default())
            .await?;
        // no records have been written
        assert!(!tail.is_sealed());
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert_eq!(read_from, reader.read_pointer());

        // Nothing is trimmed
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        let read_counter = Arc::new(AtomicUsize::new(0));
        // spawn a reader that reads 5 records and exits.
        let counter_clone = read_counter.clone();
        let id = TaskCenter::spawn(TaskKind::TestRunner, "read-records", async move {
            for i in 6..=10 {
                let record = reader.next().await.expect("to never terminate")?;
                let expected_lsn = Lsn::from(i);
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert_that!(record.sequence_number(), eq(expected_lsn));
                assert_that!(reader.read_pointer(), ge(record.sequence_number()));
                assert_that!(
                    record.decode_unchecked::<String>(),
                    eq(format!("record{expected_lsn}"))
                );
            }
            Ok(())
        })?;

        let reader_bg_handle = TaskCenter::take_task(id).expect("read-records task to exist");

        tokio::task::yield_now().await;
        // Not finished, we still didn't append records
        assert!(!reader_bg_handle.is_finished());

        // append 5 records to the log
        for i in 1..=5 {
            let lsn = appender.append(format!("record{i}")).await?;
            info!(?lsn, "appended record");
            assert_eq!(Lsn::from(i), lsn);
        }

        // Written records are not enough for the reader to finish.
        // Not finished, we still didn't append records
        tokio::task::yield_now().await;
        assert!(!reader_bg_handle.is_finished());
        assert!(read_counter.load(std::sync::atomic::Ordering::Relaxed) == 0);

        // write 5 more records.
        for i in 6..=10 {
            appender.append(format!("record{i}")).await?;
        }

        // reader has finished
        reader_bg_handle.await?;
        assert_eq!(5, read_counter.load(std::sync::atomic::Ordering::Relaxed));

        Ok(())
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    #[traced_test]
    async fn test_read_stream_with_trim() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;
        let config = Constant::new(LocalLogletOptions::default()).boxed();
        RocksDbManager::init();

        let svc = BifrostService::new(node_env.metadata_writer.clone()).enable_local_loglet(config);
        let bifrost = svc.handle();

        svc.start().await.expect("loglet must start");

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;

        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        // append 10 records [1..10]
        for i in 1..=10 {
            let lsn = appender.append("").await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        // [1..5] trimmed. trim_point = 5
        bifrost.admin().trim(LOG_ID, Lsn::from(5)).await?;

        assert_eq!(
            Lsn::from(11),
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?
                .offset()
        );
        assert_eq!(Lsn::from(5), bifrost.get_trim_point(LOG_ID).await?);

        let mut read_stream =
            bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)?;

        let record = read_stream.next().await.unwrap()?;
        assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(5))));

        for lsn in 6..=7 {
            let record = read_stream.next().await.unwrap()?;
            assert_that!(record.sequence_number(), eq(Lsn::new(lsn)));
            assert!(record.is_data_record());
        }
        assert!(!read_stream.is_terminated());
        assert_eq!(Lsn::from(8), read_stream.read_pointer());

        let tail = bifrost
            .find_tail(LOG_ID, FindTailOptions::default())
            .await?
            .offset();
        // trimming beyond the release point will fall back to the release point
        bifrost.admin().trim(LOG_ID, Lsn::from(u64::MAX)).await?;
        let trim_point = bifrost.get_trim_point(LOG_ID).await?;
        assert_eq!(Lsn::from(10), bifrost.get_trim_point(LOG_ID).await?);
        // trim point becomes the point before the next slot available for writes (aka. the
        // tail)
        assert_eq!(tail.prev(), trim_point);

        // append lsns [11..20]
        for i in 11..=20 {
            let lsn = appender.append(format!("record{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        // read stream should send a gap from 8->10
        let record = read_stream.next().await.unwrap()?;
        assert_that!(record.sequence_number(), eq(Lsn::new(8)));
        assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(10))));

        // read pointer is at 11
        assert_eq!(Lsn::from(11), read_stream.read_pointer());

        // read the rest of the records
        for lsn in 11..=20 {
            let record = read_stream.next().await.unwrap()?;
            assert_that!(record.sequence_number(), eq(Lsn::new(lsn)));
            assert!(record.is_data_record());
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("record{lsn}"))
            );
        }
        // we are at tail. polling should return pending.
        let pinned = std::pin::pin!(read_stream.next());
        let next_is_pending = futures::poll!(pinned);
        assert!(matches!(next_is_pending, Poll::Pending));

        Ok(())
    }

    // Note: This test doesn't validate read stream behaviour with zombie records at seal boundary.
    #[restate_core::test(start_paused = true)]
    async fn test_readstream_simple_multi_loglet() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let config = Constant::new(LocalLogletOptions::default()).boxed();
        RocksDbManager::init();

        // enable both in-memory and local loglet types
        let svc = BifrostService::new(node_env.metadata_writer.clone())
            .enable_local_loglet(config)
            .enable_in_memory_loglet();
        let bifrost = svc.handle();
        svc.start().await.expect("loglet must start");

        // create the reader and put it on the side.
        let mut reader = bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)?;
        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;
        // We should be at tail, any attempt to read will yield `pending`.
        assert_that!(
            futures::poll!(std::pin::pin!(reader.next())),
            pat!(Poll::Pending)
        );

        let tail = bifrost
            .find_tail(LOG_ID, FindTailOptions::default())
            .await?;
        // no records have been written
        assert!(!tail.is_sealed());
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert_eq!(Lsn::OLDEST, reader.read_pointer());

        // Nothing is trimmed
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        // append 10 records [1..10]
        for i in 1..=10 {
            let lsn = appender.append(format!("segment-1-{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        // read 5 records.
        for i in 1..=5 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(reader.read_pointer(), eq(record.sequence_number().next()));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("segment-1-{i}"))
            );
        }

        // manually seal the loglet, create a new in-memory loglet at base_lsn=11
        let raw_loglet = bifrost
            .inner
            .find_loglet_for_lsn(LOG_ID, Lsn::new(5))
            .await?
            .unwrap();
        raw_loglet.seal().await?;
        // In fact, reader is allowed to go as far as the last known unsealed tail which
        // in our case could be the real tail since we didn't have in-flight appends at seal
        // time. It's legal for loglets to have lagging indicator of the unsealed pointer but
        // we know that local loglet won't do this.
        //
        // read 5 more records.
        println!("reading records at sealed loglet");
        for i in 6..=10 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(reader.read_pointer(), eq(record.sequence_number().next()));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("segment-1-{i}"))
            );
        }

        // reads should yield pending since we are at the last known unsealed tail
        // loglet.
        assert_that!(
            futures::poll!(std::pin::pin!(reader.next())),
            pat!(Poll::Pending)
        );
        // again.
        assert_that!(
            futures::poll!(std::pin::pin!(reader.next())),
            pat!(Poll::Pending)
        );

        // this will automatically seal the chain since it detects that the tail segment is
        // actually sealed.
        let tail = bifrost
            .find_tail(LOG_ID, FindTailOptions::default())
            .await?;

        assert!(tail.is_sealed());
        assert_eq!(Lsn::from(11), tail.offset());
        let metadata = Metadata::current();
        // perform manual reconfiguration (can be replaced with bifrost reconfiguration API
        // when it's implemented)
        let old_version = metadata.logs_version();
        let mut builder = metadata
            .logs_ref()
            .clone()
            .try_into_builder()
            .expect("can create builder");
        let mut chain_builder = builder.chain(LOG_ID).unwrap();
        assert_eq!(2, chain_builder.num_segments());
        let new_segment_params = new_single_node_loglet_params(ProviderKind::InMemory);
        chain_builder.append_segment(Lsn::new(11), ProviderKind::InMemory, new_segment_params)?;

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

        // append 5 more records into the new loglet.
        for i in 11..=15 {
            let lsn = appender.append(format!("segment-2-{i}")).await?;
            println!("appended record={lsn}");
            assert_eq!(Lsn::from(i), lsn);
        }

        // read stream should jump across segments.
        for i in 11..=15 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(reader.read_pointer(), eq(record.sequence_number().next()));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("segment-2-{i}"))
            );
        }
        // We are at tail. validate.
        assert_that!(
            futures::poll!(std::pin::pin!(reader.next())),
            pat!(Poll::Pending)
        );

        assert_eq!(
            Lsn::from(16),
            appender.append("segment-2-1000".to_owned()).await?
        );

        let record = reader.next().await.expect("to stay alive")?;
        assert_that!(record.sequence_number(), eq(Lsn::new(16)));
        assert_that!(record.decode_unchecked::<String>(), eq("segment-2-1000"));

        Ok(())
    }

    #[restate_core::test(start_paused = true)]
    async fn test_readstream_sealed_multi_loglet() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let config = Constant::new(LocalLogletOptions::default()).boxed();
        RocksDbManager::init();

        // enable both in-memory and local loglet types
        let svc = BifrostService::new(node_env.metadata_writer)
            .enable_local_loglet(config)
            .enable_in_memory_loglet();
        let bifrost = svc.handle();
        svc.start().await.expect("loglet must start");

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;

        let tail = bifrost
            .find_tail(LOG_ID, FindTailOptions::default())
            .await?;
        // no records have been written
        assert!(!tail.is_sealed());
        assert_eq!(Lsn::OLDEST, tail.offset());

        // append 10 records [1..10]
        for i in 1..=10 {
            let lsn = appender.append(format!("segment-1-{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        // seal the loglet and extend with an in-memory one
        let new_segment_params = new_single_node_loglet_params(ProviderKind::InMemory);
        bifrost
            .admin()
            .seal_and_extend_chain(
                LOG_ID,
                None,
                Version::MIN,
                ProviderKind::InMemory,
                new_segment_params,
            )
            .await?;

        let tail = bifrost
            .find_tail(LOG_ID, FindTailOptions::default())
            .await?;

        assert!(!tail.is_sealed());
        assert_eq!(Lsn::from(11), tail.offset());

        // validate that we have 2 segments now
        assert_eq!(
            2,
            node_env
                .metadata
                .logs_ref()
                .chain(&LOG_ID)
                .unwrap()
                .num_segments()
        );

        // validate that the first segment is sealed
        let segment_1_loglet = bifrost
            .inner
            .find_loglet_for_lsn(LOG_ID, Lsn::from(1))
            .await?
            .unwrap();

        assert_that!(
            segment_1_loglet
                .find_tail(FindTailOptions::default())
                .await?,
            pat!(TailState::Sealed(eq(Lsn::from(11))))
        );

        // append 5 more records into the new loglet.
        for i in 11..=15 {
            let lsn = appender.append(format!("segment-2-{i}")).await?;
            info!(?lsn, "appended record");
            assert_eq!(Lsn::from(i), lsn);
        }

        // start a reader (from 3) and read everything. [3..15]
        let mut reader = bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::new(3), Lsn::MAX)?;

        // first segment records
        for i in 3..=10 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(reader.read_pointer(), eq(record.sequence_number().next()));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("segment-1-{i}"))
            );
        }

        // first segment records
        for i in 11..=15 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(reader.read_pointer(), eq(record.sequence_number().next()));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("segment-2-{i}"))
            );
        }

        // We are at tail. validate.
        assert_that!(
            futures::poll!(std::pin::pin!(reader.next())),
            pat!(Poll::Pending)
        );

        Ok(())
    }

    #[restate_core::test(start_paused = true)]
    async fn test_readstream_chain_sealing() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let config = Constant::new(LocalLogletOptions::default()).boxed();
        RocksDbManager::init();

        // enable both in-memory and local loglet types
        let svc = BifrostService::new(node_env.metadata_writer)
            .enable_local_loglet(config)
            .enable_in_memory_loglet();
        let bifrost = svc.handle();
        svc.start().await.expect("loglet must start");

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;

        // append 7 records [1..7]
        for i in 1..=7 {
            let lsn = appender.append(format!("segment-1-{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        // start a reader (from 3) and read everything. [3..]
        let mut reader = bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::new(3), Lsn::MAX)?;

        // first segment up to 7
        for i in 3..=7 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(reader.read_pointer(), eq(record.sequence_number().next()));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("segment-1-{i}"))
            );
        }

        assert_that!(reader.safe_known_tail(), some(eq(Lsn::new(8))));
        // Appending 2 more records without driving the read stream
        appender.append("segment-1-8").await?;
        appender.append("segment-1-9").await?;
        appender.append("segment-1-10").await?;
        // reader still thinks that safe tail is 8
        assert_that!(reader.safe_known_tail(), some(eq(Lsn::new(8))));
        // The loglet is sealed, we should observe that the reader is stalled
        let current_loglet = bifrost.inner.tail_loglet(LOG_ID).await?;
        current_loglet.seal().await?;
        {
            let now = tokio::time::Instant::now();
            let record = reader.next().await.expect("to stay alive")?;
            // note: change this value when patience threshold is changed in read stream logic.
            assert_that!(now.elapsed(), gt(Duration::from_secs(2)));
            assert_that!(record.sequence_number(), eq(Lsn::new(8)));
            assert_that!(record.decode_unchecked::<String>(), eq("segment-1-8"));
            assert_that!(reader.safe_known_tail(), some(eq(Lsn::new(11))));
        }
        {
            // the chain must be sealed at Lsn::new(11)
            let logs = node_env.metadata.logs_ref();
            let chain = logs.chain(&LOG_ID).unwrap();
            assert_that!(chain.num_segments(), eq(2));
            assert_that!(chain.sealed_tail(), some(eq(Lsn::new(11))));
        }

        // I can read all the way to Lsn=10
        for i in 9..=10 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(reader.read_pointer(), eq(record.sequence_number().next()));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("segment-1-{i}"))
            );
        }

        // further reads will timeout
        assert!(
            tokio::time::timeout(Duration::from_secs(200), reader.next())
                .await
                .is_err()
        );

        // extend with an in-memory one
        let new_segment_params = new_single_node_loglet_params(ProviderKind::InMemory);
        bifrost
            .admin()
            .seal_and_extend_chain(
                LOG_ID,
                None,
                Version::MIN,
                ProviderKind::InMemory,
                new_segment_params,
            )
            .await?;

        // validate that we have 2 segments now (sealed marker was replaced)
        assert_eq!(
            2,
            node_env
                .metadata
                .logs_ref()
                .chain(&LOG_ID)
                .unwrap()
                .num_segments()
        );

        // append 5 more records into the new loglet.
        for i in 11..=15 {
            let lsn = appender.append(format!("segment-2-{i}")).await?;
            info!(?lsn, "appended record");
            assert_eq!(Lsn::from(i), lsn);
        }

        // Reader is unblocked
        for i in 11..=15 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(reader.read_pointer(), eq(record.sequence_number().next()));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("segment-2-{i}"))
            );
        }

        Ok(())
    }

    #[restate_core::test(start_paused = true)]
    async fn test_readstream_prefix_trimmed() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let config = Constant::new(LocalLogletOptions::default()).boxed();
        RocksDbManager::init();

        // enable both in-memory and local loglet types
        let svc = BifrostService::new(node_env.metadata_writer.clone())
            .enable_local_loglet(config)
            .enable_in_memory_loglet();
        let bifrost = svc.handle();
        svc.start().await.expect("loglet must start");
        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::Wait)?;

        let metadata = Metadata::current();
        // prepare a chain that starts from Lsn 10 (we expect trim from OLDEST -> 9)
        let old_version = metadata.logs_version();
        let mut builder = metadata
            .logs_ref()
            .clone()
            .try_into_builder()
            .expect("can create builder");
        let mut chain_builder = builder.chain(LOG_ID).unwrap();
        assert_eq!(1, chain_builder.num_segments());
        let new_segment_params = new_single_node_loglet_params(ProviderKind::Local);
        chain_builder.append_segment(Lsn::new(10), ProviderKind::Local, new_segment_params)?;
        chain_builder.trim_prefix(Lsn::new(10));
        assert_eq!(1, chain_builder.num_segments());
        assert_eq!(Lsn::from(10), chain_builder.tail().base_lsn);

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

        let mut reader = bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)?;

        // append a few records
        for i in 10..=13 {
            let lsn = appender.append(format!("record-{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        // first read should be the trim gap.
        let record = reader.next().await.expect("to stay alive")?;
        assert_that!(record.sequence_number(), eq(Lsn::OLDEST));
        assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(9))));
        assert_that!(reader.read_pointer(), eq(Lsn::from(10)));

        // read records
        for i in 10..=13 {
            let record = reader.next().await.expect("to stay alive")?;
            assert_that!(record.sequence_number(), eq(Lsn::new(i)));
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("record-{i}"))
            );
        }

        Ok(())
    }
}
