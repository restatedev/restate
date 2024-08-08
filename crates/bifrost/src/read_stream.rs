// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use std::task::ready;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::stream::FusedStream;
use futures::Stream;
use pin_project::pin_project;

use restate_core::MetadataKind;
use restate_core::ShutdownError;
use restate_types::logs::metadata::MaybeSegment;
use restate_types::logs::KeyFilter;
use restate_types::logs::SequenceNumber;
use restate_types::logs::{LogId, Lsn};
use restate_types::Version;
use restate_types::Versioned;

use crate::bifrost::BifrostInner;
use crate::bifrost::MaybeLoglet;
use crate::loglet::LogletBase;
use crate::loglet::OperationError;
use crate::loglet_wrapper::LogletReadStreamWrapper;
use crate::Error;
use crate::LogRecord;
use crate::Result;
use crate::TailState;

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
    /// Waiting for the tail LSN of the substream's loglet to be determined (sealing in-progress)
    AwaitingReconfiguration {
        /// Future to continue waiting on log metadata updates
        #[pin]
        log_metadata_watch_fut: Option<BoxFuture<'static, Result<Version, ShutdownError>>>,
    },
    Terminated,
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
        Ok(Self {
            bifrost_inner,
            log_id,
            filter,
            read_pointer: start_lsn,
            end_lsn,
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
    fn calculate_read_pointer(record: &LogRecord) -> Lsn {
        match &record.record {
            // On trim gaps, we fast-forward the read pointer beyond the end of the gap. We do
            // this after delivering a TrimGap record. This means that the next read operation
            // skips over the boundary of the gap.
            crate::Record::TrimGap(trim_gap) => trim_gap.to,
            crate::Record::Data(_) => record.offset,
        }
        .next()
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
    type Item = Result<LogRecord>;

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
                        loglet.create_wrapped_read_stream(this.filter.clone(), *this.read_pointer),
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
                        Some(substream.loglet().watch_tail())
                    } else {
                        None
                    };
                    // => Start Reading
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
                                // Wait for tail update...
                                let Some(tail_watch) = tail_watch.as_pin_mut() else {
                                    panic!("tail_watch must be set on non-sealed read streams");
                                };
                                // If the loglet is being sealed, we must wait for reconfiguration to complete.
                                let maybe_tail = ready!(tail_watch.poll_next(cx));
                                match maybe_tail {
                                    None => {
                                        // Shutdown....
                                        this.substream.set(None);
                                        this.state.set(State::Terminated);
                                        return Poll::Ready(Some(Err(ShutdownError.into())));
                                    }
                                    Some(TailState::Open(tail)) => {
                                        // Safe to consider this as a tail.
                                        *safe_known_tail = Some(tail);
                                    }
                                    Some(TailState::Sealed(_)) => {
                                        // Wait for reconfiguration to complete.
                                        //
                                        // Note that we don't reset the substream here because
                                        // reconfiguration might bring us back to Reading on the
                                        // same substream, we don't want to lose the resources
                                        // allocated by underlying the stream.
                                        this.state.set(State::AwaitingReconfiguration {
                                            log_metadata_watch_fut: None,
                                        });
                                        continue;
                                    }
                                }
                            }
                        }
                        // We are well within the bounds of this loglet. Continue reading.
                        Some(_) => { /* fall-through */ }
                    }
                    let maybe_record = ready!(substream.poll_next(cx));
                    match maybe_record {
                        Some(Ok(record)) => {
                            let record = record
                                .decode()
                                .expect("decoding a bifrost envelope succeeds");
                            let new_pointer = Self::calculate_read_pointer(&record);
                            debug_assert!(new_pointer > *this.read_pointer);
                            *this.read_pointer = new_pointer;
                            return Poll::Ready(Some(Ok(record)));
                        }
                        // The assumption here is that underlying stream won't move its read
                        // pointer on error.
                        Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                        None => {
                            // We should, almost never, reach this.
                            this.substream.set(None);
                            this.state.set(State::Terminated);
                            return Poll::Ready(None);
                        }
                    }
                }

                // Waiting for the substream's loglet to be sealed
                StateProj::AwaitingReconfiguration {
                    mut log_metadata_watch_fut,
                } => {
                    // If a metadata watch is set, poll it.
                    if let Some(watch_fut) = log_metadata_watch_fut.as_mut().as_pin_mut() {
                        let _ = ready!(watch_fut.poll(cx))?;
                    }

                    let Some(mut substream) = this.substream.as_mut().as_pin_mut() else {
                        panic!("substream must be set at this point");
                    };

                    let log_metadata = bifrost_inner.metadata.logs();

                    // The log is gone!
                    let Some(chain) = log_metadata.chain(this.log_id) else {
                        this.substream.set(None);
                        this.state.set(State::Terminated);
                        return Poll::Ready(Some(Err(Error::UnknownLogId(*this.log_id))));
                    };

                    match chain.find_segment_for_lsn(*this.read_pointer) {
                        MaybeSegment::Some(segment) => {
                            // This is a different segment now, we need to recreate the substream.
                            // This could mean that this is a new segment replacing the existing
                            // one (if it was an empty sealed loglet) or that the loglet has been
                            // sealed and the read_pointer points to the next segment. In all
                            // cases, we want to get the right loglet.
                            if segment.index() != substream.loglet().segment_index() {
                                this.substream.set(None);
                                let find_loglet_fut = Box::pin(
                                    bifrost_inner
                                        .find_loglet_for_lsn(*this.log_id, *this.read_pointer),
                                );
                                // => Find Loglet
                                this.state.set(State::FindingLoglet { find_loglet_fut });
                                continue;
                            }
                            if segment.tail_lsn.is_some() {
                                let sealed_tail = segment.tail_lsn.unwrap();
                                substream.set_tail_lsn(segment.tail_lsn.unwrap());
                                // go back to reading.
                                this.state.set(State::Reading {
                                    safe_known_tail: Some(sealed_tail),
                                    // No need for the tail watch since we know the tail already.
                                    tail_watch: None,
                                });
                                continue;
                            }
                            // Segment is not sealed yet.
                            // fall-through
                        }
                        // Oh, we have a prefix trim, deliver the trim-gap and fast-forward.
                        MaybeSegment::Trim { next_base_lsn } => {
                            let record = deliver_trim_gap(&mut this, next_base_lsn, bifrost_inner);
                            // Deliver the trim gap
                            return Poll::Ready(Some(Ok(record)));
                        }
                    };

                    // Reconfiguration still ongoing...
                    let metadata_version = log_metadata.version();

                    // No hope at this metadata version, wait for the next update.
                    let metadata_watch_fut = Box::pin(
                        bifrost_inner
                            .metadata
                            .wait_for_version(MetadataKind::Logs, metadata_version.next()),
                    );
                    log_metadata_watch_fut.set(Some(metadata_watch_fut));
                    continue;
                }
                StateProj::Terminated => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

fn deliver_trim_gap(
    this: &mut ReadStreamProj,
    next_base_lsn: Lsn,
    bifrost_inner: &'static BifrostInner,
) -> LogRecord {
    let read_pointer = *this.read_pointer;
    let record = LogRecord::new_trim_gap(read_pointer, next_base_lsn.prev());
    // fast-forward.
    *this.read_pointer = next_base_lsn;
    let find_loglet_fut =
        Box::pin(bifrost_inner.find_loglet_for_lsn(*this.log_id, *this.read_pointer));
    // => Find Loglet
    this.substream.set(None);
    this.state.set(State::FindingLoglet { find_loglet_fut });
    record
}

#[cfg(test)]
mod tests {

    use std::sync::atomic::AtomicUsize;

    use crate::loglet::LogletBase;
    use crate::payload::Payload;
    use crate::{
        setup_panic_handler, BifrostAdmin, BifrostService, FindTailAttributes, Record, TrimGap,
    };

    use super::*;
    use bytes::Bytes;
    use googletest::prelude::*;

    use restate_core::{
        metadata, task_center, MetadataKind, TargetVersion, TaskKind, TestCoreEnvBuilder,
    };
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{CommonOptions, Configuration};
    use restate_types::live::{Constant, Live};
    use restate_types::logs::metadata::{new_single_node_loglet_params, ProviderKind};
    use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
    use restate_types::Versioned;
    use tokio_stream::StreamExt;
    use tracing::info;
    use tracing_test::traced_test;

    use restate_types::logs::{KeyFilter, SequenceNumber};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[traced_test]
    async fn test_readstream_one_loglet() -> anyhow::Result<()> {
        setup_panic_handler();
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let tc = node_env.tc;
        tc.run_in_scope("test", None, async {
            let read_from = Lsn::from(6);

            let config = Live::from_value(Configuration::default());
            RocksDbManager::init(Constant::new(CommonOptions::default()));

            let svc = BifrostService::new(task_center(), metadata()).enable_local_loglet(&config);
            let bifrost = svc.handle();
            svc.start().await.expect("loglet must start");

            let mut reader = bifrost.create_reader(LOG_ID, KeyFilter::Any, read_from, Lsn::MAX)?;
            let mut appender = bifrost.create_appender(LOG_ID)?;

            let tail = bifrost
                .find_tail(LOG_ID, FindTailAttributes::default())
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
            let id = tc.spawn(TaskKind::TestRunner, "read-records", None, async move {
                for i in 6..=10 {
                    let record = reader.next().await.expect("to never terminate")?;
                    let expected_lsn = Lsn::from(i);
                    counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    assert_eq!(expected_lsn, record.offset);
                    assert!(reader.read_pointer() > record.offset);
                    assert_eq!(
                        Payload::new(format!("record{}", expected_lsn)).body(),
                        record.record.into_payload_unchecked().body()
                    );
                }
                Ok(())
            })?;

            let reader_bg_handle = tc.take_task(id).expect("read-records task to exist");

            tokio::task::yield_now().await;
            // Not finished, we still didn't append records
            assert!(!reader_bg_handle.is_finished());

            // append 5 records to the log
            for i in 1..=5 {
                let lsn = appender.append_raw(format!("record{}", i)).await?;
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
                appender.append_raw(format!("record{}", i)).await?;
            }

            // reader has finished
            reader_bg_handle.await?;
            assert_eq!(5, read_counter.load(std::sync::atomic::Ordering::Relaxed));

            anyhow::Ok(())
        })
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[traced_test]
    async fn test_read_stream_with_trim() -> anyhow::Result<()> {
        setup_panic_handler();
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;
        node_env
            .tc
            .run_in_scope("test", None, async {
                let config = Live::from_value(Configuration::default());
                RocksDbManager::init(Constant::new(CommonOptions::default()));

                let svc =
                    BifrostService::new(task_center(), metadata()).enable_local_loglet(&config);
                let bifrost = svc.handle();

                let bifrost_admin = BifrostAdmin::new(
                    &bifrost,
                    &node_env.metadata_writer,
                    &node_env.metadata_store_client,
                );
                svc.start().await.expect("loglet must start");

                let mut appender = bifrost.create_appender(LOG_ID)?;

                assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

                // append 10 records [1..10]
                for i in 1..=10 {
                    let lsn = appender.append_raw("").await?;
                    assert_eq!(Lsn::from(i), lsn);
                }

                // [1..5] trimmed. trim_point = 5
                bifrost_admin.trim(LOG_ID, Lsn::from(5)).await?;

                assert_eq!(
                    Lsn::from(11),
                    bifrost
                        .find_tail(LOG_ID, FindTailAttributes::default())
                        .await?
                        .offset(),
                );
                assert_eq!(Lsn::from(5), bifrost.get_trim_point(LOG_ID).await?);

                let mut read_stream =
                    bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)?;

                let record = read_stream.next().await.unwrap()?;
                assert_that!(
                    record,
                    pat!(LogRecord {
                        offset: eq(Lsn::from(1)),
                        record: pat!(Record::TrimGap(pat!(TrimGap {
                            to: eq(Lsn::from(5)),
                        })))
                    })
                );

                for lsn in 6..=7 {
                    let record = read_stream.next().await.unwrap()?;
                    assert_that!(
                        record,
                        pat!(LogRecord {
                            offset: eq(Lsn::from(lsn)),
                            record: pat!(Record::Data(_))
                        })
                    );
                }
                assert!(!read_stream.is_terminated());
                assert_eq!(Lsn::from(8), read_stream.read_pointer());

                let tail = bifrost
                    .find_tail(LOG_ID, FindTailAttributes::default())
                    .await?
                    .offset();
                // trimming beyond the release point will fall back to the release point
                bifrost_admin.trim(LOG_ID, Lsn::from(u64::MAX)).await?;
                let trim_point = bifrost.get_trim_point(LOG_ID).await?;
                assert_eq!(Lsn::from(10), bifrost.get_trim_point(LOG_ID).await?);
                // trim point becomes the point before the next slot available for writes (aka. the
                // tail)
                assert_eq!(tail.prev(), trim_point);

                // append lsns [11..20]
                for i in 11..=20 {
                    let lsn = appender.append_raw(format!("record{}", i)).await?;
                    assert_eq!(Lsn::from(i), lsn);
                }

                // read stream should send a gap from 8->10
                let record = read_stream.next().await.unwrap()?;
                assert_that!(
                    record,
                    pat!(LogRecord {
                        offset: eq(Lsn::from(8)),
                        record: pat!(Record::TrimGap(pat!(TrimGap {
                            to: eq(Lsn::from(10)),
                        })))
                    })
                );

                // read pointer is at 11
                assert_eq!(Lsn::from(11), read_stream.read_pointer());

                // read the rest of the records
                for lsn in 11..=20 {
                    let expected_body = Bytes::from(format!("record{}", lsn));
                    let record = read_stream.next().await.unwrap()?;
                    assert_that!(record.offset, eq(Lsn::from(lsn)));
                    assert!(record.record.is_data());
                    assert_that!(
                        record.record.try_as_data_ref().unwrap().body(),
                        eq(expected_body)
                    );
                }
                // we are at tail. polling should return pending.
                let pinned = std::pin::pin!(read_stream.next());
                let next_is_pending = futures::poll!(pinned);
                assert!(matches!(next_is_pending, Poll::Pending));

                Ok(())
            })
            .await
    }

    // Note: This test doesn't validate read stream behaviour with zombie records at seal boundary.
    #[tokio::test(start_paused = true)]
    async fn test_readstream_simple_multi_loglet() -> anyhow::Result<()> {
        setup_panic_handler();
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let tc = node_env.tc;
        tc.run_in_scope("test", None, async {
            let config = Live::from_value(Configuration::default());
            RocksDbManager::init(Constant::new(CommonOptions::default()));

            // enable both in-memory and local loglet types
            let svc = BifrostService::new(task_center(), metadata())
                .enable_local_loglet(&config)
                .enable_in_memory_loglet();
            let bifrost = svc.handle();
            svc.start().await.expect("loglet must start");

            // create the reader and put it on the side.
            let mut reader =
                bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)?;
            let mut appender = bifrost.create_appender(LOG_ID)?;
            // We should be at tail, any attempt to read will yield `pending`.
            assert_that!(
                futures::poll!(std::pin::pin!(reader.next())),
                pat!(Poll::Pending)
            );

            let tail = bifrost
                .find_tail(LOG_ID, FindTailAttributes::default())
                .await?;
            // no records have been written
            assert!(!tail.is_sealed());
            assert_eq!(Lsn::OLDEST, tail.offset());
            assert_eq!(Lsn::OLDEST, reader.read_pointer());

            // Nothing is trimmed
            assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

            // append 10 records [1..10]
            for i in 1..=10 {
                let lsn = appender.append_raw(format!("segment-1-{}", i)).await?;
                assert_eq!(Lsn::from(i), lsn);
            }

            // read 5 records.
            for i in 1..=5 {
                let record = reader.next().await.expect("to stay alive")?;
                assert_eq!(Lsn::from(i), record.offset);
                assert_eq!(reader.read_pointer(), record.offset.next());
                assert_eq!(
                    Payload::new(format!("segment-1-{}", i)).body(),
                    record.record.into_payload_unchecked().body()
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
                assert_eq!(Lsn::from(i), record.offset);
                assert_eq!(reader.read_pointer(), record.offset.next());
                assert_eq!(
                    Payload::new(format!("segment-1-{}", i)).body(),
                    record.record.into_payload_unchecked().body()
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

            let tail = bifrost
                .find_tail(LOG_ID, FindTailAttributes::default())
                .await?;

            assert!(tail.is_sealed());
            assert_eq!(Lsn::from(11), tail.offset());
            // perform manual reconfiguration (can be replaced with bifrost reconfiguration API
            // when it's implemented)
            let old_version = bifrost.inner.metadata.logs_version();
            let mut builder = bifrost.inner.metadata.logs().clone().into_builder();
            let mut chain_builder = builder.chain(&LOG_ID).unwrap();
            assert_eq!(1, chain_builder.num_segments());
            let new_segment_params = new_single_node_loglet_params(ProviderKind::InMemory);
            chain_builder.append_segment(
                Lsn::new(11),
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
            bifrost
                .inner
                .metadata
                .sync(MetadataKind::Logs, TargetVersion::Latest)
                .await?;

            // append 5 more records into the new loglet.
            for i in 11..=15 {
                let lsn = appender.append_raw(format!("segment-2-{}", i)).await?;
                println!("appended record={}", lsn);
                assert_eq!(Lsn::from(i), lsn);
            }

            // read stream should jump across segments.
            for i in 11..=15 {
                let record = reader.next().await.expect("to stay alive")?;
                assert_eq!(Lsn::from(i), record.offset);
                assert_eq!(reader.read_pointer(), record.offset.next());
                assert_eq!(
                    Payload::new(format!("segment-2-{}", i)).body(),
                    record.record.into_payload_unchecked().body()
                );
            }
            // We are at tail. validate.
            assert_that!(
                futures::poll!(std::pin::pin!(reader.next())),
                pat!(Poll::Pending)
            );

            assert_eq!(Lsn::from(16), appender.append_raw("segment-2-1000").await?);

            let record = reader.next().await.expect("to stay alive")?;
            assert_eq!(Lsn::from(16), record.offset);
            assert_eq!(
                Payload::new("segment-2-1000").body(),
                record.record.into_payload_unchecked().body()
            );

            anyhow::Ok(())
        })
        .await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_readstream_sealed_multi_loglet() -> anyhow::Result<()> {
        setup_panic_handler();
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let tc = node_env.tc;
        tc.run_in_scope("test", None, async {
            let config = Live::from_value(Configuration::default());
            RocksDbManager::init(Constant::new(CommonOptions::default()));

            // enable both in-memory and local loglet types
            let svc = BifrostService::new(task_center(), metadata())
                .enable_local_loglet(&config)
                .enable_in_memory_loglet();
            let bifrost = svc.handle();
            let bifrost_admin = BifrostAdmin::new(
                &bifrost,
                &node_env.metadata_writer,
                &node_env.metadata_store_client,
            );
            svc.start().await.expect("loglet must start");

            let mut appender = bifrost.create_appender(LOG_ID)?;

            let tail = bifrost
                .find_tail(LOG_ID, FindTailAttributes::default())
                .await?;
            // no records have been written
            assert!(!tail.is_sealed());
            assert_eq!(Lsn::OLDEST, tail.offset());

            // append 10 records [1..10]
            for i in 1..=10 {
                let lsn = appender.append_raw(format!("segment-1-{}", i)).await?;
                assert_eq!(Lsn::from(i), lsn);
            }

            // seal the loglet and extend with an in-memory one
            let new_segment_params = new_single_node_loglet_params(ProviderKind::InMemory);
            bifrost_admin
                .seal_and_extend_chain(
                    LOG_ID,
                    None,
                    Version::MIN,
                    ProviderKind::InMemory,
                    new_segment_params,
                )
                .await?;

            let tail = bifrost
                .find_tail(LOG_ID, FindTailAttributes::default())
                .await?;

            assert!(!tail.is_sealed());
            assert_eq!(Lsn::from(11), tail.offset());

            // validate that we have 2 segments now
            assert_eq!(
                2,
                node_env
                    .metadata
                    .logs()
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
                segment_1_loglet.find_tail().await?,
                pat!(TailState::Sealed(eq(Lsn::from(11))))
            );

            // append 5 more records into the new loglet.
            for i in 11..=15 {
                let lsn = appender.append_raw(format!("segment-2-{}", i)).await?;
                info!(?lsn, "appended record");
                assert_eq!(Lsn::from(i), lsn);
            }

            // start a reader (from 3) and read everything. [3..15]
            let mut reader =
                bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::new(3), Lsn::MAX)?;

            // first segment records
            for i in 3..=10 {
                let record = reader.next().await.expect("to stay alive")?;
                assert_eq!(Lsn::from(i), record.offset);
                assert_eq!(reader.read_pointer(), record.offset.next());
                assert_eq!(
                    Payload::new(format!("segment-1-{}", i)).body(),
                    record.record.into_payload_unchecked().body()
                );
            }

            // first segment records
            for i in 11..=15 {
                let record = reader.next().await.expect("to stay alive")?;
                assert_eq!(Lsn::from(i), record.offset);
                assert_eq!(reader.read_pointer(), record.offset.next());
                assert_eq!(
                    Payload::new(format!("segment-2-{}", i)).body(),
                    record.record.into_payload_unchecked().body()
                );
            }

            // We are at tail. validate.
            assert_that!(
                futures::poll!(std::pin::pin!(reader.next())),
                pat!(Poll::Pending)
            );

            anyhow::Ok(())
        })
        .await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_readstream_prefix_trimmed() -> anyhow::Result<()> {
        setup_panic_handler();
        const LOG_ID: LogId = LogId::new(0);

        let node_env = TestCoreEnvBuilder::new_with_mock_network()
            .set_provider_kind(ProviderKind::Local)
            .build()
            .await;

        let tc = node_env.tc;
        tc.run_in_scope("test", None, async {
            let config = Live::from_value(Configuration::default());
            RocksDbManager::init(Constant::new(CommonOptions::default()));

            // enable both in-memory and local loglet types
            let svc = BifrostService::new(task_center(), metadata())
                .enable_local_loglet(&config)
                .enable_in_memory_loglet();
            let bifrost = svc.handle();
            svc.start().await.expect("loglet must start");
            let mut appender = bifrost.create_appender(LOG_ID)?;

            // prepare a chain that starts from Lsn 10 (we expect trim from OLDEST -> 9)
            let old_version = bifrost.inner.metadata.logs_version();
            let mut builder = bifrost.inner.metadata.logs().clone().into_builder();
            let mut chain_builder = builder.chain(&LOG_ID).unwrap();
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
                .metadata_store_client
                .put(
                    BIFROST_CONFIG_KEY.clone(),
                    new_metadata,
                    restate_metadata_store::Precondition::MatchesVersion(old_version),
                )
                .await?;

            // make sure we have updated metadata.
            bifrost
                .inner
                .metadata
                .sync(MetadataKind::Logs, TargetVersion::Latest)
                .await?;

            let mut reader =
                bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)?;

            // append a few records
            for i in 10..=13 {
                let lsn = appender.append_raw(format!("record-{}", i)).await?;
                assert_eq!(Lsn::from(i), lsn);
            }

            // first read should be the trim gap.
            let record = reader.next().await.expect("to stay alive")?;

            assert_that!(
                record,
                pat!(LogRecord {
                    offset: eq(Lsn::OLDEST),
                    record: pat!(Record::TrimGap(pat!(TrimGap {
                        to: eq(Lsn::from(9)),
                    })))
                })
            );

            assert_that!(reader.read_pointer(), eq(Lsn::from(10)));

            // read records
            for i in 10..=13 {
                let record = reader.next().await.expect("to stay alive")?;
                assert_eq!(Lsn::from(i), record.offset);
                assert_eq!(
                    Payload::new(format!("record-{}", i)).body(),
                    record.record.into_payload_unchecked().body()
                );
            }

            anyhow::Ok(())
        })
        .await?;
        Ok(())
    }
}
