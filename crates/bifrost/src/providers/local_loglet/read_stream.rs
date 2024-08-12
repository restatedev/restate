// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Poll;

use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use rocksdb::{DBRawIteratorWithThreadMode, DB};
use tracing::{debug, error, warn};

use restate_core::ShutdownError;
use restate_rocksdb::RocksDbPerfGuard;
use restate_types::logs::{KeyFilter, SequenceNumber};

use crate::loglet::{LogletBase, LogletOffset, LogletReadStream, OperationError};
use crate::providers::local_loglet::LogStoreError;
use crate::{LogEntry, Result, TailState};

use super::keys::RecordKey;
use super::LocalLoglet;

pub(crate) struct LocalLogletReadStream {
    log_id: u64,
    // the next record this stream will attempt to read
    read_pointer: LogletOffset,
    /// stop when read_pointer is at or beyond this offset
    last_known_tail: LogletOffset,
    /// Last offset to read before terminating the stream. None means "tailing" reader.
    read_to: Option<LogletOffset>,
    iterator: DBRawIteratorWithThreadMode<'static, DB>,
    tail_watch: BoxStream<'static, TailState<LogletOffset>>,
    terminated: bool,
    // IMPORTANT: Do not reorder, this should be dropped last since `iterator` holds a reference
    // into the underlying database.
    loglet: Arc<LocalLoglet>,
}

// ## Safety
// The iterator is guaranteed to be dropped before the loglet is dropped, we hold to the
// loglet in this struct for as long as the stream is alive.
unsafe fn ignore_iterator_lifetime<'a>(
    iter: DBRawIteratorWithThreadMode<'a, DB>,
) -> DBRawIteratorWithThreadMode<'static, DB> {
    std::mem::transmute::<
        DBRawIteratorWithThreadMode<'a, DB>,
        DBRawIteratorWithThreadMode<'static, DB>,
    >(iter)
}

impl LocalLogletReadStream {
    pub(crate) async fn create(
        loglet: Arc<LocalLoglet>,
        _filter: KeyFilter,
        from_offset: LogletOffset,
        to: Option<LogletOffset>,
    ) -> Result<Self, OperationError> {
        // Reading from INVALID resets to OLDEST.
        let from_offset = from_offset.max(LogletOffset::OLDEST);
        // We seek to next key on every iteration, we need to setup the iterator to be
        // at the previous key within the same prefix if from_offset > 0 (saturating to
        // LogletOffset::INVALID)
        let key = RecordKey::new(loglet.loglet_id, from_offset.prev());
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_tailing(true);
        // In some cases, the underlying ForwardIterator will fail if it hits a `RangeDelete` tombstone.
        // For our purposes, we can ignore these tombstones, meaning that we will return those records
        // instead of a gap.
        // In summary, if loglet reader started before a trim point and data is readable, we should
        // continue reading them. It's the responsibility of the upper layer to decide on a sane
        // value of _from_offset_.
        read_opts.set_ignore_range_deletions(true);
        read_opts.set_prefix_same_as_start(true);
        read_opts.set_total_order_seek(false);
        read_opts.set_iterate_lower_bound(key.to_bytes());
        read_opts.set_iterate_upper_bound(RecordKey::upper_bound(loglet.loglet_id).to_bytes());

        let log_store = &loglet.log_store;
        let mut tail_watch = loglet.watch_tail();
        let last_known_tail = tail_watch
            .next()
            .await
            .expect("loglet watch returns tail pointer")
            .offset();

        // ## Safety:
        // the iterator is guaranteed to be dropped before the loglet is dropped, we hold to the
        // loglet in this struct for as long as the stream is alive.
        let iter = {
            let data_cf = log_store.data_cf();
            let iter = loglet
                .log_store
                .db()
                .raw_iterator_cf_opt(&data_cf, read_opts);
            // todo: potentially blocking but we don't create read streams often.
            unsafe { ignore_iterator_lifetime(iter) }
        };

        Ok(Self {
            log_id: loglet.loglet_id,
            loglet,
            read_pointer: from_offset,
            iterator: iter,
            terminated: false,
            tail_watch,
            last_known_tail,
            read_to: to,
        })
    }
}

impl LogletReadStream<LogletOffset> for LocalLogletReadStream {
    /// Current read pointer. This points to the next offset to be read.
    fn read_pointer(&self) -> LogletOffset {
        self.read_pointer
    }
    /// Returns true if the stream is terminated.
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

impl Stream for LocalLogletReadStream {
    type Item = Result<LogEntry<LogletOffset, Bytes>, OperationError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        let next_offset = self.read_pointer;

        let perf_guard = RocksDbPerfGuard::new("local-loglet-next");
        loop {
            // We have reached the limit we are allowed to read
            if self.read_to.is_some_and(|read_to| next_offset > read_to) {
                self.terminated = true;
                return Poll::Ready(None);
            }
            // Are we reading after commit offset?
            // We are at tail. We need to wait until new records have been released.
            if next_offset >= self.last_known_tail {
                let maybe_tail_state = match self.tail_watch.poll_next_unpin(cx) {
                    Poll::Ready(t) => t,
                    Poll::Pending => {
                        perf_guard.forget();
                        return Poll::Pending;
                    }
                };

                match maybe_tail_state {
                    Some(tail_state) => {
                        self.last_known_tail = tail_state.offset();
                        continue;
                    }
                    None => {
                        // system shutdown. Or that the loglet has been unexpectedly shutdown.
                        self.terminated = true;
                        return Poll::Ready(Some(Err(OperationError::Shutdown(ShutdownError))));
                    }
                }
            }
            // tail has been updated.
            let last_known_tail = self.last_known_tail;

            // assert that we are behind tail
            assert!(last_known_tail > next_offset);

            // Trim point is the the slot **before** the first readable record (if it exists)
            // trim point might have been updated since last time.
            let trim_point = LogletOffset(self.loglet.trim_point_offset.load(Ordering::Relaxed));
            let head_offset = trim_point.next();
            // Are we reading behind the loglet head? -> TrimGap
            assert!(next_offset > LogletOffset::from(0));

            if next_offset < head_offset {
                let trim_gap = LogEntry::new_trim_gap(next_offset, trim_point);
                // next record should be beyond at the head
                self.read_pointer = head_offset;
                let key = RecordKey::new(self.log_id, trim_point);
                // park the iterator at the trim point, next iteration will seek it forward.
                self.iterator.seek(key.to_bytes());
                return Poll::Ready(Some(Ok(trim_gap)));
            }

            let key = RecordKey::new(self.log_id, next_offset);
            if self.iterator.valid() {
                // can move to next.
                self.iterator.next();
            } else {
                self.iterator.seek(key.to_bytes());
            }
            //  todo: If status is not ok(), we should retry
            if let Err(e) = self.iterator.status() {
                self.terminated = true;
                return Poll::Ready(Some(Err(OperationError::other(LogStoreError::Rocksdb(e)))));
            }

            if !self.iterator.valid() || self.iterator.key().is_none() {
                // trim point might have been updated.
                let potentially_different_trim_point =
                    LogletOffset(self.loglet.trim_point_offset.load(Ordering::Relaxed));
                if potentially_different_trim_point != trim_point {
                    debug!("Trim point has been updated, fast-forwarding the stream");
                    continue;
                }
                // We have a bug! we shouldn't be in this location where the record
                // doesn't exist but we expect it to!
                error!(
                    log_id = self.log_id,
                    next_offset = %next_offset,
                    trim_point = %potentially_different_trim_point,
                    last_known_tail = %self.last_known_tail,
                    "poll_next() has moved to a non-existent record, that should not happen!"
                );
                panic!("poll_next() has moved to a non-existent record, that should not happen!");
            }

            assert!(self.iterator.valid());
            let loaded_key = RecordKey::from_slice(self.iterator.key().expect("log record exists"));
            debug_assert_eq!(loaded_key.offset, key.offset);

            // Defensive, the upper_bound set on the iterator should prevent this.
            if loaded_key.loglet_id != self.log_id {
                warn!(
                    log_id = self.log_id,
                    "read_after moved to the adjacent log {}, that should not happen.\
                    This is harmless but needs to be investigated!",
                    key.loglet_id,
                );
                self.terminated = true;
                return Poll::Ready(None);
            }

            let raw_value = self.iterator.value().expect("log record exists");
            let mut buf = BytesMut::with_capacity(raw_value.len());
            buf.put_slice(raw_value);
            self.read_pointer = loaded_key.offset.next();

            return Poll::Ready(Some(Ok(LogEntry::new_data(key.offset, buf.freeze()))));
        }
    }
}
