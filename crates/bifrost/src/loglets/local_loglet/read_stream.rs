// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Poll;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use rocksdb::{DBRawIteratorWithThreadMode, DB};
use tokio_stream::wrappers::WatchStream;
use tracing::{debug, error, warn};

use restate_core::ShutdownError;
use restate_rocksdb::RocksDbPerfGuard;
use restate_types::logs::SequenceNumber;

use crate::loglet::{LogletOffset, LogletReadStream};
use crate::loglets::local_loglet::LogStoreError;
use crate::{Error, LogRecord, Result};

use super::keys::RecordKey;
use super::LocalLoglet;

#[pin_project]
pub(crate) struct LocalLogletReadStream {
    log_id: u64,
    loglet: Arc<LocalLoglet>,
    // the next record this stream will attempt to read
    read_pointer: LogletOffset,
    release_pointer: LogletOffset,
    #[pin]
    iterator: DBRawIteratorWithThreadMode<'static, DB>,
    #[pin]
    release_watch: WatchStream<LogletOffset>,
    #[pin]
    terminated: bool,
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
        from_offset: LogletOffset,
    ) -> Result<Self> {
        // Reading from INVALID reset to OLDEST.
        let from_offset = from_offset.max(LogletOffset::OLDEST);
        // We seek to next key on every iteration, we need to setup the iterator to be
        // at the previous key within the same prefix if from_offset > 0 (saturating to
        // LogletOffset::INVALID)
        let key = RecordKey::new(loglet.log_id, from_offset.prev());
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_tailing(true);
        // In some cases, the underlying ForwardIterator will fail it hits a RangeDelete tombstone.
        // For our purposes, we can ignore these tombstones. TL;DR if loglet reader started before a
        // trim point and data is readable, we should continue reading them. It's the
        // responsibility of the upper layer to decide on a sane value of _from_offset_.
        read_opts.set_ignore_range_deletions(true);
        read_opts.set_prefix_same_as_start(true);
        read_opts.set_total_order_seek(false);
        read_opts.set_iterate_lower_bound(key.to_bytes());
        read_opts.set_iterate_upper_bound(RecordKey::upper_bound(loglet.log_id).to_bytes());

        let log_store = &loglet.log_store;
        let mut release_watch = loglet.release_watch.to_stream();
        let release_pointer = release_watch
            .next()
            .await
            .expect("loglet watch returns release pointer");
        //
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
            log_id: loglet.log_id,
            loglet,
            read_pointer: from_offset,
            iterator: iter,
            terminated: false,
            release_watch,
            release_pointer,
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
    type Item = Result<LogRecord<LogletOffset, Bytes>>;

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
            let mut this = self.as_mut().project();

            // Are we reading after commit offset?
            // We are at tail. We need to wait until new records have been released.
            if next_offset > *this.release_pointer {
                let updated_release_pointer = match this.release_watch.poll_next(cx) {
                    Poll::Ready(t) => t,
                    Poll::Pending => {
                        perf_guard.forget();
                        return Poll::Pending;
                    }
                };

                match updated_release_pointer {
                    Some(updated_release_pointer) => {
                        *this.release_pointer = updated_release_pointer;
                        continue;
                    }
                    None => {
                        // system shutdown. Or that the loglet has been unexpectedly shutdown.
                        this.terminated.set(true);
                        return Poll::Ready(Some(Err(Error::Shutdown(ShutdownError))));
                    }
                }
            }
            // release_pointer has been updated.
            let release_pointer = *this.release_pointer;

            // assert that we are newer
            assert!(release_pointer >= next_offset);

            // Trim point is the the slot **before** the first readable record (if it exists)
            // trim point might have been updated since last time.
            let trim_point = LogletOffset(this.loglet.trim_point_offset.load(Ordering::Relaxed));
            let head_offset = trim_point.next();
            // Are we reading behind the loglet head? -> TrimGap
            assert!(next_offset > LogletOffset::from(0));

            if next_offset < head_offset {
                let trim_gap = LogRecord::new_trim_gap(next_offset, trim_point);
                // next record should be beyond at the head
                self.read_pointer = head_offset;
                let key = RecordKey::new(self.log_id, trim_point);
                // park the iterator at the trim point, next iteration will seek it forward.
                self.iterator.seek(key.to_bytes());
                return Poll::Ready(Some(Ok(trim_gap)));
            }

            let key = RecordKey::new(*this.log_id, next_offset);
            if this.iterator.valid() {
                // can move to next.
                this.iterator.next();
            } else {
                this.iterator.seek(key.to_bytes());
            }
            //  todo: If status is not ok(), we should retry
            if let Err(e) = this.iterator.status() {
                this.terminated.set(true);
                return Poll::Ready(Some(Err(Error::LogStoreError(LogStoreError::Rocksdb(e)))));
            }

            if !this.iterator.valid() || this.iterator.key().is_none() {
                // trim point might have been updated.
                let potentially_different_trim_point =
                    LogletOffset(this.loglet.trim_point_offset.load(Ordering::Relaxed));
                if potentially_different_trim_point != trim_point {
                    debug!("Trim point has been updated, fast-forwarding the stream");
                    continue;
                }
                // We have a bug! we shouldn't be in this location where the record
                // doesn't exist but we expect it to!
                error!(
                    log_id = *this.log_id,
                    next_offset = %next_offset,
                    trim_point = %potentially_different_trim_point,
                    release_pointer = %this.release_pointer,
                    "poll_next() has moved to a non-existent record, that should not happen!"
                );
                panic!("poll_next() has moved to a non-existent record, that should not happen!");
            }

            assert!(this.iterator.valid());
            let loaded_key = RecordKey::from_slice(this.iterator.key().expect("log record exists"));
            debug_assert_eq!(loaded_key.offset, key.offset);

            // Defensive, the upper_bound set on the iterator should prevent this.
            if loaded_key.log_id != *this.log_id {
                warn!(
                    log_id = *this.log_id,
                    "read_after moved to the adjacent log {}, that should not happen.\
                    This is harmless but needs to be investigated!",
                    key.log_id,
                );
                this.terminated.set(true);
                return Poll::Ready(None);
            }

            let raw_value = this.iterator.value().expect("log record exists");
            let mut buf = BytesMut::with_capacity(raw_value.len());
            buf.put_slice(raw_value);
            *this.read_pointer = loaded_key.offset.next();

            return Poll::Ready(Some(Ok(LogRecord::new_data(key.offset, buf.freeze()))));
        }
    }
}
