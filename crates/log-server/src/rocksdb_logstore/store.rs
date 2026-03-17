// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::sync::Arc;

use rocksdb::{BoundColumnFamily, DB, ReadOptions, WriteBatch, WriteOptions};
use tracing::trace;

use restate_bifrost::loglet::OperationError;
use restate_memory::MemoryLease;
use restate_rocksdb::{IoMode, Priority, RocksDb};
use restate_types::GenerationalNodeId;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber, TailState};
use restate_types::net::log_server::{
    Digest, DigestEntry, Gap, GetDigest, GetRecords, LogServerResponseHeader, MaybeRecord,
    RecordStatus, Records, Seal, Status, Store, Trim,
};
use restate_types::protobuf::common::LogServerStatus;

use super::keys::{KeyPrefixKind, MARKER_KEY, MetadataKey};
use super::record_format::DataRecordDecoder;
use super::writer::RocksDbLogWriterHandle;
use super::{DATA_CF, METADATA_CF, RocksDbLogStoreError};

use crate::logstore::{LogStore, LogStoreState, LogletWriter};
use crate::metadata::{LogStoreMarker, LogletState};
use crate::rocksdb_logstore::keys::DataRecordKey;
use crate::tasks::{OnComplete, StoreStorageTask, TrimStorageTask};

#[derive(Clone)]
pub struct RocksDbLogStore {
    pub(super) rocksdb: Arc<RocksDb>,
    pub(super) store_state: LogStoreState,
    pub(super) writer_handle: RocksDbLogWriterHandle,
}

impl RocksDbLogStore {
    pub fn data_cf(&self) -> Arc<BoundColumnFamily<'_>> {
        self.rocksdb
            .inner()
            .cf_handle(DATA_CF)
            .expect("DATA_CF exists")
    }

    pub fn metadata_cf(&self) -> Arc<BoundColumnFamily<'_>> {
        self.rocksdb
            .inner()
            .cf_handle(METADATA_CF)
            .expect("METADATA_CF exists")
    }

    pub fn db(&self) -> &DB {
        self.rocksdb.inner().as_raw_db()
    }
}

/// Per-loglet write handle for the RocksDB log store.
///
/// Created via [`RocksDbLogStore::create_loglet_writer`]. Automatically
/// registers the loglet with the writer on creation and unregisters when dropped.
pub struct RocksDbLogletWriter {
    inner: LogletWriterState,
}

impl Drop for RocksDbLogletWriter {
    fn drop(&mut self) {
        match self.inner {
            LogletWriterState::Active {
                loglet_id,
                ref loglet_state,
                ref writer_handle,
                ..
            } => {
                // Best-effort: the writer may already be gone during shutdown.
                let _ = writer_handle.unregister_loglet(loglet_id, loglet_state.clone());
            }
            LogletWriterState::Disabled => {}
        }
    }
}

enum LogletWriterState {
    Active {
        loglet_id: LogletId,
        loglet_state: LogletState,
        writer_handle: RocksDbLogWriterHandle,
    },
    Disabled,
}

impl LogletWriter for RocksDbLogletWriter {
    fn enqueue_store(
        &mut self,
        store_message: Store,
        set_sequencer_in_metadata: bool,
        known_global_tail: LogletOffset,
        reservation: MemoryLease,
        mut task: StoreStorageTask,
    ) -> bool {
        debug_assert!(store_message.first_offset != LogletOffset::INVALID);

        match self.inner {
            LogletWriterState::Active {
                ref writer_handle, ..
            } => writer_handle.enqueue_put_records(
                store_message,
                set_sequencer_in_metadata,
                known_global_tail,
                reservation,
                task,
            ),
            LogletWriterState::Disabled => {
                task.on_complete(TailState::invalid(), known_global_tail, Status::Disabled);
                false
            }
        }
    }

    fn enqueue_seal(&mut self, seal_message: Seal, known_global_tail: LogletOffset) -> bool {
        match self.inner {
            LogletWriterState::Active {
                ref writer_handle, ..
            } => writer_handle.enqueue_seal(seal_message, known_global_tail),
            LogletWriterState::Disabled => false,
        }
    }

    fn enqueue_trim(
        &mut self,
        trim_message: Trim,
        known_global_tail: LogletOffset,
        mut task: TrimStorageTask,
    ) -> bool {
        match self.inner {
            LogletWriterState::Active {
                ref writer_handle, ..
            } => writer_handle.enqueue_trim(trim_message, known_global_tail, task),
            LogletWriterState::Disabled => {
                task.on_complete(TailState::invalid(), known_global_tail, Status::Disabled);
                false
            }
        }
    }

    fn set_known_global_tail(&mut self, known_global_tail: LogletOffset) {
        if let LogletWriterState::Active {
            loglet_id,
            ref writer_handle,
            ..
        } = self.inner
        {
            writer_handle.enqueue_set_global_tail(loglet_id, known_global_tail);
        }
    }

    fn close(&mut self) {
        self.inner = LogletWriterState::Disabled;
    }
}

impl LogStore for RocksDbLogStore {
    type Writer = RocksDbLogletWriter;

    async fn load_marker(&self) -> Result<Option<LogStoreMarker>, OperationError> {
        let metadata_cf = self.metadata_cf();
        let value = self
            .db()
            .get_pinned_cf(&metadata_cf, MARKER_KEY)
            .map_err(RocksDbLogStoreError::from)?;

        let marker = value
            .map(LogStoreMarker::from_slice)
            .transpose()
            .map_err(RocksDbLogStoreError::from)?;
        Ok(marker)
    }

    fn state(&self) -> &LogStoreState {
        &self.store_state
    }

    async fn store_marker(&self, marker: LogStoreMarker) -> Result<(), OperationError> {
        let mut batch = WriteBatch::default();
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false);
        write_opts.set_sync(true);
        batch.put_cf(&self.metadata_cf(), MARKER_KEY, marker.to_bytes());

        self.rocksdb
            .write_batch(
                "logstore-metadata-batch",
                Priority::High,
                IoMode::default(),
                write_opts,
                batch,
            )
            .await
            .map(|_| ())
            .map_err(RocksDbLogStoreError::from)?;
        Ok(())
    }

    async fn load_loglet_state(&self, loglet_id: LogletId) -> Result<LogletState, OperationError> {
        block_in_place(|| {
            let metadata_cf = self.metadata_cf();
            let data_cf = self.data_cf();
            let keys = [
                // keep byte-wise sorted
                &MetadataKey::new(KeyPrefixKind::LastKnownGlobalTail, loglet_id).to_binary_array(),
                &MetadataKey::new(KeyPrefixKind::LocalTail, loglet_id).to_binary_array(),
                &MetadataKey::new(KeyPrefixKind::Sequencer, loglet_id).to_binary_array(),
                &MetadataKey::new(KeyPrefixKind::TrimPoint, loglet_id).to_binary_array(),
                &MetadataKey::new(KeyPrefixKind::Seal, loglet_id).to_binary_array(),
            ];
            let mut readopts = ReadOptions::default();
            // no need to cache those metadata records since we won't read them again.
            readopts.fill_cache(false);
            let mut results = self.rocksdb.inner().as_raw_db().batched_multi_get_cf_opt(
                &metadata_cf,
                keys,
                true,
                &readopts,
            );
            debug_assert_eq!(5, results.len());
            // If the key exists, it means a seal is set.
            let is_sealed = results
                .pop()
                .unwrap()
                .map_err(RocksDbLogStoreError::from)?
                .is_some();
            let trim_point = results
                .pop()
                .unwrap()
                .map_err(RocksDbLogStoreError::from)?
                .map(|raw_slice| LogletOffset::decode(raw_slice.as_ref()))
                .unwrap_or(LogletOffset::INVALID);
            let sequencer = results
                .pop()
                .unwrap()
                .map_err(RocksDbLogStoreError::from)?
                .map(|raw_slice| GenerationalNodeId::decode(raw_slice.as_ref()));
            let local_tail = results
                .pop()
                .unwrap()
                .map_err(RocksDbLogStoreError::from)?
                .map(|raw_slice| LogletOffset::decode(raw_slice.as_ref()));

            let known_global_tail = results
                .pop()
                .unwrap()
                .map_err(RocksDbLogStoreError::from)?
                .map(|raw_slice| LogletOffset::decode(raw_slice.as_ref()))
                .unwrap_or(LogletOffset::OLDEST);

            let mut local_tail = match local_tail {
                Some(local_tail) => local_tail,
                None => {
                    // Find the last locally committed offset
                    let oldest_key = DataRecordKey::new(loglet_id, LogletOffset::INVALID);
                    let max_legal_record = DataRecordKey::new(loglet_id, LogletOffset::MAX);
                    let upper_bound = DataRecordKey::exclusive_upper_bound(loglet_id);
                    readopts.fill_cache(false);
                    readopts.set_total_order_seek(false);
                    readopts.set_prefix_same_as_start(true);
                    readopts.set_iterate_lower_bound(oldest_key.to_binary_array());
                    // upper bound is exclusive
                    readopts.set_iterate_upper_bound(upper_bound);
                    let mut iterator = self
                        .rocksdb
                        .inner()
                        .as_raw_db()
                        .raw_iterator_cf_opt(&data_cf, readopts);
                    // see to the max key that exists
                    iterator.seek_for_prev(max_legal_record.to_binary_array());
                    if iterator.valid() {
                        let decoded_key = DataRecordKey::from_slice(iterator.key().unwrap());
                        trace!(
                            %loglet_id,
                            "Found last record of loglet {} is {}",
                            decoded_key.loglet_id(),
                            decoded_key.offset(),
                        );
                        decoded_key.offset().next()
                    } else {
                        trace!(
                            %loglet_id,
                            "No data records for loglet {}", loglet_id
                        );
                        LogletOffset::OLDEST
                    }
                }
            };
            // If the loglet is trimmed (all records were removed) and we know the trim_point, then we
            // use the trim_point.next() as the local_tail.
            if trim_point >= local_tail {
                local_tail = trim_point.next();
            }

            // We can only trim records that are known to be globally committed, let's use that trim
            // point to adjust our known_global_tail
            let known_global_tail = known_global_tail.max(trim_point.next());

            Ok(LogletState::new(
                sequencer,
                local_tail,
                is_sealed,
                trim_point,
                known_global_tail,
            ))
        })
    }

    fn new_loglet_writer(
        &self,
        loglet_id: LogletId,
        loglet_state: &LogletState,
    ) -> RocksDbLogletWriter {
        if self
            .writer_handle
            .register_loglet(loglet_id, loglet_state.clone())
        {
            RocksDbLogletWriter {
                inner: LogletWriterState::Active {
                    loglet_id,
                    loglet_state: loglet_state.clone(),
                    writer_handle: self.writer_handle.clone(),
                },
            }
        } else {
            RocksDbLogletWriter {
                inner: LogletWriterState::Disabled,
            }
        }
    }

    async fn read_records(
        &self,
        msg: GetRecords,
        loglet_state: &LogletState,
    ) -> Result<Records, OperationError> {
        block_in_place(|| {
            let data_cf = self.data_cf();
            let loglet_id = msg.header.loglet_id;
            // The order of operations is important to remain correct.
            // The scenario that we want to avoid is the silent data loss. Although the
            // replicated-loglet reader can ensure contiguous offset range and detect trim-point
            // detection errors or data loss, we try to structure this to reduce the possibilities for
            // errors as much as possible.
            //
            // If we are reading beyond the tail, the first thing we do is to clip to the
            // local_tail.
            let local_tail = loglet_state.local_tail();
            let trim_point = loglet_state.trim_point();

            let read_from = msg.from_offset.max(trim_point.next());
            let read_to = msg.to_offset.min(local_tail.offset().prev());

            let mut size_budget = msg.total_limit_in_bytes.unwrap_or(usize::MAX);

            // why +1? to have enough room for the initial trim_gap if we have one.
            let mut records = Vec::with_capacity(
                usize::try_from(read_to.saturating_sub(*read_from)).expect("no overflow") + 1,
            );

            // Issue a trim gap until the known head
            if read_from > msg.from_offset {
                records.push((
                    msg.from_offset,
                    MaybeRecord::TrimGap(Gap {
                        to: read_from.prev_unchecked(),
                    }),
                ));
            }

            // setup the iterator
            let mut readopts = rocksdb::ReadOptions::default();
            let oldest_key = DataRecordKey::new(loglet_id, read_from);
            let upper_bound = DataRecordKey::exclusive_upper_bound(loglet_id);

            // If we ever switch to using a tailing iterator, be aware that these can fail on
            // encountering a `RangeDelete` tombstone. Doing so might also require us to
            // set_ignore_range_deletions(true).
            readopts.set_tailing(false);

            readopts.set_prefix_same_as_start(true);
            readopts.set_total_order_seek(false);
            // don't fill up the cache as the reader we often don't read the same record multiple times
            // from disk. It still can happen if multiple nodes are back-filling from disk, and in that
            // case we'd still want to favor the most recent data.
            readopts.fill_cache(false);
            readopts.set_async_io(true);
            let oldest_key_bytes = oldest_key.to_binary_array();
            readopts.set_iterate_lower_bound(oldest_key_bytes);
            readopts.set_iterate_upper_bound(upper_bound);
            let mut iterator = self
                .rocksdb
                .inner()
                .as_raw_db()
                .raw_iterator_cf_opt(&data_cf, readopts);

            // read_pointer points to the next offset we should attempt to read (or expect to read)
            let mut read_pointer = read_from;
            iterator.seek(oldest_key_bytes);
            let mut first_record_inserted = false;

            while iterator.valid() && iterator.key().is_some() && read_pointer <= read_to {
                let loaded_key =
                    DataRecordKey::from_slice(iterator.key().expect("log record exists"));
                let offset = loaded_key.offset();
                // We found a record but it's beyond what we want to read
                if offset > read_to {
                    // reset the pointer
                    read_pointer = read_to.next();
                    break;
                }

                if offset > read_pointer {
                    // we skipped records. Either because they got trimmed or we don't have copies for
                    // those offsets
                    let potentially_different_trim_point = loglet_state.trim_point();
                    if potentially_different_trim_point >= offset {
                        // drop the set of accumulated records and start over with a a fresh trim-gap
                        records.clear();
                        records.push((
                            msg.from_offset,
                            MaybeRecord::TrimGap(Gap {
                                to: potentially_different_trim_point,
                            }),
                        ));
                        read_pointer = potentially_different_trim_point.next();
                        iterator
                            .seek(DataRecordKey::new(loglet_id, read_pointer).to_binary_array());
                        continue;
                    }
                    // Another possibility is that we don't have a copy of that offset. In this case,
                    // it's safe to resume.
                }

                // Is this a filtered record?
                let decoder = DataRecordDecoder::new(iterator.value().expect("log record exists"))
                    .map_err(RocksDbLogStoreError::from)?;

                if !decoder.matches_key_query(&msg.filter) {
                    records.push((offset, MaybeRecord::FilteredGap(Gap { to: offset })));
                } else {
                    if first_record_inserted && size_budget < decoder.size() {
                        // we have reached the limit
                        read_pointer = offset;
                        break;
                    }
                    first_record_inserted = true;
                    size_budget = size_budget.saturating_sub(decoder.size());
                    let data_record = decoder.decode().map_err(RocksDbLogStoreError::from)?;
                    records.push((offset, MaybeRecord::Data(data_record)));
                }

                read_pointer = offset.next();
                if read_pointer > read_to {
                    break;
                }
                iterator.next();
            }

            // we reached the end (or an error)
            if let Err(e) = iterator.status() {
                self.store_state
                    .update_health_status(LogServerStatus::Failsafe);
                return Err(RocksDbLogStoreError::Rocksdb(e).into());
            }

            Ok(Records {
                header: LogServerResponseHeader::new(local_tail, loglet_state.known_global_tail()),
                next_offset: read_pointer,
                records,
            })
        })
    }

    async fn get_records_digest(
        &self,
        msg: GetDigest,
        loglet_state: &LogletState,
    ) -> Result<Digest, OperationError> {
        block_in_place(|| {
            let data_cf = self.data_cf();
            let loglet_id = msg.header.loglet_id;
            // If we are reading beyond the tail, the first thing we do is to clip to the
            // local_tail.
            let local_tail = loglet_state.local_tail();
            let trim_point = loglet_state.trim_point();

            // inclusive
            let read_from = msg.from_offset.max(trim_point.next());
            let read_to = msg.to_offset.min(local_tail.offset().prev());

            // allocate for near-worst-case.
            let mut entries = Vec::with_capacity(
                usize::try_from(read_to.saturating_sub(*read_from)).expect("no overflow") + 1,
            );

            // Issue a trim gap until the known head
            if read_from > msg.from_offset {
                entries.push(DigestEntry {
                    from_offset: msg.from_offset,
                    to_offset: read_from.prev_unchecked(),
                    status: RecordStatus::Trimmed,
                });
            }

            // setup the iterator
            let mut readopts = rocksdb::ReadOptions::default();
            let oldest_key = DataRecordKey::new(loglet_id, read_from);
            // the iterator has an exclusive upper bound
            let upper_bound_bytes = if read_to == LogletOffset::MAX {
                DataRecordKey::exclusive_upper_bound(loglet_id)
            } else {
                DataRecordKey::new(loglet_id, read_to.next()).to_binary_array()
            };

            // If we ever switch to using a tailing iterator, be aware that these can fail on
            // encountering a `RangeDelete` tombstone. Doing so might also require us to
            // set_ignore_range_deletions(true).
            readopts.set_tailing(false);

            readopts.set_prefix_same_as_start(true);
            readopts.set_total_order_seek(false);
            // don't fill up the cache as the reader might actually end up reading very few records and
            // we don't want to evict more important records from the cache.
            readopts.fill_cache(false);
            readopts.set_async_io(true);
            let oldest_key_bytes = oldest_key.to_binary_array();
            readopts.set_iterate_lower_bound(oldest_key_bytes);
            readopts.set_iterate_upper_bound(upper_bound_bytes);
            let mut iterator = self
                .rocksdb
                .inner()
                .as_raw_db()
                .raw_iterator_cf_opt(&data_cf, readopts);

            // read_pointer points to the next offset we should attempt to read (or expect to read)
            let mut read_pointer = read_from;
            iterator.seek(oldest_key_bytes);

            let mut current_open_entry: Option<DigestEntry> = None;

            while iterator.valid() && iterator.key().is_some() && read_pointer <= read_to {
                let loaded_key =
                    DataRecordKey::from_slice(&mut iterator.key().expect("log record exists"));
                let offset = loaded_key.offset();
                match offset.cmp(&read_pointer) {
                    Ordering::Greater => {
                        // We found a record but it's beyond what we expect as next (local gap)
                        // close the entry if we had one
                        if let Some(open) = current_open_entry.take() {
                            entries.push(open);
                        }
                        if offset > read_to {
                            // we are done.
                            break;
                        }

                        // open a new entry
                        current_open_entry = Some(DigestEntry {
                            from_offset: offset,
                            to_offset: offset,
                            status: RecordStatus::Exists,
                        });
                        // reset the pointer
                        read_pointer = offset.next();
                    }
                    Ordering::Equal => {
                        if let Some(open) = current_open_entry.as_mut() {
                            open.to_offset = read_pointer;
                        } else {
                            // open a new entry
                            current_open_entry = Some(DigestEntry {
                                from_offset: offset,
                                to_offset: offset,
                                status: RecordStatus::Exists,
                            });
                        }
                        read_pointer = read_pointer.next();
                    }
                    Ordering::Less => {
                        // offset < read_pointer?
                        panic!("Bad logic, iterator is going backward!");
                    }
                }
                iterator.next();
            }

            // we reached the end due to an error
            if read_pointer <= read_to
                && let Err(e) = iterator.status()
            {
                self.store_state
                    .update_health_status(LogServerStatus::Failsafe);
                return Err(RocksDbLogStoreError::Rocksdb(e).into());
            }

            if let Some(last) = current_open_entry.take() {
                // close the last entry
                entries.push(last);
            }

            Ok(Digest {
                header: LogServerResponseHeader::new(local_tail, loglet_state.known_global_tail()),
                entries,
            })
        })
    }
}

#[track_caller]
pub fn block_in_place<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    if cfg!(any(test, feature = "test-util")) {
        f()
    } else {
        tokio::task::block_in_place(f)
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use test_log::test;

    use restate_core::TaskCenter;
    use restate_memory::MemoryLease;
    use restate_rocksdb::RocksDbManager;
    use restate_types::logs::{LogletId, LogletOffset, Record, SequenceNumber};
    use restate_types::net::log_server::{
        DigestEntry, GetDigest, LogServerRequestHeader, RecordStatus, Status, Store, StoreFlags,
    };
    use restate_types::{GenerationalNodeId, PlainNodeId};

    use super::RocksDbLogStore;
    use crate::logstore::{LogStore, LogletWriter};
    use crate::metadata::{LogStoreMarker, LogletState};
    use crate::rocksdb_logstore::RocksDbLogStoreBuilder;
    use crate::tasks::StoreStorageTask;

    async fn setup() -> Result<RocksDbLogStore> {
        RocksDbManager::init();
        // create logstore.
        let builder = RocksDbLogStoreBuilder::create().await?;
        Ok(builder.start(Default::default()).await?)
    }

    /// Helper: enqueue a store and wait for it to be durably committed.
    ///
    /// Creates a temporary `LogletState` and writer handle, enqueues the
    /// store, and waits for the tail watch to advance. The writer handle is
    /// dropped at the end, which auto-unregisters.
    async fn enqueue_store_and_wait(
        log_store: &RocksDbLogStore,
        store_msg: Store,
        set_sequencer_in_metadata: bool,
    ) -> Result<()> {
        let loglet_id = store_msg.header.loglet_id;
        let committed_up_to = store_msg.last_offset().expect("non-empty store").next();
        let loglet_state = LogletState::new(
            Some(store_msg.sequencer),
            LogletOffset::OLDEST,
            false,
            LogletOffset::INVALID,
            LogletOffset::OLDEST,
        );
        let mut writer = log_store.new_loglet_writer(store_msg.header.loglet_id, &loglet_state);
        let (reciprocal, _rx) = restate_core::network::Reciprocal::mock();
        let task = StoreStorageTask::new(loglet_id, reciprocal);
        assert!(
            writer.enqueue_store(
                store_msg,
                set_sequencer_in_metadata,
                LogletOffset::INVALID,
                MemoryLease::unlinked(),
                task,
            ),
            "writer channel open"
        );
        loglet_state
            .get_local_tail_watch()
            .wait_for_offset(committed_up_to)
            .await
            .map_err(|_| {
                restate_bifrost::loglet::OperationError::Shutdown(restate_core::ShutdownError)
            })?;
        // Writer handle dropped here — auto-unregisters.
        drop(writer);
        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_log_store_marker() -> Result<()> {
        let log_store = setup().await?;

        let marker = log_store.load_marker().await?;
        assert!(marker.is_none());
        let marker = LogStoreMarker::new(PlainNodeId::new(111));
        log_store.store_marker(marker.clone()).await?;

        let marker_again = log_store.load_marker().await?;
        assert_that!(marker_again, some(eq(marker)));

        // unconditionally store again.
        let marker = LogStoreMarker::new(PlainNodeId::new(999));
        log_store.store_marker(marker.clone()).await?;
        let marker_again = log_store.load_marker().await?;
        assert_that!(marker_again, some(eq(marker)));

        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_load_loglet_state() -> Result<()> {
        let log_store = setup().await?;
        // fresh/unknown loglet
        let loglet_id_1 = LogletId::new_unchecked(88);
        let loglet_id_2 = LogletId::new_unchecked(89);
        let sequencer_1 = GenerationalNodeId::new(5, 213);
        let sequencer_2 = GenerationalNodeId::new(2, 212);

        let state = log_store.load_loglet_state(loglet_id_1).await?;
        assert!(!state.is_sealed());
        assert_that!(state.local_tail().offset(), eq(LogletOffset::OLDEST));
        assert_that!(state.trim_point(), eq(LogletOffset::INVALID));

        // store records
        let payloads = vec![Record::from("a sample record".to_owned())];

        let store_msg_1 = Store {
            header: LogServerRequestHeader::new(loglet_id_1, LogletOffset::INVALID),
            timeout_at: None,
            sequencer: sequencer_1,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::OLDEST,
            flags: StoreFlags::empty(),
            payloads: payloads.into(),
        };
        // add record at offset=1, no sequencer set.
        enqueue_store_and_wait(&log_store, store_msg_1.clone(), false).await?;

        let state = log_store.load_loglet_state(loglet_id_1).await?;
        assert!(!state.is_sealed());
        assert_that!(state.local_tail().offset(), eq(LogletOffset::new(2)));
        assert_that!(state.trim_point(), eq(LogletOffset::INVALID));
        assert!(state.sequencer().is_none());

        let store_msg_2 = Store {
            first_offset: LogletOffset::new(u32::MAX - 1),
            ..store_msg_1.clone()
        };

        // add record at end of range (4B) (and set sequencer)
        enqueue_store_and_wait(&log_store, store_msg_2, true).await?;

        let state = log_store.load_loglet_state(loglet_id_1).await?;
        assert!(!state.is_sealed());
        assert_that!(state.local_tail().offset(), eq(LogletOffset::new(u32::MAX)));
        assert_that!(state.trim_point(), eq(LogletOffset::INVALID));
        assert_that!(
            state.sequencer(),
            some(eq(&GenerationalNodeId::new(5, 213)))
        );
        // make sure we can still get it if adjacent log has records
        let store_msg_3 = Store {
            header: LogServerRequestHeader {
                loglet_id: loglet_id_2,
                ..store_msg_1.header
            },
            first_offset: LogletOffset::OLDEST,
            sequencer: sequencer_2,
            ..store_msg_1
        };
        enqueue_store_and_wait(&log_store, store_msg_3, true).await?;

        let state = log_store.load_loglet_state(loglet_id_1).await?;
        assert!(!state.is_sealed());
        assert_that!(state.local_tail().offset(), eq(LogletOffset::new(u32::MAX)));
        assert_that!(state.trim_point(), eq(LogletOffset::INVALID));
        assert_that!(
            state.sequencer(),
            some(eq(&GenerationalNodeId::new(5, 213)))
        );

        // the adjacent log is at 2
        let state = log_store
            .load_loglet_state((*loglet_id_1 + 1).into())
            .await?;
        assert!(!state.is_sealed());
        assert_that!(state.local_tail().offset(), eq(LogletOffset::new(2)));
        assert_that!(state.trim_point(), eq(LogletOffset::INVALID));
        assert_that!(
            state.sequencer(),
            some(eq(&GenerationalNodeId::new(2, 212)))
        );

        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_digest() -> Result<()> {
        let log_store = setup().await?;
        let loglet_id_1 = LogletId::new_unchecked(88);
        let loglet_id_2 = LogletId::new_unchecked(89);
        let sequencer_1 = GenerationalNodeId::new(5, 213);
        let sequencer_2 = GenerationalNodeId::new(2, 212);

        let mut state = log_store.load_loglet_state(loglet_id_1).await?;
        assert!(!state.is_sealed());
        assert_that!(state.local_tail().offset(), eq(LogletOffset::OLDEST));
        assert_that!(state.trim_point(), eq(LogletOffset::INVALID));

        // get digest for empty log.
        let msg = GetDigest {
            // global offset is ignored by the log-store, it's safe to set it to any value.
            header: LogServerRequestHeader::new(loglet_id_1, LogletOffset::new(10)),
            from_offset: LogletOffset::new(5),
            to_offset: LogletOffset::new(200),
        };
        let digest = log_store.get_records_digest(msg, &state).await?;
        assert_that!(digest.header.status, eq(Status::Ok));
        assert_that!(digest.header.local_tail, eq(LogletOffset::OLDEST));
        assert!(digest.entries.is_empty());

        // store some records

        // loglet_1 will have those offsets
        // [5 7 10 11 12 13 18].
        let payloads = vec![Record::from("a sample record".to_owned())];

        for offset in [5, 7, 10, 11, 12, 13, 18] {
            let offset = LogletOffset::new(offset);
            let store_msg = Store {
                // fake movement of global commit to allow this store
                header: LogServerRequestHeader::new(loglet_id_1, offset),
                timeout_at: None,
                sequencer: sequencer_1,
                known_archived: LogletOffset::INVALID,
                first_offset: offset,
                flags: StoreFlags::empty(),
                payloads: payloads.clone().into(),
            };
            enqueue_store_and_wait(&log_store, store_msg, true).await?;
        }

        // the store doesn't update local-state, so we update it manually here to 20
        state
            .get_local_tail_watch()
            .notify_offset_update(LogletOffset::new(20));

        assert!(!state.is_sealed());
        assert_that!(state.local_tail().offset(), eq(LogletOffset::new(20)));
        assert_that!(state.trim_point(), eq(LogletOffset::INVALID));
        // add records to the adjacent log just to validate we are not spilling over.
        let store_msg = Store {
            header: LogServerRequestHeader::new(loglet_id_2, LogletOffset::OLDEST),
            timeout_at: None,
            first_offset: LogletOffset::OLDEST,
            sequencer: sequencer_2,
            known_archived: LogletOffset::INVALID,
            flags: StoreFlags::empty(),
            payloads: payloads.into(),
        };
        enqueue_store_and_wait(&log_store, store_msg, true).await?;
        // the adjacent log is at 2
        let state2 = log_store.load_loglet_state(loglet_id_2).await?;
        assert!(!state2.is_sealed());
        assert_that!(state2.local_tail().offset(), eq(LogletOffset::new(2)));
        assert_that!(state2.trim_point(), eq(LogletOffset::INVALID));

        // Get the digest of loglet_1
        // Scenario 1. Reasonable request bounds
        let msg = GetDigest {
            // global offset is ignored by the log-store, it's safe to set it to any value.
            header: LogServerRequestHeader::new(loglet_id_1, LogletOffset::new(200)),
            from_offset: LogletOffset::new(1),
            to_offset: LogletOffset::new(200),
        };
        let digest = log_store.get_records_digest(msg, &state).await?;
        assert_that!(digest.header.status, eq(Status::Ok));
        assert_that!(digest.header.local_tail, eq(LogletOffset::new(20)));
        // we expect [5..5] X,[7..7] X, [10..13] X, [18..18] X
        assert_that!(digest.entries.len(), eq(4));
        // left intentionally to debug tests
        println!("Scenario 1");
        for entry in &digest.entries {
            println!("{entry}");
        }

        assert_that!(
            digest.entries,
            elements_are![
                eq(DigestEntry {
                    from_offset: 5.into(),
                    to_offset: 5.into(),
                    status: RecordStatus::Exists,
                }),
                eq(DigestEntry {
                    from_offset: 7.into(),
                    to_offset: 7.into(),
                    status: RecordStatus::Exists,
                }),
                eq(DigestEntry {
                    from_offset: 10.into(),
                    to_offset: 13.into(),
                    status: RecordStatus::Exists,
                }),
                eq(DigestEntry {
                    from_offset: 18.into(),
                    to_offset: 18.into(),
                    status: RecordStatus::Exists,
                }),
            ]
        );

        // Scenario 2.
        // Get the digest, with a trim gap that eats up offset 11.
        state.update_trim_point(LogletOffset::new(11));
        let msg = GetDigest {
            // global offset is ignored by the log-store, it's safe to set it to any value.
            header: LogServerRequestHeader::new(loglet_id_1, LogletOffset::new(200)),
            // start from 5 this time
            from_offset: LogletOffset::new(5),
            to_offset: LogletOffset::new(200),
        };
        let digest = log_store.get_records_digest(msg, &state).await?;
        assert_that!(digest.header.status, eq(Status::Ok));
        assert_that!(digest.header.local_tail, eq(LogletOffset::new(20)));
        // we expect [5..11] T, [12..13] X, [18..18] X
        assert_that!(digest.entries.len(), eq(3));
        // left intentionally to debug tests
        println!("Scenario 2");
        for entry in &digest.entries {
            println!("{entry}");
        }

        assert_that!(
            digest.entries,
            elements_are![
                eq(DigestEntry {
                    from_offset: 5.into(),
                    to_offset: 11.into(),
                    status: RecordStatus::Trimmed,
                }),
                eq(DigestEntry {
                    from_offset: 12.into(),
                    to_offset: 13.into(),
                    status: RecordStatus::Exists,
                }),
                eq(DigestEntry {
                    from_offset: 18.into(),
                    to_offset: 18.into(),
                    status: RecordStatus::Exists,
                }),
            ]
        );

        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
