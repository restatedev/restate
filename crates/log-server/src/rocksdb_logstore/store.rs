// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use rocksdb::{BoundColumnFamily, ReadOptions, WriteBatch, WriteOptions, DB};
use tracing::trace;

use restate_bifrost::loglet::OperationError;
use restate_rocksdb::{IoMode, Priority, RocksDb};
use restate_types::config::LogServerOptions;
use restate_types::live::BoxedLiveLoad;
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::net::log_server::{
    Digest, DigestEntry, Gap, GetDigest, GetRecords, LogServerResponseHeader, MaybeRecord,
    RecordStatus, Records, Seal, Store, Trim,
};
use restate_types::replicated_loglet::ReplicatedLogletId;
use restate_types::GenerationalNodeId;

use super::keys::{KeyPrefixKind, MetadataKey, MARKER_KEY};
use super::record_format::DataRecordDecoder;
use super::writer::RocksDbLogWriterHandle;
use super::{RocksDbLogStoreError, DATA_CF, METADATA_CF};
use crate::logstore::{AsyncToken, LogStore};
use crate::metadata::{LogStoreMarker, LogletState};
use crate::rocksdb_logstore::keys::DataRecordKey;

#[derive(Clone)]
pub struct RocksDbLogStore {
    pub(super) _updateable_options: BoxedLiveLoad<LogServerOptions>,
    pub(super) rocksdb: Arc<RocksDb>,
    pub(super) writer_handle: RocksDbLogWriterHandle,
}

impl RocksDbLogStore {
    pub fn data_cf(&self) -> Arc<BoundColumnFamily> {
        self.rocksdb
            .inner()
            .cf_handle(DATA_CF)
            .expect("DATA_CF exists")
    }

    pub fn metadata_cf(&self) -> Arc<BoundColumnFamily> {
        self.rocksdb
            .inner()
            .cf_handle(METADATA_CF)
            .expect("METADATA_CF exists")
    }

    pub fn db(&self) -> &DB {
        self.rocksdb.inner().as_raw_db()
    }
}

impl LogStore for RocksDbLogStore {
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
            .map_err(RocksDbLogStoreError::from)?;
        Ok(())
    }

    async fn load_loglet_state(
        &self,
        loglet_id: ReplicatedLogletId,
    ) -> Result<LogletState, OperationError> {
        let metadata_cf = self.metadata_cf();
        let data_cf = self.data_cf();
        let keys = [
            // keep byte-wise sorted
            &MetadataKey::new(KeyPrefixKind::Sequencer, loglet_id).to_bytes(),
            &MetadataKey::new(KeyPrefixKind::TrimPoint, loglet_id).to_bytes(),
            &MetadataKey::new(KeyPrefixKind::Seal, loglet_id).to_bytes(),
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
        debug_assert_eq!(3, results.len());
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

        // Find the last locally committed offset
        let oldest_key = DataRecordKey::new(loglet_id, LogletOffset::INVALID);
        let max_legal_record = DataRecordKey::new(loglet_id, LogletOffset::MAX);
        let upper_bound = DataRecordKey::exclusive_upper_bound(loglet_id);
        readopts.fill_cache(true);
        readopts.set_total_order_seek(false);
        readopts.set_prefix_same_as_start(true);
        readopts.set_iterate_lower_bound(oldest_key.to_bytes());
        // upper bound is exclusive
        readopts.set_iterate_upper_bound(upper_bound);
        let mut iterator = self
            .rocksdb
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(&data_cf, readopts);
        // see to the max key that exists
        iterator.seek_for_prev(max_legal_record.to_bytes());
        let mut local_tail = if iterator.valid() {
            let decoded_key = DataRecordKey::from_slice(iterator.key().unwrap());
            trace!(
                "Found last record of loglet {} is {}",
                decoded_key.loglet_id(),
                decoded_key.offset(),
            );
            decoded_key.offset().next()
        } else {
            trace!("No data records for loglet {}", loglet_id);
            LogletOffset::OLDEST
        };
        // If the loglet is trimmed (all records were removed) and we know the trim_point, then we
        // use the trim_point.next() as the local_tail.
        //
        // Another way to describe this is `if trim_point => local_tail` but at this stage, I
        // prefer to be conservative and explicit to catch unintended corner cases.
        if local_tail == LogletOffset::OLDEST && trim_point > LogletOffset::INVALID {
            local_tail = trim_point.next();
        }

        // todo(asoli): Persist last_known_global_tail for more efficient tail repairs
        // perhaps we can set the known_tail to the trim_point.next() here but let's do that only
        // when the need rises.
        Ok(LogletState::new(
            sequencer,
            local_tail,
            is_sealed,
            trim_point,
            LogletOffset::OLDEST,
        ))
    }

    async fn enqueue_store(
        &mut self,
        store_message: Store,
        set_sequencer_in_metadata: bool,
    ) -> Result<AsyncToken, OperationError> {
        // do not accept INVALID offsets
        if store_message.first_offset == LogletOffset::INVALID {
            return Err(RocksDbLogStoreError::InvalidOffset(store_message.first_offset).into());
        }
        self.writer_handle
            .enqueue_put_records(store_message, set_sequencer_in_metadata)
            .await
    }

    async fn enqueue_seal(&mut self, seal_message: Seal) -> Result<AsyncToken, OperationError> {
        self.writer_handle.enqueue_seal(seal_message).await
    }

    async fn enqueue_trim(&mut self, trim_message: Trim) -> Result<AsyncToken, OperationError> {
        self.writer_handle.enqueue_trim(trim_message).await
    }

    async fn read_records(
        &mut self,
        msg: GetRecords,
        loglet_state: &LogletState,
    ) -> Result<Records, OperationError> {
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
                    to: read_from.prev(),
                }),
            ));
        }

        // setup the iterator
        let mut readopts = rocksdb::ReadOptions::default();
        let oldest_key = DataRecordKey::new(loglet_id, read_from);
        let upper_bound = DataRecordKey::exclusive_upper_bound(loglet_id);
        readopts.set_tailing(false);
        // In some cases, the underlying ForwardIterator will fail if it hits a `RangeDelete` tombstone.
        // For our purposes, we can ignore these tombstones, meaning that we will return those records
        // instead of a gap.
        // In summary, if loglet reader started before a trim point and data is readable, we should
        // continue reading them. It's the responsibility of the upper layer to decide on a sane
        // value of _from_offset_.
        readopts.set_ignore_range_deletions(true);
        readopts.set_prefix_same_as_start(true);
        readopts.set_total_order_seek(false);
        readopts.set_async_io(true);
        let oldest_key_bytes = oldest_key.to_bytes();
        readopts.set_iterate_lower_bound(oldest_key_bytes.clone());
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
            let loaded_key = DataRecordKey::from_slice(iterator.key().expect("log record exists"));
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
                    iterator.seek(DataRecordKey::new(loglet_id, read_pointer).to_bytes());
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
            // Allow other tasks on this thread to run, but only if we have exhausted the coop
            // budget.
            tokio::task::consume_budget().await;
        }

        // we reached the end (or an error)
        if let Err(e) = iterator.status() {
            // whoa, we have I/O errors, we should switch into failsafe mode (todo)
            return Err(RocksDbLogStoreError::Rocksdb(e).into());
        }

        Ok(Records {
            header: LogServerResponseHeader::new(local_tail, loglet_state.known_global_tail()),
            next_offset: read_pointer,
            records,
        })
    }

    async fn get_records_digest(
        &mut self,
        msg: GetDigest,
        loglet_state: &LogletState,
    ) -> Result<Digest, OperationError> {
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
                to_offset: read_from.prev(),
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
            DataRecordKey::new(loglet_id, read_to.next()).to_bytes()
        };
        readopts.set_tailing(false);
        // In some cases, the underlying ForwardIterator will fail if it hits a `RangeDelete` tombstone.
        // For our purposes, we can ignore these tombstones, meaning that we will return those records
        // instead of a gap.
        // In summary, if loglet reader started before a trim point and data is readable, we should
        // continue reading them. It's the responsibility of the upper layer to decide on a sane
        // value of _from_offset_.
        readopts.set_ignore_range_deletions(true);
        readopts.set_prefix_same_as_start(true);
        readopts.set_total_order_seek(false);
        readopts.set_async_io(true);
        let oldest_key_bytes = oldest_key.to_bytes();
        readopts.set_iterate_lower_bound(oldest_key_bytes.clone());
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
            let loaded_key = DataRecordKey::from_slice(iterator.key().expect("log record exists"));
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
            // Allow other tasks on this thread to run, but only if we have exhausted the coop
            // budget.
            tokio::task::consume_budget().await;
        }

        // we reached the end due to an error
        if read_pointer <= read_to {
            if let Err(e) = iterator.status() {
                // whoa, we have I/O errors, we should switch into failsafe mode (todo)
                return Err(RocksDbLogStoreError::Rocksdb(e).into());
            }
        }

        if let Some(last) = current_open_entry.take() {
            // close the last entry
            entries.push(last);
        }

        Ok(Digest {
            header: LogServerResponseHeader::new(local_tail, loglet_state.known_global_tail()),
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use test_log::test;

    use restate_core::{TaskCenter, TaskCenterBuilder};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::live::Live;
    use restate_types::logs::{LogletOffset, Record, RecordCache, SequenceNumber};
    use restate_types::net::log_server::{
        DigestEntry, GetDigest, LogServerRequestHeader, RecordStatus, Status, Store, StoreFlags,
    };
    use restate_types::replicated_loglet::ReplicatedLogletId;
    use restate_types::{GenerationalNodeId, PlainNodeId};

    use super::RocksDbLogStore;
    use crate::logstore::LogStore;
    use crate::metadata::LogStoreMarker;
    use crate::rocksdb_logstore::RocksDbLogStoreBuilder;
    use crate::setup_panic_handler;

    async fn setup() -> Result<(TaskCenter, RocksDbLogStore)> {
        setup_panic_handler();
        let tc = TaskCenterBuilder::default_for_tests().build()?;
        let config = Live::from_value(Configuration::default());
        let common_rocks_opts = config.clone().map(|c| &c.common);
        let log_store = tc
            .run_in_scope("test-setup", None, async {
                RocksDbManager::init(common_rocks_opts);
                // create logstore.
                let builder = RocksDbLogStoreBuilder::create(
                    config.clone().map(|c| &c.log_server).boxed(),
                    config.map(|c| &c.log_server.rocksdb).boxed(),
                    RecordCache::new(1_000_000),
                )
                .await?;
                let log_store = builder.start(&tc).await?;
                Result::Ok(log_store)
            })
            .await?;
        Ok((tc, log_store))
    }

    #[test(tokio::test(start_paused = true))]
    async fn test_log_store_marker() -> Result<()> {
        let (tc, log_store) = setup().await?;

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

        tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test(tokio::test(start_paused = true))]
    async fn test_load_loglet_state() -> Result<()> {
        let (tc, mut log_store) = setup().await?;
        // fresh/unknown loglet
        let loglet_id_1 = ReplicatedLogletId::new(88);
        let loglet_id_2 = ReplicatedLogletId::new(89);
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
        log_store
            .enqueue_store(store_msg_1.clone(), false)
            .await?
            .await?;

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
        log_store.enqueue_store(store_msg_2, true).await?.await?;

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
        log_store.enqueue_store(store_msg_3, true).await?.await?;

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

        tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test(tokio::test(start_paused = true))]
    async fn test_digest() -> Result<()> {
        let (tc, mut log_store) = setup().await?;
        let loglet_id_1 = ReplicatedLogletId::new(88);
        let loglet_id_2 = ReplicatedLogletId::new(89);
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
            log_store
                .enqueue_store(store_msg.clone(), true)
                .await?
                .await?;
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
        log_store.enqueue_store(store_msg, true).await?.await?;
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
            println!("{}", entry);
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
            println!("{}", entry);
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

        tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
