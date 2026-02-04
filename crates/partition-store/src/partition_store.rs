// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use bytes::BytesMut;
use enum_map::Enum;
use parking_lot::Mutex;
use rocksdb::{
    BoundColumnFamily, DBPinnableSlice, DBRawIteratorWithThreadMode, PrefixRange, ReadOptions,
    SnapshotWithThreadMode,
};
use static_assertions::const_assert_eq;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace};

use restate_core::ShutdownError;
use restate_rocksdb::{IoMode, IterAction, Priority, RocksDb, RocksError};
use restate_storage_api::fsm_table::ReadFsmTable;
use restate_storage_api::protobuf_types::{PartitionStoreProtobufValue, ProtobufStorageWrapper};
use restate_storage_api::{IsolationLevel, Storage, StorageError, Transaction};
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId, WithPartitionKey};
use restate_types::logs::Lsn;
use restate_types::partitions::Partition;
use restate_types::storage::StorageCodec;
use restate_types::storage::StorageDecode;
use restate_types::storage::StorageEncode;

use crate::fsm_table::{get_locally_durable_lsn, get_storage_version, put_storage_version};
use crate::keys::KeyKind;
use crate::keys::TableKey;
use crate::keys::TableKeyPrefix;
use crate::migrations::{LATEST_VERSION, SchemaVersion};
use crate::partition_db::PartitionDb;
use crate::scan::PhysicalScan;
use crate::scan::TableScan;
use crate::snapshots::LocalPartitionSnapshot;

pub type DB = rocksdb::DB;

pub type DBIterator<'b> = DBRawIteratorWithThreadMode<'b, DB>;
pub type DBIteratorTransaction<'b> = DBRawIteratorWithThreadMode<'b, rocksdb::Transaction<'b, DB>>;

// Key prefix is 10 bytes (KeyKind(2) + PartitionKey/Id(8))
pub(crate) const DB_PREFIX_LENGTH: usize =
    KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>();

// If this changes, we need to know.
const_assert_eq!(DB_PREFIX_LENGTH, 10);

/// An internal representation of PartitionId that pads the underlying u16 into u64 to align with
/// partition-key length. This should only be used as a replacement to PartitionId when
/// compatibility with old u64-sized PartitionId is needed. Additionally. This must be aligned with
/// the size of PartitionKey to match the prefix length requirements in rocksdb.
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    derive_more::Add,
    derive_more::Display,
    derive_more::FromStr,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PaddedPartitionId(u64);

impl PaddedPartitionId {
    pub fn new(id: PartitionId) -> Self {
        Self(u16::from(id) as u64)
    }
}

impl From<PartitionId> for PaddedPartitionId {
    fn from(value: PartitionId) -> Self {
        Self(u16::from(value) as u64)
    }
}

impl From<PaddedPartitionId> for PartitionId {
    fn from(value: PaddedPartitionId) -> Self {
        Self::from(u16::try_from(value.0).expect("partition_id must fit in u16"))
    }
}

// Ensures that both types have the same length, this makes it possible to
// share prefix extractor in rocksdb.
const_assert_eq!(
    std::mem::size_of::<PartitionKey>(),
    std::mem::size_of::<PaddedPartitionId>(),
);

pub(crate) type Result<T, E = StorageError> = std::result::Result<T, E>;

pub enum TableScanIterationDecision<R> {
    Emit(Result<R>),
    Continue,
    Break,
    BreakWith(Result<R>),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Enum, strum::VariantArray)]
pub enum TableKind {
    // By Partition ID
    PartitionStateMachine,
    Deduplication,
    Outbox,
    Timers,
    // By Partition Key
    State,
    InvocationStatus,
    ServiceStatus,
    Idempotency,
    Inbox,
    Journal,
    JournalEvent,
    Promise,
    VQueue,
}

impl TableKind {
    pub const fn key_kinds(self) -> &'static [KeyKind] {
        match self {
            Self::State => &[KeyKind::State],
            Self::InvocationStatus => &[KeyKind::InvocationStatus],
            Self::ServiceStatus => &[KeyKind::ServiceStatus],
            Self::Idempotency => &[KeyKind::Idempotency],
            Self::Inbox => &[KeyKind::Inbox],
            Self::Outbox => &[KeyKind::Outbox],
            Self::Deduplication => &[KeyKind::Deduplication],
            Self::PartitionStateMachine => &[KeyKind::Fsm],
            Self::Timers => &[KeyKind::Timers],
            Self::Journal => &[
                KeyKind::Journal,
                KeyKind::InvocationStatus,
                KeyKind::JournalV2,
                KeyKind::JournalV2CompletionIdToCommandIndex,
                KeyKind::JournalV2NotificationIdToNotificationIndex,
            ],
            Self::JournalEvent => &[KeyKind::JournalEvent],
            Self::Promise => &[KeyKind::Promise],
            Self::VQueue => &[
                KeyKind::VQueueMeta,
                KeyKind::VQueueInbox,
                KeyKind::VQueueActive,
                KeyKind::VQueueEntryState,
            ],
        }
    }

    pub fn has_key_kind(self, prefix: &[u8]) -> bool {
        self.extract_key_kind(prefix).is_some()
    }

    pub fn extract_key_kind(self, prefix: &[u8]) -> Option<KeyKind> {
        if prefix.len() < KeyKind::SERIALIZED_LENGTH {
            return None;
        }
        let slice = prefix[..KeyKind::SERIALIZED_LENGTH].try_into().unwrap();
        let Some(kind) = KeyKind::from_bytes(slice) else {
            // warning
            return None;
        };
        self.key_kinds().iter().find(|k| **k == kind).copied()
    }
}

pub struct PartitionStore {
    db: PartitionDb,
    /// Key buffer for serializing keys. Uses interior mutability to allow
    /// read operations to take `&self` instead of `&mut self`.
    ///
    /// TODO: Measure the performance impact of Mutex<BytesMut> compared to
    /// UnsafeCell<BytesMut>. If benchmarks show significant overhead, we may
    /// need to reconsider using UnsafeCell with appropriate safety guarantees.
    key_buffer: Mutex<BytesMut>,
    value_buffer: Mutex<BytesMut>,
}

impl std::fmt::Debug for PartitionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStore")
            .field("partition_id", &self.db.partition().partition_id)
            .field("cf", &self.db.partition().cf_name())
            .field("key_buffer", &self.key_buffer.lock().len())
            .field("value_buffer", &self.value_buffer.lock().len())
            .finish()
    }
}

impl Clone for PartitionStore {
    fn clone(&self) -> Self {
        PartitionStore {
            db: self.db.clone(),
            key_buffer: Mutex::new(BytesMut::default()),
            value_buffer: Mutex::new(BytesMut::default()),
        }
    }
}

impl From<PartitionDb> for PartitionStore {
    fn from(db: PartitionDb) -> Self {
        Self::new(db)
    }
}

impl PartitionStore {
    pub(crate) fn new(db: PartitionDb) -> Self {
        Self {
            db,
            key_buffer: Mutex::new(BytesMut::new()),
            value_buffer: Mutex::new(BytesMut::new()),
        }
    }

    pub fn partition_db(&self) -> &PartitionDb {
        &self.db
    }

    pub fn into_inner(self) -> PartitionDb {
        self.db
    }

    #[inline]
    pub fn partition_id(&self) -> PartitionId {
        self.db.partition().partition_id
    }

    pub fn partition_key_range(&self) -> &RangeInclusive<PartitionKey> {
        &self.db.partition().key_range
    }

    #[inline]
    pub(crate) fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) -> Result<()> {
        assert_partition_key_or_err(&self.db.partition().key_range, partition_key)
    }

    pub fn contains_partition_key(&self, key: PartitionKey) -> bool {
        self.db.partition().key_range.contains(&key)
    }

    pub(crate) fn table_handle(&self, table_kind: TableKind) -> &Arc<BoundColumnFamily<'_>> {
        self.db.table_cf_handle(table_kind)
    }

    fn new_prefix_iterator_opts(&self, _key_kind: KeyKind, prefix: Bytes) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        opts.set_iterate_range(PrefixRange(prefix.clone()));
        opts.set_async_io(true);
        opts.set_total_order_seek(false);
        opts
    }

    fn new_range_iterator_opts(&self, scan_mode: ScanMode, from: Bytes, to: Bytes) -> ReadOptions {
        let mut opts = ReadOptions::default();
        // todo: use auto_prefix_mode, at the moment, rocksdb doesn't expose this through the C
        // binding.
        opts.set_total_order_seek(scan_mode == ScanMode::TotalOrder);
        opts.set_iterate_range(from..to);
        opts.set_async_io(true);
        opts
    }

    #[track_caller]
    fn iterator_from<K: TableKeyPrefix>(
        &self,
        scan: TableScan<K>,
    ) -> Result<DBRawIteratorWithThreadMode<'_, DB>> {
        let scan: PhysicalScan = scan.into();
        match scan {
            PhysicalScan::Prefix(table, key_kind, prefix) => {
                assert!(table.has_key_kind(&prefix));
                let prefix = prefix.freeze();
                let opts = self.new_prefix_iterator_opts(key_kind, prefix.clone());
                let table = self.table_handle(table);
                let mut it = self
                    .db
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(prefix);
                Ok(it)
            }
            PhysicalScan::RangeExclusive(table, _key_kind, scan_mode, start, end) => {
                assert!(table.has_key_kind(&start));
                let start = start.freeze();
                let end = end.freeze();
                let opts = self.new_range_iterator_opts(scan_mode, start.clone(), end);
                let table = self.table_handle(table);
                let mut it = self
                    .db
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(start);
                Ok(it)
            }
            PhysicalScan::RangeOpen(table, _key_kind, start) => {
                // We delayed the generate the synthetic iterator upper bound until this point
                // because we might have different prefix length requirements based on the
                // table+key_kind combination and we should keep this knowledge as low-level as
                // possible.
                //
                // make the end has the same length as all prefixes to ensure rocksdb key
                // comparator can leverage bloom filters when applicable
                // (if auto_prefix_mode is enabled)
                let mut end = BytesMut::zeroed(DB_PREFIX_LENGTH);
                // We want to ensure that Range scans fall within the same key kind.
                // So, we limit the iterator to the upper bound of this prefix
                let kind_upper_bound = K::KEY_KIND.exclusive_upper_bound();
                end[..kind_upper_bound.len()].copy_from_slice(&kind_upper_bound);
                let start = start.freeze();
                let end = end.freeze();
                let opts = self.new_range_iterator_opts(ScanMode::TotalOrder, start.clone(), end);
                let table = self.table_handle(table);
                let mut it = self
                    .db
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(start);
                Ok(it)
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn iterator_step_map<O: Send + 'static>(
        tx: mpsc::Sender<Result<O>>,
        f: impl Fn((&[u8], &[u8])) -> Result<O> + Send + 'static,
    ) -> impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static {
        move |item| {
            let res = match item {
                // apply the caller's function
                Ok((key, value)) => match f((key, value)) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        return IterAction::Stop;
                    }
                },
                Err(e) => {
                    let _ = tx.blocking_send(Err(StorageError::Generic(e.into())));
                    return IterAction::Stop;
                }
            };
            if tx.blocking_send(Ok(res)).is_err() {
                return IterAction::Stop;
            }
            // the channel is not closed yet, keep iterating
            IterAction::Next
        }
    }

    #[allow(clippy::type_complexity)]
    fn iterator_step_filter_map<O: Send + 'static>(
        tx: mpsc::Sender<Result<O>>,
        mut f: impl FnMut((&[u8], &[u8])) -> Result<Option<O>> + Send + 'static,
    ) -> impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static {
        move |item| {
            let res = match item {
                // apply the caller's function
                Ok((key, value)) => match f((key, value)) {
                    Ok(Some(v)) => v,
                    Ok(None) => {
                        if tx.is_closed() {
                            return IterAction::Stop;
                        } else {
                            return IterAction::Next;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        return IterAction::Stop;
                    }
                },
                Err(e) => {
                    let _ = tx.blocking_send(Err(StorageError::Generic(e.into())));
                    return IterAction::Stop;
                }
            };
            if tx.blocking_send(Ok(res)).is_err() {
                return IterAction::Stop;
            }
            // the channel is not closed yet, keep iterating
            IterAction::Next
        }
    }

    #[allow(clippy::type_complexity)]
    fn iterator_step_for_each(
        tx: oneshot::Sender<StorageError>,
        mut f: impl FnMut((&[u8], &[u8])) -> ControlFlow<Result<()>> + Send + 'static,
    ) -> impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send {
        let mut tx = Some(tx);
        move |item| {
            let mut must_send = |v| match tx.take() {
                Some(tx) => tx.send(v),
                None => panic!("Iterator continued after IterAction::Stop"),
            };

            match item {
                // apply the caller's function
                Ok((key, value)) => match f((key, value)) {
                    ControlFlow::Continue(()) => match &mut tx {
                        Some(tx_inner) if tx_inner.is_closed() => {
                            tx = None;
                            IterAction::Stop
                        }
                        // the channel is not closed yet, keep iterating
                        Some(_) => IterAction::Next,
                        None => panic!("Iterator continued after IterAction::Stop"),
                    },
                    ControlFlow::Break(Ok(())) => {
                        // function doesn't need any more items
                        tx = None;
                        IterAction::Stop
                    }
                    ControlFlow::Break(Err(e)) => {
                        let _ = must_send(e);
                        IterAction::Stop
                    }
                },
                Err(e) => {
                    let _ = must_send(StorageError::Generic(e.into()));
                    IterAction::Stop
                }
            }
        }
    }

    pub fn iterator_for_each<K: TableKey>(
        &self,
        name: &'static str,
        priority: Priority,
        scan: TableScan<K>,
        f: impl FnMut((&[u8], &[u8])) -> std::ops::ControlFlow<Result<()>> + Send + 'static,
    ) -> Result<impl Future<Output = Result<()>>, ShutdownError> {
        let (tx, rx) = oneshot::channel();
        let on_iter = Self::iterator_step_for_each(tx, f);
        self.run_iterator_internal(name, priority, scan, on_iter)?;
        Ok(async {
            match rx.await {
                Ok(storage_err) => Err(storage_err),
                Err(_recv_err) => {
                    // iterator was dropped without sending an error; this is actually a success condition
                    Ok(())
                }
            }
        })
    }

    pub fn run_iterator<K: TableKey, O: Send + 'static>(
        &self,
        name: &'static str,
        priority: Priority,
        scan: TableScan<K>,
        f: impl Fn((&[u8], &[u8])) -> Result<O> + Send + 'static,
    ) -> Result<ReceiverStream<Result<O>>, ShutdownError> {
        let (tx, rx) = mpsc::channel(8);
        let on_iter = Self::iterator_step_map(tx, f);
        self.run_iterator_internal(name, priority, scan, on_iter)?;
        Ok(ReceiverStream::new(rx))
    }

    pub fn iterator_filter_map<K: TableKey, O: Send + 'static>(
        &self,
        name: &'static str,
        priority: Priority,
        scan: TableScan<K>,
        f: impl FnMut((&[u8], &[u8])) -> Result<Option<O>> + Send + 'static,
    ) -> Result<ReceiverStream<Result<O>>, ShutdownError> {
        let (tx, rx) = mpsc::channel(8);
        let on_iter = Self::iterator_step_filter_map(tx, f);
        self.run_iterator_internal(name, priority, scan, on_iter)?;
        Ok(ReceiverStream::new(rx))
    }

    fn run_iterator_internal<K: TableKey>(
        &self,
        name: &'static str,
        priority: Priority,
        scan: TableScan<K>,
        on_iter: impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static,
    ) -> Result<(), ShutdownError> {
        let scan: PhysicalScan = scan.into();
        match scan {
            PhysicalScan::Prefix(table, key_kind, prefix) => {
                assert!(table.has_key_kind(&prefix));
                let prefix = prefix.freeze();
                let opts = self.new_prefix_iterator_opts(key_kind, prefix.clone());
                self.db.rocksdb().clone().run_background_iterator(
                    // todo(asoli): Pass an owned cf handle instead of name
                    self.db.partition().cf_name().into(),
                    name,
                    priority,
                    IterAction::Seek(prefix),
                    opts,
                    on_iter,
                )?;
            }
            PhysicalScan::RangeExclusive(table, _key_kind, scan_mode, start, end) => {
                assert!(table.has_key_kind(&start));
                let start = start.freeze();
                let end = end.freeze();
                let opts = self.new_range_iterator_opts(scan_mode, start.clone(), end);
                self.db.rocksdb().clone().run_background_iterator(
                    self.db.partition().cf_name().as_ref().into(),
                    name,
                    priority,
                    IterAction::Seek(start),
                    opts,
                    on_iter,
                )?;
            }
            PhysicalScan::RangeOpen(_table, _key_kind, start) => {
                // We delayed the generate the synthetic iterator upper bound until this point
                // because we might have different prefix length requirements based on the
                // table+key_kind combination and we should keep this knowledge as low-level as
                // possible.
                //
                // make the end has the same length as all prefixes to ensure rocksdb key
                // comparator can leverage bloom filters when applicable
                // (if auto_prefix_mode is enabled)
                let mut end = BytesMut::zeroed(DB_PREFIX_LENGTH);
                // We want to ensure that Range scans fall within the same key kind.
                // So, we limit the iterator to the upper bound of this prefix
                let kind_upper_bound = K::KEY_KIND.exclusive_upper_bound();
                end[..kind_upper_bound.len()].copy_from_slice(&kind_upper_bound);
                let start = start.freeze();
                let end = end.freeze();
                let opts = self.new_range_iterator_opts(ScanMode::TotalOrder, start.clone(), end);
                self.db.rocksdb().clone().run_background_iterator(
                    self.db.partition().cf_name().into(),
                    name,
                    priority,
                    IterAction::Seek(start),
                    opts,
                    on_iter,
                )?;
            }
        }
        Ok(())
    }

    pub fn transaction(&mut self) -> PartitionStoreTransaction<'_> {
        self.transaction_with_isolation(IsolationLevel::Committed)
    }

    /// Returns the durably persisted applied LSN, if any.
    pub async fn get_durable_lsn(&mut self) -> Result<watch::Receiver<Option<Lsn>>> {
        {
            let sender = self.db.durable_lsn_sender();
            if sender.borrow().is_some() {
                let mut rx = self.db.durable_lsn_sender().subscribe();
                rx.mark_changed();
                return Ok(rx);
            }
        }

        let local = get_locally_durable_lsn(self).await?;
        let sender = self.db.durable_lsn_sender();
        if let Some(lsn) = local {
            sender.send_if_modified(|current_mut| {
                if current_mut.is_none_or(|c| c < lsn) {
                    *current_mut = Some(lsn);
                    true
                } else {
                    false
                }
            });
        }

        let mut rx = sender.subscribe();
        rx.mark_changed();
        Ok(rx)
    }

    pub fn transaction_with_isolation(
        &mut self,
        isolation_level: IsolationLevel,
    ) -> PartitionStoreTransaction<'_> {
        // An optimization to avoid looking up the cf handle everytime, if we split into more
        // column families, we will need to cache those cfs here as well.
        let data_cf_handle = self.db.cf_handle();
        let snapshot = match isolation_level {
            IsolationLevel::Committed => None,
            IsolationLevel::RepeatableReads => {
                Some(self.db.rocksdb().inner().as_raw_db().snapshot())
            }
        };

        PartitionStoreTransaction {
            write_batch_with_index: rocksdb::WriteBatchWithIndex::new(0, true),
            data_cf_handle,
            rocksdb: self.db.rocksdb(),
            // Borrow buffers from PartitionStore to avoid allocation per transaction.
            // Interior mutability allows read operations to take &self.
            key_buffer: &self.key_buffer,
            value_buffer: &self.value_buffer,
            meta: self.db.partition(),
            snapshot,
        }
    }

    /// Creates a snapshot of the partition in the given directory, which must not exist prior to
    /// the export. The snapshot is atomic and contains, at a minimum, the reported applied LSN.
    /// Additional log records may have been applied between when the LSN was read, and when the
    /// snapshot was actually created. The actual snapshot applied LSN will always be equal to, or
    /// greater than, the reported applied LSN.
    ///
    /// *NB:* Creating a snapshot causes an implicit flush of the column family!
    ///
    /// See [rocksdb::checkpoint::Checkpoint::export_column_family] for additional implementation details.
    pub(crate) async fn create_local_snapshot(
        &mut self,
        snapshot_base_path: &Path,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
    ) -> Result<LocalPartitionSnapshot> {
        let applied_lsn = self
            .get_applied_lsn()
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?
            .ok_or(StorageError::DataIntegrityError)?; // if PartitionStore::get_applied_lsn returns None

        if let Some(min_target_lsn) = min_target_lsn
            && applied_lsn < min_target_lsn
        {
            return Err(StorageError::PreconditionFailed(anyhow!(
                "Applied LSN below the target LSN: {applied_lsn} < {min_target_lsn}",
            )));
        };

        // RocksDB will create the snapshot directory but the parent must exist first:
        tokio::fs::create_dir_all(snapshot_base_path)
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?;
        let snapshot_dir = snapshot_base_path.join(snapshot_id.to_string());

        let export_files = self
            .db
            .rocksdb()
            .clone()
            .export_cf(self.db.partition().cf_name().into(), snapshot_dir.clone())
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?;

        trace!(
            cf_name = %self.db.partition().cf_name(),
            %applied_lsn,
            "Exported column family snapshot to {:?}",
            snapshot_dir
        );

        Ok(LocalPartitionSnapshot {
            base_dir: snapshot_dir,
            files: export_files.get_files(),
            db_comparator_name: export_files.get_db_comparator_name(),
            log_id: self.db.partition().log_id(),
            min_applied_lsn: applied_lsn,
            key_range: self.db.partition().key_range.clone(),
        })
    }

    pub fn partition(&self) -> &Arc<Partition> {
        self.db.partition()
    }

    pub async fn verify_and_run_migrations(&mut self) -> Result<()> {
        // We assume the partition store to be empty if it does not contain any applied lsn. The
        // reason is that we always commit changes to the partition store via a transaction which
        // also updates the applied lsn field.
        let is_empty = self.get_applied_lsn().await?.is_none();
        if is_empty {
            put_storage_version(self, self.partition_id(), LATEST_VERSION as u16).await?;
            return Ok(());
        }

        let mut schema_version: SchemaVersion =
            get_storage_version(self, self.partition_id()).await?.into();
        if schema_version != LATEST_VERSION {
            // We need to run some migrations!
            debug!(
                "Running storage migration from {:?} to {:?}",
                schema_version, LATEST_VERSION
            );
            schema_version = schema_version.run_all_migrations(self).await?;
            put_storage_version(self, self.partition_id(), schema_version as u16).await?;
        }

        Ok(())
    }
}

impl Storage for PartitionStore {
    type TransactionType<'a> = PartitionStoreTransaction<'a>;

    fn transaction_with_isolation(
        &mut self,
        read_isolation: IsolationLevel,
    ) -> Self::TransactionType<'_> {
        PartitionStore::transaction_with_isolation(self, read_isolation)
    }
}

impl StorageAccess for PartitionStore {
    type DBAccess<'a>
        = DB
    where
        Self: 'a;

    fn iterator_from<K: TableKeyPrefix>(
        &self,
        scan: TableScan<K>,
    ) -> Result<DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>>> {
        self.iterator_from(scan)
    }

    #[inline]
    fn with_key_buffer<F>(&self, min_size: usize, f: F) -> Bytes
    where
        F: FnOnce(&mut BytesMut),
    {
        let mut guard = self.key_buffer.lock();
        guard.clear();
        guard.reserve(min_size);
        f(&mut guard);
        guard.split().freeze()
    }

    #[inline]
    fn with_value_buffer<F>(&self, min_size: usize, f: F) -> Bytes
    where
        F: FnOnce(&mut BytesMut),
    {
        let mut guard = self.value_buffer.lock();
        guard.clear();
        guard.reserve(min_size);
        f(&mut guard);
        guard.split().freeze()
    }

    #[inline]
    fn try_with_value_buffer<F, E>(&self, min_size: usize, f: F) -> std::result::Result<Bytes, E>
    where
        F: FnOnce(&mut BytesMut) -> std::result::Result<(), E>,
    {
        let mut guard = self.value_buffer.lock();
        guard.clear();
        guard.reserve(min_size);
        f(&mut guard)?;
        Ok(guard.split().freeze())
    }

    #[inline]
    fn get<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<DBPinnableSlice<'_>>> {
        let table = self.table_handle(table);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .get_pinned_cf(table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    fn get_durable<K: AsRef<[u8]>>(
        &self,
        table: TableKind,
        key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>> {
        let table = self.table_handle(table);
        let mut read_opts = ReadOptions::default();
        read_opts.set_read_tier(rocksdb::ReadTier::Persisted);

        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .get_pinned_cf_opt(table, key, &read_opts)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn put_cf(
        &mut self,
        table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let table = self.table_handle(table);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .put_cf(table, key, value)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) -> Result<()> {
        let table = self.table_handle(table);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .delete_cf(table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ScanMode {
    /// Scan is bound to a single fixed key prefix (partition id, or a single partition key).
    WithinPrefix,
    /// Scan/iterator requires total order seek, this means that the iterator is not bound to a
    /// fixed prefix that matches the column family prefix extractor length. For instance, if
    /// scanning data across multiple partition IDs or multiple partition keys.
    TotalOrder,
}

pub struct PartitionStoreTransaction<'a> {
    meta: &'a Arc<Partition>,
    write_batch_with_index: rocksdb::WriteBatchWithIndex,
    rocksdb: &'a Arc<RocksDb>,
    data_cf_handle: &'a Arc<BoundColumnFamily<'a>>,
    /// Key buffer for serializing keys. Borrowed from PartitionStore to avoid
    /// allocation per transaction. Uses interior mutability to allow read
    /// operations to take `&self` instead of `&mut self`, enabling multiple
    /// concurrent iterators from the same transaction.
    ///
    /// TODO: Measure the performance impact of Mutex<BytesMut> compared to
    /// UnsafeCell<BytesMut>. If benchmarks show significant overhead, we may
    /// need to reconsider using UnsafeCell with appropriate safety guarantees.
    key_buffer: &'a Mutex<BytesMut>,
    /// Value buffer for serializing values. Borrowed from PartitionStore.
    value_buffer: &'a Mutex<BytesMut>,
    snapshot: Option<SnapshotWithThreadMode<'a, rocksdb::DB>>,
}

impl PartitionStoreTransaction<'_> {
    fn read_options(&self) -> ReadOptions {
        let mut opts = ReadOptions::default();

        if let Some(snapshot) = self.snapshot.as_ref() {
            opts.set_snapshot(snapshot);
        }

        opts
    }

    #[inline]
    pub fn raw_put_cf(
        &mut self,
        _key_kind: KeyKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        self.write_batch_with_index
            .put_cf(self.data_cf_handle, key, value);
    }

    #[inline]
    pub fn raw_merge_cf(
        &mut self,
        _key_kind: KeyKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        self.write_batch_with_index
            .merge_cf(self.data_cf_handle, key, value);
    }

    #[inline]
    pub fn raw_delete_cf(&mut self, _key_kind: KeyKind, key: impl AsRef<[u8]>) {
        self.write_batch_with_index
            .delete_cf(self.data_cf_handle, key);
    }

    pub(crate) fn prefix_iterator(
        &self,
        table: TableKind,
        _key_kind: KeyKind,
        prefix: Bytes,
    ) -> Result<DBIterator<'_>> {
        let table = self.table_handle(table);
        let mut opts = self.read_options();
        opts.set_iterate_range(PrefixRange(prefix.clone()));
        opts.set_prefix_same_as_start(true);
        opts.set_total_order_seek(false);

        let it = self
            .rocksdb
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(table, opts);
        let mut it = self.write_batch_with_index.iterator_with_base_cf(it, table);
        it.seek(prefix);
        Ok(it)
    }

    pub(crate) fn range_iterator(
        &self,
        table: TableKind,
        _key_kind: KeyKind,
        scan_mode: ScanMode,
        from: Bytes,
        to: Bytes,
    ) -> Result<DBIterator<'_>> {
        let table = self.table_handle(table);
        let mut opts = self.read_options();
        // todo: use auto_prefix_mode, at the moment, rocksdb doesn't expose this through the C
        // binding.
        opts.set_total_order_seek(scan_mode == ScanMode::TotalOrder);
        opts.set_iterate_range(from.clone()..to);

        let it = self
            .rocksdb
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(table, opts);
        let mut it = self.write_batch_with_index.iterator_with_base_cf(it, table);
        it.seek(from);
        Ok(it)
    }

    pub(crate) fn table_handle(&self, _table_kind: TableKind) -> &Arc<BoundColumnFamily<'_>> {
        // Right now, everything is in one cf, return a reference and save CPU.
        self.data_cf_handle
    }

    #[inline]
    pub(crate) fn partition_id(&self) -> PartitionId {
        self.meta.partition_id
    }

    #[inline]
    pub(crate) fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) -> Result<()> {
        assert_partition_key_or_err(&self.meta.key_range, partition_key)
    }
}

fn assert_partition_key_or_err(
    partition_key_range: &RangeInclusive<PartitionKey>,
    partition_key: &impl WithPartitionKey,
) -> Result<()> {
    let partition_key = partition_key.partition_key();
    if partition_key_range.contains(&partition_key) {
        return Ok(());
    }
    Err(StorageError::Generic(anyhow!(
        "Partition key '{partition_key}' is not part of PartitionStore's partition '{partition_key_range:?}'. This indicates a bug."
    )))
}

impl Transaction for PartitionStoreTransaction<'_> {
    async fn commit(self) -> Result<()> {
        // We cannot directly commit the txn because it might fail because of unrelated concurrent
        // writes to RocksDB. However, it is safe to write the WriteBatch for a given partition,
        // because there can only be a single writer (the leading PartitionProcessor).
        if self.write_batch_with_index.is_empty() {
            return Ok(());
        }
        let io_mode = if Configuration::pinned()
            .worker
            .storage
            .always_commit_in_background
        {
            IoMode::AlwaysBackground
        } else {
            IoMode::Default
        };
        let mut opts = rocksdb::WriteOptions::default();
        // We disable WAL since bifrost is our durable distributed log.
        opts.disable_wal(true);
        self.rocksdb
            .write_batch_with_index(
                "partition-store-txn-commit",
                Priority::High,
                io_mode,
                opts,
                self.write_batch_with_index,
            )
            .await
            .map_err(|error| StorageError::Generic(error.into()))
    }
}

impl StorageAccess for PartitionStoreTransaction<'_> {
    type DBAccess<'b>
        = DB
    where
        Self: 'b;

    fn iterator_from<K: TableKeyPrefix>(
        &self,
        scan: TableScan<K>,
    ) -> Result<DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>>> {
        let scan: PhysicalScan = scan.into();
        match scan {
            PhysicalScan::Prefix(table, key_kind, prefix) => {
                self.prefix_iterator(table, key_kind, prefix.freeze())
            }
            PhysicalScan::RangeExclusive(table, key_kind, scan_mode, start, end) => {
                self.range_iterator(table, key_kind, scan_mode, start.freeze(), end.freeze())
            }
            PhysicalScan::RangeOpen(table, key_kind, start) => {
                // We delayed the generate the synthetic iterator upper bound until this point
                // because we might have different prefix length requirements based on the
                // table+key_kind combination and we should keep this knowledge as low-level as
                // possible.
                //
                // make the end has the same length as all prefixes to ensure rocksdb key
                // comparator can leverage bloom filters when applicable
                // (if auto_prefix_mode is enabled)
                let mut end = BytesMut::zeroed(DB_PREFIX_LENGTH);
                // We want to ensure that Range scans fall within the same key kind.
                // So, we limit the iterator to the upper bound of this prefix
                let kind_upper_bound = K::KEY_KIND.exclusive_upper_bound();
                end[..kind_upper_bound.len()].copy_from_slice(&kind_upper_bound);
                self.range_iterator(
                    table,
                    key_kind,
                    ScanMode::TotalOrder,
                    start.freeze(),
                    end.freeze(),
                )
            }
        }
    }

    #[inline]
    fn with_key_buffer<F>(&self, min_size: usize, f: F) -> Bytes
    where
        F: FnOnce(&mut BytesMut),
    {
        let mut guard = self.key_buffer.lock();
        guard.clear();
        guard.reserve(min_size);
        f(&mut guard);
        guard.split().freeze()
    }

    #[inline]
    fn with_value_buffer<F>(&self, min_size: usize, f: F) -> Bytes
    where
        F: FnOnce(&mut BytesMut),
    {
        let mut guard = self.value_buffer.lock();
        guard.clear();
        guard.reserve(min_size);
        f(&mut guard);
        guard.split().freeze()
    }

    #[inline]
    fn try_with_value_buffer<F, E>(&self, min_size: usize, f: F) -> std::result::Result<Bytes, E>
    where
        F: FnOnce(&mut BytesMut) -> std::result::Result<(), E>,
    {
        let mut guard = self.value_buffer.lock();
        guard.clear();
        guard.reserve(min_size);
        f(&mut guard)?;
        Ok(guard.split().freeze())
    }

    #[inline]
    fn get<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<DBPinnableSlice<'_>>> {
        let table = self.table_handle(table);
        self.write_batch_with_index
            .get_pinned_from_batch_and_db_cf(
                self.rocksdb.inner().as_raw_db(),
                table,
                key,
                &self.read_options(),
            )
            .map_err(|error| StorageError::Generic(error.into()))
    }

    fn get_durable<K: AsRef<[u8]>>(
        &self,
        _table: TableKind,
        _key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>> {
        unreachable!("ReadTier::Persisted is not meant to be used in WBI");
    }

    #[inline]
    fn put_cf(
        &mut self,
        _table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.write_batch_with_index
            .put_cf(self.data_cf_handle, key, value);
        Ok(())
    }

    #[inline]
    fn delete_cf(&mut self, _table: TableKind, key: impl AsRef<[u8]>) -> Result<()> {
        self.write_batch_with_index
            .delete_cf(self.data_cf_handle, key);
        Ok(())
    }
}

pub(crate) trait StorageAccess {
    type DBAccess<'a>: rocksdb::DBAccess
    where
        Self: 'a;

    fn iterator_from<K: TableKeyPrefix>(
        &self,
        scan: TableScan<K>,
    ) -> Result<DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>>>;

    /// Provides access to a cleared key buffer with at least `min_size` capacity,
    /// calls the closure to serialize into it, then splits and freezes the result.
    /// Takes `&self` to allow interior mutability, enabling multiple concurrent reads.
    fn with_key_buffer<F>(&self, min_size: usize, f: F) -> Bytes
    where
        F: FnOnce(&mut BytesMut);

    /// Provides access to a cleared value buffer with at least `min_size` capacity,
    /// calls the closure to serialize into it, then splits and freezes the result.
    /// Takes `&self` to allow interior mutability, enabling multiple concurrent reads.
    fn with_value_buffer<F>(&self, min_size: usize, f: F) -> Bytes
    where
        F: FnOnce(&mut BytesMut);

    /// Like `with_value_buffer` but allows the closure to return an error.
    fn try_with_value_buffer<F, E>(&self, min_size: usize, f: F) -> std::result::Result<Bytes, E>
    where
        F: FnOnce(&mut BytesMut) -> std::result::Result<(), E>;

    fn get<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<DBPinnableSlice<'_>>>;

    /// Forces a read from persistent storage, bypassing memtables and block cache.
    ///
    /// This means that that you might not get the latest value, but the value that was persisted
    /// (flushed) at the time of the call.
    fn get_durable<K: AsRef<[u8]>>(
        &self,
        table: TableKind,
        key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>>;

    fn put_cf(
        &mut self,
        table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()>;

    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) -> Result<()>;

    #[inline]
    fn put_kv_raw<K: TableKey, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<()> {
        let key_bytes = self.with_key_buffer(key.serialized_length(), |buf| {
            key.serialize_to(buf);
        });
        self.put_cf(K::TABLE, key_bytes, value)
    }

    #[inline]
    fn put_kv_proto<K: TableKey, V: PartitionStoreProtobufValue + Clone + 'static>(
        &mut self,
        key: K,
        value: &V,
    ) -> Result<()> {
        self.put_kv_storage_codec(
            key,
            &ProtobufStorageWrapper::<V::ProtobufType>(value.clone().into()),
        )
    }

    #[inline]
    fn put_kv_storage_codec<K: TableKey, V: StorageEncode + 'static>(
        &mut self,
        key: K,
        value: &V,
    ) -> Result<()> {
        let key_bytes = self.with_key_buffer(key.serialized_length(), |buf| {
            key.serialize_to(buf);
        });
        let value_bytes = self.try_with_value_buffer(0, |buf| {
            StorageCodec::encode(value, buf).map_err(|e| StorageError::Generic(e.into()))
        })?;
        self.put_cf(K::TABLE, key_bytes, value_bytes)
    }

    #[inline]
    fn delete_key<K: TableKey>(&mut self, key: &K) -> Result<()> {
        let key_bytes = self.with_key_buffer(key.serialized_length(), |buf| {
            key.serialize_to(buf);
        });
        self.delete_cf(K::TABLE, key_bytes)
    }

    #[inline]
    fn get_value_proto<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: TableKey,
        V: PartitionStoreProtobufValue,
        <<V as PartitionStoreProtobufValue>::ProtobufType as TryInto<V>>::Error:
            Into<anyhow::Error>,
    {
        let value: Option<ProtobufStorageWrapper<V::ProtobufType>> =
            self.get_value_storage_codec(key)?;

        value
            .map(|v| v.0.try_into())
            .transpose()
            .map_err(|err| StorageError::Conversion(err.into()))
    }

    #[inline]
    fn get_value_storage_codec<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: TableKey,
        V: StorageDecode,
    {
        let key_bytes = self.with_key_buffer(key.serialized_length(), |buf| {
            key.serialize_to(buf);
        });

        self.get(K::TABLE, &key_bytes)?
            .map(|value| {
                let mut slice = value.as_ref();
                StorageCodec::decode(&mut slice)
            })
            .transpose()
            .map_err(|err| StorageError::Generic(err.into()))
    }

    /// Forces a read from persistent storage, bypassing memtables and block cache.
    /// This means that that you might not get the latest value, but the value that was
    /// persisted (flushed) at the time of the call.
    #[inline]
    fn get_durable_value<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: TableKey,
        V: PartitionStoreProtobufValue,
        <<V as PartitionStoreProtobufValue>::ProtobufType as TryInto<V>>::Error:
            Into<anyhow::Error>,
    {
        let key_bytes = self.with_key_buffer(key.serialized_length(), |buf| {
            key.serialize_to(buf);
        });

        match self.get_durable(K::TABLE, &key_bytes) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());

                if let Some(mut slice) = slice {
                    Ok(Some(V::decode(&mut slice)?))
                } else {
                    Ok(None)
                }
            }
            Err(err) => Err(err),
        }
    }

    #[inline]
    fn get_first_blocking<K, F, R>(&self, scan: TableScan<K>, f: F) -> Result<R>
    where
        K: TableKeyPrefix,
        F: FnOnce(Option<(&[u8], &[u8])>) -> Result<R>,
    {
        let iterator = self.iterator_from(scan)?;
        f(iterator.item())
    }

    #[inline]
    fn get_kv_raw<K, F, R>(&self, key: K, f: F) -> Result<R>
    where
        K: TableKey,
        F: FnOnce(&[u8], Option<&[u8]>) -> Result<R>,
    {
        let key_bytes = self.with_key_buffer(key.serialized_length(), |buf| {
            key.serialize_to(buf);
        });

        match self.get(K::TABLE, &key_bytes) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());
                f(&key_bytes, slice)
            }
            Err(err) => Err(err),
        }
    }

    #[inline]
    fn for_each_key_value_in_place<K, F, R>(
        &self,
        scan: TableScan<K>,
        mut op: F,
    ) -> Result<Vec<Result<R>>>
    where
        K: TableKeyPrefix,
        F: FnMut(&[u8], &[u8]) -> TableScanIterationDecision<R>,
    {
        let mut res = Vec::new(); // TODO: this should be passed in.

        let mut iterator = self.iterator_from(scan)?;

        while let Some((k, v)) = iterator.item() {
            match op(k, v) {
                TableScanIterationDecision::Emit(result) => {
                    res.push(result);
                    iterator.next();
                }
                TableScanIterationDecision::BreakWith(result) => {
                    res.push(result);
                    break;
                }
                TableScanIterationDecision::Continue => {
                    iterator.next();
                    continue;
                }
                TableScanIterationDecision::Break => {
                    break;
                }
            };
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use crate::keys::{KeyKind, TableKey};
    use crate::partition_store::StorageAccess;
    use crate::{PartitionStoreManager, TableKind};
    use bytes::{Buf, BufMut};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::{IsolationLevel, StorageError, Transaction};
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;

    impl TableKey for String {
        const TABLE: TableKind = TableKind::State;
        const KEY_KIND: KeyKind = KeyKind::State;

        fn serialize_to<B: BufMut>(&self, bytes: &mut B) {
            Self::KEY_KIND.serialize(bytes);
            bytes.put_u32(self.len() as u32);
            bytes.put_slice(self.as_bytes());
        }

        fn deserialize_from<B: Buf>(bytes: &mut B) -> crate::partition_store::Result<Self> {
            let key_kind = KeyKind::deserialize(bytes)?;
            assert_eq!(key_kind, Self::KEY_KIND);

            let len = bytes.get_u32() as usize;
            let mut string_bytes = Vec::with_capacity(len);
            bytes.copy_to_slice(&mut string_bytes);
            Ok(String::from_utf8(string_bytes).expect("valid key"))
        }

        fn serialized_length(&self) -> usize {
            KeyKind::SERIALIZED_LENGTH + self.len()
        }
    }

    #[restate_core::test]
    async fn concurrent_writes_and_reads() -> googletest::Result<()> {
        let rocksdb = RocksDbManager::init();
        let partition_store_manager = PartitionStoreManager::create().await?;
        let mut partition_store = partition_store_manager
            .open(
                &Partition::new(PartitionId::MIN, PartitionKey::MIN..=PartitionKey::MAX),
                None,
            )
            .await?;
        let key_a = "a".to_owned();
        let key_b = "b".to_owned();

        // put the initial values
        partition_store.put_kv_raw(key_a.clone(), 0_u32.to_be_bytes())?;
        partition_store.put_kv_raw(key_b.clone(), 0_u32.to_be_bytes())?;

        let mut partition_store_clone = partition_store.clone();
        // repeatable reads shouldn't see writes of concurrently finishing transactions
        let read_txn = partition_store.transaction_with_isolation(IsolationLevel::RepeatableReads);

        let value_a = read_txn.get_kv_raw(key_a.clone(), decode_u32)?;

        // concurrent write which should not be visible by read_txn
        let mut write_txn = partition_store_clone.transaction();
        write_txn.put_kv_raw(key_a.clone(), 42_u32.to_be_bytes())?;
        write_txn.put_kv_raw(key_b.clone(), 42_u32.to_be_bytes())?;
        write_txn.commit().await?;

        let value_b = read_txn.get_kv_raw(key_b.clone(), decode_u32)?;

        assert_eq!(value_a, value_b);

        rocksdb.shutdown().await;
        Ok(())
    }

    fn decode_u32(_key: &[u8], bytes: Option<&[u8]>) -> Result<Option<u32>, StorageError> {
        if let Some(bytes) = bytes {
            bytes
                .try_into()
                .map(u32::from_be_bytes)
                .map(Some)
                .map_err(|err| StorageError::Conversion(err.into()))
        } else {
            Ok(None)
        }
    }
}
