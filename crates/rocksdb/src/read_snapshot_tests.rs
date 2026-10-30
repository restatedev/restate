// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Waker};
use std::time::Duration;

use rocksdb::{BoundColumnFamily, ErrorKind, ReadOptions, ReadTier};
use tokio::time::timeout;

use restate_core::TaskCenterBuilder;
use restate_platform::sync::Mutex;
use restate_types::protobuf::common::DatabaseKind;

use crate::configuration::{
    CfConfigurator, DbConfigurator, create_default_cf_options, create_default_db_options,
};
use crate::{
    CfName, CfPrefixPattern, DatabaseScope, DbEnv, DbName, DbSpec, DbSpecBuilder, GlobalExecutor,
    OwnedReadSnapshot, PinnedBatch, ReadSnapshotOwner, RocksAccess, RocksDb, RocksDbManager,
    RocksError, SnapshotIterator,
};

const DEADLINE: Duration = Duration::from_secs(10);

// Turns off the block cache, so that reads restricted to the cache can't find
// data that was flushed to disk and fail with `Incomplete` instead.
struct Configurator;

impl DbConfigurator for Configurator {
    fn get_db_options(&self, name: &DbName, env: &DbEnv) -> rocksdb::Options {
        create_default_db_options(env, name)
    }
}

impl CfConfigurator for Configurator {
    fn get_cf_options(&self, _: &DbName, _: &str, env: &DbEnv) -> rocksdb::Options {
        let mut opts = create_default_cf_options(env.write_buffer_manager.as_ref());
        let mut blocks = rocksdb::BlockBasedOptions::default();
        blocks.disable_cache();
        opts.set_block_based_table_factory(&blocks);
        opts
    }
}

fn spec(path: &Path) -> DbSpec {
    DbSpecBuilder::new(
        DbName::new("read-snapshot"),
        DatabaseKind::PartitionStore,
        path.to_owned(),
        Configurator,
    )
    .add_cf_pattern(CfPrefixPattern::ANY, Configurator)
    .ensure_column_families(vec![CfName::new("data"), CfName::new("other")])
    .build()
    .unwrap()
}

/// Opens a database without a manager. Dropping its last handle closes it right
/// away, on the dropping thread.
fn standalone(path: &Path) -> anyhow::Result<Arc<RocksDb>> {
    let executor = GlobalExecutor::new(
        threadpool::ThreadPool::new(1),
        threadpool::ThreadPool::new(1),
    );
    let native = RocksAccess::open_db(spec(path), DbEnv::default())?;
    Ok(Arc::new(RocksDb::new(native, executor)))
}

#[derive(Clone, Copy)]
enum Cf {
    Data,
    Other,
}

/// What a test owner saw when it was dropped.
#[derive(Debug, PartialEq)]
struct OwnerDrop {
    /// How many native snapshots were still open in the database.
    native_snapshots: Option<u64>,
    /// Whether the owner's scope had no operations in flight, i.e. whether the
    /// snapshot's lease had already been released.
    admission_idle: bool,
}

/// A snapshot owner for tests. Like a partition, it looks up its column families
/// once, when it is created. It also records what it sees when it is dropped.
struct TestOwner {
    // The column families are dropped before the database.
    cfs: [Arc<BoundColumnFamily<'static>>; 2],
    db: Arc<RocksDb>,
    scope: Arc<DatabaseScope>,
    dropped: Arc<Mutex<Option<OwnerDrop>>>,
    // If set, the second call to `rocksdb()` panics. `capture` makes that call
    // after it has taken the lease, so this simulates a panic halfway through
    // taking a snapshot.
    fail_capture: bool,
    rocksdb_called: AtomicBool,
}

impl TestOwner {
    fn new(db: &Arc<RocksDb>, scope: &Arc<DatabaseScope>) -> Self {
        let cf = |name| {
            let cf = db.inner().cf_handle(name).unwrap();
            // SAFETY: the handle is stored in `cfs`, which is dropped before `db`,
            // so the handle never outlives the database. The partition store
            // extends its handles' lifetime the same way.
            unsafe { Arc::from_raw(Arc::into_raw(cf).cast::<BoundColumnFamily<'static>>()) }
        };
        Self {
            cfs: [cf("data"), cf("other")],
            db: Arc::clone(db),
            scope: Arc::clone(scope),
            dropped: Arc::default(),
            fail_capture: false,
            rocksdb_called: AtomicBool::new(false),
        }
    }
}

impl Drop for TestOwner {
    fn drop(&mut self) {
        let native_snapshots = self
            .db
            .inner()
            .as_raw_db()
            .property_int_value(rocksdb::properties::NUM_SNAPSHOTS)
            .ok()
            .flatten();
        *self.dropped.lock() = Some(OwnerDrop {
            native_snapshots,
            admission_idle: self.scope.tracker_for(&self.db).is_idle(),
        });
    }
}

// SAFETY: `db` is never replaced, and the column families were looked up in it.
unsafe impl ReadSnapshotOwner for TestOwner {
    type Cf = Cf;

    fn rocksdb(&self) -> &Arc<RocksDb> {
        if self.fail_capture && self.rocksdb_called.swap(true, Ordering::Relaxed) {
            panic!("injected capture failure");
        }
        &self.db
    }

    fn admission(&self) -> &DatabaseScope {
        &self.scope
    }

    fn cf(&self, cf: Cf) -> &Arc<BoundColumnFamily<'_>> {
        &self.cfs[cf as usize]
    }
}

fn some(value: &str) -> Option<Vec<u8>> {
    Some(value.into())
}

type Entry = (Vec<u8>, Vec<u8>);

fn kv(key: &str, value: &str) -> Entry {
    (key.into(), value.into())
}

fn values<R>(batch: &PinnedBatch<'_, R>) -> Result<Vec<Option<Vec<u8>>>, RocksError> {
    (0..batch.len())
        .map(|index| Ok(batch.get(index)?.map(<[u8]>::to_vec)))
        .collect()
}

/// Moves the iterator to the first key and collects every entry from there.
fn entries<R>(iterator: &mut SnapshotIterator<'_, R>) -> Result<Vec<Entry>, RocksError> {
    let mut entries = Vec::new();
    iterator.seek(b"");
    while let Some((key, value)) = iterator.item()? {
        entries.push((key.to_vec(), value.to_vec()));
        iterator.next();
    }
    Ok(entries)
}

fn is_error<T>(result: &Result<T, RocksError>, kind: ErrorKind) -> bool {
    matches!(result, Err(RocksError::Other(error)) if error.kind() == kind)
}

fn is_pending(future: impl Future) -> bool {
    pin!(future)
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending()
}

#[test]
fn reads_see_the_database_as_it_was_when_the_snapshot_was_taken() -> anyhow::Result<()> {
    let tempdir = restate_test_util::tempdir()?;
    let db = standalone(&tempdir.path().join("db"))?;
    let scope = db.register_scope();
    let raw = db.inner().as_raw_db();
    let data = db.inner().cf_handle("data").unwrap();
    let other = db.inner().cf_handle("other").unwrap();
    raw.put_cf(&data, b"a", b"1")?;
    raw.put_cf(&data, b"b", b"")?;
    raw.put_cf(&data, b"c", b"3")?;
    raw.put_cf(&other, b"a", b"other")?;

    let owner = TestOwner::new(&db, &scope);
    let dropped = Arc::clone(&owner.dropped);
    let snapshot = OwnedReadSnapshot::capture(owner)?;
    let clone = snapshot.clone();
    raw.put_cf(&data, b"a", b"changed")?;
    raw.delete_cf(&data, b"c")?;
    raw.put_cf(&data, b"d", b"4")?;
    raw.put_cf(&other, b"a", b"changed")?;

    // Reads through a clone don't see the later writes, in either column family.
    // A key with an empty value exists, unlike a key that was never written.
    let reader = clone.reader(Cf::Data, ReadOptions::default());
    assert_eq!(reader.get(b"a")?, some("1"));
    assert_eq!(reader.get_pinned(b"b")?.as_deref(), Some(&b""[..]));
    assert_eq!(reader.get(b"d")?, None);
    let other_reader = snapshot.reader(Cf::Other, ReadOptions::default());
    assert_eq!(other_reader.get(b"a")?, some("other"));

    // Multi-get returns results in the same order as the keys: for unsorted keys
    // with duplicates, for sorted keys, and for no keys at all.
    let unsorted = reader.multi_get_pinned([b"c", b"d", b"a", b"b", b"a"], false)?;
    assert_eq!(
        values(&unsorted)?,
        [some("3"), None, some("1"), some(""), some("1")]
    );
    let sorted = reader.multi_get_pinned([b"a", b"b", b"c", b"d"], true)?;
    assert_eq!(values(&sorted)?, [some("1"), some(""), some("3"), None]);
    let empty = reader.multi_get_pinned(std::iter::empty::<&[u8]>(), false)?;
    assert!(empty.is_empty());

    // Iterators ignore a request for tailing. They don't see a write made after
    // they were created, neither when moving forward nor when seeking again.
    let tailing = || {
        let mut options = ReadOptions::default();
        options.set_tailing(true);
        options
    };
    let mut borrowed = snapshot.iterator(Cf::Data, tailing());
    let mut owned = snapshot.owned_iterator(Cf::Data, tailing());
    borrowed.seek(b"");
    owned.seek(b"");
    raw.put_cf(&data, b"aa", b"later")?;
    borrowed.next();
    owned.next();
    assert_eq!(borrowed.item()?, Some((&b"b"[..], &b""[..])));
    assert_eq!(owned.item()?, Some((&b"b"[..], &b""[..])));
    assert!(borrowed.valid());
    assert!(owned.valid());
    borrowed.status()?;
    owned.status()?;
    let captured = [kv("a", "1"), kv("b", ""), kv("c", "3")];
    assert_eq!(entries(&mut borrowed)?, captured);
    assert!(!borrowed.valid());
    borrowed.status()?;

    // After sealing the scope, no new snapshot can be taken, but reads through
    // the existing one keep working.
    scope.seal();
    assert!(OwnedReadSnapshot::capture(TestOwner::new(&db, &scope)).is_err());
    assert_eq!(reader.get(b"a")?, some("1"));

    // Owned values keep the database open by themselves. Drop every other handle,
    // including the test's own database and column family handles, and watch for
    // the database to close without keeping it open.
    let owned_reader = snapshot.owned_reader(Cf::Data, ReadOptions::default());
    let first = owned_reader.owned_multi_get_pinned([b"a", b"d"], true)?;
    let second = owned_reader.owned_multi_get_pinned([b"c"], false)?;
    let mut closed = pin!(db.closed());
    drop(unsorted);
    drop(sorted);
    drop(empty);
    drop(reader);
    drop(other_reader);
    drop(borrowed);
    drop(clone);
    drop(snapshot);
    drop(data);
    drop(other);
    drop(db);
    assert!(is_pending(closed.as_mut()));

    // The owned values still work on another thread. The reader has looked up two
    // batches, and the batches are read after the reader itself is dropped.
    let read = std::thread::spawn(move || -> Result<_, RocksError> {
        let value = owned_reader.get(b"a")?;
        drop(owned_reader);
        Ok((
            value,
            values(&first)?,
            values(&second)?,
            entries(&mut owned)?,
        ))
    });
    let (value, first, second, owned_entries) = read.join().unwrap()?;
    assert_eq!(value, some("1"));
    assert_eq!(first, [some("1"), None]);
    assert_eq!(second, [some("3")]);
    assert_eq!(owned_entries, captured);

    // Dropping the last owned value closed the database. The native snapshot was
    // released before the owner, and the lease after it, so the scope now has no
    // operations in flight.
    assert!(!is_pending(closed.as_mut()));
    let released = dropped.lock().take();
    assert_eq!(
        released,
        Some(OwnerDrop {
            native_snapshots: Some(0),
            admission_idle: false,
        })
    );
    assert!(!is_pending(scope.drain()));
    Ok(())
}

#[test]
fn read_errors_are_reported_per_key_and_by_iterators() -> anyhow::Result<()> {
    let tempdir = restate_test_util::tempdir()?;
    let db = standalone(&tempdir.path().join("db"))?;
    let scope = db.register_scope();
    {
        let raw = db.inner().as_raw_db();
        let data = db.inner().cf_handle("data").unwrap();
        raw.put_cf(&data, b"flushed", b"1")?;
        raw.flush_cf(&data)?;
        raw.put_cf(&data, b"memtable-1", b"1")?;
        raw.put_cf(&data, b"memtable-2", b"2")?;
    }
    let snapshot = OwnedReadSnapshot::capture(TestOwner::new(&db, &scope))?;
    let cache_only = || {
        let mut options = ReadOptions::default();
        options.set_read_tier(ReadTier::BlockCache);
        options
    };

    // A multi-get can succeed as a whole while one of its keys fails. With a
    // value size limit of 0, RocksDB reads the first key and aborts the rest.
    // Both keys are still in memory (not flushed), so RocksDB reads them in key
    // order and we know which one is aborted.
    let mut limited = ReadOptions::default();
    limited.set_value_size_soft_limit(0);
    let reader = snapshot.reader(Cf::Data, limited);
    let batch = reader.multi_get_pinned([b"memtable-1", b"memtable-2"], true)?;
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.get(0)?, Some(&b"1"[..]));
    assert!(
        is_error(&batch.get(1), ErrorKind::Aborted),
        "{:?}",
        batch.get(1)
    );
    assert!(catch_unwind(AssertUnwindSafe(|| batch.get(2))).is_err());

    // A read restricted to the cache can't find the flushed key, because there
    // is no block cache.
    let reader = snapshot.reader(Cf::Data, cache_only());
    let flushed = reader.get(b"flushed");
    assert!(is_error(&flushed, ErrorKind::Incomplete), "{flushed:?}");

    // An iterator that hits the same problem returns the error, rather than
    // looking as if it had reached the end.
    let mut iterator = snapshot.iterator(Cf::Data, cache_only());
    iterator.seek(b"");
    assert!(!iterator.valid());
    assert!(is_error(&iterator.status(), ErrorKind::Incomplete));
    let item = iterator.item();
    assert!(is_error(&item, ErrorKind::Incomplete), "{item:?}");
    Ok(())
}

#[tokio::test]
async fn drains_wait_until_snapshots_have_released_everything() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let manager = task_center.run_sync(RocksDbManager::init);
    let tempdir = restate_test_util::tempdir()?;
    let (_, opening) = manager.open_db(spec(&tempdir.path().join("db")))?;
    let db = opening.await?;
    let scope = db.register_scope();
    let sibling = db.register_scope();

    // If taking a snapshot panics after the lease was taken, the owner is still
    // dropped before the lease is released.
    let mut failing = TestOwner::new(&db, &scope);
    failing.fail_capture = true;
    let dropped = Arc::clone(&failing.dropped);
    assert!(catch_unwind(AssertUnwindSafe(|| OwnedReadSnapshot::capture(failing))).is_err());
    let unwound = dropped.lock().take();
    assert_eq!(
        unwound,
        Some(OwnerDrop {
            native_snapshots: Some(0),
            admission_idle: false,
        })
    );
    assert!(scope.tracker_for(&db).is_idle());

    // An owned iterator keeps the snapshot (and its lease) alive after the
    // snapshot handle itself is dropped.
    let owner = TestOwner::new(&db, &scope);
    let dropped = Arc::clone(&owner.dropped);
    let snapshot = OwnedReadSnapshot::capture(owner)?;
    let mut iterator = snapshot.owned_iterator(Cf::Data, ReadOptions::default());
    drop(snapshot);

    // Once sealed, the scope refuses new snapshots, and draining it waits for the
    // iterator. Another scope of the same database drains independently.
    scope.seal();
    assert!(OwnedReadSnapshot::capture(TestOwner::new(&db, &scope)).is_err());
    assert!(is_pending(scope.drain()));
    sibling.seal();
    timeout(DEADLINE, sibling.drain()).await?;

    // Draining the whole database also waits for the iterator, even after the
    // test drops its handle to the scope. The owner holds the last one.
    drop(scope);
    let mut drained = Box::pin(db.drain());
    assert!(is_pending(drained.as_mut()));
    iterator.seek(b"");
    assert_eq!(iterator.item()?, None);
    assert!(dropped.lock().is_none());

    // Dropping the iterator releases things in order: first the native snapshot,
    // then the owner, and only then the lease, which lets the drain finish.
    drop(iterator);
    let released = dropped.lock().take();
    assert_eq!(
        released,
        Some(OwnerDrop {
            native_snapshots: Some(0),
            admission_idle: false,
        })
    );
    timeout(DEADLINE, drained).await?;
    assert!(OwnedReadSnapshot::capture(TestOwner::new(&db, &db.register_scope())).is_err());

    drop(db);
    task_center.shutdown_node("test completed", 0).await;
    Ok(())
}
