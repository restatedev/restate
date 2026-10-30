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
use std::sync::{Arc, OnceLock};
use std::task::{Context, Waker};
use std::time::Duration;

use rocksdb::{ErrorKind, ReadOptions, ReadTier};
use tokio::time::timeout;

use restate_core::TaskCenterBuilder;
use restate_core::io::Gate;
use restate_platform::sync::Mutex;
use restate_types::protobuf::common::DatabaseKind;
use restate_util_lifecycle::{KeepAlive, Scope, ScopePayload};

use super::{ColumnFamilies, PinnedBatch, ReadIterator, ReadSnapshot, ScopedReads};
use crate::configuration::{
    CfConfigurator, DbConfigurator, create_default_cf_options, create_default_db_options,
};
use crate::{
    CfHandles, CfName, CfPrefixPattern, DbEnv, DbName, DbSpec, DbSpecBuilder, GlobalExecutor,
    RocksAccess, RocksDb, RocksDbManager, RocksError,
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
        crate::SinglePoolExecutor::new("test-high", 1),
        crate::SinglePoolExecutor::new("test-low", 1),
    );
    let native = RocksAccess::open_db(spec(path), DbEnv::default())?;
    Ok(Arc::new(RocksDb::new(native, executor)))
}

#[derive(Clone, Copy, enum_map::Enum)]
enum Cf {
    Data,
    Other,
}

/// What a test scope saw when its payload was dropped.
#[derive(Debug, PartialEq)]
struct PayloadDrop {
    /// How many native snapshots were still open in the database.
    native_snapshots: Option<u64>,
    /// Whether the watched gate had no leases out, i.e. whether the snapshot's
    /// lease had already been released. `None` if no gate was watched.
    gate_idle: Option<bool>,
}

/// The payload of a test scope. Like a partition, it looks up its column
/// families once, when it is created. It also records what it sees when it is
/// dropped.
struct TestCfs {
    // The gate whose leases to check when the payload is dropped, if a test
    // chose one with `watch`.
    watched_gate: OnceLock<Arc<Gate>>,
    dropped: Arc<Mutex<Option<PayloadDrop>>>,
    // If set, `handles()` panics. Taking a snapshot calls it once, after it has
    // taken the lease, so this simulates a panic halfway through.
    fail_capture: bool,
    handles: CfHandles<Cf>,
}

impl Drop for TestCfs {
    fn drop(&mut self) {
        let native_snapshots = self
            .handles
            .database()
            .inner()
            .as_raw_db()
            .property_int_value(rocksdb::properties::NUM_SNAPSHOTS)
            .ok()
            .flatten();
        *self.dropped.lock() = Some(PayloadDrop {
            native_snapshots,
            gate_idle: self.watched_gate.get().map(|gate| gate.is_idle()),
        });
    }
}

// SAFETY: `handles` is a field that is never replaced.
unsafe impl ColumnFamilies for TestCfs {
    type Cf = Cf;

    fn handles(&self) -> &CfHandles<Cf> {
        assert!(!self.fail_capture, "injected capture failure");
        &self.handles
    }
}

impl ScopePayload for TestCfs {
    type Parent = RocksDb;

    fn parent(&self) -> &KeepAlive<RocksDb> {
        self.handles.database()
    }
}

/// Creates a scope inside `db` with the "data" and "other" column families.
fn test_scope(db: &Arc<RocksDb>, fail_capture: bool) -> Scope<TestCfs> {
    let handles = CfHandles::new(db, ["data", "other"], |cf: Cf| cf as usize).unwrap();
    Scope::new(
        "test",
        TestCfs {
            watched_gate: OnceLock::new(),
            dropped: Arc::default(),
            fail_capture,
            handles,
        },
    )
}

/// Makes `scope` record whether `gate` had leases out when its payload is dropped.
fn watch(scope: &Scope<TestCfs>, gate: &Arc<Gate>) {
    scope.payload().watched_gate.set(gate.clone()).unwrap();
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
fn entries<R>(iterator: &mut ReadIterator<'_, R>) -> Result<Vec<Entry>, RocksError> {
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
    let raw = db.inner().as_raw_db();
    let data = db.inner().cf_handle("data").unwrap();
    let other = db.inner().cf_handle("other").unwrap();
    raw.put_cf(&data, b"a", b"1")?;
    raw.put_cf(&data, b"b", b"")?;
    raw.put_cf(&data, b"c", b"3")?;
    raw.put_cf(&other, b"a", b"other")?;

    let scope = test_scope(&db, false);
    let gate = scope.gate().clone();
    watch(&scope, &gate);
    let dropped = Arc::clone(&scope.payload().dropped);
    let snapshot = ReadSnapshot::new(&scope, &gate)?;
    drop(scope);
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

    // After sealing the scope, no new snapshot of it can be taken, but reads
    // through the existing one keep working.
    gate.seal();
    assert!(ReadSnapshot::new(snapshot.scope(), &gate).is_err());
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
    // released before the scope, and the lease after it, so the gate now has no
    // leases out.
    assert!(!is_pending(closed.as_mut()));
    let released = dropped.lock().take();
    assert_eq!(
        released,
        Some(PayloadDrop {
            native_snapshots: Some(0),
            gate_idle: Some(false),
        })
    );
    assert!(!is_pending(gate.drain()));
    Ok(())
}

#[test]
fn latest_reads_see_new_writes_and_keep_the_database_open_without_a_lease() -> anyhow::Result<()> {
    let tempdir = restate_test_util::tempdir()?;
    let db = standalone(&tempdir.path().join("db"))?;
    let raw = db.inner().as_raw_db();
    let data = db.inner().cf_handle("data").unwrap();
    raw.put_cf(&data, b"a", b"1")?;
    raw.put_cf(&data, b"b", b"2")?;

    // Reads without a snapshot take no lease, so the gate has nothing out, and
    // they keep working after the gate is sealed.
    let scope = test_scope(&db, false);
    watch(&scope, scope.gate());
    let dropped = Arc::clone(&scope.payload().dropped);
    assert!(scope.gate().is_idle());
    scope.gate().seal();

    // Point lookups and multi-gets see the latest data.
    let reader = scope.reader(Cf::Data, ReadOptions::default());
    assert_eq!(reader.get(b"a")?, some("1"));
    raw.put_cf(&data, b"a", b"changed")?;
    assert_eq!(reader.get(b"a")?, some("changed"));
    let batch = reader.multi_get_pinned([b"a", b"b", b"c"], true)?;
    assert_eq!(values(&batch)?, [some("changed"), some("2"), None]);

    // An iterator sees the data as it was when the iterator was created, even when
    // asked to tail, both when moving forward and when seeking again. A new
    // iterator sees the later write.
    let tailing = || {
        let mut options = ReadOptions::default();
        options.set_tailing(true);
        options
    };
    let mut borrowed = scope.iterator(Cf::Data, tailing());
    let mut owned = scope.owned_iterator(Cf::Data, tailing());
    borrowed.seek(b"");
    owned.seek(b"");
    raw.put_cf(&data, b"aa", b"later")?;
    borrowed.next();
    owned.next();
    assert_eq!(borrowed.item()?, Some((&b"b"[..], &b"2"[..])));
    assert_eq!(owned.item()?, Some((&b"b"[..], &b"2"[..])));
    let created = [kv("a", "changed"), kv("b", "2")];
    assert_eq!(entries(&mut borrowed)?, created);
    let mut fresh = scope.iterator(Cf::Data, ReadOptions::default());
    assert_eq!(
        entries(&mut fresh)?,
        [kv("a", "changed"), kv("aa", "later"), kv("b", "2")]
    );

    // Owned values keep the database open by themselves. Drop every other handle,
    // including the test's own, and use them on another thread.
    let owned_reader = scope.owned_reader(Cf::Data, ReadOptions::default());
    let owned_batch = owned_reader.owned_multi_get_pinned([b"b"], false)?;
    let mut closed = pin!(db.closed());
    drop(fresh);
    drop(batch);
    drop(reader);
    drop(borrowed);
    drop(scope);
    drop(data);
    drop(db);
    assert!(is_pending(closed.as_mut()));
    let read = std::thread::spawn(move || -> Result<_, RocksError> {
        let value = owned_reader.get(b"aa")?;
        drop(owned_reader);
        Ok((value, values(&owned_batch)?, entries(&mut owned)?))
    });
    let (value, batch_values, owned_entries) = read.join().unwrap()?;
    assert_eq!(value, some("later"));
    assert_eq!(batch_values, [some("2")]);
    assert_eq!(owned_entries, created);

    // Dropping the last owned value closed the database. No lease was held at any
    // point, and there was no native snapshot to release.
    assert!(!is_pending(closed.as_mut()));
    let released = dropped.lock().take();
    assert_eq!(
        released,
        Some(PayloadDrop {
            native_snapshots: Some(0),
            gate_idle: Some(true),
        })
    );
    Ok(())
}

#[test]
fn read_errors_are_reported_per_key_and_by_iterators() -> anyhow::Result<()> {
    let tempdir = restate_test_util::tempdir()?;
    let db = standalone(&tempdir.path().join("db"))?;
    {
        let raw = db.inner().as_raw_db();
        let data = db.inner().cf_handle("data").unwrap();
        raw.put_cf(&data, b"flushed", b"1")?;
        raw.flush_cf(&data)?;
        raw.put_cf(&data, b"memtable-1", b"1")?;
        raw.put_cf(&data, b"memtable-2", b"2")?;
    }
    let scope = test_scope(&db, false);
    let snapshot = ReadSnapshot::new(&scope, scope.gate())?;
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

    // If taking a snapshot panics after the lease was taken, the lease is still
    // released.
    let failing = test_scope(&db, true);
    watch(&failing, failing.gate());
    let dropped = Arc::clone(&failing.payload().dropped);
    assert!(
        catch_unwind(AssertUnwindSafe(|| ReadSnapshot::new(
            &failing,
            failing.gate()
        )))
        .is_err()
    );
    assert!(failing.gate().is_idle());
    drop(failing);
    assert_eq!(
        dropped.lock().take(),
        Some(PayloadDrop {
            native_snapshots: Some(0),
            gate_idle: Some(true),
        })
    );

    // A snapshot can be counted in a gate below its scope's gate, like a
    // partition's session, but not in a gate outside of it.
    let scope = test_scope(&db, false);
    let gate = scope.gate().child();
    watch(&scope, &gate);
    let dropped = Arc::clone(&scope.payload().dropped);
    let sibling = test_scope(&db, false);
    if cfg!(debug_assertions) {
        assert!(
            catch_unwind(AssertUnwindSafe(|| ReadSnapshot::new(
                &scope,
                sibling.gate()
            )))
            .is_err()
        );
    }

    // An owned iterator keeps the snapshot (and its lease) alive after the
    // snapshot handle itself is dropped.
    let snapshot = ReadSnapshot::new(&scope, &gate)?;
    drop(scope);
    let mut iterator = snapshot.owned_iterator(Cf::Data, ReadOptions::default());
    drop(snapshot);

    // Once sealed, the gate refuses new snapshots, and draining it waits for the
    // iterator. Another scope of the same database drains independently.
    gate.seal();
    assert!(ReadSnapshot::new(iterator.source().scope(), &gate).is_err());
    assert!(is_pending(gate.drain()));
    sibling.gate().seal();
    timeout(DEADLINE, sibling.gate().drain()).await?;

    // Draining the whole database also waits for the iterator. The iterator
    // holds the last handle to its scope.
    let mut drained = Box::pin(db.drain());
    assert!(is_pending(drained.as_mut()));
    iterator.seek(b"");
    assert_eq!(iterator.item()?, None);
    assert!(dropped.lock().is_none());

    // Dropping the iterator releases things in order: first the native snapshot,
    // then the scope, and only then the lease, which lets the drain finish.
    drop(iterator);
    let released = dropped.lock().take();
    assert_eq!(
        released,
        Some(PayloadDrop {
            native_snapshots: Some(0),
            gate_idle: Some(false),
        })
    );
    timeout(DEADLINE, drained).await?;
    let late = test_scope(&db, false);
    assert!(ReadSnapshot::new(&late, late.gate()).is_err());

    drop((late, sibling, db));
    task_center.shutdown_node("test completed", 0).await;
    Ok(())
}
