// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reads from the column families of a [`Scope`], such as an open partition.
//!
//! # Setting up
//!
//! Reads go through a scope whose payload holds the column families to read,
//! in a [`CfHandles`]. The payload implements [`ColumnFamilies`] to hand them
//! out, and chooses a key type ([`ColumnFamilies::Cf`]) that says which column
//! family a reader or iterator reads:
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use restate_rocksdb::reads::{ColumnFamilies, ReadSnapshot, ScopedReads};
//! use restate_rocksdb::{CfHandles, RocksDb};
//! use restate_util_lifecycle::{KeepAlive, Scope, ScopePayload};
//!
//! // Chooses one of the table's column families.
//! #[derive(Clone, Copy, enum_map::Enum)]
//! enum Cf {
//!     Rows,
//!     Index,
//! }
//!
//! // The payload of a table's scope.
//! struct Table {
//!     cfs: CfHandles<Cf>,
//! }
//!
//! impl ScopePayload for Table {
//!     type Parent = RocksDb;
//!
//!     fn parent(&self) -> &KeepAlive<RocksDb> {
//!         self.cfs.database()
//!     }
//! }
//!
//! // SAFETY: `cfs` is a field that is never replaced.
//! unsafe impl ColumnFamilies for Table {
//!     type Cf = Cf;
//!
//!     fn handles(&self) -> &CfHandles<Cf> {
//!         &self.cfs
//!     }
//! }
//!
//! fn read(db: &Arc<RocksDb>) -> anyhow::Result<()> {
//!     let cfs = CfHandles::new(db, ["rows", "index"], |cf: Cf| cf as usize)?;
//!     let table = Scope::new("orders", Table { cfs });
//!
//!     // Every read through a snapshot sees the table as it was at this moment.
//!     let snapshot = ReadSnapshot::new(&table, table.gate())?;
//!     let order = snapshot.reader(Cf::Rows, Default::default()).get(b"order-1")?;
//!
//!     let mut rows = snapshot.iterator(Cf::Rows, Default::default());
//!     rows.seek(b"order-");
//!     while let Some((key, value)) = rows.item()? {
//!         // ... use `key` and `value` ...
//!         rows.next();
//!     }
//!
//!     // An owned iterator holds its own clone of the snapshot. A query cursor
//!     // can keep it between batches, and move it to another thread.
//!     let mut cursor = snapshot.owned_iterator(Cf::Rows, Default::default());
//!     std::thread::spawn(move || {
//!         cursor.seek(b"order-");
//!         // ...
//!     });
//!
//!     // Without a snapshot, a read sees the latest data.
//!     let latest = table.reader(Cf::Index, Default::default()).get(b"by-date")?;
//!     Ok(())
//! }
//! ```
//!
//! # Two ways to read
//!
//! |                                  | [`ReadSnapshot`]                         | [`ScopedReads`]                                                                       |
//! |----------------------------------|------------------------------------------|---------------------------------------------------------------------------------------|
//! | What a read sees                 | The data as it was when the snapshot was taken | The latest data. A lookup sees it as it is at that moment, an iterator as it was when the iterator was created |
//! | Reads are consistent with each other | Yes                                  | No                                                                                    |
//! | Holds a lease                    | Yes, from the gate it was given          | No                                                                                    |
//! | Closing that gate waits for it   | Yes                                      | No                                                                                    |
//!
//! Both read all of the scope's column families: each reader or iterator reads
//! one of them. Both offer the same readers, iterators, and multi-get batches.
//!
//! Reads block the calling thread. This module doesn't schedule or cancel
//! reads; that is up to the caller. Work that reads on a background executor
//! should hold its own lease, like other background work.
//!
//! # Borrowed and owned
//!
//! Readers, iterators, and multi-get batches come in two versions:
//!
//! | Version  | Example               | What it holds                         | Use it for                                    |
//! |----------|-----------------------|---------------------------------------|-----------------------------------------------|
//! | Borrowed | [`ReadIterator<'_>`]  | A reference to its snapshot or scope  | Reads that are done before the snapshot or scope is dropped |
//! | Owned    | [`OwnedReadIterator`] | Its own clone of its snapshot or scope | Cursors kept across pauses or moved between threads |
//!
//! # What keeps what alive
//!
//! Each value keeps the next one alive, down to the database:
//!
//! ```text
//! owned reader, iterator, or batch
//!  └─ ReadSnapshot ── a lease on a gate: closing that gate waits for it
//!      └─ the scope, e.g. Scope<Table>
//!          └─ its payload, e.g. Table
//!              └─ CfHandles
//!                  └─ the database
//! ```
//!
//! An owned value from [`ScopedReads`] skips the snapshot: it holds the scope
//! directly, and no lease.
//!
//! So, for example, a partition can't be closed, and its column families can't
//! be deleted, while a query still holds a cursor over one of its snapshots.
//!
//! # The compiler checks lifetimes
//!
//! The compiler stops borrowed values from outliving what they came from:
//!
//! ```compile_fail,E0597
//! # use restate_rocksdb::reads::{ColumnFamilies, ReadSnapshot};
//! # use restate_util_lifecycle::Scope;
//! fn pinned_value<C: ColumnFamilies>(scope: Scope<C>, cf: C::Cf) {
//!     let value = {
//!         let snapshot = ReadSnapshot::new(&scope, scope.gate()).unwrap();
//!         let reader = snapshot.reader(cf, Default::default());
//!         reader.get_pinned(b"key").unwrap()
//!     };
//!     drop(value);
//! }
//! ```
//!
//! ```compile_fail,E0597
//! # use restate_rocksdb::reads::{ColumnFamilies, ReadSnapshot};
//! # use restate_util_lifecycle::Scope;
//! fn batch<C: ColumnFamilies>(scope: Scope<C>, cf: C::Cf) {
//!     let snapshot = ReadSnapshot::new(&scope, scope.gate()).unwrap();
//!     let batch = {
//!         let reader = snapshot.reader(cf, Default::default());
//!         reader.multi_get_pinned([b"key"], false).unwrap()
//!     };
//!     drop(batch);
//! }
//! ```
//!
//! ```compile_fail,E0597
//! # use restate_rocksdb::reads::{ColumnFamilies, ReadSnapshot};
//! # use restate_util_lifecycle::Scope;
//! fn iterator<C: ColumnFamilies>(scope: Scope<C>, cf: C::Cf) {
//!     let iterator = {
//!         let snapshot = ReadSnapshot::new(&scope, scope.gate()).unwrap();
//!         snapshot.iterator(cf, Default::default())
//!     };
//!     drop(iterator);
//! }
//! ```
//!
//! ```compile_fail,E0597
//! # use restate_rocksdb::reads::{ColumnFamilies, ScopedReads};
//! # use restate_util_lifecycle::Scope;
//! fn latest_iterator<C: ColumnFamilies>(scope: Scope<C>, cf: C::Cf) {
//!     let iterator = {
//!         let scope = scope;
//!         scope.iterator(cf, Default::default())
//!     };
//!     drop(iterator);
//! }
//! ```
//!
//! Data read through an owned iterator, reader, or batch borrows that value, so
//! it can't outlive it either, even while the snapshot itself is still alive:
//!
//! ```compile_fail,E0505
//! # use restate_rocksdb::reads::{ColumnFamilies, ReadSnapshot};
//! fn owned_item<C: ColumnFamilies>(
//!     snapshot: &ReadSnapshot<C>,
//!     cf: C::Cf,
//! ) -> Vec<u8> {
//!     let mut iterator = snapshot.owned_iterator(cf, Default::default());
//!     iterator.seek(b"");
//!     let key = iterator.item().unwrap().unwrap().0;
//!     drop(iterator);
//!     key.to_vec()
//! }
//! ```
//!
//! ```compile_fail,E0505
//! # use restate_rocksdb::reads::{ColumnFamilies, ReadSnapshot};
//! fn owned_reader_value<C: ColumnFamilies>(
//!     snapshot: &ReadSnapshot<C>,
//!     cf: C::Cf,
//! ) -> Vec<u8> {
//!     let reader = snapshot.owned_reader(cf, Default::default());
//!     let value = reader.get_pinned(b"key").unwrap().unwrap();
//!     drop(reader);
//!     value.to_vec()
//! }
//! ```
//!
//! ```compile_fail,E0505
//! # use restate_rocksdb::reads::{ColumnFamilies, ReadSnapshot};
//! fn owned_batch_value<C: ColumnFamilies>(
//!     snapshot: &ReadSnapshot<C>,
//!     cf: C::Cf,
//! ) -> Vec<u8> {
//!     let reader = snapshot.owned_reader(cf, Default::default());
//!     let batch = reader.owned_multi_get_pinned([b"key"], false).unwrap();
//!     let value = batch.get(0).unwrap().unwrap();
//!     drop(batch);
//!     value.to_vec()
//! }
//! ```

mod iterator;
mod reader;
mod scoped;
mod snapshot;
#[cfg(test)]
mod tests;

use enum_map::Enum;
use rocksdb::ReadOptions;

use restate_util_lifecycle::{Scope, ScopePayload};

use crate::CfHandles;

pub use self::iterator::{OwnedReadIterator, ReadIterator};
pub use self::reader::{OwnedPinnedBatch, OwnedReader, PinnedBatch, Reader};
pub use self::scoped::ScopedReads;
pub use self::snapshot::ReadSnapshot;

/// The payload of a [`Scope`] that reads go through: it holds the column
/// families to read. For the partition store, that's a partition's physical
/// scope. The [module documentation](self) shows how to implement it.
///
/// # Safety
///
/// Snapshots, readers, and iterators keep references into the handles for as
/// long as they keep the scope alive, which the compiler doesn't check.
/// [`Self::handles`] must therefore always return the same handles, which live
/// as long as `self` and are never replaced. Returning a field of `self` does
/// that.
pub unsafe trait ColumnFamilies: ScopePayload {
    /// Chooses which of the column families a reader or iterator reads.
    ///
    /// A snapshot always covers all of the column families. This type only says
    /// which one a particular reader or iterator uses, because RocksDB reads from
    /// one column family at a time.
    ///
    /// The implementer decides what the key means. A partition, for example,
    /// uses the key kind: each key kind is stored in exactly one column family,
    /// so the key kind tells the partition which column family holds those keys.
    /// This doesn't limit a reader to keys of that kind. It can read any key in
    /// that column family, which may also hold keys of other kinds.
    type Cf: Enum + Copy;

    /// The column families to read, and the database they belong to.
    fn handles(&self) -> &CfHandles<Self::Cf>;
}

/// What an owned reader, iterator, or batch keeps alive: a [`ReadSnapshot`], or
/// the [`Scope`] it reads the latest data of. No other type can implement this.
///
/// Both are shared handles: moving one doesn't move the database, column
/// families, or snapshot it points to. Owned readers and iterators rely on that.
pub trait ReadSource: Clone + sealed::Sealed {}

mod sealed {
    pub trait Sealed {}
}

impl<C: ColumnFamilies> sealed::Sealed for ReadSnapshot<C> {}
impl<C: ColumnFamilies> ReadSource for ReadSnapshot<C> {}
impl<C: ColumnFamilies> sealed::Sealed for Scope<C> {}
impl<C: ColumnFamilies> ReadSource for Scope<C> {}

// Asynchronous reads are always requested. RocksDB turns them off by itself when
// the build or the filesystem doesn't support them.
fn reader_options(options: &mut ReadOptions) {
    options.set_async_io(true);
}

fn iterator_options(options: &mut ReadOptions) {
    // A tailing iterator ignores its snapshot, and its own creation point, and
    // reads the latest data instead.
    options.set_tailing(false);
    options.set_async_io(true);
    // Load each value when the iterator moves to it: the iterator offers no way
    // to load it later.
    options.set_allow_unprepared_value(false);
}
