// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use enum_map::{Enum, EnumMap};
use rocksdb::BoundColumnFamily;

use restate_util_lifecycle::KeepAlive;

use crate::{CfName, RocksDb, RocksError};

/// Handles to some of a database's column families, looked up once by name,
/// together with a [`KeepAlive`] that keeps the database open.
///
/// Code that uses the handles never looks up a column family by name again, so
/// it can't pick up a column family that was dropped and recreated under the
/// same name.
///
/// A key of type `K` chooses one of the column families. The owner decides
/// what the key means. A partition, for example, uses the key kind: each kind
/// of key is stored in exactly one column family, and several kinds can share
/// one.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use restate_rocksdb::{CfHandles, RocksDb, RocksError};
/// #[derive(Clone, Copy, enum_map::Enum)]
/// enum Key {
///     Order,
///     OrderByDate,
///     Customer,
/// }
///
/// # fn open(db: &Arc<RocksDb>) -> Result<(), RocksError> {
/// // Orders and their index share one column family; customers have their own.
/// let cfs = CfHandles::new(db, ["orders", "customers"], |key| match key {
///     Key::Order | Key::OrderByDate => 0,
///     Key::Customer => 1,
/// })?;
/// let orders = cfs.get(Key::OrderByDate);
/// # Ok(())
/// # }
/// ```
///
/// The handles keep the database open, through the [`KeepAlive`] returned by
/// [`Self::database`]. A scope payload that holds them can return that from
/// [`ScopePayload::parent`](restate_util_lifecycle::ScopePayload::parent).
pub struct CfHandles<K: Enum> {
    // Which of `cfs` each key chooses.
    cf_of: EnumMap<K, u8>,
    // Each column family once. The handles borrow from the database, so they
    // are declared before `database` and dropped first. They are never handed
    // out for longer than `self` is borrowed.
    cfs: Box<[Arc<BoundColumnFamily<'static>>]>,
    database: KeepAlive<RocksDb>,
}

impl<K: Enum> CfHandles<K> {
    /// Looks up the column families called `names` in `database`. `cf_of` says
    /// which of them holds each key, as an index into `names`.
    ///
    /// Fails if one of the column families doesn't exist.
    ///
    /// # Panics
    /// Panics if `cf_of` returns an index outside `names`.
    pub fn new<N: AsRef<str>>(
        database: &Arc<RocksDb>,
        names: impl IntoIterator<Item = N>,
        cf_of: impl Fn(K) -> usize,
    ) -> Result<Self, RocksError> {
        let cfs = names
            .into_iter()
            .map(|name| lookup(database, name.as_ref()))
            .collect::<Result<Box<[_]>, _>>()?;
        let cf_of = EnumMap::from_fn(|key| {
            let index = cf_of(key);
            assert!(
                index < cfs.len(),
                "column family index {index} is outside the {} names given",
                cfs.len()
            );
            u8::try_from(index).expect("at most 256 column families")
        });
        Ok(Self {
            cf_of,
            cfs,
            database: database.keep_alive(),
        })
    }

    /// The column family that `key` chooses.
    pub fn get(&self, key: K) -> &Arc<BoundColumnFamily<'_>> {
        &self.cfs[usize::from(self.cf_of[key])]
    }

    /// The database that the column families belong to.
    pub fn database(&self) -> &KeepAlive<RocksDb> {
        &self.database
    }
}

fn lookup(
    database: &Arc<RocksDb>,
    name: &str,
) -> Result<Arc<BoundColumnFamily<'static>>, RocksError> {
    let cf = database
        .inner()
        .cf_handle(name)
        .ok_or_else(|| RocksError::UnknownColumnFamily(CfName::new(name)))?;
    // SAFETY: the handle borrows from the database. `CfHandles` stores it next to
    // a keep-alive of that database and drops it first (see the field order), and
    // only hands it out for as long as the `CfHandles` is borrowed.
    Ok(unsafe { Arc::from_raw(Arc::into_raw(cf).cast()) })
}
