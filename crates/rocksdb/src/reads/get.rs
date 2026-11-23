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

use rocksdb::{ErrorKind, ReadTier};

use restate_core::io::Gate;

use super::{ColumnFamilies, OwnedPinnedValue, ReadSnapshot, ReadSource, Reader, TOKEN};
use crate::background::{CachedOpFn, OpFn};
use crate::{Gated, Inline, OpContext, RocksError, StorageOp, StorageTaskKind};

impl<C: ColumnFamilies> ReadSnapshot<C> {
    /// Prepares a lookup of `key` in the column family `cf`, counted in `gate`,
    /// which must be inside the snapshot's database. It returns the value
    /// without copying it, see [`OwnedPinnedValue`].
    ///
    /// The lookup can start on the calling thread:
    /// [`call_cached`](StorageOp::call_cached) answers it from the memtables and
    /// the block cache if it can, and otherwise hands it back, to submit to an
    /// executor. [`call_cached_or_spawn`](StorageOp::call_cached_or_spawn) does
    /// both. The key stays borrowed on the calling thread, and is only copied
    /// if the lookup moves to a storage worker.
    ///
    /// ```ignore
    /// let value = snapshot
    ///     .prepare_get(gate, cf, key)
    ///     .call_cached_or_spawn(executor)?
    ///     .await??;
    /// ```
    ///
    /// # Panics
    /// In debug builds, panics if `gate` is outside the snapshot's database.
    pub fn prepare_get<'a, 'g>(
        &'a self,
        gate: &'g Arc<Gate>,
        cf: C::Cf,
        key: &'a [u8],
    ) -> StorageOp<Lookup<'a, Self>, Get<C::Cf>, Gated<'g>> {
        prepare(self, gate, cf, key)
    }
}

// Prepares a lookup of `key` in `source`'s column family `cf`, for the
// `prepare_get` of a snapshot and of a scope.
pub(super) fn prepare<'a, 'g, H: ReadSource>(
    source: &'a H,
    gate: &'g Arc<Gate>,
    cf: H::CfKey,
    key: &'a [u8],
) -> StorageOp<Lookup<'a, H>, Get<H::CfKey>, Gated<'g>> {
    source.handles(TOKEN).database().debug_assert_inside(gate);
    StorageOp::from_parts(
        Lookup { source, key },
        gate,
        StorageTaskKind::Get,
        Get { cf },
    )
}

/// What a lookup from [`ReadSnapshot::prepare_get`] or
/// [`ScopedReads::prepare_get`](super::ScopedReads::prepare_get) works on: the
/// snapshot or scope `H` to read through, and the key to look up.
///
/// Both stay borrowed while the lookup runs on the calling thread. A lookup
/// that moves to a storage worker takes a clone of the snapshot or scope and a
/// copy of the key with it.
pub struct Lookup<'a, H> {
    source: &'a H,
    key: &'a [u8],
}

impl<H> OpContext for Lookup<'_, H>
where
    H: ReadSource + Send + Sync + 'static,
{
    type Owned = (H, Box<[u8]>);
    type Admission = ();

    fn into_owned(self, (): ()) -> Self::Owned {
        (self.source.clone(), self.key.into())
    }
}

/// The work of an operation from [`ReadSnapshot::prepare_get`] or
/// [`ScopedReads::prepare_get`](super::ScopedReads::prepare_get).
pub struct Get<Cf> {
    cf: Cf,
}

impl<H> OpFn<Lookup<'_, H>> for Get<H::CfKey>
where
    H: ReadSource + Send + Sync + 'static,
    H::CfKey: Send + 'static,
{
    type Output = Result<Option<OwnedPinnedValue<H>>, RocksError>;

    async fn run_async(self, (source, key): (H, Box<[u8]>)) -> Self::Output {
        // A storage worker may wait for the disk.
        Reader::new(&source).get_pinned_owned(self.cf, &key)
    }
}

impl<H> CachedOpFn<Lookup<'_, H>> for Get<H::CfKey>
where
    H: ReadSource + Send + Sync + 'static,
    H::CfKey: Send + 'static,
{
    fn run_cached(self, lookup: &Lookup<'_, H>) -> Inline<Self::Output, Self> {
        let reader = Reader::new(lookup.source).with_read_tier(ReadTier::BlockCache);
        match reader.get_pinned_owned(self.cf, lookup.key) {
            // The value isn't in memory.
            Err(RocksError::Other(err)) if err.kind() == ErrorKind::Incomplete => {
                Inline::WouldBlock(self)
            }
            result => Inline::Done(result),
        }
    }
}
