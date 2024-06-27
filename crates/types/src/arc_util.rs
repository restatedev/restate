// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::sync::Arc;

use arc_swap::cache::MapCache;
use arc_swap::strategy::Strategy;
use arc_swap::{ArcSwap, ArcSwapAny, Cache, RefCnt};
use serde::Serialize;

pub struct Pinned<T> {
    guard: arc_swap::Guard<Arc<T>, arc_swap::DefaultStrategy>,
}

impl<T> Pinned<T> {
    pub fn new(swap: &ArcSwap<T>) -> Self {
        Self { guard: swap.load() }
    }
    /// Upgrade this pinned reference to a full-fledged Arc
    pub fn into_arc(self) -> Arc<T> {
        arc_swap::Guard::into_inner(self.guard)
    }
}

impl<T> Deref for Pinned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.as_ref()
    }
}

impl<T> AsRef<T> for Pinned<T> {
    fn as_ref(&self) -> &T {
        self.guard.as_ref()
    }
}

impl<T: Serialize> Serialize for Pinned<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.guard.as_ref().serialize(serializer)
    }
}

pub type Caching<T> = arc_swap::Cache<Arc<ArcSwapAny<Arc<T>>>, Arc<T>>;
/// Provides read-only access to an updateable T
#[derive(Clone, Default, derive_more::From)]
pub struct Updateable<T>(Arc<ArcSwap<T>>);

impl<T> Updateable<T> {
    pub fn into_caching(self) -> Caching<T> {
        arc_swap::Cache::new(self.0)
    }
}

impl<T: Sized + 'static> ArcSwapExt<T> for Updateable<T> {
    fn pinned(&self) -> Pinned<T> {
        self.0.pinned()
    }

    fn snapshot(&self) -> Arc<T> {
        self.0.snapshot()
    }

    fn to_caching_updateable(&self) -> impl CachingUpdateable<T> {
        arc_swap::Cache::new(self.0.clone())
    }

    fn map_as_updateable<F, U>(&self, f: F) -> impl CachingUpdateable<U>
    where
        F: FnMut(&Arc<T>) -> &U,
    {
        let cached = arc_swap::Cache::new(self.0.deref());
        cached.map(f)
    }

    fn map_as_updateable_owned<F, U>(self, f: F) -> impl CachingUpdateable<U> + Clone
    where
        F: FnMut(&Arc<T>) -> &U + Clone,
    {
        let cached = arc_swap::Cache::new(self.0);
        cached.map(f)
    }
}

/// Make it possible to create a CachingUpdateable of a fixed arc value.
#[derive(Clone)]
pub struct Constant<T>(Arc<T>);

impl<T> Constant<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> CachingUpdateable<T> for Constant<T> {
    fn load(&mut self) -> &T {
        &self.0
    }
}

pub trait CachingUpdateable<T> {
    fn load(&mut self) -> &T;
}

impl<A, T, S> CachingUpdateable<T::Target> for Cache<A, T>
where
    A: Deref<Target = ArcSwapAny<T, S>>,
    T: RefCnt + Deref<Target = <T as RefCnt>::Base>,
    S: Strategy<T>,
{
    fn load(&mut self) -> &T::Target {
        arc_swap::cache::Access::load(self)
    }
}

impl<A, T, S, F, U> CachingUpdateable<U> for MapCache<A, T, F>
where
    A: Deref<Target = ArcSwapAny<T, S>>,
    T: RefCnt,
    S: Strategy<T>,
    F: FnMut(&T) -> &U,
{
    fn load(&mut self) -> &U {
        arc_swap::cache::Access::load(self)
    }
}

pub trait ArcSwapExt<T> {
    /// Potentially fast access to a snapshot, should be used if an CachingUpdateable<T>
    /// isn't possible (CachingUpdateable trait is not object-safe, and requires mut to load()).
    /// Guard acquired doesn't track config updates. ~10x slower than CachingUpdateable's load().
    ///
    /// There’s only limited number of “fast” slots for borrowing from the underlying ArcSwap
    /// for each single thread (currently 8, but this might change). If these run out, the
    /// algorithm falls back to slower path (fallback to `snapshot()`).
    ///
    /// If too many Guards are kept around, the performance might be poor. These are not intended
    /// to be stored in data structures or used across async yield points.
    fn pinned(&self) -> Pinned<T>;

    /// The best way to access an updateable when holding a mutable CachingUpdateable is
    /// viable.
    ///
    /// ~10% slower than `snapshot()` to create (YMMV), load() is as fast as accessing local objects,
    /// and will always load the latest configuration reference. The downside is that `load()` requires
    /// exclusive reference. This should be the preferred method for accessing the updateable, but
    /// avoid using `to_caching_updateable()` or `snapshot()` in tight loops. Instead, get a new updateable,
    /// and pass it down to the loop by value for very efficient access.
    fn to_caching_updateable(&self) -> impl CachingUpdateable<T>;

    /// Get the latest snapshot of the loaded value, once snapshot is acquired,
    /// access is fast, but acquiring the snapshot is expensive (roughly Atomic + Mutex).
    /// Roughly 10x slower under heavy read contention scenarios than get_pinned()
    ///
    /// Use this if when intending to hold the configuration object for long-ish
    /// periods, e.g., across async yield points.
    fn snapshot(&self) -> Arc<T>;

    /// Returns an updateable that maps the original object to another. This can be used for
    /// updateable projections.
    fn map_as_updateable<F, U>(&self, f: F) -> impl CachingUpdateable<U>
    where
        F: FnMut(&Arc<T>) -> &U;

    fn map_as_updateable_owned<F, U>(self, f: F) -> impl CachingUpdateable<U> + Clone
    where
        F: FnMut(&Arc<T>) -> &U + Clone;
}

impl<K, T> ArcSwapExt<T> for K
where
    K: Deref<Target = ArcSwapAny<Arc<T>>> + Clone,
    T: 'static,
{
    fn pinned(&self) -> Pinned<T> {
        Pinned::new(self.deref())
    }

    fn snapshot(&self) -> Arc<T> {
        self.deref().load_full()
    }

    fn to_caching_updateable(&self) -> impl CachingUpdateable<T> {
        Cache::new(self.deref())
    }

    fn map_as_updateable<F, U>(&self, f: F) -> impl CachingUpdateable<U>
    where
        F: FnMut(&Arc<T>) -> &U,
    {
        let cached = Cache::new(self.deref());
        cached.map(f)
    }

    fn map_as_updateable_owned<F, U>(self, f: F) -> impl CachingUpdateable<U> + Clone
    where
        F: FnMut(&Arc<T>) -> &U + Clone,
    {
        let cached = Cache::new(self);
        cached.map(f)
    }
}
