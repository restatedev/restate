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

use arc_swap::ArcSwap;
use serde::Serialize;

/// A trait to use in cases where a projection is applied on an updateable since it's impossible to
/// spell out the closure type in Updateable<T, ...>.
pub trait LiveLoad<T> {
    /// Instead of loading the updateable on every request from the shared storage, this keeps
    /// another copy inside itself. Upon request it only cheaply revalidates it is up to
    /// date. If it is, access is significantly faster. If it is stale, a full load is performed and the
    /// cache value is replaced. Under a read-heavy loads, the measured speedup are 10-25 times,
    /// depending on the architecture.
    fn live_load(&mut self) -> &T;
}

/// A live view into a shared object
///
/// This is optimized for high read contention scenarios, where the object is updated infrequently.
/// Use () for F when not projecting into the updateable.
///
/// Note that projections cannot be nested. A projected updateable will not provide access to `map()`
#[derive(Clone)]
pub struct Live<T, F = ()> {
    inner: arc_swap::cache::Cache<Arc<ArcSwap<T>>, Arc<T>>,
    projection: F,
}

impl<U, T: 'static, F> LiveLoad<U> for Live<T, F>
where
    F: FnMut(&T) -> &U + 'static,
{
    fn live_load(&mut self) -> &U {
        self.live_load()
    }
}

impl<T> LiveLoad<T> for Live<T, ()> {
    fn live_load(&mut self) -> &T {
        self.live_load()
    }
}

impl<T: 'static> Live<T, ()> {
    pub fn boxed(self) -> Box<dyn LiveLoad<T>> {
        Box::new(self)
    }
}

impl<T> Live<T, ()> {
    pub fn from_value(value: T) -> Self {
        Self::from(Arc::new(ArcSwap::from_pointee(value)))
    }
    /// Potentially fast access to a snapshot, should be used if using [[Updateable]]
    /// isn't possible (requires mutablility to call load()).
    /// Pinned doesn't track updates.
    ///
    /// There’s only limited number of “fast” slots for borrowing from the underlying ArcSwap
    /// for each single thread (currently 8, but this might change). If these run out, the
    /// algorithm falls back to slower path (fallback to `snapshot()`).
    ///
    /// If too many Guards are kept around, the performance might be poor. These are not intended
    /// to be stored in data structures or used across async yield points.
    pub fn pinned(&self) -> Pinned<T> {
        Pinned::new(self.inner.arc_swap())
    }

    /// Instead of loading the updateable on every request from the shared storage, this keeps
    /// another copy inside itself. Upon request it only cheaply revalidates it is up to
    /// date. If it is, access is significantly faster. If it is stale, a full load is performed and the
    /// cache value is replaced. Under a read-heavy loads, the measured speedup are 10-25 times,
    /// depending on the architecture.
    pub fn live_load(&mut self) -> &T {
        arc_swap::cache::Access::load(&mut self.inner)
    }

    /// Get the latest snapshot of the loaded value, once snapshot is acquired,
    /// access is fast, but acquiring the snapshot is expensive (roughly Atomic + Mutex).
    /// Roughly 10x slower under heavy read contention scenarios than pinned()
    ///
    /// Use this if when intending to hold the configuration object for long-ish
    /// periods, e.g., across async yield points.
    pub fn snapshot(&self) -> Arc<T> {
        self.inner.arc_swap().load_full()
    }

    /// Creates an updateable projection into the original updateable.
    pub fn map<F, U>(self, f: F) -> Live<T, F>
    where
        F: FnMut(&T) -> &U,
    {
        Live {
            inner: self.inner,
            projection: f,
        }
    }
}

impl<T> From<Arc<ArcSwap<T>>> for Live<T, ()> {
    fn from(owner: Arc<ArcSwap<T>>) -> Self {
        let inner = arc_swap::Cache::new(owner);
        Self {
            inner,
            projection: (),
        }
    }
}

impl<T, F, U> Live<T, F>
where
    F: FnMut(&T) -> &U,
{
    /// Instead of loading the updateable on every request from the shared storage, this keeps
    /// another copy inside itself. Upon request it only cheaply revalidates it is up to
    /// date. If it is, access is significantly faster. If it is stale, a full load is performed and the
    /// cache value is replaced. Under a read-heavy loads, the measured speedup are 10-25 times,
    /// depending on the architecture.
    pub fn live_load(&mut self) -> &U {
        let loaded = arc_swap::cache::Access::load(&mut self.inner);
        (self.projection)(loaded)
    }
}

impl<T: 'static, F, U> Live<T, F>
where
    F: FnMut(&T) -> &U + 'static,
{
    // Can be used for type-erasing a projection
    pub fn boxed(self) -> Box<dyn LiveLoad<U>> {
        Box::new(self)
    }
}

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

/// Make it possible to create a Updateable of a fixed arc value.
#[derive(Clone)]
pub struct Constant<T>(Arc<T>);

impl<T> Constant<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> LiveLoad<T> for Constant<T> {
    fn live_load(&mut self) -> &T {
        &self.0
    }
}
