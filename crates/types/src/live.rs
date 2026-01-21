// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use dyn_clone::DynClone;
use serde::Serialize;

/// A trait to load a live value. It allows to hide closure types that originate from projections
/// into a live value.
pub trait LiveLoad: DynClone + Send + Sync {
    /// Type to live load
    type Live;

    /// Loads the current value of self.
    fn live_load(&mut self) -> &Self::Live;
}

pub type BoxLiveLoad<T> = Box<dyn LiveLoad<Live = T>>;

// Implements Clone over BoxLiveLoad.
dyn_clone::clone_trait_object!(<T> LiveLoad<Live = T>);

impl<T> LiveLoad for BoxLiveLoad<T>
where
    T: 'static,
{
    type Live = T;

    fn live_load(&mut self) -> &Self::Live {
        self.as_mut().live_load()
    }
}

/// Extension trait for the [`LiveLoad`] trait.
pub trait LiveLoadExt: LiveLoad {
    /// Projects into the live loadable value.
    fn map<F, T>(self, projection: F) -> MapLiveLoad<Self, F>
    where
        F: FnMut(&Self::Live) -> &T,
        Self: Sized,
    {
        MapLiveLoad {
            inner: self,
            projection,
        }
    }

    /// Boxes the live loadable value. This can be useful to hide projection closure types if one
    /// needs to own the value.
    fn boxed(self) -> BoxLiveLoad<Self::Live>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

impl<T> LiveLoadExt for T where T: LiveLoad {}

/// A live view into a shared object
///
/// This is optimized for high read contention scenarios, where the object is updated infrequently.
#[derive(Clone)]
pub struct Live<T> {
    inner: arc_swap::cache::Cache<Arc<ArcSwap<T>>, Arc<T>>,
}

impl<T> LiveLoad for Live<T>
where
    T: Send + Sync + Clone + 'static,
{
    type Live = T;

    fn live_load(&mut self) -> &T {
        self.live_load()
    }
}

impl<T> Live<T> {
    pub fn from_value(value: T) -> Self {
        Self::from(Arc::new(ArcSwap::from_pointee(value)))
    }

    /// Potentially fast access to a snapshot, should be used if using live_load()
    /// isn't possible (requires mutability to call load()).
    ///
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

    /// Instead of loading the live value on every request from the shared storage, this keeps
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
}

impl<T> From<Arc<ArcSwap<T>>> for Live<T> {
    fn from(owner: Arc<ArcSwap<T>>) -> Self {
        let inner = arc_swap::Cache::new(owner);
        Self { inner }
    }
}

#[derive(Debug)]
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

/// Make it possible to create a LiveLoad of a fixed arc value.
#[derive(Clone)]
pub struct Constant<T>(Arc<T>);

impl<T> Constant<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }

    pub fn live_load(&mut self) -> &T {
        &self.0
    }
}

impl<T: Send + Sync + Clone + 'static> LiveLoad for Constant<T> {
    type Live = T;

    fn live_load(&mut self) -> &T {
        self.live_load()
    }
}

/// Projection into a live load value.
pub struct MapLiveLoad<I, F> {
    inner: I,
    projection: F,
}

impl<I, F, T> LiveLoad for MapLiveLoad<I, F>
where
    I: LiveLoad,
    F: FnMut(&I::Live) -> &T + Send + Sync + Clone,
{
    type Live = T;

    fn live_load(&mut self) -> &T {
        (self.projection)(self.inner.live_load())
    }
}

impl<I, F> Clone for MapLiveLoad<I, F>
where
    I: LiveLoad,
    F: Clone,
{
    fn clone(&self) -> Self {
        MapLiveLoad {
            inner: dyn_clone::clone(&self.inner),
            projection: self.projection.clone(),
        }
    }
}
