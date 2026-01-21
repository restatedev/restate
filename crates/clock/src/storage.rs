// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A trait for a storage type that can be used to store the HLC clock state.
pub trait HlcClockStorage: super::private::Sealed {
    fn load(&self) -> u64;
    fn store(&self, value: u64);
    fn compare_exchange_weak(&self, current: u64, new: u64) -> Result<(), u64>;
}

impl<T: super::private::Sealed> super::private::Sealed for Arc<T> {}
impl<T: super::private::Sealed> super::private::Sealed for Rc<T> {}

impl<T: HlcClockStorage> HlcClockStorage for Arc<T> {
    #[inline]
    fn load(&self) -> u64 {
        T::load(self)
    }

    #[inline]
    fn store(&self, value: u64) {
        T::store(self, value)
    }

    #[inline]
    fn compare_exchange_weak(&self, current: u64, new: u64) -> Result<(), u64> {
        T::compare_exchange_weak(self, current, new)
    }
}

impl<T: HlcClockStorage> HlcClockStorage for Rc<T> {
    #[inline]
    fn load(&self) -> u64 {
        T::load(self)
    }

    #[inline]
    fn store(&self, value: u64) {
        T::store(self, value)
    }

    #[inline]
    fn compare_exchange_weak(&self, current: u64, new: u64) -> Result<(), u64> {
        T::compare_exchange_weak(self, current, new)
    }
}

#[repr(transparent)]
#[derive(Default)]
pub struct LocalStorage(Cell<u64>);
impl super::private::Sealed for LocalStorage {}

#[repr(transparent)]
#[derive(Default)]
pub struct AtomicStorage(AtomicU64);
impl super::private::Sealed for AtomicStorage {}

impl HlcClockStorage for AtomicStorage {
    #[inline]
    fn load(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[inline]
    fn store(&self, value: u64) {
        self.0.store(value, std::sync::atomic::Ordering::Relaxed)
    }

    #[inline]
    fn compare_exchange_weak(&self, current: u64, new: u64) -> Result<(), u64> {
        self.0
            .compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed)
            .map(|_| ())
    }
}

impl HlcClockStorage for LocalStorage {
    #[inline]
    fn load(&self) -> u64 {
        self.0.get()
    }

    #[inline]
    fn store(&self, value: u64) {
        self.0.set(value)
    }

    #[inline]
    fn compare_exchange_weak(&self, _current: u64, new: u64) -> Result<(), u64> {
        self.0.set(new);
        Ok(())
    }
}
