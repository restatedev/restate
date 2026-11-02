// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`CachePadded`] keeps a value on cache lines of its own, so that threads writing neighbouring
//! data do not take the line from each other (false sharing).
//!
//! The alignment is the target's cache-line size, or the pair of lines its hardware prefetches
//! together, following crossbeam-utils' `CachePadded`:
//!
//! | Targets | Alignment | Why |
//! | --- | --- | --- |
//! | x86-64 | 128 | 64-byte lines, but Intel's spatial prefetcher, since Sandy Bridge, fetches them in 128-byte-aligned pairs |
//! | aarch64, arm64ec, powerpc64 | 128 | 128-byte lines (the big cores of big.LITTLE designs, Apple silicon included) |
//! | s390x | 256 | 256-byte lines |
//! | arm, mips, mips64, sparc, hexagon | 32 | 32-byte lines |
//! | m68k | 16 | 16-byte lines |
//! | all others (x86, wasm, riscv, …) | 64 | 64-byte lines |
//!
//! ```
//! use std::sync::atomic::{AtomicU64, Ordering};
//!
//! use restate_util_cache_padded::CachePadded;
//!
//! /// One thread bumps `sent`, another `received`: each on lines of its own.
//! struct Counters {
//!     sent: CachePadded<AtomicU64>,
//!     received: CachePadded<AtomicU64>,
//! }
//!
//! let counters = Counters {
//!     sent: CachePadded::new(AtomicU64::new(0)),
//!     received: CachePadded::new(AtomicU64::new(0)),
//! };
//! counters.sent.fetch_add(1, Ordering::Relaxed);
//! let sent = &counters.sent as *const _ as usize;
//! let received = &counters.received as *const _ as usize;
//! assert!(received.abs_diff(sent) >= CachePadded::<AtomicU64>::ALIGN);
//! ```

#![no_std]

use core::fmt;
use core::ops::{Deref, DerefMut};

/// `T`, aligned and padded to [`Self::ALIGN`] bytes: see the crate docs.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
#[cfg_attr(
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
    ),
    repr(align(128))
)]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    any(
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6",
        target_arch = "sparc",
        target_arch = "hexagon",
    ),
    repr(align(32))
)]
#[cfg_attr(target_arch = "m68k", repr(align(16)))]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
        target_arch = "s390x",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6",
        target_arch = "sparc",
        target_arch = "hexagon",
        target_arch = "m68k",
    )),
    repr(align(64))
)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    /// The alignment, and so the least size, of every `CachePadded` on this target.
    pub const ALIGN: usize = align_of::<Self>();

    /// Pads `value`.
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// The value, unpadded.
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> From<T> for CachePadded<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

// Every padded value fills whole lines of its own, on every target: checked as the crate builds.
const _: () = {
    assert!(CachePadded::<u8>::ALIGN >= 16);
    assert!(size_of::<CachePadded<u8>>() == CachePadded::<u8>::ALIGN);
    assert!(size_of::<CachePadded<[u8; 129]>>().is_multiple_of(CachePadded::<u8>::ALIGN));
};
#[cfg(target_arch = "x86_64")]
const _: () = assert!(CachePadded::<u8>::ALIGN == 128);

impl<T: fmt::Debug> fmt::Debug for CachePadded<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachePadded")
            .field("value", &self.value)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn padded_values_own_their_lines_and_read_through() {
        // Two padded values side by side start on their own aligned lines, never sharing one
        // (the sizes are checked at compile time); the value reads and writes through.
        let mut pair = [CachePadded::new(1u64), CachePadded::new(2u64)];
        let (first, second) = (&pair[0] as *const _ as usize, &pair[1] as *const _ as usize);
        assert_eq!(first % CachePadded::<u64>::ALIGN, 0);
        assert!(second - first >= CachePadded::<u64>::ALIGN);
        *pair[1] += 40;
        assert_eq!((*pair[0], pair[1].into_inner()), (1, 42));
    }
}
