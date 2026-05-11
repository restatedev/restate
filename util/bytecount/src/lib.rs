// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(feature = "serde")]
mod serde;

#[cfg(feature = "schema")]
mod schema;

use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::{Add, Mul};
use std::str::FromStr;

use bytesize::ByteSize;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Copy, Hash)]
pub struct ByteCount<const CAN_BE_ZERO: bool = true>(u64);
pub type NonZeroByteCount = ByteCount<false>;

impl ByteCount<true> {
    pub const ZERO: Self = Self(0);
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn saturating_add(self, other: Self) -> ByteCount<true> {
        ByteCount(self.0.saturating_add(other.0))
    }

    pub const fn saturating_mul(self, other: u64) -> ByteCount<true> {
        ByteCount(self.0.saturating_mul(other))
    }
}

impl Default for ByteCount<true> {
    fn default() -> Self {
        ByteCount::ZERO
    }
}

impl ByteCount<false> {
    pub const fn new(value: NonZeroUsize) -> Self {
        Self(value.get() as u64)
    }

    pub const fn as_non_zero_usize(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0 as usize).expect("ByteCount is not zero")
    }

    pub const fn saturating_add(self, other: Self) -> ByteCount<false> {
        ByteCount(self.0.saturating_add(other.0))
    }

    pub const fn saturating_mul(self, other: NonZeroU64) -> ByteCount<false> {
        ByteCount(self.0.saturating_mul(other.get()))
    }

    /// Calculates the quotient of self and rhs, rounding the result towards positive infinity.
    ///
    /// # Panics
    ///
    /// Panics if `rhs` is 0.
    pub fn div_ceil(self, rhs: u64) -> ByteCount<false> {
        ByteCount(self.0.div_ceil(rhs))
    }
}

impl Mul<u64> for ByteCount<false> {
    type Output = ByteCount<true>;

    fn mul(self, rhs: u64) -> Self::Output {
        ByteCount(self.0 * rhs)
    }
}

impl<const CAN_BE_ZERO: bool> Display for ByteCount<CAN_BE_ZERO> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ByteSize(self.0).fmt(f)
    }
}

impl<const CAN_BE_ZERO: bool> ByteCount<CAN_BE_ZERO> {
    pub const MAX: Self = ByteCount(u64::MAX);

    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl Add for ByteCount<true> {
    type Output = ByteCount<true>;

    fn add(self, rhs: Self) -> Self::Output {
        ByteCount(self.0 + rhs.0)
    }
}

impl Add for ByteCount<false> {
    type Output = ByteCount<false>;

    fn add(self, rhs: Self) -> Self::Output {
        ByteCount(self.0 + rhs.0)
    }
}

impl Mul for ByteCount<true> {
    type Output = ByteCount<true>;

    fn mul(self, rhs: Self) -> Self::Output {
        ByteCount(self.0 * rhs.0)
    }
}

impl Mul for ByteCount<false> {
    type Output = ByteCount<false>;

    fn mul(self, rhs: Self) -> Self::Output {
        ByteCount(self.0 * rhs.0)
    }
}

// Comparisons between ByteCount<false> and ByteCount<true>

impl PartialEq<ByteCount<false>> for ByteCount<true> {
    fn eq(&self, other: &ByteCount<false>) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd<ByteCount<false>> for ByteCount<true> {
    fn partial_cmp(&self, other: &ByteCount<false>) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl PartialEq<ByteCount<true>> for ByteCount<false> {
    fn eq(&self, other: &ByteCount<true>) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd<ByteCount<true>> for ByteCount<false> {
    fn partial_cmp(&self, other: &ByteCount<true>) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl FromStr for ByteCount<true> {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s: ByteSize = s.parse()?;
        Ok(ByteCount(s.0))
    }
}

impl From<NonZeroUsize> for ByteCount<false> {
    fn from(value: NonZeroUsize) -> Self {
        let v: usize = value.into();
        ByteCount(v as u64)
    }
}

impl From<NonZeroU64> for ByteCount<false> {
    fn from(value: NonZeroU64) -> Self {
        ByteCount(value.into())
    }
}

impl TryFrom<u64> for ByteCount<false> {
    type Error = std::num::TryFromIntError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(Self::from(NonZeroU64::try_from(value)?))
    }
}

impl From<u64> for ByteCount<true> {
    fn from(value: u64) -> Self {
        ByteCount(value)
    }
}

impl From<u32> for ByteCount<true> {
    fn from(value: u32) -> Self {
        ByteCount(value as u64)
    }
}

impl From<usize> for ByteCount<true> {
    fn from(value: usize) -> Self {
        ByteCount(value as u64)
    }
}

impl<const CAN_BE_ZERO: bool> From<ByteCount<CAN_BE_ZERO>> for u64 {
    fn from(value: ByteCount<CAN_BE_ZERO>) -> Self {
        value.0
    }
}

impl From<ByteCount<false>> for ByteCount<true> {
    fn from(value: ByteCount<false>) -> Self {
        Self(value.0)
    }
}
