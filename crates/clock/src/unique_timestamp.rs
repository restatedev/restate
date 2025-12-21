// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Debug, Formatter};
use std::num::NonZeroU64;
use std::time::SystemTime;

use crate::RESTATE_EPOCH;
use crate::time::MillisSinceEpoch;

/// Number of bits to represent physical time in millis since restate epoch.
const PHY_TIME_BITS: u8 = 42;

/// Maximum value for physical time (4_398_046_511_103 -> Fri May 15 2161 07:35:11 GMT)
/// This is also the physical time mask (LSB 42 bits)
pub(super) const PHY_TIME_MAX: u64 = (1 << PHY_TIME_BITS) - 1;

/// Number of bits to represent logical clock counter.
pub(super) const LC_BITS: u8 = 22;

/// A mask to extract the logical clock counter (LSB 22 bits)
pub(super) const LC_MAX: u64 = (1 << LC_BITS) - 1;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("HLC timestamp outside the valid range")]
    TimestampExceedsMax,

    /// Physical time exceeds maximum value.
    #[error("physical time exceeds maximum value: {0} > {PHY_TIME_MAX}")]
    PhysicalTimeExceedsMax(u64),

    /// Logical clock exceeds maximum value.
    #[error("logical clock exceeds maximum value: {0} > {1}")]
    LogicalClockExceedsMax(u64, u64),

    /// Unix Timestamp is below the allowed minimum.
    #[error("unix timestamp is below the minimum value: {0} < {RESTATE_EPOCH}")]
    UnixTimestampBelowMin(MillisSinceEpoch),

    #[error("HLC clock update would exceed the maximum allowed drift: {actual} > {max_drift}")]
    UnacceptableClockDrift { actual: usize, max_drift: usize },
}

/// A hybrid logical timestamp with millisecond precision.
///
///
/// # Internal Representation
///
/// The timestamp is represented as a 64-bit unsigned integer. The upper 42 bits
/// represent the physical time in milliseconds since [`RESTATE_EPOCH`], and the
/// lower 22 bits represent the logical clock count.
///
/// This type uses [`NonZeroU64`] internally to enable niche optimization, allowing
/// `Option<UniqueTimestamp>` to have the same size as `UniqueTimestamp` (8 bytes).
///
/// The internal value is stored as `raw + 1` (using wrapping arithmetic), which maps
/// the valid input range `0..=u64::MAX-1` to `1..=u64::MAX`. This assumes that
/// `u64::MAX` is not a valid value and will never be used (see [`PHY_TIME_MAX`]
/// for the upper bound of the physical time).
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct UniqueTimestamp(NonZeroU64);

// Makes sure that it doesn't go unnoticed if this changed by mistake.
const _: () = {
    assert!(
        size_of::<u64>() == size_of::<UniqueTimestamp>(),
        "UniqueTimestamp should be the same size as u64"
    );

    assert!(
        size_of::<Option<UniqueTimestamp>>() == size_of::<UniqueTimestamp>(),
        "UniqueTimestamp should be the same size as Option<UniqueTimestamp>"
    );
};

impl UniqueTimestamp {
    pub const MIN: UniqueTimestamp = UniqueTimestamp::new(0);
    pub const MAX: UniqueTimestamp = UniqueTimestamp::new(u64::MAX - 1);

    /// Creates a new `UniqueTimestamp` from its raw value.
    ///
    /// Values are clamped to [`Self::MAX`] (which is `u64::MAX - 1`) since `u64::MAX`
    /// cannot be represented due to the internal `NonZeroU64` representation.
    pub(crate) const fn new(raw: u64) -> Self {
        // We store millis + 1 to enable niche optimization with NonZeroU64.
        // This maps 0..=u64::MAX-1 to 1..=u64::MAX (all valid NonZeroU64 values).
        // We use saturating_add to clamp u64::MAX to u64::MAX (which represents MAX).
        let shifted = raw.saturating_add(1);
        // SAFETY: saturating_add(1) on any u64 value produces a value >= 1,
        // so this is always valid for NonZeroU64.
        unsafe { Self(NonZeroU64::new_unchecked(shifted)) }
    }

    pub const fn try_from_parts(phy: u64, lc: u64) -> Result<Self, Error> {
        // unlikely.
        if phy > PHY_TIME_MAX {
            return Err(Error::PhysicalTimeExceedsMax(phy));
        }

        // unlikely.
        if lc > LC_MAX {
            return Err(Error::LogicalClockExceedsMax(lc, LC_MAX));
        }

        if lc == LC_MAX && phy == PHY_TIME_MAX {
            return Err(Error::TimestampExceedsMax);
        }

        Ok(Self::new((phy << LC_BITS) | lc))
    }

    /// Panics if the timestamp is out of range.
    pub const fn from_parts_unchecked(phy: u64, lc: u64) -> Self {
        // unlikely.
        assert!(phy <= PHY_TIME_MAX);
        assert!(lc <= LC_MAX);

        let combined: u64 = (phy << LC_BITS) | lc;
        assert!(combined < u64::MAX);

        Self::new(combined)
    }

    pub const fn try_from_unix_millis(unix_millis: MillisSinceEpoch) -> Result<Self, Error> {
        if unix_millis.as_u64() < RESTATE_EPOCH.as_u64() {
            return Err(Error::UnixTimestampBelowMin(unix_millis));
        }
        Self::try_from_parts(unix_millis.as_u64() - RESTATE_EPOCH.as_u64(), 0)
    }

    /// Panics (in debug mode) if the timestamp is out of range.
    pub const fn from_unix_millis_unchecked(unix_millis: MillisSinceEpoch) -> Self {
        debug_assert!(
            unix_millis.as_u64() >= RESTATE_EPOCH.as_u64(),
            "HLC timestamps below Jan 01 2022 00:00:00 GMT+0000 are invalid"
        );
        Self::from_parts_unchecked(unix_millis.as_u64() - RESTATE_EPOCH.as_u64(), 0)
    }

    pub const fn to_unix_millis(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::new(self.physical_raw() + RESTATE_EPOCH.as_u64())
    }

    /// Compare the physical clock of both timestamps and ignore the logical clock.
    ///
    /// Note that this has millisecond precision.
    pub fn cmp_physical(&self, other: &Self) -> std::cmp::Ordering {
        self.physical_raw().cmp(&other.physical_raw())
    }

    /// Returns the raw value of the timestamp
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        self.0.get() - 1
    }

    /// Splits the timestamp into its physical and logical components.
    #[cfg(feature = "hlc")]
    pub(super) const fn split(&self) -> (u64, u64) {
        let raw = self.as_u64();
        let phy = (raw >> LC_BITS) & PHY_TIME_MAX;
        let lc = raw & LC_MAX;
        (phy, lc)
    }

    /// Physical time is the raw number of milliseconds since restate's epoch.
    #[inline(always)]
    pub(super) const fn physical_raw(&self) -> u64 {
        // extract the physical time
        (self.as_u64() >> LC_BITS) & PHY_TIME_MAX
    }

    #[inline(always)]
    pub(super) const fn logical_raw(&self) -> u64 {
        // extract the logical clock
        self.as_u64() & LC_MAX
    }
}

impl Debug for UniqueTimestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            // Output unique timestamp as a pair of physical and logical timestamps. Convert the physical
            // timestamp from the RESTATE_EPOCH to the UNIX_EPOCH.
            f.debug_struct("UniqueTimestamp")
                .field("physical", &self.to_unix_millis().as_u64())
                .field("logical", &self.logical_raw())
                .finish()
        } else {
            f.debug_struct("UniqueTimestamp")
                .field("phy", &self.physical_raw())
                .field("lc", &self.logical_raw())
                .finish()
        }
    }
}

impl From<SystemTime> for UniqueTimestamp {
    fn from(value: SystemTime) -> Self {
        // The assumption is that SystemTime will always be > RESTATE_EPOCH
        UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::from(value))
    }
}

impl TryFrom<MillisSinceEpoch> for UniqueTimestamp {
    type Error = Error;

    fn try_from(value: MillisSinceEpoch) -> Result<Self, Self::Error> {
        Self::try_from_unix_millis(value)
    }
}

impl TryFrom<u64> for UniqueTimestamp {
    type Error = Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let pt = (value >> LC_BITS) & PHY_TIME_MAX;
        let lc = value & LC_MAX;
        Self::try_from_parts(pt, lc)
    }
}

mod bilrost_encoding {
    use super::*;

    use bilrost::Canonicity::Canonical;
    use bilrost::encoding::{DistinguishedProxiable, ForOverwrite, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind};

    impl Proxiable for UniqueTimestamp {
        type Proxy = u64;

        fn encode_proxy(&self) -> Self::Proxy {
            self.as_u64()
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = Self::try_from(proxy).map_err(|_| DecodeErrorKind::InvalidValue)?;
            Ok(())
        }
    }

    impl DistinguishedProxiable for UniqueTimestamp {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonical)
        }
    }

    impl ForOverwrite<(), UniqueTimestamp> for () {
        fn for_overwrite() -> UniqueTimestamp {
            UniqueTimestamp::MIN
        }
    }

    bilrost::empty_state_via_for_overwrite!(UniqueTimestamp);

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (UniqueTimestamp)
        with general encodings including distinguished
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_from_unix_timestamp() {
        // 2024-01-01 00:00:00.120 UTC
        let ts = MillisSinceEpoch::new(1704067200120);
        let unique = UniqueTimestamp::from_unix_millis_unchecked(ts);

        assert_eq!(unique.to_unix_millis(), ts);
    }
}
