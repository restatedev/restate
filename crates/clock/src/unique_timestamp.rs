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
use std::time::{Duration, SystemTime};

use super::RESTATE_EPOCH;
use crate::time::MillisSinceEpoch;

/// Number of bits to represent physical time in millis since restate epoch.
const PHY_TIME_BITS: u8 = 42;

/// Maximum value for physical time (4_398_046_511_103 -> Fri May 15 2161 07:35:11 GMT)
const PHY_TIME_MAX: HlcPhysicalClock =
    HlcPhysicalClock(NonZeroU64::new((1 << PHY_TIME_BITS) - 1).unwrap());

/// Number of bits to represent logical clock counter.
const LC_BITS: u8 = 22;

/// Maximum value for logical clock (4194303)
const LC_MAX: u64 = (1 << LC_BITS) - 1;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The raw value is zero, this is invalid.
    #[error("unique timestamp's raw value must be higher than zero")]
    TimestampIsZero,
    /// Physical time exceeds maximum value.
    #[error("physical time exceeds maximum value: {0} > {PHY_TIME_MAX}")]
    PhysicalTimeExceedsMax(u64),

    /// Logical clock exceeds maximum value.
    #[error("logical clock exceeds maximum value: {0} > {LC_MAX}")]
    LogicalClockExceedsMax(u64),

    /// Unix Timestamp is below the allowed minimum.
    #[error("unix timestamp is below the minimum value: {0} <= {RESTATE_EPOCH}")]
    UnixTimestampBelowMin(MillisSinceEpoch),

    #[error("HLC clock update would exceed the maximum allowed drift: {actual} > {max_drift}")]
    UnacceptableClockDrift { actual: usize, max_drift: usize },
}

/// A u64-based unique-hybrid-logical timestamp with millisecond granularity.
///
/// The timestamp is represented as a 64-bit unsigned integer. The upper 42 bits
/// represent the physical time in milliseconds since [`RESTATE_EPOCH`], and the
/// lower 22 bits represent the logical clock count.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct UniqueTimestamp(pub(super) NonZeroU64);

// Makes sure that it doesn't go unnoticed if this changed by mistake.
const _: () = {
    assert!(std::mem::size_of::<u64>() == std::mem::size_of::<UniqueTimestamp>());
};

impl UniqueTimestamp {
    /// The minimum valid timestamp.
    pub const MIN: UniqueTimestamp = UniqueTimestamp(NonZeroU64::new(1).unwrap());

    /// Parses a raw u64, returning `Ok(None)` for zero.
    pub fn try_from_raw(raw: u64) -> Result<Option<Self>, Error> {
        if raw == 0 {
            return Ok(None);
        }
        Self::try_from(raw).map(Some)
    }

    /// Creates a timestamp from unix milliseconds (logical clock set to 0).
    pub const fn from_unix_millis(unix_millis: MillisSinceEpoch) -> Result<Self, Error> {
        // Can't use ? operator in const context.
        let phy = match HlcPhysicalClock::try_from_unix(unix_millis) {
            Ok(phy) => phy,
            Err(e) => return Err(e),
        };

        Ok(Self::from_parts_unchecked(phy, 0))
    }

    /// Converts the physical component to unix milliseconds.
    #[inline(always)]
    pub const fn to_unix_millis(&self) -> MillisSinceEpoch {
        self.physical_raw().into_unix()
    }

    /// Creates a timestamp from physical clock and logical counter.
    #[inline]
    pub const fn from_parts(phy: HlcPhysicalClock, lc: u64) -> Result<Self, Error> {
        if lc > LC_MAX {
            return Err(Error::LogicalClockExceedsMax(lc));
        }

        match NonZeroU64::new((phy.0.get() << LC_BITS) | lc) {
            Some(nz) => Ok(Self(nz)),
            None => Err(Error::TimestampIsZero),
        }
    }

    /// Creates a new timestamp from the given physical and logical clock counters
    /// without checks. The caller must ensure that the timestamp is valid.
    #[inline(always)]
    pub(super) const fn from_parts_unchecked(phy: HlcPhysicalClock, lc: u64) -> Self {
        let raw = (phy.0.get() << LC_BITS) | lc;
        debug_assert!(phy.0.get() <= PHY_TIME_MAX.0.get());
        debug_assert!(lc <= LC_MAX);
        debug_assert!(raw > 0);
        Self(unsafe { NonZeroU64::new_unchecked(raw) })
    }

    /// Compare the physical clock of both timestamps and ignore the logical clock.
    ///
    /// Note that this has millisecond precision.
    pub fn cmp_physical(&self, other: &Self) -> std::cmp::Ordering {
        self.physical_raw().cmp(&other.physical_raw())
    }

    /// Returns the raw u64 representation.
    pub const fn as_u64(&self) -> u64 {
        self.0.get()
    }

    /// Fails if the resulting timestamp is out of range
    pub const fn add_millis(&self, millis: u64) -> Result<Self, Error> {
        Self::from_parts(self.physical_raw().add_millis(millis), self.logical_raw())
    }

    /// Physical time is the raw number of milliseconds since restate's epoch.
    #[inline(always)]
    pub(super) const fn physical_raw(&self) -> HlcPhysicalClock {
        // extract the physical time
        HlcPhysicalClock::from_raw_unchecked((self.0.get() >> LC_BITS) & PHY_TIME_MAX.0.get())
    }

    /// Logical clock counter component.
    #[inline(always)]
    pub(super) const fn logical_raw(&self) -> u64 {
        self.0.get() & LC_MAX
    }

    /// Calculates the number of milliseconds by which this timestamp is ahead of the other,
    /// or return 0 if the other timestamp is ahead.
    pub fn milliseconds_since(&self, other: Self) -> u64 {
        self.physical_raw().milliseconds_since(other.physical_raw())
    }

    /// Returns the number of fractional seconds since RESTATE_EPOCH.
    pub fn as_secs_f64(&self) -> f64 {
        // NOTE: physical clock is in millis.
        Duration::from_millis(self.physical_raw().0.get()).as_secs_f64()
    }
}

impl Debug for UniqueTimestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            // Output unique timestamp as a pair of physical and logical timestamps. Convert the physical
            // timestamp from the RESTATE_EPOCH to the UNIX_EPOCH.
            f.debug_struct("UniqueTimestamp")
                .field("unix_millis", &self.to_unix_millis().as_u64())
                .field("logical", &self.logical_raw())
                .finish()
        } else {
            f.debug_tuple("UniqueTimestamp")
                .field(&self.physical_raw())
                .field(&self.logical_raw())
                .finish()
        }
    }
}

impl From<SystemTime> for UniqueTimestamp {
    fn from(value: SystemTime) -> Self {
        // The assumption is that SystemTime will always be > RESTATE_EPOCH
        UniqueTimestamp::from_unix_millis(MillisSinceEpoch::from(value)).unwrap()
    }
}

impl TryFrom<MillisSinceEpoch> for UniqueTimestamp {
    type Error = Error;

    fn try_from(value: MillisSinceEpoch) -> Result<Self, Self::Error> {
        Self::from_unix_millis(value)
    }
}

impl TryFrom<u64> for UniqueTimestamp {
    type Error = Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let phy = (value >> LC_BITS) & PHY_TIME_MAX.0.get();
        let lc = value & LC_MAX;
        Self::from_parts(HlcPhysicalClock::try_from(phy)?, lc)
    }
}

/// The physical clock component of an HLC, in milliseconds since [`RESTATE_EPOCH`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct HlcPhysicalClock(NonZeroU64);

impl From<HlcPhysicalClock> for MillisSinceEpoch {
    fn from(value: HlcPhysicalClock) -> Self {
        value.into_unix()
    }
}

impl TryFrom<u64> for HlcPhysicalClock {
    type Error = Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let phy = NonZeroU64::new(value).ok_or(Error::TimestampIsZero)?;
        if phy.get() > PHY_TIME_MAX.0.get() {
            return Err(Error::PhysicalTimeExceedsMax(value));
        }
        Ok(Self(phy))
    }
}

impl std::fmt::Display for HlcPhysicalClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl HlcPhysicalClock {
    /// The minimum valid physical clock value (1ms since RESTATE_EPOCH).
    pub const MIN: HlcPhysicalClock = HlcPhysicalClock(NonZeroU64::new(1).unwrap());

    /// Creates from unix timestamp, validating it's after [`RESTATE_EPOCH`].
    pub const fn try_from_unix(timestamp: MillisSinceEpoch) -> Result<Self, Error> {
        if timestamp.as_u64() <= RESTATE_EPOCH.as_u64() {
            return Err(Error::UnixTimestampBelowMin(timestamp));
        }
        Ok(Self::from_raw_unchecked(
            timestamp.as_u64() - RESTATE_EPOCH.as_u64(),
        ))
    }

    /// Creates from unix timestamp without validation.
    #[inline(always)]
    pub const fn from_unix_unchecked(timestamp: MillisSinceEpoch) -> Self {
        debug_assert!(timestamp.as_u64() > RESTATE_EPOCH.as_u64());
        Self::from_raw_unchecked(timestamp.as_u64() - RESTATE_EPOCH.as_u64())
    }

    /// Adds milliseconds to the clock.
    #[inline(always)]
    pub const fn add_millis(&self, millis: u64) -> Self {
        Self::from_raw_unchecked(self.0.get() + millis)
    }

    /// Converts to unix milliseconds.
    #[inline(always)]
    pub const fn into_unix(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::new(self.0.get() + RESTATE_EPOCH.as_u64())
    }

    /// Calculates the number of milliseconds by which this timestamp is ahead of the other,
    /// or return 0 if the other timestamp is ahead.
    #[inline(always)]
    pub const fn milliseconds_since(&self, other: HlcPhysicalClock) -> u64 {
        self.0.get().saturating_sub(other.0.get())
    }

    /// It's the responsibility of the caller to ensure the input value is non-zero
    #[inline(always)]
    const fn from_raw_unchecked(raw: u64) -> HlcPhysicalClock {
        debug_assert!(raw > 0);
        Self(unsafe { NonZeroU64::new_unchecked(raw) })
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

    // -- HlcPhysicalClock --

    impl Proxiable for HlcPhysicalClock {
        type Proxy = u64;

        fn encode_proxy(&self) -> Self::Proxy {
            self.0.get()
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = Self::try_from(proxy).map_err(|_| DecodeErrorKind::InvalidValue)?;
            Ok(())
        }
    }

    impl DistinguishedProxiable for HlcPhysicalClock {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonical)
        }
    }

    impl ForOverwrite<(), HlcPhysicalClock> for () {
        fn for_overwrite() -> HlcPhysicalClock {
            HlcPhysicalClock::MIN
        }
    }

    bilrost::empty_state_via_for_overwrite!(HlcPhysicalClock);

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (HlcPhysicalClock)
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
        let unique = UniqueTimestamp::from_unix_millis(ts).unwrap();

        assert_eq!(unique.to_unix_millis(), ts);
    }
}
