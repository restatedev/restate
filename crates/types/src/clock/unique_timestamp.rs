// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU64;
use std::time::SystemTime;

use crate::time::MillisSinceEpoch;

/// Number of bits to represent physical time in millis since restate epoch.
static PHY_TIME_BITS: u8 = 42;

/// Maximum value for physical time (4_398_046_511_103 -> Fri May 15 2161 07:35:11 GMT)
static PHY_TIME_MAX: u64 = (1 << PHY_TIME_BITS) - 1;

/// Number of bits to represent logical clock counter.
static LC_BITS: u8 = 22;

/// Maximum value for logical clock (4194303)
static LC_MAX: u64 = (1 << LC_BITS) - 1;

const RESTATE_EPOCH: MillisSinceEpoch = MillisSinceEpoch::new(1_640_995_200_000);

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The raw value is zero, this is invalid.
    #[error("unique timestamp's raw value must be higher than zero")]
    TimestampIsZero,
    /// Physical time exceeds maximum value.
    #[error("physical time exceeds maximum value: {0} > {PHY_TIME_MAX}")]
    PhysicalTimeExceedsMax(u64),

    /// Logical clock exceeds maximum value.
    #[error("logical clock exceeds maximum value: {0} > {1}")]
    LogicalClockExceedsMax(u64, u64),

    /// Unix Timestamp is below the allowed minimum.
    #[error("unix timestamp is below the minimum value: {0} <= {RESTATE_EPOCH}")]
    UnixTimestampBelowMin(MillisSinceEpoch),
}

/// This is a placeholder for a future u64 unique-hybrid-logical timestamp
///
/// The timestamp is represented as a 64-bit unsigned integer. The upper 42 bits
/// represent the physical time in milliseconds since [`RESTATE_EPOCH`], and the
/// lower 22 bits represent the logical clock count.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct UniqueTimestamp(NonZeroU64);

// Makes sure that it doesn't go unnoticed if this changed by mistake.
static_assertions::const_assert_eq!(
    std::mem::size_of::<u64>(),
    std::mem::size_of::<UniqueTimestamp>()
);

impl UniqueTimestamp {
    const MIN: UniqueTimestamp =
        UniqueTimestamp(NonZeroU64::new(RESTATE_EPOCH.as_u64() + 1).unwrap());

    pub fn try_from_raw(raw: u64) -> Result<Option<Self>, Error> {
        if raw == 0 {
            return Ok(None);
        }
        Self::try_from(raw).map(Some)
    }

    pub fn from_unix_millis(unix_millis: MillisSinceEpoch) -> Result<Self, Error> {
        if unix_millis <= RESTATE_EPOCH {
            return Err(Error::UnixTimestampBelowMin(unix_millis));
        }
        let pt = unix_millis.as_u64() - RESTATE_EPOCH.as_u64();
        Self::from_parts(pt, 0)
    }

    pub fn to_unix_millis(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::new(self.physical_raw() + RESTATE_EPOCH.as_u64())
    }

    pub fn from_parts(pt: u64, lc: u64) -> Result<Self, Error> {
        if pt > PHY_TIME_MAX {
            return Err(Error::PhysicalTimeExceedsMax(pt));
        }

        if lc > LC_MAX {
            return Err(Error::LogicalClockExceedsMax(lc, LC_MAX));
        }

        let nz = NonZeroU64::new((pt << LC_BITS) | lc).ok_or(Error::TimestampIsZero)?;
        Ok(Self(nz))
    }

    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }

    #[inline(always)]
    fn physical_raw(&self) -> u64 {
        // extract the physical time
        (self.0.get() >> LC_BITS) & PHY_TIME_MAX
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn logical_raw(&self) -> u64 {
        // extract the logical clock
        self.0.get() & LC_MAX
    }

    /// Calculates the number of milliseconds by which this timestamp is ahead of the other,
    /// or return 0 if the other timestamp is ahead.
    pub fn milliseconds_since(&self, other: Self) -> u64 {
        self.physical_raw().saturating_sub(other.physical_raw())
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
        let pt = (value >> LC_BITS) & PHY_TIME_MAX;
        let lc = value & LC_MAX;
        Self::from_parts(pt, lc)
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
        let unique = UniqueTimestamp::from_unix_millis(ts).unwrap();

        assert_eq!(unique.to_unix_millis(), ts);
    }
}
