// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::fmt::Display;
use std::num::NonZeroU32;
use std::ops::{Add, Sub};
use std::time::Duration;

use restate_platform::network::NetSerde;

use crate::WallClock;
use crate::time::MillisSinceEpoch;
use crate::unique_timestamp::UniqueTimestamp;

const RESTATE_EPOCH_SECONDS: u64 = crate::RESTATE_EPOCH.as_u64() / 1_000;

/// A coarse-grained timestamp with second precision since restate epoch.
///
/// The representable range is from `0` to `u32::MAX - 1` seconds since restate epoch,
/// so [`Self::MAX`] corresponds to `2158-02-07 06:28:14 UTC`.
///
/// # Internal Representation
///
/// This type uses [`NonZeroU32`] internally to enable niche optimization, allowing
/// `Option<RoughTimestamp>` to have the same size as `RoughTimestamp` (4 bytes).
///
/// The internal value is stored as `seconds + 1`, which maps the valid input range
/// `0..=u32::MAX-1` to `1..=u32::MAX`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct RoughTimestamp(NonZeroU32);

impl fmt::Debug for RoughTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RoughTimestamp")
            .field(&self.as_u32())
            .finish()
    }
}

impl NetSerde for RoughTimestamp {}

const _: () = {
    assert!(
        std::mem::size_of::<u32>() == std::mem::size_of::<RoughTimestamp>(),
        "RoughTimestamp should be the same size as u32"
    );

    assert!(
        std::mem::size_of::<Option<RoughTimestamp>>() == std::mem::size_of::<RoughTimestamp>(),
        "RoughTimestamp should be the same size as Option<RoughTimestamp>"
    );
};

impl RoughTimestamp {
    /// Restate epoch (2022-01-01 00:00:00 UTC) represented in this type.
    pub const RESTATE_EPOCH: Self = Self::new(0);
    /// The maximum representable timestamp (`u32::MAX - 1` seconds since restate epoch).
    pub const MAX: Self = Self::new(u32::MAX - 1);

    /// Creates a new `RoughTimestamp` from seconds since restate epoch.
    ///
    /// Values are clamped to [`Self::MAX`] (`u32::MAX - 1`) since `u32::MAX`
    /// cannot be represented due to the internal `NonZeroU32` representation.
    pub const fn new(seconds_since_restate_epoch: u32) -> Self {
        let shifted = seconds_since_restate_epoch.saturating_add(1);
        // SAFETY: saturating_add(1) always yields at least 1.
        unsafe { Self(NonZeroU32::new_unchecked(shifted)) }
    }

    /// Returns the current rough timestamp (seconds since restate epoch).
    pub fn now() -> Self {
        let recent = WallClock::recent_ms();
        if recent.as_u64() > 0 {
            Self::from(recent)
        } else {
            Self::from_system_now()
        }
    }

    #[cold]
    #[inline(never)]
    fn from_system_now() -> Self {
        Self::from(WallClock::now_ms())
    }

    /// Returns seconds since restate epoch.
    #[inline]
    pub const fn as_u32(&self) -> u32 {
        self.0.get() - 1
    }

    /// Returns unix seconds corresponding to this timestamp.
    #[inline]
    pub const fn as_unix_seconds(&self) -> u64 {
        self.as_u32() as u64 + RESTATE_EPOCH_SECONDS
    }

    /// Returns unix milliseconds corresponding to this timestamp.
    #[inline]
    pub const fn as_unix_millis(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::new(self.as_unix_seconds().saturating_mul(1_000))
    }

    /// Creates a `RoughTimestamp` from unix milliseconds.
    ///
    /// Values before restate epoch are clamped to [`Self::RESTATE_EPOCH`].
    /// Values beyond the representable range are clamped to [`Self::MAX`].
    #[inline]
    pub const fn from_unix_millis_clamped(unix_millis: MillisSinceEpoch) -> Self {
        let unix_secs = unix_millis.as_u64() / 1_000;
        let since_restate_epoch = unix_secs.saturating_sub(RESTATE_EPOCH_SECONDS);
        let clamped = if since_restate_epoch > u32::MAX as u64 {
            u32::MAX
        } else {
            since_restate_epoch as u32
        };
        Self::new(clamped)
    }

    /// Creates a `RoughTimestamp` from unix milliseconds, rounding UP to the next
    /// whole second if there is any sub-second fraction. Use this for future-scheduled
    /// times so an entry is never eligible before its real `execution_time`.
    #[inline]
    pub const fn from_unix_millis_ceil(unix_millis: MillisSinceEpoch) -> Self {
        let ms = unix_millis.as_u64();
        let unix_secs = ms / 1_000;
        let rounded = if ms.is_multiple_of(1_000) {
            unix_secs
        } else {
            unix_secs + 1
        };
        let since_restate_epoch = rounded.saturating_sub(RESTATE_EPOCH_SECONDS);
        let clamped = if since_restate_epoch > u32::MAX as u64 {
            u32::MAX
        } else {
            since_restate_epoch as u32
        };
        Self::new(clamped)
    }

    /// Returns true when this timestamp equals the restate epoch.
    #[inline]
    pub const fn is_zero(&self) -> bool {
        self.0.get() == Self::RESTATE_EPOCH.0.get()
    }

    /// Returns zero duration if `self` is in the future.
    pub fn elapsed(&self) -> Duration {
        Duration::from_secs(Self::now().0.get().saturating_sub(self.0.get()) as u64)
    }

    /// Calculates the number of seconds between this and an earlier timestamp.
    #[inline]
    pub const fn saturating_sub_secs(&self, earlier: Self) -> u32 {
        self.0.get().saturating_sub(earlier.0.get())
    }

    /// Returns elapsed duration from another timestamp to this one.
    pub const fn duration_since(&self, earlier: Self) -> Duration {
        Duration::from_secs(self.0.get().saturating_sub(earlier.0.get()) as u64)
    }

    /// Floors the timestamp to the nearest minute by zeroing the seconds part.
    #[inline]
    pub const fn floor_to_minute(&self) -> Self {
        let secs = self.as_u32();
        Self::new(secs - (secs % 60))
    }

    /// Floors the timestamp to the nearest minute and adds a random second in `[0, 59]`.
    ///
    /// For values near [`Self::MAX`], the result is saturating and can be clamped to [`Self::MAX`].
    #[inline]
    pub fn smear_to_minute(&self) -> Self {
        let random_second = rand::random_range(0..60_u32);
        Self::new(
            self.floor_to_minute()
                .as_u32()
                .saturating_add(random_second),
        )
    }
}

impl From<RoughTimestamp> for u32 {
    #[inline]
    fn from(value: RoughTimestamp) -> Self {
        value.as_u32()
    }
}

impl From<u32> for RoughTimestamp {
    #[inline]
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl From<MillisSinceEpoch> for RoughTimestamp {
    #[inline]
    fn from(value: MillisSinceEpoch) -> Self {
        Self::from_unix_millis_clamped(value)
    }
}

impl From<RoughTimestamp> for MillisSinceEpoch {
    #[inline]
    fn from(value: RoughTimestamp) -> Self {
        value.as_unix_millis()
    }
}

impl From<UniqueTimestamp> for RoughTimestamp {
    #[inline]
    fn from(value: UniqueTimestamp) -> Self {
        let since_restate_epoch = value.physical_raw() / 1_000;
        let clamped = if since_restate_epoch > u32::MAX as u64 {
            u32::MAX
        } else {
            since_restate_epoch as u32
        };
        Self::new(clamped)
    }
}

impl Add<Duration> for RoughTimestamp {
    type Output = RoughTimestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        let secs = if rhs.as_secs() > u64::from(u32::MAX) {
            u32::MAX
        } else {
            rhs.as_secs() as u32
        };
        RoughTimestamp::new(self.as_u32().saturating_add(secs))
    }
}

impl Sub<Duration> for RoughTimestamp {
    type Output = RoughTimestamp;

    fn sub(self, rhs: Duration) -> Self::Output {
        let secs = if rhs.as_secs() > u64::from(u32::MAX) {
            u32::MAX
        } else {
            rhs.as_secs() as u32
        };
        RoughTimestamp::new(self.as_u32().saturating_sub(secs))
    }
}

impl Display for RoughTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self == &RoughTimestamp::MAX {
            write!(f, "INF")
        } else {
            write!(f, "{} s since restate epoch", self.as_u32())
        }
    }
}

// Compare RoughTimestamps to MillisSinceEpoch
impl PartialEq<MillisSinceEpoch> for RoughTimestamp {
    fn eq(&self, other: &MillisSinceEpoch) -> bool {
        let other = RoughTimestamp::from_unix_millis_clamped(*other);
        self.eq(&other)
    }
}

impl PartialOrd<MillisSinceEpoch> for RoughTimestamp {
    fn partial_cmp(&self, other: &MillisSinceEpoch) -> Option<std::cmp::Ordering> {
        let other = RoughTimestamp::from_unix_millis_clamped(*other);
        self.partial_cmp(&other)
    }
}

impl PartialEq<RoughTimestamp> for MillisSinceEpoch {
    fn eq(&self, other: &RoughTimestamp) -> bool {
        let this = RoughTimestamp::from_unix_millis_clamped(*self);
        this.eq(other)
    }
}

impl PartialOrd<RoughTimestamp> for MillisSinceEpoch {
    fn partial_cmp(&self, other: &RoughTimestamp) -> Option<std::cmp::Ordering> {
        let this = RoughTimestamp::from_unix_millis_clamped(*self);
        this.partial_cmp(other)
    }
}

mod serde_encoding {
    use super::RoughTimestamp;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    impl Serialize for RoughTimestamp {
        #[inline]
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.as_u32().serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for RoughTimestamp {
        #[inline]
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let seconds = u32::deserialize(deserializer)?;
            Ok(Self::new(seconds))
        }
    }
}

mod bilrost_encoding {
    use super::RoughTimestamp;

    use bilrost::Canonicity::Canonical;
    use bilrost::encoding::{DistinguishedProxiable, EmptyState, ForOverwrite, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind};

    impl Proxiable for RoughTimestamp {
        type Proxy = u32;

        fn encode_proxy(&self) -> Self::Proxy {
            self.as_u32()
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = Self::new(proxy);
            Ok(())
        }
    }

    impl DistinguishedProxiable for RoughTimestamp {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonical)
        }
    }

    impl ForOverwrite<(), RoughTimestamp> for () {
        fn for_overwrite() -> RoughTimestamp {
            RoughTimestamp::RESTATE_EPOCH
        }
    }

    impl EmptyState<(), RoughTimestamp> for () {
        fn empty() -> RoughTimestamp {
            RoughTimestamp::RESTATE_EPOCH
        }

        fn is_empty(value: &RoughTimestamp) -> bool {
            value.as_u32() == 0
        }

        fn clear(value: &mut RoughTimestamp) {
            *value = RoughTimestamp::RESTATE_EPOCH;
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (RoughTimestamp)
        with general encodings including distinguished
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    use bilrost::OwnedMessage;

    #[test]
    fn round_trip_values() {
        let test_values = [0u32, 1, 100, 1_000, u32::MAX / 2, u32::MAX - 1];

        for &val in &test_values {
            let ts = RoughTimestamp::new(val);
            assert_eq!(ts.as_u32(), val, "round-trip failed for {val}");
        }
    }

    #[test]
    fn u32_max_clamps_to_max() {
        let ts = RoughTimestamp::new(u32::MAX);
        assert_eq!(ts, RoughTimestamp::MAX);
        assert_eq!(ts.as_u32(), u32::MAX - 1);
    }

    #[test]
    fn from_unix_millis_uses_restate_epoch() {
        let epoch = RoughTimestamp::from(crate::RESTATE_EPOCH);
        let one_second = RoughTimestamp::from(crate::RESTATE_EPOCH + Duration::from_secs(1));

        assert_eq!(epoch.as_u32(), 0);
        assert_eq!(one_second.as_u32(), 1);
    }

    #[test]
    fn from_unix_millis_before_restate_epoch_clamps_to_restate_epoch() {
        let before_epoch = crate::RESTATE_EPOCH - Duration::from_millis(1);

        assert_eq!(
            RoughTimestamp::from_unix_millis_clamped(before_epoch),
            RoughTimestamp::RESTATE_EPOCH
        );
        assert_eq!(
            RoughTimestamp::from(before_epoch),
            RoughTimestamp::RESTATE_EPOCH
        );
    }

    #[test]
    fn from_unique_timestamp_uses_restate_epoch() {
        let unique = crate::UniqueTimestamp::from_unix_millis_unchecked(
            crate::RESTATE_EPOCH + Duration::from_secs(123),
        );

        let rough = RoughTimestamp::from(unique);
        assert_eq!(rough.as_u32(), 123);
    }

    #[test]
    fn floor_to_minute_zeroes_seconds() {
        assert_eq!(RoughTimestamp::new(0).floor_to_minute().as_u32(), 0);
        assert_eq!(RoughTimestamp::new(59).floor_to_minute().as_u32(), 0);
        assert_eq!(RoughTimestamp::new(60).floor_to_minute().as_u32(), 60);
        assert_eq!(RoughTimestamp::new(121).floor_to_minute().as_u32(), 120);

        let floored_max = RoughTimestamp::MAX.floor_to_minute();
        assert_eq!(floored_max.as_u32() % 60, 0);
        assert_eq!(
            floored_max.as_u32(),
            RoughTimestamp::MAX.as_u32() - (RoughTimestamp::MAX.as_u32() % 60)
        );
    }

    #[test]
    fn smear_to_minute_stays_within_minute_window() {
        let test_values = [0u32, 1, 59, 60, 121, 61_234, u32::MAX - 1];

        for value in test_values {
            let ts = RoughTimestamp::new(value);
            let floored = ts.floor_to_minute().as_u32();

            for _ in 0..16 {
                let smeared = ts.smear_to_minute().as_u32();
                assert!(
                    smeared >= floored,
                    "smeared value should not be below minute floor"
                );
                assert!(
                    smeared <= floored.saturating_add(59),
                    "smeared value should not be above minute floor + 59"
                );
                assert!(
                    smeared <= RoughTimestamp::MAX.as_u32(),
                    "smeared value should not exceed RoughTimestamp::MAX"
                );
            }
        }
    }

    #[test]
    fn serde_round_trip() {
        let test_values = [0u32, 1, 100, 1_000, u32::MAX / 2, u32::MAX - 1];

        for &val in &test_values {
            let ts = RoughTimestamp::new(val);

            let json = serde_json::to_string(&ts).unwrap();
            assert_eq!(
                json,
                val.to_string(),
                "JSON serialization mismatch for {val}"
            );

            let deserialized: RoughTimestamp = serde_json::from_str(&json).unwrap();
            assert_eq!(
                deserialized.as_u32(),
                val,
                "JSON deserialization mismatch for {val}"
            );
        }
    }

    #[test]
    fn bilrost_round_trip() {
        use bilrost::Message;

        #[derive(bilrost::Message, PartialEq, Debug)]
        struct TestMessage {
            #[bilrost(1)]
            timestamp: RoughTimestamp,
        }

        let test_values = [0u32, 1, 100, 1_000, u32::MAX / 2, u32::MAX - 1];

        for &val in &test_values {
            let msg = TestMessage {
                timestamp: RoughTimestamp::new(val),
            };

            let encoded = msg.encode_to_vec();
            let decoded = TestMessage::decode(encoded.as_slice()).unwrap();
            assert_eq!(
                decoded.timestamp.as_u32(),
                val,
                "bilrost round-trip failed for {val}"
            );
        }
    }
}
