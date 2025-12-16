// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::num::NonZeroU64;
use std::ops::{Add, Sub};
use std::time::{Duration, SystemTime};

// Note: BilrostNewType and NetSerde derives are not used since both MillisSinceEpoch
// and NanosSinceEpoch have custom implementations for niche optimization.

use crate::WallClock;

/// Milliseconds since the unix epoch.
///
/// # Internal Representation
///
/// This type uses [`NonZeroU64`] internally to enable niche optimization, allowing
/// `Option<MillisSinceEpoch>` to have the same size as `MillisSinceEpoch` (8 bytes).
///
/// The internal value is stored as `millis + 1` (using wrapping arithmetic), which maps
/// the valid input range `0..=u64::MAX-1` to `1..=u64::MAX`. This assumes that
/// `u64::MAX` milliseconds (~584 million years from Unix epoch) will never be used
/// as an actual timestamp.
///
/// Serialization (serde, bilrost) always uses the original `u64` value for backward
/// compatibility.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct MillisSinceEpoch(NonZeroU64);

impl fmt::Debug for MillisSinceEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MillisSinceEpoch")
            .field(&self.as_u64())
            .finish()
    }
}

impl restate_encoding::NetSerde for MillisSinceEpoch {}

// Static assertions to ensure that MillisSinceEpoch is the same size as u64
// and that niche optimization works.
const _: () = {
    assert!(
        size_of::<u64>() == size_of::<MillisSinceEpoch>(),
        "MillisSinceEpoch should be the same size as u64"
    );

    assert!(
        size_of::<Option<MillisSinceEpoch>>() == size_of::<MillisSinceEpoch>(),
        "MillisSinceEpoch should be the same size as Option<MillisSinceEpoch>"
    );
};

impl MillisSinceEpoch {
    pub const UNIX_EPOCH: Self = Self::new(0);
    /// The maximum representable timestamp. Note: This is `u64::MAX - 1` because
    /// `u64::MAX` cannot be represented due to the internal NonZeroU64 representation.
    /// This is ~584 million years from Unix epoch, so it's not a practical limitation.
    pub const MAX: Self = Self::new(u64::MAX - 1);

    /// Creates a new `MillisSinceEpoch` from the given milliseconds value.
    ///
    /// Values are clamped to [`Self::MAX`] (which is `u64::MAX - 1`) since `u64::MAX`
    /// cannot be represented due to the internal `NonZeroU64` representation.
    /// This is not a practical limitation as `u64::MAX` milliseconds is ~584 million
    /// years from Unix epoch.
    pub const fn new(millis_since_epoch: u64) -> Self {
        // We store millis + 1 to enable niche optimization with NonZeroU64.
        // This maps 0..=u64::MAX-1 to 1..=u64::MAX (all valid NonZeroU64 values).
        // We use saturating_add to clamp u64::MAX to u64::MAX (which represents MAX).
        let shifted = millis_since_epoch.saturating_add(1);
        // SAFETY: saturating_add(1) on any u64 value produces a value >= 1,
        // so this is always valid for NonZeroU64.
        // We use unsafe here because NonZeroU64::new() is not const-stable for unwrap.
        unsafe { Self(NonZeroU64::new_unchecked(shifted)) }
    }

    /// Returns the current unix timestamp in milliseconds.
    ///
    /// This method uses the cached [`WallClock::recent_ms()`] timestamp when available,
    /// providing ~100x better performance than a direct `SystemTime::now()` syscall.
    /// The cached value is refreshed every 500μs by [`ClockUpkeep`](crate::ClockUpkeep),
    /// so it may be up to ~1ms stale.
    ///
    /// # Fallback Behavior
    ///
    /// If [`ClockUpkeep`](crate::ClockUpkeep) has not been started (e.g., in tests or
    /// standalone tools), this falls back to `SystemTime::now()`. The fallback path
    /// is marked `#[cold]` to hint to the compiler that it's not the expected hot path.
    ///
    /// # Usage
    ///
    /// For production server binaries, ensure [`ClockUpkeep::start()`](crate::ClockUpkeep::start)
    /// is called early in initialization to enable the fast path.
    pub fn now() -> Self {
        let recent = WallClock::recent_ms();
        if recent.as_u64() > 0 {
            recent
        } else {
            // In tests or binaries where the upkeep thread is not running, we don't
            // care about the performance of this call, so we can always call now()
            // but we keep is non-inlined to hint to the compiler that it's unlikely
            // the case.
            Self::from_system_now()
        }
    }

    // a hint to the compiler that this is unlikely to be called in the hot path.
    #[cold]
    #[inline(never)]
    fn from_system_now() -> Self {
        WallClock::now_ms()
    }

    #[inline]
    pub fn after(duration: Duration) -> Self {
        Self::now() + duration
    }

    /// Returns the milliseconds since Unix epoch as a `u64`.
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        // Convert back from internal representation: stored = millis + 1
        self.0.get().wrapping_sub(1)
    }

    /// Returns true if the timestamp is zero (Unix epoch).
    #[inline]
    pub const fn is_zero(&self) -> bool {
        self.0.get() == Self::UNIX_EPOCH.0.get()
    }

    /// Note, this doesn't fail if the timestamp is higher than Timestamp::MAX instead
    /// it returns the default value (now). There are no practical cases where this can happen
    /// so it's decided to do this for API convenience.
    #[cfg(feature = "jiff")]
    pub fn into_timestamp(self) -> jiff::Timestamp {
        jiff::Timestamp::from_millisecond(self.as_u64() as i64).unwrap_or_default()
    }

    /// Returns zero duration if self is in the future. Should not be used where monotonic
    /// clock/duration is expected.
    pub fn elapsed(&self) -> Duration {
        Duration::from_millis(Self::now().0.get().saturating_sub(self.0.get()))
    }

    /// Calculates the number of milliseconds between this timestamp and the earlier timestamp
    /// If the earlier timestamp is later than this timestamp, this will return zero.
    #[inline]
    pub const fn saturating_sub_ms(&self, earlier: Self) -> u64 {
        // Since both values have the same +1 shift internally, we can subtract
        // the internal representations directly: (a+1) - (b+1) = a - b
        self.0.get().saturating_sub(earlier.0.get())
    }

    /// Returns the amount of time elapsed from another instant to this one, or
    /// zero duration if that instant is later than this one.
    pub const fn duration_since(&self, earlier: Self) -> Duration {
        Duration::from_millis(self.0.get().saturating_sub(earlier.0.get()))
    }
}

impl From<MillisSinceEpoch> for u64 {
    #[inline]
    fn from(value: MillisSinceEpoch) -> Self {
        value.as_u64()
    }
}

impl From<u64> for MillisSinceEpoch {
    #[inline]
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl Add<Duration> for MillisSinceEpoch {
    type Output = MillisSinceEpoch;

    fn add(self, rhs: Duration) -> Self::Output {
        let millis =
            u64::try_from(rhs.as_millis()).expect("millis since Unix epoch should fit in u64");
        MillisSinceEpoch::new(self.as_u64().saturating_add(millis))
    }
}

impl Sub<Duration> for MillisSinceEpoch {
    type Output = MillisSinceEpoch;

    fn sub(self, rhs: Duration) -> Self::Output {
        let millis =
            u64::try_from(rhs.as_millis()).expect("millis since Unix epoch should fit in u64");
        MillisSinceEpoch::new(self.as_u64().saturating_sub(millis))
    }
}

impl From<SystemTime> for MillisSinceEpoch {
    fn from(value: SystemTime) -> Self {
        MillisSinceEpoch::new(
            u64::try_from(
                value
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("duration since Unix epoch should be well-defined")
                    .as_millis(),
            )
            .expect("millis since Unix epoch should fit in u64"),
        )
    }
}

#[cfg(feature = "prost-types")]
/// # Panics
/// If timestamp is out of range (e.g. older than UNIX_EPOCH) this conversion will panic.
impl From<prost_types::Timestamp> for MillisSinceEpoch {
    fn from(value: prost_types::Timestamp) -> Self {
        // safest approach is to convert into SystemTime first, then calculate distance to
        // UNIX_EPOCH
        Self::from(std::time::SystemTime::try_from(value).expect("Timestamp is after UNIX_EPOCH"))
    }
}

#[cfg(feature = "prost-types")]
impl From<MillisSinceEpoch> for prost_types::Timestamp {
    fn from(value: MillisSinceEpoch) -> Self {
        // safest approach is to convert into SystemTime first, then calculate distance to
        // UNIX_EPOCH
        Self::from(std::time::SystemTime::from(value))
    }
}

impl Display for MillisSinceEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ms since epoch", self.as_u64())
    }
}

impl From<MillisSinceEpoch> for SystemTime {
    fn from(value: MillisSinceEpoch) -> Self {
        SystemTime::UNIX_EPOCH.add(Duration::from_millis(value.as_u64()))
    }
}

mod serde_encoding {
    use super::MillisSinceEpoch;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    impl Serialize for MillisSinceEpoch {
        #[inline]
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.as_u64().serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for MillisSinceEpoch {
        #[inline]
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let millis = u64::deserialize(deserializer)?;
            Ok(Self::new(millis))
        }
    }
}

mod bilrost_encoding {
    use super::MillisSinceEpoch;

    use bilrost::Canonicity::Canonical;
    use bilrost::encoding::{DistinguishedProxiable, EmptyState, ForOverwrite, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind};

    impl Proxiable for MillisSinceEpoch {
        type Proxy = u64;

        fn encode_proxy(&self) -> Self::Proxy {
            self.as_u64()
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = Self::new(proxy);
            Ok(())
        }
    }

    impl DistinguishedProxiable for MillisSinceEpoch {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonical)
        }
    }

    impl ForOverwrite<(), MillisSinceEpoch> for () {
        fn for_overwrite() -> MillisSinceEpoch {
            MillisSinceEpoch::UNIX_EPOCH
        }
    }

    // Custom EmptyState implementation that matches u64 behavior:
    // - 0 is the empty/default value
    // - is_empty returns true only for 0 to match u64's behavior
    impl EmptyState<(), MillisSinceEpoch> for () {
        fn empty() -> MillisSinceEpoch {
            MillisSinceEpoch::UNIX_EPOCH
        }

        fn is_empty(value: &MillisSinceEpoch) -> bool {
            value.as_u64() == 0
        }

        fn clear(value: &mut MillisSinceEpoch) {
            *value = MillisSinceEpoch::UNIX_EPOCH;
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (MillisSinceEpoch)
        with general encodings including distinguished
    );
}

/// Nanos since the unix epoch. Used internally to get rough latency measurements across nodes.
/// It's vulnerable to clock skews and sync issues, so use with care. That said, it's fairly
/// accurate when used on the same node. This roughly maps to std::time::Instant except that the
/// value is portable across nodes.
///
/// # Internal Representation
///
/// This type uses [`NonZeroU64`] internally to enable niche optimization, allowing
/// `Option<NanosSinceEpoch>` to have the same size as `NanosSinceEpoch` (8 bytes).
///
/// The internal value is stored as `nanos + 1` (using wrapping arithmetic), which maps
/// the valid input range `0..=u64::MAX-1` to `1..=u64::MAX`. This assumes that
/// `u64::MAX` nanoseconds (~584 years from Unix epoch) will never be used
/// as an actual timestamp.
///
/// Serialization (serde, bilrost) always uses the original `u64` value for backward
/// compatibility.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct NanosSinceEpoch(NonZeroU64);

impl fmt::Debug for NanosSinceEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NanosSinceEpoch")
            .field(&self.as_u64())
            .finish()
    }
}

impl restate_encoding::NetSerde for NanosSinceEpoch {}

// Static assertions to ensure that NanosSinceEpoch is the same size as u64
// and that niche optimization works.
const _: () = {
    assert!(
        size_of::<u64>() == size_of::<NanosSinceEpoch>(),
        "NanosSinceEpoch should be the same size as u64"
    );

    assert!(
        size_of::<Option<NanosSinceEpoch>>() == size_of::<NanosSinceEpoch>(),
        "NanosSinceEpoch should be the same size as Option<NanosSinceEpoch>"
    );
};

impl NanosSinceEpoch {
    pub const UNIX_EPOCH: Self = Self::new(0);
    /// The maximum representable timestamp. Note: This is `u64::MAX - 1` because
    /// `u64::MAX` cannot be represented due to the internal NonZeroU64 representation.
    /// This is ~584 years from Unix epoch, so it's not a practical limitation.
    pub const MAX: Self = Self::new(u64::MAX - 1);

    /// Creates a new `NanosSinceEpoch` from the given nanoseconds value.
    ///
    /// Values are clamped to [`Self::MAX`] (which is `u64::MAX - 1`) since `u64::MAX`
    /// cannot be represented due to the internal `NonZeroU64` representation.
    /// This is not a practical limitation as `u64::MAX` nanoseconds is ~584 years
    /// from Unix epoch.
    pub const fn new(nanos_since_epoch: u64) -> Self {
        // We store nanos + 1 to enable niche optimization with NonZeroU64.
        // This maps 0..=u64::MAX-1 to 1..=u64::MAX (all valid NonZeroU64 values).
        // We use saturating_add to clamp u64::MAX to u64::MAX (which represents MAX).
        let shifted = nanos_since_epoch.saturating_add(1);
        // SAFETY: saturating_add(1) on any u64 value produces a value >= 1,
        // so this is always valid for NonZeroU64.
        unsafe { Self(NonZeroU64::new_unchecked(shifted)) }
    }

    pub fn now() -> Self {
        let recent_us = WallClock::recent_us();
        if recent_us > 0 {
            // the input is in microseconds, so we scale it to nanoseconds
            Self::new(recent_us * 1_000)
        } else {
            // In tests or binaries where the upkeep thread is not running, we don't
            // care about the performance of this call, so we can always call now()
            // but we keep is non-inlined to hint to the compiler that it's unlikely
            // the case.
            Self::from_system_now()
        }
    }

    // a hint to the compiler that this is unlikely to be called in the hot path.
    #[cold]
    #[inline(never)]
    fn from_system_now() -> Self {
        Self::new(WallClock::now_us() * 1_000)
    }

    /// Returns the nanoseconds since Unix epoch as a `u64`.
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        // Convert back from internal representation: stored = nanos + 1
        self.0.get().wrapping_sub(1)
    }

    /// Returns zero duration if self is in the future. Should not be used where monotonic
    /// clock/duration is expected.
    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(Self::now().0.get().saturating_sub(self.0.get()))
    }

    /// Returns the amount of time elapsed from another instant to this one, or
    /// zero duration if that instant is later than this one.
    pub const fn duration_since(&self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.get().saturating_sub(earlier.0.get()))
    }
}

impl Default for NanosSinceEpoch {
    fn default() -> Self {
        Self::now()
    }
}

impl From<u64> for NanosSinceEpoch {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<SystemTime> for NanosSinceEpoch {
    fn from(value: SystemTime) -> Self {
        Self::new(
            u64::try_from(
                value
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("duration since Unix epoch should be well-defined")
                    .as_nanos(),
            )
            .expect("nanos since Unix epoch should fit in u64"),
        )
    }
}

#[cfg(feature = "prost-types")]
/// # Panics
/// If timestamp is out of range (e.g. older than UNIX_EPOCH) this conversion will panic.
impl From<prost_types::Timestamp> for NanosSinceEpoch {
    fn from(value: prost_types::Timestamp) -> Self {
        // safest approach is to convert into SystemTime first, then calculate distance to
        // UNIX_EPOCH
        let ts = std::time::SystemTime::try_from(value).expect("Timestamp is after UNIX_EPOCH");
        Self::from(ts)
    }
}

impl From<NanosSinceEpoch> for MillisSinceEpoch {
    fn from(value: NanosSinceEpoch) -> Self {
        Self::new(value.as_u64() / 1_000_000)
    }
}

mod nanos_serde_encoding {
    use super::NanosSinceEpoch;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    impl Serialize for NanosSinceEpoch {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.as_u64().serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for NanosSinceEpoch {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let nanos = u64::deserialize(deserializer)?;
            Ok(Self::new(nanos))
        }
    }
}

mod nanos_bilrost_encoding {
    use super::NanosSinceEpoch;

    use bilrost::Canonicity::Canonical;
    use bilrost::encoding::{DistinguishedProxiable, EmptyState, ForOverwrite, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind};

    impl Proxiable for NanosSinceEpoch {
        type Proxy = u64;

        fn encode_proxy(&self) -> Self::Proxy {
            self.as_u64()
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = Self::new(proxy);
            Ok(())
        }
    }

    impl DistinguishedProxiable for NanosSinceEpoch {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonical)
        }
    }

    impl ForOverwrite<(), NanosSinceEpoch> for () {
        fn for_overwrite() -> NanosSinceEpoch {
            NanosSinceEpoch::UNIX_EPOCH
        }
    }

    // Custom EmptyState implementation that matches u64 behavior:
    // - 0 is the empty/default value
    // - is_empty returns true only for 0 to match u64's behavior
    impl EmptyState<(), NanosSinceEpoch> for () {
        fn empty() -> NanosSinceEpoch {
            NanosSinceEpoch::UNIX_EPOCH
        }

        fn is_empty(value: &NanosSinceEpoch) -> bool {
            value.as_u64() == 0
        }

        fn clear(value: &mut NanosSinceEpoch) {
            *value = NanosSinceEpoch::UNIX_EPOCH;
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (NanosSinceEpoch)
        with general encodings including distinguished
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    use bilrost::OwnedMessage;
    use std::time::SystemTime;

    #[test]
    fn millis_should_not_overflow() {
        // Test with MAX (which is u64::MAX - 1 due to niche optimization)
        let t: SystemTime = MillisSinceEpoch::MAX.into();
        println!("{t:?}");
    }

    #[test]
    fn nanos_should_not_overflow() {
        // it's ~580 years from unix epoch until u64 wouldn't become sufficient to store nanos.
        let t = NanosSinceEpoch::now().as_u64();
        assert!(t < u64::MAX);
        println!("{t:?}");
    }

    #[test]
    fn elapsed_saturating_to_zero() {
        let future = SystemTime::now().add(Duration::from_secs(10));
        let ms_epoch = MillisSinceEpoch::from(future);
        let ns_epoch = NanosSinceEpoch::from(future);

        assert_eq!(ms_epoch.elapsed(), Duration::ZERO);
        assert_eq!(ns_epoch.elapsed(), Duration::ZERO);
    }

    #[test]
    fn round_trip_values() {
        // Test round-trip for various values
        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let ts = MillisSinceEpoch::new(val);
            assert_eq!(ts.as_u64(), val, "Round-trip failed for {val}");
        }
    }

    #[test]
    fn unix_epoch_is_zero() {
        assert_eq!(MillisSinceEpoch::UNIX_EPOCH.as_u64(), 0);
    }

    #[test]
    fn max_is_max_minus_one() {
        assert_eq!(MillisSinceEpoch::MAX.as_u64(), u64::MAX - 1);
    }

    #[test]
    fn u64_max_clamps_to_max() {
        // u64::MAX is clamped to MAX (u64::MAX - 1) due to saturating_add
        let ts = MillisSinceEpoch::new(u64::MAX);
        assert_eq!(ts, MillisSinceEpoch::MAX);
        assert_eq!(ts.as_u64(), u64::MAX - 1);
    }

    #[test]
    fn serde_round_trip() {
        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let ts = MillisSinceEpoch::new(val);

            // JSON round-trip
            let json = serde_json::to_string(&ts).unwrap();
            assert_eq!(
                json,
                val.to_string(),
                "JSON serialization mismatch for {val}"
            );

            let deserialized: MillisSinceEpoch = serde_json::from_str(&json).unwrap();
            assert_eq!(
                deserialized.as_u64(),
                val,
                "JSON deserialization mismatch for {val}"
            );
        }
    }

    #[test]
    fn serde_backward_compatible_with_u64() {
        // This test verifies that our serde encoding is identical to u64
        // (equivalent to #[serde(transparent)] on a newtype wrapper)
        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let ts = MillisSinceEpoch::new(val);

            // Encode both as JSON
            let ts_json = serde_json::to_string(&ts).unwrap();
            let u64_json = serde_json::to_string(&val).unwrap();
            assert_eq!(
                ts_json, u64_json,
                "JSON encoding differs from u64 for value {val}"
            );

            // Cross-decode: decode u64 JSON as MillisSinceEpoch
            let decoded_ts: MillisSinceEpoch = serde_json::from_str(&u64_json).unwrap();
            assert_eq!(
                decoded_ts.as_u64(),
                val,
                "Cross-decode u64->MillisSinceEpoch failed for {val}"
            );

            // Cross-decode: decode MillisSinceEpoch JSON as u64
            let decoded_u64: u64 = serde_json::from_str(&ts_json).unwrap();
            assert_eq!(
                decoded_u64, val,
                "Cross-decode MillisSinceEpoch->u64 failed for {val}"
            );
        }
    }

    #[test]
    fn ordering_preserved() {
        let a = MillisSinceEpoch::new(100);
        let b = MillisSinceEpoch::new(200);
        let c = MillisSinceEpoch::new(100);

        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, c);
    }

    #[test]
    fn bilrost_round_trip() {
        use bilrost::Message;

        // Create a wrapper message for testing bilrost encoding
        #[derive(bilrost::Message, PartialEq, Debug)]
        struct TestMessage {
            #[bilrost(1)]
            timestamp: MillisSinceEpoch,
        }

        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let msg = TestMessage {
                timestamp: MillisSinceEpoch::new(val),
            };

            // Encode
            let encoded = msg.encode_to_vec();

            // Decode
            let decoded = TestMessage::decode(encoded.as_slice()).unwrap();
            assert_eq!(
                decoded.timestamp.as_u64(),
                val,
                "Bilrost round-trip failed for {val}"
            );
        }
    }

    #[test]
    fn bilrost_backward_compatible_with_u64() {
        use bilrost::Message;

        // This test verifies that our custom encoding produces the same bytes as u64
        // ensuring backward compatibility with existing data

        #[derive(bilrost::Message, PartialEq, Debug, Default)]
        struct U64Message {
            #[bilrost(1)]
            value: u64,
        }

        #[derive(bilrost::Message, PartialEq, Debug)]
        struct TimestampMessage {
            #[bilrost(1)]
            timestamp: MillisSinceEpoch,
        }

        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let u64_msg = U64Message { value: val };
            let ts_msg = TimestampMessage {
                timestamp: MillisSinceEpoch::new(val),
            };

            let u64_encoded = u64_msg.encode_to_vec();
            let ts_encoded = ts_msg.encode_to_vec();

            assert_eq!(
                u64_encoded, ts_encoded,
                "Bilrost encoding differs from u64 for value {val}"
            );

            // Cross-decode: decode u64 bytes as timestamp
            let decoded_ts = TimestampMessage::decode(u64_encoded.as_slice()).unwrap();
            assert_eq!(
                decoded_ts.timestamp.as_u64(),
                val,
                "Cross-decode failed for {val}"
            );
        }
    }

    #[test]
    fn debug_format() {
        let ts = MillisSinceEpoch::new(12345);
        assert_eq!(format!("{:?}", ts), "MillisSinceEpoch(12345)");
    }

    // --- NanosSinceEpoch tests ---

    #[test]
    fn nanos_round_trip_values() {
        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let ts = NanosSinceEpoch::new(val);
            assert_eq!(
                ts.as_u64(),
                val,
                "NanosSinceEpoch round-trip failed for {val}"
            );
        }
    }

    #[test]
    fn nanos_unix_epoch_is_zero() {
        assert_eq!(NanosSinceEpoch::UNIX_EPOCH.as_u64(), 0);
    }

    #[test]
    fn nanos_max_is_max_minus_one() {
        assert_eq!(NanosSinceEpoch::MAX.as_u64(), u64::MAX - 1);
    }

    #[test]
    fn nanos_u64_max_clamps_to_max() {
        let ts = NanosSinceEpoch::new(u64::MAX);
        assert_eq!(ts, NanosSinceEpoch::MAX);
        assert_eq!(ts.as_u64(), u64::MAX - 1);
    }

    #[test]
    fn nanos_serde_round_trip() {
        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let ts = NanosSinceEpoch::new(val);

            let json = serde_json::to_string(&ts).unwrap();
            assert_eq!(
                json,
                val.to_string(),
                "NanosSinceEpoch JSON serialization mismatch for {val}"
            );

            let deserialized: NanosSinceEpoch = serde_json::from_str(&json).unwrap();
            assert_eq!(
                deserialized.as_u64(),
                val,
                "NanosSinceEpoch JSON deserialization mismatch for {val}"
            );
        }
    }

    #[test]
    fn nanos_serde_backward_compatible_with_u64() {
        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let ts = NanosSinceEpoch::new(val);

            // Encode both as JSON
            let ts_json = serde_json::to_string(&ts).unwrap();
            let u64_json = serde_json::to_string(&val).unwrap();
            assert_eq!(
                ts_json, u64_json,
                "NanosSinceEpoch JSON encoding differs from u64 for value {val}"
            );

            // Cross-decode: decode u64 JSON as NanosSinceEpoch
            let decoded_ts: NanosSinceEpoch = serde_json::from_str(&u64_json).unwrap();
            assert_eq!(
                decoded_ts.as_u64(),
                val,
                "Cross-decode u64->NanosSinceEpoch failed for {val}"
            );

            // Cross-decode: decode NanosSinceEpoch JSON as u64
            let decoded_u64: u64 = serde_json::from_str(&ts_json).unwrap();
            assert_eq!(
                decoded_u64, val,
                "Cross-decode NanosSinceEpoch->u64 failed for {val}"
            );
        }
    }

    #[test]
    fn nanos_ordering_preserved() {
        let a = NanosSinceEpoch::new(100);
        let b = NanosSinceEpoch::new(200);
        let c = NanosSinceEpoch::new(100);

        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, c);
    }

    #[test]
    fn nanos_bilrost_round_trip() {
        use bilrost::Message;

        #[derive(bilrost::Message, PartialEq, Debug)]
        struct TestMessage {
            #[bilrost(1)]
            timestamp: NanosSinceEpoch,
        }

        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let msg = TestMessage {
                timestamp: NanosSinceEpoch::new(val),
            };

            let encoded = msg.encode_to_vec();
            let decoded = TestMessage::decode(encoded.as_slice()).unwrap();
            assert_eq!(
                decoded.timestamp.as_u64(),
                val,
                "NanosSinceEpoch bilrost round-trip failed for {val}"
            );
        }
    }

    #[test]
    fn nanos_bilrost_backward_compatible_with_u64() {
        use bilrost::Message;

        #[derive(bilrost::Message, PartialEq, Debug, Default)]
        struct U64Message {
            #[bilrost(1)]
            value: u64,
        }

        #[derive(bilrost::Message, PartialEq, Debug)]
        struct NanosMessage {
            #[bilrost(1)]
            timestamp: NanosSinceEpoch,
        }

        let test_values = [0u64, 1, 100, 1000, u64::MAX / 2, u64::MAX - 1];

        for &val in &test_values {
            let u64_msg = U64Message { value: val };
            let ts_msg = NanosMessage {
                timestamp: NanosSinceEpoch::new(val),
            };

            let u64_encoded = u64_msg.encode_to_vec();
            let ts_encoded = ts_msg.encode_to_vec();

            assert_eq!(
                u64_encoded, ts_encoded,
                "NanosSinceEpoch bilrost encoding differs from u64 for value {val}"
            );

            // Cross-decode: decode u64 bytes as NanosSinceEpoch
            let decoded_ts = NanosMessage::decode(u64_encoded.as_slice()).unwrap();
            assert_eq!(
                decoded_ts.timestamp.as_u64(),
                val,
                "NanosSinceEpoch cross-decode failed for {val}"
            );
        }
    }

    #[test]
    fn nanos_debug_format() {
        let ts = NanosSinceEpoch::new(12345);
        assert_eq!(format!("{:?}", ts), "NanosSinceEpoch(12345)");
    }
}
