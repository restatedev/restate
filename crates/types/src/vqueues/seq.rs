// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::logs::Lsn;

/// Sequence number used by vqueue entries.
///
/// `Seq` is intentionally limited to 56 bits to allow future keys to stores its
/// sequence component in 7 bytes (the remaining byte in a `u64` is not encoded).
///
/// Construction wraps to the low 56 bits (`value & ((1 << 56) - 1)`), so callers should
/// treat `Seq::new` as a lossy conversion from `u64`.
///
/// Current limitations:
/// - values larger than [`Seq::MAX`] are wrapped, not rejected;
/// - `0` is currently a valid value.
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    derive_more::Deref,
    derive_more::Into,
    derive_more::Display,
)]
#[repr(transparent)]
pub struct Seq(u64);

impl Seq {
    /// Maximum representable sequence value (56 bits set).
    pub const MAX: Self = Seq((1u64 << 56) - 1);
    /// Minimum representable sequence value. Use when deduplication is done externally
    /// or if the order doesn't matter (e.g. migration from old data).
    pub const MIN: Self = Seq(0);

    /// Creates a [`Seq`] by keeping only the low 56 bits of `seq`.
    pub const fn new(seq: u64) -> Self {
        Self(seq & Self::MAX.0)
    }

    /// Returns this sequence as a primitive `u64`.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for Seq {
    #[inline]
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<Lsn> for Seq {
    #[inline]
    fn from(value: Lsn) -> Self {
        Self::new(value.as_u64())
    }
}

mod bilrost_encoding {
    use super::Seq;

    use bilrost::encoding::{DistinguishedProxiable, EmptyState, ForOverwrite, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind};

    impl Proxiable for Seq {
        type Proxy = u64;

        fn encode_proxy(&self) -> Self::Proxy {
            self.as_u64()
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = Self::new(proxy);
            Ok(())
        }
    }

    impl DistinguishedProxiable for Seq {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonicity::Canonical)
        }
    }

    impl ForOverwrite<(), Seq> for () {
        fn for_overwrite() -> Seq {
            Seq::MIN
        }
    }

    impl EmptyState<(), Seq> for () {
        fn empty() -> Seq {
            Seq::MIN
        }

        fn is_empty(value: &Seq) -> bool {
            value == &Seq::MIN
        }

        fn clear(value: &mut Seq) {
            *value = Seq::MIN;
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (Seq)
        with general encodings including distinguished
    );
}

#[cfg(test)]
mod tests {
    use super::Seq;

    #[test]
    fn new_wraps_to_56_bits() {
        assert_eq!(Seq::new(Seq::MAX.as_u64()).as_u64(), Seq::MAX.as_u64());
        assert_eq!(Seq::new(1u64 << 56).as_u64(), 0);
        assert_eq!(Seq::new((1u64 << 56) + 7).as_u64(), 7);
    }
}
