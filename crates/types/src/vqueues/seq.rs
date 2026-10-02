// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use zerocopy::big_endian::{self};

use crate::logs::Lsn;

/// Sequence number used by vqueue entries.
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
    zerocopy::Immutable,
    zerocopy::KnownLayout,
    zerocopy::IntoBytes,
    zerocopy::FromZeros,
    zerocopy::Unaligned,
)]
#[repr(transparent)]
pub struct Seq(big_endian::U64);

impl Seq {
    /// Maximum representable sequence value
    pub const MAX: Self = Seq(big_endian::U64::MAX_VALUE);
    /// Minimum representable sequence value. Use when deduplication is done externally
    /// or if the order doesn't matter (e.g. migration from old data).
    pub const MIN: Self = Seq(big_endian::U64::ZERO);

    /// Creates a [`Seq`] value.
    pub const fn new(seq: u64) -> Self {
        Self(big_endian::U64::new(seq))
    }

    /// Returns this sequence as a primitive `u64`.
    pub const fn as_u64(self) -> u64 {
        self.0.get()
    }

    /// Encodes into 8 bytes (big-endian)
    pub const fn to_bytes(self) -> [u8; 8] {
        zerocopy::transmute!(self)
    }

    /// Decodes from raw byte representation (big-endian)
    pub const fn from_bytes(bytes: [u8; 8]) -> Self {
        Seq(big_endian::U64::from_bytes(bytes))
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

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Fixed)
        to encode proxied type (Seq)
        with encoding (bilrost::encoding::Fixed)
        including distinguished
    );
}

#[cfg(test)]
mod tests {
    use super::Seq;

    #[test]
    fn fixed_encoding_round_trips() {
        use bilrost::{Message, OwnedMessage};

        #[derive(Debug, PartialEq, bilrost::Message)]
        struct EncodedSeq {
            #[bilrost(tag(1), encoding(fixed))]
            value: Seq,
        }

        let value = EncodedSeq { value: Seq::MAX };
        let encoded = value.encode_to_bytes();

        assert_eq!(encoded.len(), 9);
        assert_eq!(EncodedSeq::decode(encoded).unwrap(), value);
    }
}
