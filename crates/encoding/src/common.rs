// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::{
    Canonicity, DecodeErrorKind,
    encoding::{DistinguishedProxiable, EmptyState, ForOverwrite, Proxiable},
};
use restate_platform::network::NetSerde;

use crate::bilrost_encodings::RestateEncoding;

struct U128Tag;

impl Proxiable<U128Tag> for u128 {
    type Proxy = (u64, u64);

    fn encode_proxy(&self) -> Self::Proxy {
        ((*self >> 64) as u64, *self as u64)
    }

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        *self = (proxy.0 as u128) << 64 | proxy.1 as u128;
        Ok(())
    }
}

impl ForOverwrite<RestateEncoding, u128> for () {
    fn for_overwrite() -> u128 {
        0
    }
}

impl EmptyState<RestateEncoding, u128> for () {
    fn empty() -> u128 {
        0
    }

    fn is_empty(val: &u128) -> bool {
        *val == 0
    }

    fn clear(val: &mut u128) {
        *val = 0;
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (::bilrost::encoding::General)
    to encode proxied type (u128)
    using proxy tag (U128Tag)
    with encoding (RestateEncoding)
);

/// A Bilrost compatible U128 type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct U128([u8; core::mem::size_of::<u128>()]);

impl From<u128> for U128 {
    fn from(value: u128) -> Self {
        Self(value.to_le_bytes())
    }
}

impl From<U128> for u128 {
    fn from(value: U128) -> Self {
        Self::from_le_bytes(value.0)
    }
}

impl NetSerde for U128 {}

struct GeneralU128Tag;

impl Proxiable<GeneralU128Tag> for U128 {
    type Proxy = (u64, u64);

    fn encode_proxy(&self) -> Self::Proxy {
        let value = u128::from(*self);
        ((value >> 64) as u64, value as u64)
    }

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        *self = U128::from((u128::from(proxy.0) << 64) | u128::from(proxy.1));
        Ok(())
    }
}

impl DistinguishedProxiable<GeneralU128Tag> for U128 {
    fn decode_proxy_distinguished(
        &mut self,
        proxy: Self::Proxy,
    ) -> Result<Canonicity, DecodeErrorKind> {
        <U128 as Proxiable<GeneralU128Tag>>::decode_proxy(self, proxy)?;
        Ok(Canonicity::Canonical)
    }
}

impl ForOverwrite<(), U128> for () {
    fn for_overwrite() -> U128 {
        U128([0; core::mem::size_of::<u128>()])
    }
}

impl EmptyState<(), U128> for () {
    fn empty() -> U128 {
        U128([0; core::mem::size_of::<u128>()])
    }

    fn is_empty(val: &U128) -> bool {
        val.0 == [0; core::mem::size_of::<u128>()]
    }

    fn clear(val: &mut U128) {
        val.0 = [0; core::mem::size_of::<u128>()];
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (bilrost::encoding::General)
    to encode proxied type (U128) using proxy tag (GeneralU128Tag)
    with general encodings including distinguished
);

struct FixedU128Tag;

// Keep the general `U128` encoding as the historical `(u64, u64)` tuple above
// for existing wire compatibility. The fixed encoding is opt-in via
// `#[bilrost(encoding(fixed))]` and uses a length-delimited 16-byte little-endian
// payload because bilrost has no fixed128 scalar wire type.
impl Proxiable<FixedU128Tag> for U128 {
    type Proxy = [u8; core::mem::size_of::<u128>()];

    fn encode_proxy(&self) -> Self::Proxy {
        self.0
    }

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        self.0 = proxy;
        Ok(())
    }
}

impl DistinguishedProxiable<FixedU128Tag> for U128 {
    fn decode_proxy_distinguished(
        &mut self,
        proxy: Self::Proxy,
    ) -> Result<Canonicity, DecodeErrorKind> {
        <U128 as Proxiable<FixedU128Tag>>::decode_proxy(self, proxy)?;
        Ok(Canonicity::Canonical)
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (bilrost::encoding::PlainBytes)
    to encode proxied type (U128) using proxy tag (FixedU128Tag)
    with encoding (bilrost::encoding::Fixed)
    including distinguished
);

#[cfg(test)]
mod test {
    use bilrost::{Message, OwnedMessage};
    use rand::random;

    use super::U128;

    #[test]
    fn u128() {
        (0..100).for_each(|_| {
            let num = random::<u128>();
            let value = U128::from(num);

            assert_eq!(num, u128::from(value));
        });
    }

    #[test]
    fn u128_fixed_bilrost_round_trip() {
        #[derive(Debug, PartialEq, bilrost::Message)]
        struct EncodedU128 {
            #[bilrost(tag(1), encoding(fixed))]
            value: U128,
        }

        let raw = 0x1234_5678_90ab_cdef_0123_4567_89ab_cdef;
        let value = EncodedU128 {
            value: U128::from(raw),
        };
        let encoded = value.encode_to_bytes();

        assert_eq!(encoded.len(), 18);
        assert_eq!(encoded[0], 0x05);
        assert_eq!(encoded[1], 16);
        assert_eq!(&encoded[2..], &raw.to_le_bytes());
        assert_eq!(EncodedU128::decode(encoded).unwrap(), value);
    }

    #[test]
    fn u128_general_bilrost_keeps_high_low_tuple_encoding() {
        #[derive(Debug, PartialEq, bilrost::Message)]
        struct EncodedU128 {
            #[bilrost(tag(1))]
            value: U128,
        }

        #[derive(Debug, PartialEq, bilrost::Message)]
        struct HistoricalEncodedU128 {
            #[bilrost(tag(1))]
            value: (u64, u64),
        }

        let raw = 0x1234_5678_90ab_cdef_0123_4567_89ab_cdef;
        let value = EncodedU128 {
            value: U128::from(raw),
        };
        let historical = HistoricalEncodedU128 {
            value: ((raw >> 64) as u64, raw as u64),
        };
        let encoded = value.encode_to_bytes();

        assert_eq!(encoded, historical.encode_to_bytes());
        assert_eq!(EncodedU128::decode(encoded).unwrap(), value);
    }
}
