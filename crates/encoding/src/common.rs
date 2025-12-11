// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::{
    DecodeErrorKind,
    encoding::{EmptyState, ForOverwrite, Proxiable},
};
use restate_encoding_derive::BilrostNewType;

use crate::{NetSerde, bilrost_encodings::RestateEncoding};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, BilrostNewType)]
pub struct U128((u64, u64));

impl From<u128> for U128 {
    fn from(value: u128) -> Self {
        Self(((value >> 64) as u64, value as u64))
    }
}

impl From<U128> for u128 {
    fn from(value: U128) -> Self {
        (value.0.0 as u128) << 64 | value.0.1 as u128
    }
}

impl NetSerde for U128 {}

#[cfg(test)]
mod test {
    use rand::random;

    use super::U128;

    #[test]
    fn test_u128() {
        (0..100).for_each(|_| {
            let num = random::<u128>();
            let value = U128::from(num);

            assert_eq!(num, u128::from(value));
        });
    }
}
