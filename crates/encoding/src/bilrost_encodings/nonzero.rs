// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZero;

use bilrost::{
    DecodeErrorKind,
    encoding::{ForOverwrite, Proxiable},
};

use crate::bilrost_encodings::RestateEncoding;

struct NonZeroTag;

macro_rules! impl_nonzero_encoding {

    ($ty:ty) => {
        impl Proxiable<NonZeroTag> for NonZero<$ty> {
            type Proxy = $ty;
            fn encode_proxy(&self) -> Self::Proxy {
                self.get()
            }

            fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
                let v = NonZero::new(proxy).ok_or(DecodeErrorKind::InvalidValue)?;
                *self = v;
                Ok(())
            }
        }

        impl ForOverwrite<RestateEncoding, NonZero<$ty>> for () {
            fn for_overwrite() -> NonZero<$ty> {
                NonZero::new(1).unwrap()
            }
        }


        bilrost::delegate_proxied_encoding!(
            use encoding (bilrost::encoding::General)
            to encode proxied type (NonZero<$ty>) using proxy tag (NonZeroTag)
            with encoding (RestateEncoding)
        );
    };
    ($($ty:ty),+) => {
        $(impl_nonzero_encoding!($ty);)+
    };
}

impl_nonzero_encoding!(u64, u32, u16, usize);

#[cfg(test)]
mod test {

    use std::num::NonZeroU64;

    use bytes::Bytes;

    use super::*;

    #[derive(bilrost::Message)]
    struct NonZeroMessage {
        #[bilrost(tag(1), encoding(RestateEncoding))]
        inner: Option<NonZero<u64>>,
    }

    #[test]
    fn test_range_encoding() {
        let message = NonZeroMessage {
            inner: Some(NonZeroU64::new(10).unwrap()),
        };
        let encoded = <NonZeroMessage as bilrost::Message>::encode_to_vec(&message);

        let decoded =
            <NonZeroMessage as bilrost::OwnedMessage>::decode(Bytes::from(encoded)).unwrap();
        assert_eq!(message.inner, decoded.inner);
    }
}
