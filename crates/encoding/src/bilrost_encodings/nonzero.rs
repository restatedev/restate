// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
                // This function is called by the decoder to get a "zero" value of this type (NonZero<T> in this case)
                // that then immediately gets overridden by the `decoder_proxy`
                // But we can't get a `NonZero<T>` with a value of 0, so we use 1 instead.
                // so we can use any value here. The compiler will optimize it out.
                //
                // For more details see https://github.com/restatedev/restate/pull/3359#discussion_r2132719080
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
