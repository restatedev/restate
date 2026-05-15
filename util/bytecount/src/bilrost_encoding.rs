// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::Canonicity::Canonical;
use bilrost::encoding::{DistinguishedProxiable, ForOverwrite, Proxiable};
use bilrost::{Canonicity, DecodeErrorKind};

use crate::ByteCount;

impl Proxiable for ByteCount<true> {
    type Proxy = u64;

    fn encode_proxy(&self) -> Self::Proxy {
        self.as_u64()
    }

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        *self = ByteCount(proxy);
        Ok(())
    }
}

impl DistinguishedProxiable for ByteCount<true> {
    fn decode_proxy_distinguished(
        &mut self,
        proxy: Self::Proxy,
    ) -> Result<Canonicity, DecodeErrorKind> {
        self.decode_proxy(proxy)?;
        Ok(Canonical)
    }
}

bilrost::empty_state_via_default!(ByteCount<true>);

bilrost::delegate_proxied_encoding!(
    use encoding (bilrost::encoding::Varint)
    to encode proxied type (ByteCount<true>)
    with general encodings including distinguished
);

impl Proxiable for ByteCount<false> {
    type Proxy = u64;

    fn encode_proxy(&self) -> Self::Proxy {
        self.as_u64()
    }

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        *self = Self::try_from(proxy).map_err(|_| DecodeErrorKind::InvalidValue)?;
        Ok(())
    }
}

impl DistinguishedProxiable for ByteCount<false> {
    fn decode_proxy_distinguished(
        &mut self,
        proxy: Self::Proxy,
    ) -> Result<Canonicity, DecodeErrorKind> {
        self.decode_proxy(proxy)?;
        Ok(Canonical)
    }
}

impl ForOverwrite<(), ByteCount<false>> for () {
    fn for_overwrite() -> ByteCount<false> {
        // default to MIN (1 in case of non-zero). This value is never used
        // it's simply an initialization value until decoding is done.
        ByteCount(1)
    }
}

bilrost::empty_state_via_for_overwrite!(ByteCount<false>);

bilrost::delegate_proxied_encoding!(
    use encoding (bilrost::encoding::Varint)
    to encode proxied type (ByteCount<false>)
    with general encodings including distinguished
);
