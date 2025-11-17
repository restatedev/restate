// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod entry;
pub mod metadata;
mod store;
mod tables;

use std::hash::Hash;

use restate_types::clock::UniqueTimestamp;
use restate_types::time::MillisSinceEpoch;

pub use entry::*;
pub use store::*;
pub use tables::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VisibleAt {
    Now,
    At(UniqueTimestamp),
}

impl VisibleAt {
    pub fn from_unix_millis(millis: MillisSinceEpoch) -> Self {
        VisibleAt::At(UniqueTimestamp::from_unix_millis(millis).unwrap())
    }

    pub fn from_raw(raw: u64) -> Self {
        if raw == 0 {
            return VisibleAt::Now;
        }
        VisibleAt::At(UniqueTimestamp::try_from(raw).unwrap())
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            VisibleAt::Now => 0,
            VisibleAt::At(ts) => ts.as_u64(),
        }
    }

    pub fn is_visible(&self, now: UniqueTimestamp) -> bool {
        match self {
            VisibleAt::Now => true,
            VisibleAt::At(ts) => *ts <= now,
        }
    }
}

impl From<Option<MillisSinceEpoch>> for VisibleAt {
    fn from(value: Option<MillisSinceEpoch>) -> Self {
        value.map_or(VisibleAt::Now, VisibleAt::from_unix_millis)
    }
}

impl PartialEq<UniqueTimestamp> for VisibleAt {
    fn eq(&self, other: &UniqueTimestamp) -> bool {
        match self {
            VisibleAt::Now => false,
            VisibleAt::At(ts) => ts == other,
        }
    }
}

impl PartialOrd<UniqueTimestamp> for VisibleAt {
    fn partial_cmp(&self, other: &UniqueTimestamp) -> Option<std::cmp::Ordering> {
        match self {
            VisibleAt::Now => None,
            VisibleAt::At(ts) => ts.partial_cmp(other),
        }
    }
}

mod bilrost_encoding {
    use super::*;

    use bilrost::Canonicity::Canonical;
    use bilrost::encoding::{DistinguishedProxiable, ForOverwrite, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind};

    impl Proxiable for VisibleAt {
        type Proxy = u64;

        fn encode_proxy(&self) -> Self::Proxy {
            self.as_u64()
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = Self::from_raw(proxy);
            Ok(())
        }
    }

    impl DistinguishedProxiable for VisibleAt {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonical)
        }
    }

    impl ForOverwrite<(), VisibleAt> for () {
        fn for_overwrite() -> VisibleAt {
            VisibleAt::Now
        }
    }

    bilrost::empty_state_via_for_overwrite!(VisibleAt);

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (VisibleAt)
        with general encodings including distinguished
    );
}
