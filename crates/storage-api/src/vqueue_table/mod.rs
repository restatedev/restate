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

use restate_clock::time::MillisSinceEpoch;

pub use entry::*;
pub use store::*;
pub use tables::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VisibleAt {
    Now,
    At(MillisSinceEpoch),
}

const _: () = {
    assert!(
        size_of::<MillisSinceEpoch>() == size_of::<VisibleAt>(),
        "VisibleAt should be the same size as MilliSinceEpoch"
    );
};

impl VisibleAt {
    pub const fn new(millis: MillisSinceEpoch) -> Self {
        if millis.is_zero() {
            Self::Now
        } else {
            Self::At(millis)
        }
    }

    pub const fn from_raw(raw: u64) -> Self {
        if raw == 0 {
            return Self::Now;
        }
        Self::At(MillisSinceEpoch::new(raw))
    }

    pub const fn as_u64(&self) -> u64 {
        match self {
            Self::Now => 0,
            Self::At(ts) => ts.as_u64(),
        }
    }

    pub fn is_visible(&self, now: MillisSinceEpoch) -> bool {
        match self {
            Self::Now => true,
            Self::At(ts) => *ts <= now,
        }
    }
}

impl From<Option<MillisSinceEpoch>> for VisibleAt {
    fn from(value: Option<MillisSinceEpoch>) -> Self {
        value.map_or(Self::Now, Self::new)
    }
}

impl PartialEq<MillisSinceEpoch> for VisibleAt {
    fn eq(&self, other: &MillisSinceEpoch) -> bool {
        match self {
            Self::Now => false,
            Self::At(ts) => ts == other,
        }
    }
}

impl PartialOrd<MillisSinceEpoch> for VisibleAt {
    fn partial_cmp(&self, other: &MillisSinceEpoch) -> Option<std::cmp::Ordering> {
        match self {
            Self::Now => None,
            Self::At(ts) => ts.partial_cmp(other),
        }
    }
}

/// Some stats that are collected from the scheduler. Emitted along the item
/// at decision time.
#[derive(Debug, Clone, Default, bilrost::Message)]
pub struct WaitStats {
    /// Total milliseconds the item spent waiting on global invoker capacity
    #[bilrost(tag(1))]
    pub blocked_on_global_capacity_ms: u32,
    /// Total milliseconds the item was throttled on vqueue's "start" token bucket
    #[bilrost(tag(2))]
    pub vqueue_start_throttling_ms: u32,
    /// Total milliseconds the item was throttled on global "run" token bucket
    #[bilrost(tag(3))]
    pub global_throttling_ms: u32,
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
