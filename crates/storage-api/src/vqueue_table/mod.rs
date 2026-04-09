// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
pub mod scheduler;
mod store;
mod tables;

use std::fmt::{Display, Formatter};
use std::hash::Hash;

use restate_clock::rough_ts::RoughTimestamp;
use restate_clock::time::MillisSinceEpoch;

pub use entry::*;
pub use store::*;
pub use tables::*;

pub type Seq = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct RunAt(RoughTimestamp);

const _: () = {
    assert!(
        size_of::<u32>() == size_of::<RunAt>(),
        "RunAt should be the same size as RoughTimestamp"
    );
};

impl RunAt {
    pub const fn new(millis: MillisSinceEpoch) -> Self {
        Self(RoughTimestamp::from_unix_millis_clamped(millis))
    }

    pub const fn from_raw(raw: u64) -> Self {
        Self::new(MillisSinceEpoch::new(raw))
    }

    pub const fn as_u64(&self) -> u64 {
        self.as_millis().as_u64()
    }

    pub const fn as_millis(&self) -> MillisSinceEpoch {
        self.0.as_unix_millis()
    }

    pub fn can_run(&self, now: MillisSinceEpoch) -> bool {
        self.as_millis() <= now
    }
}

impl From<MillisSinceEpoch> for RunAt {
    fn from(value: MillisSinceEpoch) -> Self {
        Self::new(value)
    }
}

impl PartialEq<MillisSinceEpoch> for RunAt {
    fn eq(&self, other: &MillisSinceEpoch) -> bool {
        self.as_millis() == *other
    }
}

impl PartialOrd<MillisSinceEpoch> for RunAt {
    fn partial_cmp(&self, other: &MillisSinceEpoch) -> Option<std::cmp::Ordering> {
        self.as_millis().partial_cmp(other)
    }
}

impl Display for RunAt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.as_millis(), f)
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
    pub global_invoker_throttling_ms: u32,
    /// Total milliseconds the item spent waiting on invoker memory pool
    #[bilrost(tag(4))]
    pub blocked_on_invoker_memory_ms: u32,
}

mod bilrost_encoding {
    use super::*;

    use bilrost::Canonicity::Canonical;
    use bilrost::encoding::{DistinguishedProxiable, ForOverwrite, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind};

    impl Proxiable for RunAt {
        type Proxy = u64;

        fn encode_proxy(&self) -> Self::Proxy {
            self.as_u64()
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = Self::from_raw(proxy);
            Ok(())
        }
    }

    impl DistinguishedProxiable for RunAt {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonical)
        }
    }

    impl ForOverwrite<(), RunAt> for () {
        fn for_overwrite() -> RunAt {
            RunAt(RoughTimestamp::RESTATE_EPOCH)
        }
    }

    bilrost::empty_state_via_for_overwrite!(RunAt);

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (RunAt)
        with general encodings including distinguished
    );
}
