// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Identifying the partition
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    derive_more::Add,
    derive_more::Display,
    derive_more::Debug,
    derive_more::FromStr,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[repr(transparent)]
#[debug("{}", _0)]
pub struct PartitionId(u16);

impl From<PartitionId> for u32 {
    fn from(value: PartitionId) -> Self {
        u32::from(value.0)
    }
}

impl From<PartitionId> for u64 {
    fn from(value: PartitionId) -> Self {
        u64::from(value.0)
    }
}

impl PartitionId {
    /// It's your responsibility to ensure the value is within the valid range.
    pub const fn new_unchecked(v: u16) -> Self {
        Self(v)
    }

    pub const MIN: Self = Self(u16::MIN);
    // 65535 partitions.
    pub const MAX: Self = Self(u16::MAX);

    #[inline]
    pub fn next(self) -> Self {
        Self(std::cmp::min(*Self::MAX, self.0.saturating_add(1)))
    }
}

#[cfg(feature = "bilrost")]
mod bilrost_impl {
    use super::PartitionId;

    use bilrost::DecodeErrorKind;
    use bilrost::encoding::{EmptyState, ForOverwrite, Proxiable};

    impl Proxiable for PartitionId {
        type Proxy = u16;

        fn encode_proxy(&self) -> Self::Proxy {
            self.0
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            self.0 = proxy;
            Ok(())
        }
    }

    impl ForOverwrite<(), PartitionId> for () {
        fn for_overwrite() -> PartitionId {
            PartitionId(0)
        }
    }

    impl EmptyState<(), PartitionId> for () {
        fn empty() -> PartitionId {
            PartitionId(0)
        }

        fn is_empty(val: &PartitionId) -> bool {
            val.0 == 0
        }

        fn clear(val: &mut PartitionId) {
            val.0 = 0;
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Varint)
        to encode proxied type (PartitionId)
        with general encodings
    );
}
