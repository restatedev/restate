// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use bilrost::{
    DecodeErrorKind,
    encoding::{EmptyState, ForOverwrite, Proxiable},
};

use crate::bilrost_encodings::RestateEncoding;

struct RangeTag;

impl<T> Proxiable<RangeTag> for RangeInclusive<T>
where
    T: Default + Copy,
{
    type Proxy = (T, T);

    fn encode_proxy(&self) -> Self::Proxy {
        (*self.start(), *self.end())
    }

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        *self = proxy.0..=proxy.1;
        Ok(())
    }
}

impl<T> ForOverwrite<RestateEncoding, RangeInclusive<T>> for ()
where
    T: Default,
{
    fn for_overwrite() -> RangeInclusive<T> {
        T::default()..=T::default()
    }
}

impl<T> EmptyState<RestateEncoding, RangeInclusive<T>> for ()
where
    T: Default + Copy + PartialEq<T>,
{
    fn empty() -> RangeInclusive<T> {
        T::default()..=T::default()
    }

    fn is_empty(val: &RangeInclusive<T>) -> bool {
        let empty = T::default();
        *val.start() == empty && *val.end() == empty
    }

    fn clear(val: &mut RangeInclusive<T>) {
        *val = Self::empty();
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (::bilrost::encoding::General)
    to encode proxied type (RangeInclusive<u64>)
    using proxy tag (RangeTag)
    with encoding (RestateEncoding)
);

impl ForOverwrite<RestateEncoding, restate_util_sharding::KeyRange> for () {
    fn for_overwrite() -> restate_util_sharding::KeyRange {
        restate_util_sharding::KeyRange::new(0, 0)
    }
}

impl EmptyState<RestateEncoding, restate_util_sharding::KeyRange> for () {
    fn empty() -> restate_util_sharding::KeyRange {
        restate_util_sharding::KeyRange::new(0, 0)
    }

    fn is_empty(val: &restate_util_sharding::KeyRange) -> bool {
        val.start() == 0 && val.end() == 0
    }

    fn clear(val: &mut restate_util_sharding::KeyRange) {
        *val = <() as EmptyState<RestateEncoding, restate_util_sharding::KeyRange>>::empty();
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (::bilrost::encoding::General)
    to encode proxied type (restate_util_sharding::KeyRange)
    with encoding (RestateEncoding)
);
