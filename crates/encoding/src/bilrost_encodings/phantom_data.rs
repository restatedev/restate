// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use bilrost::{
    DecodeErrorKind,
    encoding::{EmptyState, ForOverwrite, Proxiable},
};

use crate::bilrost_encodings::RestateEncoding;

struct PhantomDataTag;

impl<T> Proxiable<PhantomDataTag> for PhantomData<T> {
    type Proxy = ();

    fn encode_proxy(&self) -> Self::Proxy {}

    fn decode_proxy(&mut self, _: Self::Proxy) -> Result<(), DecodeErrorKind> {
        Ok(())
    }
}

impl<T> ForOverwrite<RestateEncoding, PhantomData<T>> for () {
    fn for_overwrite() -> PhantomData<T> {
        PhantomData
    }
}

impl<T> EmptyState<RestateEncoding, PhantomData<T>> for () {
    fn empty() -> PhantomData<T> {
        PhantomData
    }

    fn is_empty(_: &PhantomData<T>) -> bool {
        true
    }

    fn clear(_: &mut PhantomData<T>) {}
}

bilrost::delegate_proxied_encoding!(
    use encoding (::bilrost::encoding::General)
    to encode proxied type (PhantomData<T>)
    using proxy tag (PhantomDataTag)
    with encoding (RestateEncoding)
    with generics (T)
);
