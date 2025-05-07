// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{fmt::Display, str::FromStr};

use bilrost::{
    DecodeError, DecodeErrorKind,
    encoding::{EmptyState, ForOverwrite, General, ValueDecoder, ValueEncoder, Wiretyped},
};

/// Adaptor trait used by the `BilrostAs` derive macro. The macro will
/// create an instance of the adaptor to be able to convert from the original
/// type to the `As` type.
///
/// Any [`bilrost::Message`] that implements `From<T> for As`
/// and `From<As> for T` will automatically implement the Adaptor trait.
/// Hence is possible to do something like
///
/// ```ignore
/// #[derive(Default, Clone, BilrostAs)]
/// #[bilrost_as(Wire)]
/// struct Inner{
///
/// }
///
/// #[derive(bilrost::Message)]
/// struct Wire{
/// }
///
/// // impl From<Inner> for Wire{ .. }
/// // impl From<Wire> for Inner{ .. }
/// ```
pub trait BilrostAsAdaptor<'a, Source> {
    fn create(value: &'a Source) -> Self;
    fn into_inner(self) -> Result<Source, DecodeError>;
}

impl<'a, Target, Source> BilrostAsAdaptor<'a, Source> for Target
where
    Target: bilrost::Message,
    Source: 'static,
    Target: From<&'a Source>,
    Source: From<Target>,
{
    fn create(value: &'a Source) -> Self {
        Target::from(value)
    }

    fn into_inner(self) -> Result<Source, DecodeError> {
        Ok(Source::from(self))
    }
}

/// An adaptor that serializes the type as String using its
/// `Display` and `FromStr` implementation
pub struct BilrostDisplayFromStr {
    inner: String,
}

impl<'a, Source> BilrostAsAdaptor<'a, Source> for BilrostDisplayFromStr
where
    Source: Display,
    Source: FromStr,
{
    fn create(value: &'a Source) -> Self {
        Self {
            inner: value.to_string(),
        }
    }

    fn into_inner(self) -> Result<Source, DecodeError> {
        self.inner
            .parse()
            .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidValue))
    }
}

impl Wiretyped<General> for BilrostDisplayFromStr {
    const WIRE_TYPE: bilrost::encoding::WireType = <String as Wiretyped<General>>::WIRE_TYPE;
}

impl ValueEncoder<General> for BilrostDisplayFromStr {
    fn encode_value<B: bytes::BufMut + ?Sized>(value: &Self, buf: &mut B) {
        <String as ValueEncoder<General>>::encode_value(&value.inner, buf)
    }

    fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(value: &Self, buf: &mut B) {
        <String as ValueEncoder<General>>::prepend_value(&value.inner, buf)
    }

    fn value_encoded_len(value: &Self) -> usize {
        <String as ValueEncoder<General>>::value_encoded_len(&value.inner)
    }
}

impl ValueDecoder<General> for BilrostDisplayFromStr {
    fn decode_value<B: bytes::Buf + ?Sized>(
        value: &mut Self,
        buf: bilrost::encoding::Capped<B>,
        ctx: bilrost::encoding::DecodeContext,
    ) -> Result<(), bilrost::DecodeError> {
        <String as ValueDecoder<General>>::decode_value(&mut value.inner, buf, ctx)
    }
}

impl ForOverwrite for BilrostDisplayFromStr {
    fn for_overwrite() -> Self
    where
        Self: Sized,
    {
        Self {
            inner: String::default(),
        }
    }
}

impl EmptyState for BilrostDisplayFromStr {
    fn clear(&mut self) {
        self.inner = String::default();
    }
    fn empty() -> Self
    where
        Self: Sized,
    {
        Self {
            inner: String::default(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// A special adaptor that always skip the field on encode and decode the value
/// always using [`Default::default()`]
pub struct BilrostSkip;

impl<'a, Source> BilrostAsAdaptor<'a, Source> for BilrostSkip
where
    Source: Default,
{
    fn create(_value: &'a Source) -> Self {
        Self
    }

    fn into_inner(self) -> Result<Source, DecodeError> {
        Ok(Source::default())
    }
}

impl Wiretyped<General> for BilrostSkip {
    const WIRE_TYPE: bilrost::encoding::WireType = <() as Wiretyped<General>>::WIRE_TYPE;
}

impl ValueEncoder<General> for BilrostSkip {
    fn encode_value<B: bytes::BufMut + ?Sized>(_value: &Self, _buf: &mut B) {}

    fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(_value: &Self, _buf: &mut B) {}

    fn value_encoded_len(_value: &Self) -> usize {
        0
    }
}

impl ValueDecoder<General> for BilrostSkip {
    fn decode_value<B: bytes::Buf + ?Sized>(
        _value: &mut Self,
        _buf: bilrost::encoding::Capped<B>,
        _ctx: bilrost::encoding::DecodeContext,
    ) -> Result<(), bilrost::DecodeError> {
        Ok(())
    }
}

impl ForOverwrite for BilrostSkip {
    fn for_overwrite() -> Self
    where
        Self: Sized,
    {
        Self
    }
}

impl EmptyState for BilrostSkip {
    fn clear(&mut self) {}
    fn empty() -> Self
    where
        Self: Sized,
    {
        Self
    }

    fn is_empty(&self) -> bool {
        true
    }
}
