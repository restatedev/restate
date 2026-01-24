// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines messages between replicated loglet instances

use bilrost::buf::ReverseBuf;
use bilrost::bytes::{Buf, BufMut};
use bilrost::encoding::{
    BorrowDecoder, Capped, DecodeContext, Decoder, DistinguishedBorrowDecoder,
    DistinguishedDecoder, DistinguishedValueBorrowDecoder, DistinguishedValueDecoder, EmptyState,
    Encoder, ForOverwrite, RestrictedDecodeContext, TagMeasurer, TagRevWriter, TagWriter,
    ValueBorrowDecoder, ValueDecoder, ValueEncoder, WireType, Wiretyped,
};
use bilrost::{Canonicity, DecodeError};
use std::sync::Arc;

/// The `Arced` encoding can be used to encode and decode any type `T` that is wrapped in `Arc<T>`.
/// The parametrized encoding type `E` is the encoding that is used to encode and decode `T` inside
/// the `Arc`.
///
/// If `Arc`s are constructed with cyclic references, encoding them with `bilrost` will crash the
/// program. If self-referential types must be encoded, take great care that the data does not
/// contain cycles before it is encoded.
pub struct Arced<E = bilrost::encoding::General>(E);

/// The `ArcedSlice` encoding can be used to encode and decode `Arc<[T]>` directly with `bilrost`.
/// The parametrized type `E` is the inner encoding, which should be either "packed" or "unpacked",
/// with whatever further parameters are required to name the encoding of the type.
///
/// If `Arc`s are constructed with cyclic references, encoding them with `bilrost` will crash the
/// program. If self-referential types must be encoded, take great care that the data does not
/// contain cycles before it is encoded.
pub struct ArcedSlice<E>(E);

// This enables `Option<Arc<T>>` and `[Arc<T>; N]`
bilrost::implement_core_empty_state_rules!(Arced<E>, with generics (E));
bilrost::implement_core_empty_state_rules!(ArcedSlice<E>, with generics (E));

mod impl_arc_encoding {
    use super::*;

    impl<T, E> ForOverwrite<Arced<E>, Arc<T>> for ()
    where
        (): ForOverwrite<E, T>,
    {
        #[inline(always)]
        fn for_overwrite() -> Arc<T> {
            Arc::new(<() as ForOverwrite<E, T>>::for_overwrite())
        }
    }

    impl<T, E> EmptyState<Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): EmptyState<E, T>,
    {
        #[inline(always)]
        fn empty() -> Arc<T> {
            Arc::new(<() as EmptyState<E, T>>::empty())
        }

        #[inline(always)]
        fn is_empty(val: &Arc<T>) -> bool {
            <() as EmptyState<E, T>>::is_empty(val)
        }

        #[inline(always)]
        fn clear(val: &mut Arc<T>) {
            <() as EmptyState<E, T>>::clear(Arc::make_mut(val))
        }
    }

    impl<T, E> Wiretyped<Arced<E>, Arc<T>> for ()
    where
        (): Wiretyped<E, T>,
    {
        const WIRE_TYPE: WireType = <() as Wiretyped<E, T>>::WIRE_TYPE;
    }

    impl<T, E> ValueEncoder<Arced<E>, Arc<T>> for ()
    where
        (): ValueEncoder<E, T>,
    {
        #[inline(always)]
        fn encode_value<B: BufMut + ?Sized>(value: &Arc<T>, buf: &mut B) {
            <() as ValueEncoder<E, T>>::encode_value(value, buf)
        }

        #[inline(always)]
        fn prepend_value<B: ReverseBuf + ?Sized>(value: &Arc<T>, buf: &mut B) {
            <() as ValueEncoder<E, T>>::prepend_value(value, buf)
        }

        #[inline(always)]
        fn value_encoded_len(value: &Arc<T>) -> usize {
            <() as ValueEncoder<E, T>>::value_encoded_len(value)
        }
    }

    impl<T, E> ValueDecoder<Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): ValueDecoder<E, T>,
    {
        #[inline(always)]
        fn decode_value<B: Buf + ?Sized>(
            value: &mut Arc<T>,
            buf: Capped<B>,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            <() as ValueDecoder<E, T>>::decode_value(Arc::make_mut(value), buf, ctx)
        }
    }

    impl<T, E> DistinguishedValueDecoder<Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): DistinguishedValueDecoder<E, T>,
    {
        const CHECKS_EMPTY: bool = <() as DistinguishedValueDecoder<E, T>>::CHECKS_EMPTY;

        #[inline(always)]
        fn decode_value_distinguished<const ALLOW_EMPTY: bool>(
            value: &mut Arc<T>,
            buf: Capped<impl Buf + ?Sized>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            <() as DistinguishedValueDecoder<E, T>>::decode_value_distinguished::<ALLOW_EMPTY>(
                Arc::make_mut(value),
                buf,
                ctx,
            )
        }
    }

    impl<'a, T, E> ValueBorrowDecoder<'a, Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): ValueBorrowDecoder<'a, E, T>,
    {
        #[inline(always)]
        fn borrow_decode_value(
            value: &mut Arc<T>,
            buf: Capped<&'a [u8]>,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            <() as ValueBorrowDecoder<E, T>>::borrow_decode_value(Arc::make_mut(value), buf, ctx)
        }
    }

    impl<'a, T, E> DistinguishedValueBorrowDecoder<'a, Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): DistinguishedValueBorrowDecoder<'a, E, T>,
    {
        const CHECKS_EMPTY: bool = <() as DistinguishedValueBorrowDecoder<'a, E, T>>::CHECKS_EMPTY;

        #[inline(always)]
        fn borrow_decode_value_distinguished<const ALLOW_EMPTY: bool>(
            value: &mut Arc<T>,
            buf: Capped<&'a [u8]>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            <() as DistinguishedValueBorrowDecoder<E, T>>::borrow_decode_value_distinguished::<
                ALLOW_EMPTY,
            >(Arc::make_mut(value), buf, ctx)
        }
    }

    impl<T, E> Encoder<Arced<E>, Arc<T>> for ()
    where
        (): Encoder<E, T>,
    {
        #[inline(always)]
        fn encode<B: BufMut + ?Sized>(tag: u32, value: &Arc<T>, buf: &mut B, tw: &mut TagWriter) {
            <() as Encoder<E, T>>::encode(tag, value, buf, tw)
        }

        #[inline(always)]
        fn prepend_encode<B: ReverseBuf + ?Sized>(
            tag: u32,
            value: &Arc<T>,
            buf: &mut B,
            tw: &mut TagRevWriter,
        ) {
            <() as Encoder<E, T>>::prepend_encode(tag, value, buf, tw)
        }

        #[inline(always)]
        fn encoded_len(tag: u32, value: &Arc<T>, tm: &mut impl TagMeasurer) -> usize {
            <() as Encoder<E, T>>::encoded_len(tag, value, tm)
        }
    }

    impl<T, E> Decoder<Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): Decoder<E, T>,
    {
        #[inline(always)]
        fn decode<B: Buf + ?Sized>(
            wire_type: WireType,
            value: &mut Arc<T>,
            buf: Capped<B>,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            <() as Decoder<E, T>>::decode(wire_type, Arc::make_mut(value), buf, ctx)
        }
    }

    impl<T, E> DistinguishedDecoder<Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): DistinguishedDecoder<E, T>,
    {
        #[inline(always)]
        fn decode_distinguished<B: Buf + ?Sized>(
            wire_type: WireType,
            value: &mut Arc<T>,
            buf: Capped<B>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            <() as DistinguishedDecoder<E, T>>::decode_distinguished(
                wire_type,
                Arc::make_mut(value),
                buf,
                ctx,
            )
        }
    }

    impl<'a, T, E> BorrowDecoder<'a, Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): BorrowDecoder<'a, E, T>,
    {
        #[inline(always)]
        fn borrow_decode(
            wire_type: WireType,
            value: &mut Arc<T>,
            buf: Capped<&'a [u8]>,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            <() as BorrowDecoder<E, T>>::borrow_decode(wire_type, Arc::make_mut(value), buf, ctx)
        }
    }

    impl<'a, T, E> DistinguishedBorrowDecoder<'a, Arced<E>, Arc<T>> for ()
    where
        T: Clone,
        (): DistinguishedBorrowDecoder<'a, E, T>,
    {
        #[inline(always)]
        fn borrow_decode_distinguished(
            wire_type: WireType,
            value: &mut Arc<T>,
            buf: Capped<&'a [u8]>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            <() as DistinguishedBorrowDecoder<E, T>>::borrow_decode_distinguished(
                wire_type,
                Arc::make_mut(value),
                buf,
                ctx,
            )
        }
    }
}

// When decoding, the `ArcedSlice` encoding always decodes into a `Vec<T>` and then copies the final
// result into an `Arc<[T]>`. Without knowing the precise number of elements that are going to be in
// the slice this is always necessary, and it is almost certainly faster than counting the number of
// items first.
mod impl_arc_slice_encoding {
    use super::*;

    impl<T, E> ForOverwrite<ArcedSlice<E>, Arc<[T]>> for () {
        #[inline(always)]
        fn for_overwrite() -> Arc<[T]> {
            Arc::new([]) // tragically, this always allocates :(
        }
    }

    impl<T, E> EmptyState<ArcedSlice<E>, Arc<[T]>> for () {
        #[inline(always)]
        fn empty() -> Arc<[T]> {
            Arc::new([]) // tragically, this always allocates :(
        }

        #[inline(always)]
        fn is_empty(val: &Arc<[T]>) -> bool {
            val.is_empty()
        }

        #[inline(always)]
        fn clear(val: &mut Arc<[T]>) {
            *val = Arc::new([]);
        }
    }

    impl<T, E> Wiretyped<ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): Wiretyped<E, [T]>,
    {
        const WIRE_TYPE: WireType = <() as Wiretyped<E, [T]>>::WIRE_TYPE;
    }

    // We will pass-through value-encoding support to any encoder E which can value-encode `[T]`;
    // that is, the "packed" encoding.
    impl<T, E> ValueEncoder<ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): ValueEncoder<E, [T]>,
    {
        #[inline(always)]
        fn encode_value<B: BufMut + ?Sized>(value: &Arc<[T]>, buf: &mut B) {
            <() as ValueEncoder<E, [T]>>::encode_value(value, buf)
        }

        #[inline(always)]
        fn prepend_value<B: ReverseBuf + ?Sized>(value: &Arc<[T]>, buf: &mut B) {
            <() as ValueEncoder<E, [T]>>::prepend_value(value, buf)
        }

        #[inline(always)]
        fn value_encoded_len(value: &Arc<[T]>) -> usize {
            <() as ValueEncoder<E, [T]>>::value_encoded_len(value)
        }
    }

    impl<T, E> ValueDecoder<ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): ValueDecoder<E, Vec<T>> + ValueEncoder<E, [T]>,
    {
        #[inline(always)]
        fn decode_value<B: Buf + ?Sized>(
            value: &mut Arc<[T]>,
            buf: Capped<B>,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            let mut decoded = vec![];
            <() as ValueDecoder<E, Vec<T>>>::decode_value(&mut decoded, buf, ctx)?;
            *value = decoded.into();
            Ok(())
        }
    }

    impl<T, E> DistinguishedValueDecoder<ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): DistinguishedValueDecoder<E, Vec<T>> + ValueEncoder<E, [T]>,
        (): ValueEncoder<E, [T]>,
    {
        const CHECKS_EMPTY: bool = <() as DistinguishedValueDecoder<E, Vec<T>>>::CHECKS_EMPTY;

        #[inline(always)]
        fn decode_value_distinguished<const ALLOW_EMPTY: bool>(
            value: &mut Arc<[T]>,
            buf: Capped<impl Buf + ?Sized>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            let mut decoded = vec![];
            let canon = <() as DistinguishedValueDecoder<E, Vec<T>>>::decode_value_distinguished::<
                ALLOW_EMPTY,
            >(&mut decoded, buf, ctx)?;
            *value = decoded.into();
            Ok(canon)
        }
    }

    impl<'a, T, E> ValueBorrowDecoder<'a, ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): ValueBorrowDecoder<'a, E, Vec<T>> + ValueEncoder<E, [T]>,
    {
        #[inline(always)]
        fn borrow_decode_value(
            value: &mut Arc<[T]>,
            buf: Capped<&'a [u8]>,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            let mut decoded = vec![];
            <() as ValueBorrowDecoder<E, Vec<T>>>::borrow_decode_value(&mut decoded, buf, ctx)?;
            *value = decoded.into();
            Ok(())
        }
    }

    impl<'a, T, E> DistinguishedValueBorrowDecoder<'a, ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): DistinguishedValueBorrowDecoder<'a, E, Vec<T>> + ValueEncoder<E, [T]>,
    {
        const CHECKS_EMPTY: bool =
            <() as DistinguishedValueBorrowDecoder<'a, E, Vec<T>>>::CHECKS_EMPTY;

        #[inline(always)]
        fn borrow_decode_value_distinguished<const ALLOW_EMPTY: bool>(
            value: &mut Arc<[T]>,
            buf: Capped<&'a [u8]>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            let mut decoded = vec![];
            let canon = <() as DistinguishedValueBorrowDecoder<E, Vec<T>>>::borrow_decode_value_distinguished::<
                ALLOW_EMPTY,
            >(&mut decoded, buf, ctx)?;
            *value = decoded.into();
            Ok(canon)
        }
    }

    // This means we will pass-through encoding support to any encoder E which can encode `[T]`;
    // that is, the "packed" or "unpacked" encodings.
    impl<T, E> Encoder<ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): Encoder<E, [T]>,
    {
        #[inline(always)]
        fn encode<B: BufMut + ?Sized>(tag: u32, value: &Arc<[T]>, buf: &mut B, tw: &mut TagWriter) {
            <() as Encoder<E, [T]>>::encode(tag, value, buf, tw)
        }

        #[inline(always)]
        fn prepend_encode<B: ReverseBuf + ?Sized>(
            tag: u32,
            value: &Arc<[T]>,
            buf: &mut B,
            tw: &mut TagRevWriter,
        ) {
            <() as Encoder<E, [T]>>::prepend_encode(tag, value, buf, tw)
        }

        #[inline(always)]
        fn encoded_len(tag: u32, value: &Arc<[T]>, tm: &mut impl TagMeasurer) -> usize {
            <() as Encoder<E, [T]>>::encoded_len(tag, value, tm)
        }
    }

    impl<T, E> Decoder<ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): Decoder<E, Vec<T>> + Encoder<E, [T]>,
    {
        #[inline(always)]
        fn decode<B: Buf + ?Sized>(
            wire_type: WireType,
            value: &mut Arc<[T]>,
            buf: Capped<B>,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            let mut decoded = vec![];
            <() as Decoder<E, Vec<T>>>::decode(wire_type, &mut decoded, buf, ctx)?;
            *value = decoded.into();
            Ok(())
        }
    }

    impl<T, E> DistinguishedDecoder<ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): DistinguishedDecoder<E, Vec<T>> + Encoder<E, [T]>,
    {
        #[inline(always)]
        fn decode_distinguished<B: Buf + ?Sized>(
            wire_type: WireType,
            value: &mut Arc<[T]>,
            buf: Capped<B>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            let mut decoded = vec![];
            let canon = <() as DistinguishedDecoder<E, Vec<T>>>::decode_distinguished(
                wire_type,
                &mut decoded,
                buf,
                ctx,
            )?;
            *value = decoded.into();
            Ok(canon)
        }
    }

    impl<'a, T, E> BorrowDecoder<'a, ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): BorrowDecoder<'a, E, Vec<T>> + Encoder<E, [T]>,
    {
        #[inline(always)]
        fn borrow_decode(
            wire_type: WireType,
            value: &mut Arc<[T]>,
            buf: Capped<&'a [u8]>,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            let mut decoded = vec![];
            <() as BorrowDecoder<E, Vec<T>>>::borrow_decode(wire_type, &mut decoded, buf, ctx)?;
            *value = decoded.into();
            Ok(())
        }
    }

    impl<'a, T, E> DistinguishedBorrowDecoder<'a, ArcedSlice<E>, Arc<[T]>> for ()
    where
        (): DistinguishedBorrowDecoder<'a, E, Vec<T>> + Encoder<E, [T]>,
    {
        #[inline(always)]
        fn borrow_decode_distinguished(
            wire_type: WireType,
            value: &mut Arc<[T]>,
            buf: Capped<&'a [u8]>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            let mut decoded = vec![];
            let canon = <() as DistinguishedBorrowDecoder<E, Vec<T>>>::borrow_decode_distinguished(
                wire_type,
                &mut decoded,
                buf,
                ctx,
            )?;
            *value = decoded.into();
            Ok(canon)
        }
    }
}
