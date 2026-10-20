// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Defines an ordered key payload, its encoding inputs and decoded views, and
/// typed prefix/decoder steps. Fields appear on disk in declaration order.
///
/// Every field selects an `OrderedFieldCodec`, including borrowed
/// inputs, unconstrained fields, and tie-breakers. All fields must satisfy its
/// mem-comparable, prefix-free contract. This differs from `define_table_key!`,
/// whose table-specific `KeyEncode` codecs have no general ordering guarantee.
/// An ordered prefix alone cannot make a mixed-format payload ordered.
///
/// For a key named `RecordKey`, generates:
/// - `RecordKey`: owned fields from each codec's `FieldCodec::Value` type.
/// - `RecordKeyRef`: encoding inputs constructed with `RecordKey::borrowed`.
///   A field annotated `=> str` borrows its string input (preserving `Option`);
///   unannotated fields are passed by value as the codec's owned type.
/// - `RecordKeyView`: borrowed encoded fields, each wrapped in a `FieldDecoder`.
/// - `RecordKeyFieldCodecs`: the selected codec types, shared with selection binding.
/// - `RecordKeyPrefix`: steps that append only the next field, in physical order.
/// - `KeyDecoder<RecordKey>` methods for owned or borrowed progressive decoding.
///
/// `KeyEncode`/`KeyDecode` operate only on the payload. The caller supplies
/// `key_kind`, `context`, and `start(context, buffer)` to write the fixed physical
/// identity. `RecordKey::prefix` invokes `start` and borrows the caller's `BufMut`;
/// each field step appends immediately without retaining the field input. A
/// builder may stop at any field boundary, with no finalization step.
///
/// The optional selector binding names a logical target implementing `Selector`;
/// field names must match its tags and CamelCase `Field`/`Clause` variants, but
/// their order may differ. Each physical codec must implement `FieldPredicateCodec`
/// with the logical field's literal type. The generated `prepare_selection` rejects
/// constrained fields absent from this key and unsupported prefix clauses.
macro_rules! define_index_key {
    (
        $(#[$meta:meta])*
        $key:ident,
        key_kind: $key_kind:expr,
        context: $context:ty,
        start: $start:expr,
        fields { $($field:ident: $codec:ty $(=> $borrowed:ty)?),+ $(,)? }
        $(selector: $target:ty)?
    ) => {
        $(#[$meta])*
        #[allow(dead_code)]
        pub struct $key {
            $(pub $field: <$codec as crate::keys::FieldCodec>::Value,)+
        }

        impl crate::keys::selection::IndexKeySchema for $key {
            const KEY_KIND: crate::keys::KeyKind = $key_kind;
            const FIELDS: &'static [crate::keys::selection::IndexKeyField] = &[
                $(crate::keys::selection::IndexKeyField::new::<$codec>(stringify!($field)),)+
            ];
        }

        impl crate::keys::KeyEncode for $key {
            fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                $(<$codec as crate::keys::FieldCodec>::encode_field(&self.$field, bytes);)+
            }

            fn serialized_length(&self) -> usize {
                0 $(+ <$codec as crate::keys::FieldCodec>::serialized_length(&self.$field))+
            }
        }

        impl crate::keys::KeyDecode for $key {
            fn decode(bytes: &mut &[u8]) -> crate::Result<Self> {
                $(let $field = <$codec as crate::keys::FieldCodec>::decode_field(bytes)?;)+
                Ok(Self { $($field,)+ })
            }
        }

        paste::paste! {
            /// The schema's selected codecs, also used by separately declared selection bindings.
            #[allow(dead_code)]
            pub trait [<$key FieldCodecs>] {
                $(type [<$field:camel>]: crate::keys::OrderedFieldCodec;)+
            }

            impl [<$key FieldCodecs>] for $key {
                $(type [<$field:camel>] = $codec;)+
            }

            /// Borrowed encoded fields, materialized and value-checked individually on demand.
            #[allow(dead_code)]
            pub struct [<$key View>]<'a> {
                $(pub $field: crate::keys::FieldDecoder<'a, $codec>,)+
            }

            #[allow(dead_code)]
            impl<'a> crate::keys::KeyDecoder<'a, $key> {
                /// Takes every field and rejects trailing bytes without materializing values.
                pub fn take_all(mut self) -> crate::Result<[<$key View>]<'a>> {
                    $(let $field = crate::keys::FieldDecoder::<$codec>::take(&mut self.remaining)?;)+
                    if !self.remaining.is_empty() {
                        return Err(restate_storage_api::StorageError::DataIntegrityError);
                    }
                    Ok([<$key View>] { $($field,)+ })
                }
            }

            /// Encoding inputs: annotated fields borrow their values; others are owned.
            #[allow(dead_code)]
            pub struct [<$key Ref>]<'a> {
                $($field: crate::keys::macros::define_index_key!(@ref_type 'a; $codec $(; $borrowed)?),)+
                _marker: std::marker::PhantomData<&'a ()>,
            }

            #[allow(dead_code)]
            impl $key {
                /// Constructs encoding inputs without serializing them or adding a header.
                pub fn borrowed<'a>(
                    $($field: crate::keys::macros::define_index_key!(@arg_type 'a; $codec $(; $borrowed)?),)+
                ) -> [<$key Ref>]<'a> {
                    [<$key Ref>] {
                        $($field: crate::keys::macros::define_index_key!(@borrow $field $(; $borrowed)?),)+
                        _marker: std::marker::PhantomData,
                    }
                }
            }

            impl crate::keys::KeyEncode for [<$key Ref>]<'_> {
                fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                    $(<$codec as crate::keys::FieldCodec>::encode_ref(
                        crate::keys::macros::define_index_key!(@input self.$field; $codec $(; $borrowed)?), bytes,
                    );)+
                }

                fn serialized_length(&self) -> usize {
                    0 $(+ <$codec as crate::keys::FieldCodec>::serialized_length_ref(
                        crate::keys::macros::define_index_key!(@input self.$field; $codec $(; $borrowed)?),
                    ))+
                }
            }
        }

        crate::keys::macros::define_index_key_prefix!(
            $key, $context, $start;
            $($field: $codec $(=> $borrowed)?),+
        );
        crate::keys::macros::define_index_key_decoder!(@fields $key; []; $($field: $codec),+);
        crate::keys::macros::define_key_selection!(
            $key; [$($target)?]; $($field),+
        );
    };

    (@ref_type $lt:lifetime; $codec:ty; $borrowed:ty) => {
        <$codec as crate::keys::FieldCodec>::Ref<$lt>
    };
    (@ref_type $lt:lifetime; $codec:ty) => {
        <$codec as crate::keys::FieldCodec>::Value
    };
    (@arg_type $lt:lifetime; $codec:ty; $borrowed:ty) => {
        impl crate::keys::IntoFieldRef<
            $lt, $borrowed,
            Output = <$codec as crate::keys::FieldCodec>::Ref<$lt>,
        >
    };
    (@arg_type $lt:lifetime; $codec:ty) => {
        <$codec as crate::keys::FieldCodec>::Value
    };
    (@borrow $field:ident; $borrowed:ty) => {
        crate::keys::IntoFieldRef::into_field_ref($field)
    };
    (@borrow $field:ident) => { $field };
    (@input $field:expr; $codec:ty; $borrowed:ty) => { $field };
    (@input $field:expr; $codec:ty) => {
        <$codec as crate::keys::FieldCodec>::borrow(&$field)
    };
}

/// Generates field-boundary prefix builders over a caller-owned `BufMut`.
/// `start(context, buffer)` and each field step append without clearing the buffer.
/// The builder does not allocate its own buffer or require finalization.
macro_rules! define_index_key_prefix {
    ($key:ident, $context:ty, $start:expr;
        $($field:ident: $codec:ty $(=> $borrowed:ty)?),+
    ) => {
        paste::paste! {
            #[allow(dead_code)]
            pub struct [<$key Prefix>]<B: bytes::BufMut, const FIELD: usize> {
                buffer: B,
            }

            #[allow(dead_code)]
            impl<B: bytes::BufMut, const FIELD: usize> [<$key Prefix>]<B, FIELD> {
                fn push<C: crate::keys::OrderedFieldCodec, const NEXT: usize>(
                    mut self, value: C::Ref<'_>,
                ) -> [<$key Prefix>]<B, NEXT> {
                    C::encode_ref(value, &mut self.buffer);
                    [<$key Prefix>] { buffer: self.buffer }
                }
            }

            #[allow(dead_code)]
            impl $key {
                /// Appends the fixed header and starts a typed field sequence.
                /// The caller owns the buffer and supplies capacity as required by `BufMut`.
                pub fn prefix<B: bytes::BufMut>(
                    context: $context, buffer: &mut B,
                ) -> [<$key Prefix>]<&mut B, 0> {
                    ($start)(&context, &mut *buffer);
                    [<$key Prefix>] { buffer }
                }
            }
        }
        crate::keys::macros::define_index_key_prefix!(@fields $key; []; $($field: $codec $(=> $borrowed)?),+);
    };

    (@fields $key:ident; [$($done:ident),*]; $field:ident: $codec:ty $(=> $borrowed:ty)? $(, $next:ident: $next_codec:ty $(=> $next_borrowed:ty)?)* ) => {
        crate::keys::macros::define_index_key_prefix!(
            @method $key;
            { crate::keys::macros::index_key_count!($($done),*) };
            { crate::keys::macros::index_key_count!($($done,)* $field) };
            $field: $codec $(=> $borrowed)?
        );
        crate::keys::macros::define_index_key_prefix!(@fields $key; [$($done,)* $field]; $($next: $next_codec $(=> $next_borrowed)?),*);
    };
    (@fields $key:ident; [$($done:ident),*]; ) => {};

    (@method $key:ident; $current:expr; $next:expr; $field:ident: $codec:ty => $borrowed:ty) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<B: bytes::BufMut> [<$key Prefix>]<B, $current> {
                pub fn $field<'a>(
                    self,
                    $field: crate::keys::macros::define_index_key!(@arg_type 'a; $codec; $borrowed),
                ) -> [<$key Prefix>]<B, $next> {
                    let value = crate::keys::macros::define_index_key!(@borrow $field; $borrowed);
                    self.push::<$codec, $next>(value)
                }
            }
        }
    };
    (@method $key:ident; $current:expr; $next:expr; $field:ident: $codec:ty) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<B: bytes::BufMut> [<$key Prefix>]<B, $current> {
                pub fn $field(
                    self, $field: <$codec as crate::keys::FieldCodec>::Value,
                ) -> [<$key Prefix>]<B, $next> {
                    self.push::<$codec, $next>(<$codec as crate::keys::FieldCodec>::borrow(&$field))
                }
            }
        }
    };
}

/// Generates progressive owned and borrowed decoding in declared physical order.
/// Intermediate steps return `(field, next_decoder)`; the last returns only the field.
/// The final step rejects trailing bytes rather than silently accepting a suffix.
macro_rules! define_index_key_decoder {
    (@fields $key:ident; [$($done:ident),*]; $field:ident: $codec:ty, $($next:ident: $next_codec:ty),+) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<'a> crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::index_key_count!($($done),*) }> {
                pub fn tag(&self) -> &'static str { stringify!($field) }

                pub fn [<decode_$field>](mut self) -> crate::Result<(
                    <$codec as crate::keys::FieldCodec>::Value,
                    crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::index_key_count!($($done,)* $field) }>,
                )> {
                    let value = <$codec as crate::keys::FieldCodec>::decode_field(&mut self.remaining)?;
                    Ok((value, crate::keys::KeyDecoder { remaining: self.remaining, _marker: std::marker::PhantomData }))
                }

                pub fn [<take_$field>](mut self) -> crate::Result<(
                    crate::keys::FieldDecoder<'a, $codec>,
                    crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::index_key_count!($($done,)* $field) }>,
                )> {
                    let value = crate::keys::FieldDecoder::<$codec>::take(&mut self.remaining)?;
                    Ok((value, crate::keys::KeyDecoder { remaining: self.remaining, _marker: std::marker::PhantomData }))
                }
            }
        }
        crate::keys::macros::define_index_key_decoder!(@fields $key; [$($done,)* $field]; $($next: $next_codec),+);
    };
    (@fields $key:ident; [$($done:ident),*]; $field:ident: $codec:ty) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<'a> crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::index_key_count!($($done),*) }> {
                pub fn tag(&self) -> &'static str { stringify!($field) }

                pub fn [<decode_$field>](mut self) -> crate::Result<<$codec as crate::keys::FieldCodec>::Value> {
                    let value = <$codec as crate::keys::FieldCodec>::decode_field(&mut self.remaining)?;
                    if !self.remaining.is_empty() { return Err(restate_storage_api::StorageError::DataIntegrityError); }
                    Ok(value)
                }

                pub fn [<take_$field>](mut self) -> crate::Result<crate::keys::FieldDecoder<'a, $codec>> {
                    let value = crate::keys::FieldDecoder::<$codec>::take(&mut self.remaining)?;
                    if !self.remaining.is_empty() { return Err(restate_storage_api::StorageError::DataIntegrityError); }
                    Ok(value)
                }
            }
        }
    };
}

/// Generates typed clause dispatch. Encoding, validation, and nullability belong
/// to each physical codec; bounds and evaluation remain in `PreparedKeySelection`.
/// Prefix clauses are identified through `Selector::starts_with_prefix`;
/// other clauses are matched by the physical field's CamelCase variant name.
/// Bindings list field names only; codec types come from the generated schema.
macro_rules! define_key_selection {
    ($key:ident; []; $($fields:tt)*) => {};
    ($key:ident; [$target:ty]; $($field:ident),+ $(,)?) => {
        paste::paste! {
            impl $key {
                /// Binds logical clauses to this key's declared physical codecs.
                #[allow(dead_code)] // Some schemas are written before they gain scan callers.
                pub(crate) fn prepare_selection(
                    selection: &restate_storage_api::selection::Selection<$target>,
                ) -> crate::Result<crate::keys::selection::PreparedKeySelection<Self>> {
                    type Clause = <$target as restate_storage_api::selection::Selector>::Clause;
                    type Field = <$target as restate_storage_api::selection::Selector>::Field;
                    crate::keys::selection::PreparedKeySelection::new(selection, |clause, literals| {
                        // A key may cover only a subset of the logical target.
                        #[allow(unreachable_patterns)]
                        if let Some(prefix) = <$target as restate_storage_api::selection::Selector>::starts_with_prefix(clause) {
                            match <$target as restate_storage_api::selection::Selector>::field(clause) {
                                $(Field::[<$field:camel>] => <<$key as [<$key FieldCodecs>]>::[<$field:camel>] as crate::keys::selection::codec::FieldPredicateCodec>::prepare_prefix(prefix, literals),)+
                                _ => Err(restate_storage_api::StorageError::Conversion(anyhow::anyhow!(
                                    "selection field is not supported by this key"
                                ))),
                            }
                        } else {
                            match clause {
                                $(Clause::[<$field:camel>](predicate) => <<$key as [<$key FieldCodecs>]>::[<$field:camel>] as crate::keys::selection::codec::FieldPredicateCodec>::prepare_value(predicate, literals),)+
                                _ => Err(restate_storage_api::StorageError::Conversion(anyhow::anyhow!(
                                    "selection clause is not supported by this key"
                                ))),
                            }
                        }
                    })
                }
            }
        }
    };
}

macro_rules! index_key_count {
    ($($field:ident),*) => { <[()]>::len(&[$(crate::keys::macros::index_key_count!(@unit $field)),*]) };
    (@unit $field:ident) => { () };
}

pub(crate) use {
    define_index_key, define_index_key_decoder, define_index_key_prefix, define_key_selection,
    index_key_count,
};

#[cfg(test)]
mod tests;
