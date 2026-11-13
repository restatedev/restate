// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Declares an **ordered key**: a key whose fields, after a fixed header, are laid
/// out in declaration order with codecs whose bytes sort the way the values sort.
/// Such a key can be scanned by ranges of its fields and searched by seeking,
/// which is what [`selection`](crate::keys::selection) builds on.
///
/// # Declaring a key
///
/// ```ignore
/// /// Records of one partition, numbered, in an optional category.
/// define_ordered_key!(
///     RecordKey,
///     key_kind: KeyKind::ScopedState,           // the kind of every such key
///     context: (PartitionId, u16),              // what the header is written from
///     start: write_record_header,               // fn(&context, &mut impl BufMut)
///     fields {
///         sequence: U64Codec,                   // big-endian, ascending
///         category: Option<StringCodec> => str, // nullable; `=> str` borrows a &str
///         name: ServiceNameCodec => str,
///         kind: EntryKindCodec,
///     }
///     schema: Record                            // optional: the logical schema
/// );
///
/// fn write_record_header<B: BufMut>((partition, index): &(PartitionId, u16), buffer: &mut B) {
///     buffer.put_slice(KeyKind::ScopedState.as_bytes());
///     buffer.put_u32(u32::from(u16::from(*partition)));
///     buffer.put_u16(*index);
/// }
/// ```
///
/// On disk, a key is the header `start` writes, followed by the fields:
///
/// ```text
///  header (written by `start`)        payload (the fields, in declaration order)
/// ┌──────┬───────────┬───────┐      ┌──────────┬──────────┬──────┬──────┐
/// │ kind │ partition │ index │      │ sequence │ category │ name │ kind │
/// └──────┴───────────┴───────┘      └──────────┴──────────┴──────┴──────┘
/// ```
///
/// Every field's codec must be an [`OrderedFieldCodec`](crate::keys::OrderedFieldCodec):
/// its bytes compare like its values and no encoding is a prefix of another (see
/// the `keys` module guide). That is the difference from `define_table_key!`,
/// whose legacy codecs make no such promise; an ordered prefix in front of a
/// legacy field does not make the key ordered.
///
/// # What the declaration generates
///
/// - `RecordKey`: the owned key, one field per codec's `Value` type. It implements
///   `KeyEncode` and `KeyDecode` for the payload only; the header is the caller's.
/// - `RecordKey::borrowed(7, Some("alpha"), "svc", EntryKind::Invocation)`: a
///   `RecordKeyRef` that encodes the same bytes without owning the strings. A
///   field written `=> str` takes a `&str` (or `Option<&str>`); any other field
///   takes its value.
/// - `RecordKey::prefix(context, &mut buffer)`: writes the header and returns a
///   builder with one method per field, in order. `.sequence(7).category(Some("alpha"))`
///   stops after two fields and leaves the bytes of a two-field prefix in `buffer`,
///   ready for a prefix scan. Each step appends straight into the caller's buffer;
///   nothing has to be finalized.
/// - `KeyDecoder::<RecordKey>::from_payload(bytes)`: decodes field by field.
///   `decode_sequence()` returns `(7, next)`, the value and the decoder for the
///   following field; `take_category()` returns a borrowed
///   [`FieldDecoder`](crate::keys::FieldDecoder) instead, so a reader that only
///   compares bytes never builds the value. The last step rejects trailing bytes,
///   and `take_all()` borrows every field at once as a `RecordKeyView`.
/// - `RecordKeyFieldCodecs`: a trait naming each field's codec
///   (`<RecordKey as RecordKeyFieldCodecs>::Category`), for code generic over a
///   field.
///
/// # Binding a logical schema
///
/// With `schema: Record`, the clauses of a logical selection over `Record` can be
/// prepared against this key. Field names must match the schema's column tags;
/// their order may differ from the schema's. Every codec must implement
/// [`FieldPredicateCodec`](crate::keys::selection::codec::FieldPredicateCodec) for
/// its column's literal type.
///
/// - `RecordKey::prepare_selection(&selection)` is strict: a constrained column
///   the key does not have, or a clause the codec cannot translate, is an error.
/// - `RecordKey::prepare_candidates(&selection)` leaves such clauses out and marks
///   the result inexact; the caller keeps them as a residual filter on the rows.
///
/// A key declared without `schema:` can be bound separately with
/// [`define_key_selection!`], which also allows `field => non_null` for a nullable
/// column stored in a field that never holds NULL.
macro_rules! define_ordered_key {
    (
        $(#[$meta:meta])*
        $key:ident,
        key_kind: $key_kind:expr,
        context: $context:ty,
        start: $start:expr,
        fields { $($field:ident: $codec:ty $(=> $borrowed:ty)?),+ $(,)? }
        $(schema: $target:ty)?
    ) => {
        $(#[$meta])*
        #[allow(dead_code)]
        pub struct $key {
            $(pub $field: <$codec as crate::keys::FieldCodec>::Value,)+
        }

        impl crate::keys::selection::OrderedKeySchema for $key {
            const KEY_KIND: crate::keys::KeyKind = $key_kind;
            const FIELDS: &'static [crate::keys::selection::OrderedKeyField] = &[
                $(crate::keys::selection::OrderedKeyField::new::<$codec>(stringify!($field)),)+
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
                $($field: crate::keys::macros::define_ordered_key!(@ref_type 'a; $codec $(; $borrowed)?),)+
                _marker: std::marker::PhantomData<&'a ()>,
            }

            #[allow(dead_code)]
            impl $key {
                /// Constructs encoding inputs without serializing them or adding a header.
                pub fn borrowed<'a>(
                    $($field: crate::keys::macros::define_ordered_key!(@arg_type 'a; $codec $(; $borrowed)?),)+
                ) -> [<$key Ref>]<'a> {
                    [<$key Ref>] {
                        $($field: crate::keys::macros::define_ordered_key!(@borrow $field $(; $borrowed)?),)+
                        _marker: std::marker::PhantomData,
                    }
                }
            }

            impl crate::keys::KeyEncode for [<$key Ref>]<'_> {
                fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                    $(<$codec as crate::keys::FieldCodec>::encode_ref(
                        crate::keys::macros::define_ordered_key!(@input self.$field; $codec $(; $borrowed)?), bytes,
                    );)+
                }

                fn serialized_length(&self) -> usize {
                    0 $(+ <$codec as crate::keys::FieldCodec>::serialized_length_ref(
                        crate::keys::macros::define_ordered_key!(@input self.$field; $codec $(; $borrowed)?),
                    ))+
                }
            }
        }

        crate::keys::macros::define_ordered_key_prefix!(
            $key, $context, $start;
            $($field: $codec $(=> $borrowed)?),+
        );
        crate::keys::macros::define_ordered_key_decoder!(@fields $key; []; $($field: $codec),+);
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
macro_rules! define_ordered_key_prefix {
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
        crate::keys::macros::define_ordered_key_prefix!(@fields $key; []; $($field: $codec $(=> $borrowed)?),+);
    };

    (@fields $key:ident; [$($done:ident),*]; $field:ident: $codec:ty $(=> $borrowed:ty)? $(, $next:ident: $next_codec:ty $(=> $next_borrowed:ty)?)* ) => {
        crate::keys::macros::define_ordered_key_prefix!(
            @method $key;
            { crate::keys::macros::ordered_key_count!($($done),*) };
            { crate::keys::macros::ordered_key_count!($($done,)* $field) };
            $field: $codec $(=> $borrowed)?
        );
        crate::keys::macros::define_ordered_key_prefix!(@fields $key; [$($done,)* $field]; $($next: $next_codec $(=> $next_borrowed)?),*);
    };
    (@fields $key:ident; [$($done:ident),*]; ) => {};

    (@method $key:ident; $current:expr; $next:expr; $field:ident: $codec:ty => $borrowed:ty) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<B: bytes::BufMut> [<$key Prefix>]<B, $current> {
                pub fn $field<'a>(
                    self,
                    $field: crate::keys::macros::define_ordered_key!(@arg_type 'a; $codec; $borrowed),
                ) -> [<$key Prefix>]<B, $next> {
                    let value = crate::keys::macros::define_ordered_key!(@borrow $field; $borrowed);
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
macro_rules! define_ordered_key_decoder {
    (@fields $key:ident; [$($done:ident),*]; $field:ident: $codec:ty, $($next:ident: $next_codec:ty),+) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<'a> crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::ordered_key_count!($($done),*) }> {
                pub fn tag(&self) -> &'static str { stringify!($field) }

                pub fn [<decode_$field>](mut self) -> crate::Result<(
                    <$codec as crate::keys::FieldCodec>::Value,
                    crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::ordered_key_count!($($done,)* $field) }>,
                )> {
                    let value = <$codec as crate::keys::FieldCodec>::decode_field(&mut self.remaining)?;
                    Ok((value, crate::keys::KeyDecoder { remaining: self.remaining, _marker: std::marker::PhantomData }))
                }

                pub fn [<take_$field>](mut self) -> crate::Result<(
                    crate::keys::FieldDecoder<'a, $codec>,
                    crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::ordered_key_count!($($done,)* $field) }>,
                )> {
                    let value = crate::keys::FieldDecoder::<$codec>::take(&mut self.remaining)?;
                    Ok((value, crate::keys::KeyDecoder { remaining: self.remaining, _marker: std::marker::PhantomData }))
                }
            }
        }
        crate::keys::macros::define_ordered_key_decoder!(@fields $key; [$($done,)* $field]; $($next: $next_codec),+);
    };
    (@fields $key:ident; [$($done:ident),*]; $field:ident: $codec:ty) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<'a> crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::ordered_key_count!($($done),*) }> {
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

/// Binds a key's fields to the clauses of a logical schema, generating
/// `prepare_selection` (strict) and `prepare_candidates` (widening) on the key;
/// see [`define_ordered_key!`] for what they do. `define_ordered_key!` invokes
/// this for its `schema:`; call it directly for a key declared without one,
/// listing the fields to bind by name:
///
/// ```ignore
/// define_key_selection!(RecordKey; [Record]; sequence, category => non_null, name, kind);
/// ```
///
/// `field => non_null` binds a nullable logical column to a field that stores no
/// NULLs: `IS NULL` matches nothing and NULL drops out of IN lists and ranges.
/// Encoding, validation and nullability belong to the field's
/// [`FieldPredicateCodec`](crate::keys::selection::codec::FieldPredicateCodec);
/// bounds and evaluation to [`KeySelection`](crate::keys::selection::KeySelection).
/// Prefix clauses (`starts_with`) are recognized through
/// `SelectionSchema::starts_with_prefix`; other clauses by the field's CamelCase
/// variant of the schema's `Clause`.
macro_rules! define_key_selection {
    ($key:ident; []; $($fields:tt)*) => {};
    ($key:ident; [$target:ty]; $($field:ident $(=> $prepare:ident)?),+ $(,)?) => {
        impl crate::keys::selection::PrepareKeySelection<$target> for $key {
            fn prepare_candidates(
                selection: &restate_storage_query_api::selection::Selection<$target>,
            ) -> crate::Result<crate::keys::selection::KeySelection<Self>> {
                crate::keys::selection::KeySelection::for_key_fields(selection, Self::prepare_candidate_clause)
            }
        }

        paste::paste! {
            impl $key {
                /// Binds logical clauses to this key's declared physical codecs.
                #[allow(dead_code)] // Some schemas are written before they gain scan callers.
                pub(crate) fn prepare_selection(
                    selection: &restate_storage_query_api::selection::Selection<$target>,
                ) -> crate::Result<crate::keys::selection::KeySelection<Self>> {
                    crate::keys::selection::KeySelection::new(selection, Self::prepare_clause)
                }

                /// Encodes a key-local clause with the codec selected by the declaration.
                /// Call through `KeySelection` to check column coverage first.
                pub(crate) fn prepare_clause(
                    clause: &<$target as restate_storage_query_api::SelectionSchema>::Clause,
                    literals: &mut Vec<u8>,
                ) -> crate::Result<crate::keys::selection::PreparedFieldPredicate> {
                    Self::prepare_candidate_clause(clause, literals)?.ok_or_else(|| {
                        restate_storage_api::StorageError::Conversion(anyhow::anyhow!("selection clause is not supported by this key"))
                    })
                }

                fn prepare_candidate_clause(
                    clause: &<$target as restate_storage_query_api::SelectionSchema>::Clause,
                    literals: &mut Vec<u8>,
                ) -> crate::Result<Option<crate::keys::selection::PreparedFieldPredicate>> {
                    type Clause = <$target as restate_storage_query_api::SelectionSchema>::Clause;
                    type Column = <$target as restate_storage_query_api::SelectionSchema>::Column;
                        // A key may cover only a subset of the logical target.
                        #[allow(unreachable_patterns)]
                        if let Some(prefix) = <$target as restate_storage_query_api::SelectionSchema>::starts_with_prefix(clause) {
                            match <$target as restate_storage_query_api::SelectionSchema>::column(clause) {
                                $(Column::[<$field:camel>] => <<$key as [<$key FieldCodecs>]>::[<$field:camel>] as crate::keys::selection::codec::FieldPredicateCodec>::try_prepare_prefix(prefix, literals),)+
                                _ => Ok(None),
                            }
                        } else {
                            match clause {
                                $(Clause::[<$field:camel>](predicate) => crate::keys::macros::define_key_selection!(
                                    @prepare [$( $prepare )?] [<$key as [<$key FieldCodecs>]>::[<$field:camel>]] predicate, literals
                                ),)+
                                _ => Ok(None),
                            }
                        }
                }
            }
        }
    };
    (@prepare [] [$codec:ty] $predicate:ident, $literals:ident) => {
        <$codec as crate::keys::selection::codec::FieldPredicateCodec>::try_prepare_value($predicate, $literals)
    };
    (@prepare [non_null] [$codec:ty] $predicate:ident, $literals:ident) => {
        crate::keys::selection::codec::prepare_non_null::<$codec>($predicate, $literals)
    };
}

macro_rules! ordered_key_count {
    ($($field:ident),*) => { <[()]>::len(&[$(crate::keys::macros::ordered_key_count!(@unit $field)),*]) };
    (@unit $field:ident) => { () };
}

pub(crate) use {
    define_key_selection, define_ordered_key, define_ordered_key_decoder,
    define_ordered_key_prefix, ordered_key_count,
};

#[cfg(test)]
mod tests;
