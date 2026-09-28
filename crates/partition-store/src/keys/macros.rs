// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Defines an ordered key payload, its borrowed view, physical schema, and typed
/// prefix/decoder steps. The caller supplies the table and fixed-prefix adapter.
///
/// `start` writes the fixed identity from a borrowed context into a supplied
/// `BufMut`. Prefix builders borrow that buffer and append fields directly;
/// callers read or reuse their buffer after the builder's last use.
/// No statistic IDs, partition layout, or value aggregation are assumed here.
///
/// The optional filter binding names a logical target implementing `FilterTarget`;
/// its associated `Clause` type supplies the variants used by the generated binding.
/// Prefix clauses are identified by the logical target and prepared by the codec.
macro_rules! define_index_key {
    (
        $key:ident,
        table: $table:expr,
        context: $context:ty,
        start: $start:expr,
        fields { $($field:ident: $codec:ty $(=> $borrowed:ty)?),+ $(,)? }
        $(filter: $target:ty)?
    ) => {
        #[allow(dead_code)]
        pub struct $key {
            $(pub $field: <$codec as crate::keys::IndexFieldDecode>::Owned,)+
        }

        impl crate::keys::filter::IndexKeySchema for $key {
            const TABLE: crate::TableKind = $table;
            const FIELDS: &'static [crate::keys::filter::IndexKeyField] = &[
                $(crate::keys::filter::IndexKeyField::new::<$codec>(stringify!($field)),)+
            ];
        }

        impl crate::keys::EncodeIndexKey for $key {
            fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                $(crate::keys::IndexFieldEncode::encode_field(&self.$field, bytes);)+
            }

            fn encoded_len(&self) -> usize {
                0 $(+ crate::keys::IndexFieldEncode::serialized_length(&self.$field))+
            }
        }

        impl crate::keys::DecodeIndexKey for $key {
            fn decode(bytes: &mut &[u8]) -> crate::Result<Self> {
                $(let $field = <$codec as crate::keys::IndexFieldDecode>::decode_field(bytes)?;)+
                Ok(Self { $($field,)+ })
            }
        }

        paste::paste! {
            #[allow(dead_code)]
            pub struct [<$key Ref>]<'a> {
                $($field: crate::keys::macros::define_index_key!(@ref_type 'a; $codec $(; $borrowed)?),)+
                _marker: std::marker::PhantomData<&'a ()>,
            }

            #[allow(dead_code)]
            impl $key {
                pub fn borrowed<'a>(
                    $($field: crate::keys::macros::define_index_key!(@arg_type 'a; $codec $(; $borrowed)?),)+
                ) -> [<$key Ref>]<'a> {
                    [<$key Ref>] {
                        $($field: crate::keys::macros::define_index_key!(@borrow $field $(; $borrowed)?),)+
                        _marker: std::marker::PhantomData,
                    }
                }
            }

            impl crate::keys::EncodeIndexKey for [<$key Ref>]<'_> {
                fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                    $(crate::keys::IndexFieldEncode::encode_field(&self.$field, bytes);)+
                }

                fn encoded_len(&self) -> usize {
                    0 $(+ crate::keys::IndexFieldEncode::serialized_length(&self.$field))+
                }
            }
        }

        crate::keys::macros::define_index_key_prefix!(
            $key, $context, $start;
            $($field: $codec $(=> $borrowed)?),+
        );
        crate::keys::macros::define_index_key_decoder!(@fields $key; []; $($field: $codec),+);
        crate::keys::macros::define_index_key_filter!(
            $key; [$($target)?]; $($field: $codec),+
        );
    };

    (@ref_type $lt:lifetime; $codec:ty; $borrowed:ty) => {
        <$codec as crate::keys::IndexFieldView<$borrowed>>::Ref<$lt>
    };
    (@ref_type $lt:lifetime; $codec:ty) => {
        <$codec as crate::keys::IndexFieldDecode>::Owned
    };
    (@arg_type $lt:lifetime; $codec:ty; $borrowed:ty) => {
        impl crate::keys::IntoIndexFieldRef<
            $lt, $borrowed,
            Output = <$codec as crate::keys::IndexFieldView<$borrowed>>::Ref<$lt>,
        >
    };
    (@arg_type $lt:lifetime; $codec:ty) => {
        <$codec as crate::keys::IndexFieldDecode>::Owned
    };
    (@borrow $field:ident; $borrowed:ty) => {
        crate::keys::IntoIndexFieldRef::into_index_field_ref($field)
    };
    (@borrow $field:ident) => { $field };
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
                fn push<T: crate::keys::IndexFieldEncode + ?Sized, const NEXT: usize>(
                    mut self, value: &T,
                ) -> [<$key Prefix>]<B, NEXT> {
                    crate::keys::IndexFieldEncode::encode_field(value, &mut self.buffer);
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
                    self.push::<_, $next>(&value)
                }
            }
        }
    };
    (@method $key:ident; $current:expr; $next:expr; $field:ident: $codec:ty) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<B: bytes::BufMut> [<$key Prefix>]<B, $current> {
                pub fn $field(
                    self, $field: <$codec as crate::keys::IndexFieldDecode>::Owned,
                ) -> [<$key Prefix>]<B, $next> {
                    self.push::<_, $next>(&$field)
                }
            }
        }
    };
}

/// Generates progressive owned and borrowed decoding in declared physical order.
/// The final step rejects trailing bytes rather than silently accepting a suffix.
macro_rules! define_index_key_decoder {
    (@fields $key:ident; [$($done:ident),*]; $field:ident: $codec:ty, $($next:ident: $next_codec:ty),+) => {
        paste::paste! {
            #[allow(dead_code)]
            impl<'a> crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::index_key_count!($($done),*) }> {
                pub fn tag(&self) -> &'static str { stringify!($field) }

                pub fn [<decode_$field>](mut self) -> crate::Result<(
                    <$codec as crate::keys::IndexFieldDecode>::Owned,
                    crate::keys::KeyDecoder<'a, $key, { crate::keys::macros::index_key_count!($($done,)* $field) }>,
                )> {
                    let value = <$codec as crate::keys::IndexFieldDecode>::decode_field(&mut self.remaining)?;
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

                pub fn [<decode_$field>](mut self) -> crate::Result<<$codec as crate::keys::IndexFieldDecode>::Owned> {
                    let value = <$codec as crate::keys::IndexFieldDecode>::decode_field(&mut self.remaining)?;
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
/// to each physical codec; bounds and evaluation remain in PreparedKeyFilter.
macro_rules! define_index_key_filter {
    ($key:ident; []; $($fields:tt)*) => {};
    ($key:ident; [$target:ty]; $($field:ident: $codec:ty),+ $(,)?) => {
        paste::paste! {
            impl $key {
                /// Binds logical clauses to this key's declared physical codecs.
                pub(crate) fn prepare_filter(
                    filter: &restate_storage_api::filter::Filter<$target>,
                ) -> crate::Result<crate::keys::filter::PreparedKeyFilter<Self>> {
                    type Clause = <$target as restate_storage_api::filter::FilterTarget>::Clause;
                    type Field = <$target as restate_storage_api::filter::FilterTarget>::Field;
                    crate::keys::filter::PreparedKeyFilter::new(filter, |clause, literals| {
                        // A key may cover only a subset of the logical target.
                        #[allow(unreachable_patterns)]
                        if let Some(prefix) = <$target as restate_storage_api::filter::FilterTarget>::starts_with_prefix(clause) {
                            match <$target as restate_storage_api::filter::FilterTarget>::field(clause) {
                                $(Field::[<$field:camel>] => <$codec as crate::keys::filter::codec::IndexFilterCodec>::prepare_prefix(prefix),)+
                                _ => Err(restate_storage_api::StorageError::Conversion(anyhow::anyhow!(
                                    "filter field is not supported by this index key"
                                ))),
                            }
                        } else {
                            match clause {
                                $(Clause::[<$field:camel>](predicate) => <$codec as crate::keys::filter::codec::IndexFilterCodec>::prepare_value(predicate, literals),)+
                                _ => Err(restate_storage_api::StorageError::Conversion(anyhow::anyhow!(
                                    "filter clause is not supported by this index key"
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
    define_index_key, define_index_key_decoder, define_index_key_filter, define_index_key_prefix,
    index_key_count,
};

#[cfg(test)]
mod tests;
