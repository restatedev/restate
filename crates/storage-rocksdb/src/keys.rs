// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut, BytesMut};

pub trait TableKey: Sized + Send + 'static {
    fn is_complete(&self) -> bool;
    fn serialize_to<B: BufMut>(&self, bytes: &mut B);
    fn deserialize_from<B: Buf>(bytes: &mut B) -> crate::Result<Self>;
    fn table() -> TableKind;

    fn serialize(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.serialized_length());
        self.serialize_to(&mut buf);
        buf
    }

    fn serialized_length(&self) -> usize;
}

/// The following macro defines an ordered, named key tuple, that is used as a rocksdb key.
///
/// Given the following definition
/// ```ignore
/// define_table_key!(FooBarTable, FooBarKey(foo: u32, bar: Bytes));
/// ```
///
/// This macro expands to:
/// ```ignore
/// use bytes::{Buf, BufMut, Bytes};
/// use restate_storage_rocksdb::TableKind;
/// #[derive(Default, Debug, Eq, PartialEq)]
/// pub struct FooBarKey {
///     pub foo: Option<u32>,
///     pub bar: Option<Bytes>,
/// }
///
/// impl FooBarKey {
///     pub fn foo(&mut self, foo: u32) -> &mut Self {
///         self.foo = Some(foo);
///         self
///     }
///
///     pub fn bar(&mut self, bar: Bytes) -> &mut Self {
///         self.bar = Some(bar);
///         self
///     }
///
///     pub fn into_inner(self) -> (Option<u32>, Option<Bytes>) {
///         return (self.foo, self.bar);
///     }
/// }
///
/// impl crate::keys::TableKey for FooBarKey {
///     fn is_complete(&self) -> bool {
///                 if self.foo.is_none() {
///                     return false;
///                 }
///                 if self.bar.is_none() {
///                     return false;
///                 }
///                 return true;
///     }
///
///      fn serialize_to<B: BufMut>(&self, bytes: &mut B) {
///                 crate::keys::serialize(&self.foo, bytes);
///                 crate::keys::serialize(&self.bar, bytes);
///       }
///
///       fn deserialize_from<B: Buf>(bytes: &mut B) -> crate::Result<Self> {
///                 let mut this: Self = Default::default();
///
///                 this.foo = crate::keys::deserialize(bytes)?;
///                 this.bar = crate::keys::deserialize(bytes)?;
///
///                 return Ok(this);
///       }
///
///     fn table() -> TableKind {
///         FooBarTable
///     }
/// }
///```
///
macro_rules! define_table_key {

    ($table_kind:expr , $key_name: ident ( $($element: ident: $ty: ty),+ $(,)? ) ) => (paste::paste! {
        // main key holder
        #[derive(Default, Debug, Eq, PartialEq, Clone)]
        pub struct $key_name { $(pub $element: Option<$ty>),+ }

        // builder
        impl $key_name {
            $(pub fn $element(mut self, $element: $ty) -> Self {
                self.$element = Some($element);
                self
            })+

             $(pub fn [< $element _ok_or >](&self) -> crate::Result<& $ty> {
                    self.$element.as_ref().ok_or_else(|| restate_storage_api::StorageError::DataIntegrityError)
             })+

            pub fn into_inner(self) -> ($(Option<$ty>,)+) {
                 return ( $(self.$element,)+ )
            }
        }

        // serde
        impl crate::keys::TableKey for $key_name {

            #[inline]
            fn table() -> crate::TableKind {
                $table_kind
            }

            fn is_complete(&self) -> bool {
                $(
                if self.$element.is_none() {
                    return false;
                }
                )+
                return true;
            }

            #[inline]
            fn serialize_to<B: bytes::BufMut>(&self, bytes: &mut B) {
                $(
                crate::codec::serialize(&self.$element, bytes);
                )+
            }

            #[inline]
            fn deserialize_from<B: bytes::Buf>(bytes: &mut B) -> crate::Result<Self> {
                let mut this: Self = Default::default();

                $(
                    this.$element = crate::codec::deserialize(bytes)?;
                )+

                return Ok(this);
            }

            #[inline]
            fn serialized_length(&self) -> usize {
                let mut serialized_length = 0;
                $(
                    serialized_length += crate::codec::Codec::serialized_length(&self.$element);
                )+
                serialized_length
            }
        }
    })
}

use crate::TableKind;
pub(crate) use define_table_key;
