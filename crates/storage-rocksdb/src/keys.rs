// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use prost::encoding::encoded_len_varint;

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
                $crate::keys::serialize(&self.$element, bytes);
                )+
            }

            #[inline]
            fn deserialize_from<B: bytes::Buf>(bytes: &mut B) -> crate::Result<Self> {
                let mut this: Self = Default::default();

                $(
                    this.$element = $crate::keys::deserialize(bytes)?;
                )+

                return Ok(this);
            }

            #[inline]
            fn serialized_length(&self) -> usize {
                let mut serialized_length = 0;
                $(
                    serialized_length += $crate::keys::KeyCodec::serialized_length(&self.$element);
                )+
                serialized_length
            }
        }
    })
}

use crate::TableKind;
pub(crate) use define_table_key;
use restate_storage_api::deduplication_table::ProducerId;
use restate_storage_api::StorageError;
use restate_types::identifiers::InvocationUuid;

pub(crate) trait KeyCodec: Sized {
    fn encode<B: BufMut>(&self, target: &mut B);
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self>;

    fn serialized_length(&self) -> usize;
}

impl KeyCodec for Bytes {
    fn encode<B: BufMut>(&self, target: &mut B) {
        write_delimited(self, target);
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        read_delimited(source)
    }

    fn serialized_length(&self) -> usize {
        self.len()
            + encoded_len_varint(u64::try_from(self.len()).expect("usize should fit into u64"))
    }
}

impl KeyCodec for ByteString {
    fn encode<B: BufMut>(&self, target: &mut B) {
        write_delimited(self, target);
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let bs = read_delimited(source)?;

        unsafe { Ok(ByteString::from_bytes_unchecked(bs)) }
    }

    fn serialized_length(&self) -> usize {
        self.len()
            + encoded_len_varint(u64::try_from(self.len()).expect("usize should fit into u64"))
    }
}

impl KeyCodec for u64 {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u64(*self);
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(source.get_u64())
    }

    fn serialized_length(&self) -> usize {
        8
    }
}

impl KeyCodec for u32 {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u32(*self);
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(source.get_u32())
    }

    fn serialized_length(&self) -> usize {
        4
    }
}

///
/// Blanket implementation for Option.
///
impl<T: KeyCodec> KeyCodec for Option<T> {
    fn encode<B: BufMut>(&self, target: &mut B) {
        if let Some(t) = self {
            t.encode(target);
        }
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        if !source.has_remaining() {
            return Ok(None);
        }
        let res = T::decode(source)?;
        Ok(Some(res))
    }

    fn serialized_length(&self) -> usize {
        self.as_ref().map(|v| v.serialized_length()).unwrap_or(0)
    }
}

impl<'a> KeyCodec for &'a [u8] {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put(*self);
    }

    fn decode<B: Buf>(_source: &mut B) -> crate::Result<Self> {
        unimplemented!("could not decode into a slice u8");
    }

    fn serialized_length(&self) -> usize {
        self.len()
    }
}

impl KeyCodec for InvocationUuid {
    fn encode<B: BufMut>(&self, target: &mut B) {
        let slice = self.to_bytes();
        debug_assert_eq!(slice.len(), self.serialized_length());
        target.put_slice(&slice);
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        // note: this is a zero-copy when the source is bytes::Bytes.
        if source.remaining() < InvocationUuid::SIZE_IN_BYTES {
            return Err(StorageError::DataIntegrityError);
        }
        let bytes = source.copy_to_bytes(InvocationUuid::SIZE_IN_BYTES);
        InvocationUuid::from_slice(&bytes).map_err(|err| StorageError::Generic(err.into()))
    }

    fn serialized_length(&self) -> usize {
        InvocationUuid::SIZE_IN_BYTES
    }
}

#[inline]
fn write_delimited<B: BufMut>(source: impl AsRef<[u8]>, target: &mut B) {
    let source = source.as_ref();
    prost::encoding::encode_varint(source.len() as u64, target);
    target.put(source);
}

#[inline]
fn read_delimited<B: Buf>(source: &mut B) -> crate::Result<Bytes> {
    let len = prost::encoding::decode_varint(source)
        .map_err(|error| StorageError::Generic(error.into()))?;
    // note: this is a zero-copy when the source is bytes::Bytes.
    Ok(source.copy_to_bytes(len as usize))
}

#[inline]
pub(crate) fn serialize<T: KeyCodec, B: BufMut>(what: &T, target: &mut B) {
    what.encode(target);
}

#[inline]
pub(crate) fn deserialize<T: KeyCodec, B: Buf>(source: &mut B) -> crate::Result<T> {
    T::decode(source)
}

impl KeyCodec for ProducerId {
    fn encode<B: BufMut>(&self, target: &mut B) {
        match self {
            ProducerId::Partition(p) => {
                target.put_u8(0);
                KeyCodec::encode(p, target)
            }
            ProducerId::Other(i) => {
                target.put_u8(1);
                KeyCodec::encode(i, target)
            }
        }
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(match source.get_u8() {
            0 => ProducerId::Partition(KeyCodec::decode(source)?),
            1 => ProducerId::Other(KeyCodec::decode(source)?),
            i => {
                return Err(StorageError::Generic(anyhow!(
                    "Unexpected wrong discriminator for SequenceNumberSource: {}",
                    i
                )))
            }
        })
    }

    fn serialized_length(&self) -> usize {
        1 + match self {
            ProducerId::Partition(p) => KeyCodec::serialized_length(p),
            ProducerId::Other(i) => KeyCodec::serialized_length(i),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn write_read_round_trip() {
        let mut buf = BytesMut::new();
        write_delimited("hello", &mut buf);
        write_delimited(" ", &mut buf);
        write_delimited("world", &mut buf);

        let mut got = buf.freeze();
        assert_eq!(read_delimited(&mut got).unwrap(), "hello");
        assert_eq!(read_delimited(&mut got).unwrap(), " ");
        assert_eq!(read_delimited(&mut got).unwrap(), "world");
    }

    fn concat(a: &'static str, b: &'static str) -> Bytes {
        let mut buf = BytesMut::new();
        write_delimited(a, &mut buf);
        write_delimited(b, &mut buf);
        buf.freeze()
    }

    #[test]
    fn write_delim_keeps_lexicographical_sorting() {
        assert!(concat("a", "b") < concat("a", "c"));
        assert!(concat("a", "") < concat("d", ""));
    }

    #[test]
    fn invocation_uuid_roundtrip() {
        let uuid = InvocationUuid::new();

        let mut buf = BytesMut::new();
        uuid.encode(&mut buf);

        let mut got_bytes = buf.freeze();

        assert_eq!(got_bytes.len(), uuid.serialized_length());
        let got = InvocationUuid::decode(&mut got_bytes).expect("deserialization should work");

        assert_eq!(uuid, got);
    }
}
