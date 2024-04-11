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
use bytes::{Buf, BufMut, Bytes};
use bytestring::ByteString;
use prost::encoding::encoded_len_varint;
use prost::Message;
use restate_storage_api::StorageError;
use restate_types::dedup::ProducerId;
use restate_types::identifiers::InvocationUuid;
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};

pub trait Codec: Sized {
    fn encode<B: BufMut>(&self, target: &mut B);
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self>;

    fn serialized_length(&self) -> usize;
}

impl Codec for Bytes {
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

impl Codec for ByteString {
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

impl Codec for u64 {
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

impl Codec for u32 {
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
impl<T: Codec> Codec for Option<T> {
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

impl<'a> Codec for &'a [u8] {
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

pub struct StorageSerdeValue<V>(pub V);

impl<V> StorageSerdeValue<V> {
    pub fn into_inner(self) -> V {
        self.0
    }
}

impl<V> Codec for StorageSerdeValue<V>
where
    V: StorageEncode + StorageDecode,
{
    fn encode<B: BufMut>(&self, target: &mut B) {
        StorageCodec::encode(&self.0, target).expect("unable to serialize value");
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        StorageCodec::decode::<V>(source.chunk())
            .map_err(|err| StorageError::Generic(err.into()))
            .map(|value| StorageSerdeValue(value))
    }

    fn serialized_length(&self) -> usize {
        0
    }
}

pub struct ProtoValue<M>(pub M);

///
/// Blanket implementation for Protobuf.
///
impl<M: Message + Default> Codec for ProtoValue<M> {
    fn encode<B: BufMut>(&self, target: &mut B) {
        self.0
            .encode(target)
            .expect("Unable to serialize a protobuf message");
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        M::decode(source)
            .map_err(|err| StorageError::Generic(err.into()))
            .map(ProtoValue)
    }

    fn serialized_length(&self) -> usize {
        self.0.encoded_len()
    }
}

impl Codec for InvocationUuid {
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
pub(crate) fn serialize<T: Codec, B: BufMut>(what: &T, target: &mut B) {
    what.encode(target);
}

#[inline]
pub(crate) fn deserialize<T: Codec, B: Buf>(source: &mut B) -> crate::Result<T> {
    T::decode(source)
}

impl Codec for ProducerId {
    fn encode<B: BufMut>(&self, target: &mut B) {
        match self {
            ProducerId::Partition(p) => {
                target.put_u8(0);
                Codec::encode(p, target)
            }
            ProducerId::Other(i) => {
                target.put_u8(1);
                Codec::encode(i, target)
            }
        }
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(match source.get_u8() {
            0 => ProducerId::Partition(Codec::decode(source)?),
            1 => ProducerId::Other(Codec::decode(source)?),
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
            ProducerId::Partition(p) => Codec::serialized_length(p),
            ProducerId::Other(i) => Codec::serialized_length(i),
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
