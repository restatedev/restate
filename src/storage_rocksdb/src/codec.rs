use bytes::{Buf, BufMut, Bytes};
use bytestring::ByteString;
use prost::Message;
use restate_storage_api::StorageError;

pub trait Codec: Sized {
    fn encode<B: BufMut>(&self, target: &mut B);
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self>;
}

impl Codec for Bytes {
    fn encode<B: BufMut>(&self, target: &mut B) {
        write_delimited(self, target);
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        read_delimited(source)
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
}

impl Codec for u64 {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u64(*self);
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(source.get_u64())
    }
}

impl Codec for u32 {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u32(*self);
    }

    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(source.get_u32())
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
}

impl<'a> Codec for &'a [u8] {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put(*self);
    }

    fn decode<B: Buf>(_source: &mut B) -> crate::Result<Self> {
        unimplemented!("could not decode into a slice u8");
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
}
