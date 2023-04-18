use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use restate_common::types::PartitionKey;
use restate_storage_api::StorageError;

pub(crate) fn write_delimited<B: BufMut>(source: impl AsRef<[u8]>, target: &mut B) {
    let source = source.as_ref();
    prost::encoding::encode_varint(source.len() as u64, target);
    target.put(source);
}

pub(crate) fn read_delimited(source: &mut Bytes) -> crate::Result<Bytes> {
    let len = prost::encoding::decode_varint(source)
        .map_err(|error| StorageError::Generic(error.into()))?;

    Ok(source.split_to(len as usize))
}

pub(crate) fn skip_delimited(source: &mut Bytes) -> crate::Result<()> {
    let len = prost::encoding::decode_varint(source)
        .map_err(|error| StorageError::Generic(error.into()))?;

    source.advance(len as usize);

    Ok(())
}

#[inline]
pub(crate) fn end_key_successor(partition_key: PartitionKey) -> Option<[u8; 8]> {
    partition_key
        .checked_add(1)
        .map(|successor| successor.to_be_bytes())
}

#[inline]
pub(crate) fn u64_pair(a: u64, b: u64) -> [u8; 16] {
    let mut buffer = [0u8; 16];
    buffer[0..8].copy_from_slice(&a.to_be_bytes());
    buffer[8..].copy_from_slice(&b.to_be_bytes());
    buffer
}

#[inline]
pub(crate) fn u64_pair_from_slice(slice: &[u8]) -> (u64, u64) {
    let mut buf = [0u8; 8];

    buf[0..8].copy_from_slice(&slice[0..8]);
    let a = u64::from_be_bytes(buf);

    buf[0..8].copy_from_slice(&slice[8..16]);
    let b = u64::from_be_bytes(buf);

    (a, b)
}

#[cfg(test)]
mod tests {
    use crate::composite_keys::{
        end_key_successor, read_delimited, u64_pair, u64_pair_from_slice, write_delimited,
    };
    use bytes::{Bytes, BytesMut};

    #[test]
    fn u64_lex_sorting() {
        let a = u64_pair(1336, 200);
        let b = u64_pair(1337, 100);

        assert!(a < b);
    }

    #[test]
    fn u64_pair_round_trip() {
        let buf = u64_pair(1336, 200);
        let (a, b) = u64_pair_from_slice(&buf);

        assert_eq!(a, 1336);
        assert_eq!(b, 200);
    }

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
    fn end_key_successor_test() {
        assert_eq!(None, end_key_successor(u64::MAX));

        assert_eq!(
            Some(u64::MAX.to_be_bytes()),
            end_key_successor(u64::MAX - 1)
        );

        assert_eq!(Some(15u64.to_be_bytes()), end_key_successor(14));
    }
}
