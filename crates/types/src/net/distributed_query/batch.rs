// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arrow_buffer::{Buffer, MutableBuffer};
use bilrost::DecodeError;
use bilrost::buf::ReverseBuf;
use bilrost::encoding::{
    Capped, DecodeContext, EmptyState, ForOverwrite, GeneralGeneric, ValueDecoder, ValueEncoder,
    WireType, Wiretyped, encode_varint, encoded_len_varint, prepend_varint,
};
use bytes::{Buf, BufMut, Bytes};

/// The part of an output partition's Arrow IPC stream that a `Batch` reply
/// carries, as the buffers its encoder produced.
///
/// On the wire it is one byte string, exactly as a `Bytes` field: the buffers
/// are written into the message one after the other, without first being
/// copied into one. A decoded batch is a single buffer that borrows the
/// message's bytes.
#[derive(Clone, Default)]
pub struct QueryTaskBatch(Vec<Buffer>);

impl QueryTaskBatch {
    /// The bytes of all buffers.
    pub fn len(&self) -> usize {
        self.0.iter().map(Buffer::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(Buffer::is_empty)
    }

    /// The batch's bytes as one buffer: a decoded batch's own buffer, else a
    /// copy of its buffers.
    pub fn into_buffer(self) -> Buffer {
        match <[Buffer; 1]>::try_from(self.0) {
            Ok([buffer]) => buffer,
            Err(buffers) => {
                let mut bytes = MutableBuffer::with_capacity(buffers.iter().map(Buffer::len).sum());
                for buffer in &buffers {
                    bytes.extend_from_slice(buffer);
                }
                bytes.into()
            }
        }
    }
}

impl From<Vec<Buffer>> for QueryTaskBatch {
    fn from(buffers: Vec<Buffer>) -> Self {
        Self(buffers)
    }
}

impl From<Bytes> for QueryTaskBatch {
    fn from(bytes: Bytes) -> Self {
        Self(vec![bytes.into()])
    }
}

impl std::fmt::Debug for QueryTaskBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueryTaskBatch({} bytes)", self.len())
    }
}

impl<const P: u8> Wiretyped<GeneralGeneric<P>, QueryTaskBatch> for () {
    const WIRE_TYPE: WireType = WireType::LengthDelimited;
}

impl<const P: u8> ValueEncoder<GeneralGeneric<P>, QueryTaskBatch> for () {
    fn encode_value<B: BufMut + ?Sized>(value: &QueryTaskBatch, buf: &mut B) {
        encode_varint(value.len() as u64, buf);
        for buffer in &value.0 {
            buf.put_slice(buffer);
        }
    }

    fn prepend_value<B: ReverseBuf + ?Sized>(value: &QueryTaskBatch, buf: &mut B) {
        for buffer in value.0.iter().rev() {
            buf.prepend_slice(buffer);
        }
        prepend_varint(value.len() as u64, buf);
    }

    fn value_encoded_len(value: &QueryTaskBatch) -> usize {
        let len = value.len();
        encoded_len_varint(len as u64) + len
    }
}

impl<const P: u8> ValueDecoder<GeneralGeneric<P>, QueryTaskBatch> for () {
    fn decode_value<B: Buf + ?Sized>(
        value: &mut QueryTaskBatch,
        buf: Capped<B>,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let mut bytes = Bytes::new();
        <() as ValueDecoder<GeneralGeneric<P>, Bytes>>::decode_value(&mut bytes, buf, ctx)?;
        *value = bytes.into();
        Ok(())
    }
}

impl ForOverwrite<(), QueryTaskBatch> for () {
    fn for_overwrite() -> QueryTaskBatch {
        QueryTaskBatch::default()
    }
}

impl EmptyState<(), QueryTaskBatch> for () {
    fn is_empty(val: &QueryTaskBatch) -> bool {
        val.is_empty()
    }

    fn clear(val: &mut QueryTaskBatch) {
        val.0.clear();
    }
}
