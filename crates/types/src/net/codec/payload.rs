// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use bytes::{BufMut, Bytes, BytesMut};

/// A version-checked payload ready to write into the transport's final buffer.
/// Most messages are already contiguous. Prepared messages retain their shared
/// backing until the transport encodes them, without an intermediate payload copy.
#[derive(Debug)]
pub struct EncodedPayload(Inner);

#[derive(Debug)]
enum Inner {
    Bytes(Bytes),
    Deferred {
        len: usize,
        encoder: Box<dyn PayloadEncoder>,
    },
}

trait PayloadEncoder: Debug + Send + Sync {
    fn encode_into(&self, dst: &mut dyn BufMut);
}

impl<T: bilrost::Message + Debug + Send + Sync> PayloadEncoder for T {
    fn encode_into(&self, mut dst: &mut dyn BufMut) {
        self.encode(&mut dst)
            .expect("payload buffer has enough space");
    }
}

impl EncodedPayload {
    /// Retain an immutable Bilrost message for forward encoding. Protocol
    /// validation must be completed by the caller before constructing this value.
    pub fn from_bilrost(message: impl bilrost::Message + Debug + Send + Sync + 'static) -> Self {
        Self(Inner::Deferred {
            len: message.encoded_len(),
            encoder: Box::new(message),
        })
    }

    pub fn len(&self) -> usize {
        match &self.0 {
            Inner::Bytes(bytes) => bytes.len(),
            Inner::Deferred { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the bytes only if the payload is already materialized.
    pub fn try_into_bytes(self) -> Result<Bytes, Self> {
        match self.0 {
            Inner::Bytes(bytes) => Ok(bytes),
            _ => Err(self),
        }
    }

    /// The destination must have at least `self.len()` bytes remaining.
    pub fn encode_into(&self, dst: &mut impl BufMut) {
        match &self.0 {
            Inner::Bytes(bytes) => dst.put_slice(bytes),
            Inner::Deferred { encoder, .. } => encoder.encode_into(dst),
        }
    }

    /// Materialize only where a contiguous payload is required, e.g. loopback.
    pub fn into_bytes(self) -> Bytes {
        match self.0 {
            Inner::Bytes(bytes) => bytes,
            Inner::Deferred { len, encoder } => {
                let mut dst = BytesMut::with_capacity(len);
                encoder.encode_into(&mut dst);
                debug_assert_eq!(dst.len(), len);
                dst.freeze()
            }
        }
    }
}

impl From<Bytes> for EncodedPayload {
    fn from(bytes: Bytes) -> Self {
        Self(Inner::Bytes(bytes))
    }
}
