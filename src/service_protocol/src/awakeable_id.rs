// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::Engine as _;
use bytes::{BufMut, BytesMut};
use restate_types::identifiers::{
    EncodedInvocationId, EntryIndex, InvocationId, InvocationIdParseError,
};
use std::mem::size_of;

#[derive(Debug)]
pub struct AwakeableIdentifier {
    invocation_id: InvocationId,
    entry_index: EntryIndex,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cannot parse the awakeable id, bad length")]
    BadLength,
    #[error("cannot parse the invocation id within the awakeable id: {0}")]
    Uuid(#[from] InvocationIdParseError),
    #[error("cannot parse the awakeable identifier id encoded as base64: {0}")]
    Base64(#[from] base64::DecodeError),
}

impl AwakeableIdentifier {
    pub fn new(invocation_id: InvocationId, entry_index: EntryIndex) -> Self {
        Self {
            invocation_id,
            entry_index,
        }
    }

    pub fn decode<T: AsRef<[u8]>>(id: T) -> Result<Self, Error> {
        let buffer = restate_base64_util::URL_SAFE.decode(id)?;
        if buffer.len() != size_of::<EncodedInvocationId>() + size_of::<EntryIndex>() {
            return Err(Error::BadLength);
        }

        let mut encoded_invocation_id = EncodedInvocationId::default();
        encoded_invocation_id.copy_from_slice(&buffer[..size_of::<EncodedInvocationId>()]);
        let invocation_id = encoded_invocation_id.try_into()?;

        let mut encoded_entry_index: [u8; size_of::<EntryIndex>()] = Default::default();
        encoded_entry_index.copy_from_slice(&buffer[size_of::<EncodedInvocationId>()..]);
        let entry_index = EntryIndex::from_be_bytes(encoded_entry_index);

        Ok(Self {
            invocation_id,
            entry_index,
        })
    }

    pub fn encode(&self) -> String {
        let mut input_buf =
            BytesMut::with_capacity(size_of::<EncodedInvocationId>() + size_of::<EntryIndex>());
        input_buf.put_slice(&self.invocation_id.as_bytes());
        input_buf.put_u32(self.entry_index);
        restate_base64_util::URL_SAFE.encode(input_buf.freeze())
    }

    pub fn into_inner(self) -> (InvocationId, EntryIndex) {
        (self.invocation_id, self.entry_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_types::identifiers::InvocationUuid;

    #[test]
    fn test_encode_decode() {
        let expected_invocation_id = InvocationId::new(92, InvocationUuid::now_v7());
        let expected_entry_index = 2_u32;

        let input_str = AwakeableIdentifier {
            invocation_id: expected_invocation_id.clone(),
            entry_index: expected_entry_index,
        }
        .encode();

        let actual = AwakeableIdentifier::decode(input_str).unwrap();
        let (actual_invocation_id, actual_entry_index) = actual.into_inner();

        assert_eq!(expected_invocation_id, actual_invocation_id);
        assert_eq!(expected_entry_index, actual_entry_index);
    }
}
