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
use restate_types::errors::IdDecodeError;
use restate_types::identifiers::{EncodedInvocationId, EntryIndex, InvocationId, ResourceId};
use restate_types::{IdDecoder, IdEncoder, IdResourceType};
use std::fmt::Display;
use std::mem::size_of;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct AwakeableIdentifier {
    invocation_id: InvocationId,
    entry_index: EntryIndex,
}

impl ResourceId for AwakeableIdentifier {
    const SIZE_IN_BYTES: usize = InvocationId::SIZE_IN_BYTES + size_of::<EntryIndex>();
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Awakeable;
    const STRING_CAPACITY_HINT: usize = 0; /* Not needed since encoding is custom */

    /// We use a custom strategy for awakeable identifiers since they need to be encoded as base64
    /// for wider language support.
    fn push_contents_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        let mut input_buf =
            BytesMut::with_capacity(size_of::<EncodedInvocationId>() + size_of::<EntryIndex>());
        input_buf.put_slice(&self.invocation_id.to_bytes());
        input_buf.put_u32(self.entry_index);
        let encoded_base64 = restate_base64_util::URL_SAFE.encode(input_buf.freeze());
        encoder.push_str(encoded_base64);
    }
}

impl AwakeableIdentifier {
    pub fn new(invocation_id: InvocationId, entry_index: EntryIndex) -> Self {
        Self {
            invocation_id,
            entry_index,
        }
    }

    pub fn into_inner(self) -> (InvocationId, EntryIndex) {
        (self.invocation_id, self.entry_index)
    }
}

impl FromStr for AwakeableIdentifier {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the right type
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }
        let remaining = decoder.cursor.take_remaining()?;

        let buffer = restate_base64_util::URL_SAFE
            .decode(remaining)
            .map_err(|_| IdDecodeError::Codec)?;

        if buffer.len() != size_of::<EncodedInvocationId>() + size_of::<EntryIndex>() {
            return Err(IdDecodeError::Length);
        }

        let invocation_id: InvocationId =
            InvocationId::from_slice(&buffer[..size_of::<EncodedInvocationId>()])?;
        let entry_index = EntryIndex::from_be_bytes(
            buffer[size_of::<EncodedInvocationId>()..]
                .try_into()
                // Unwrap is safe because we check the size above.
                .unwrap(),
        );

        Ok(Self {
            invocation_id,
            entry_index,
        })
    }
}

impl Display for AwakeableIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut encoder = IdEncoder::<Self>::new();
        self.push_contents_to_encoder(&mut encoder);
        std::fmt::Display::fmt(&encoder.finalize(), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_types::identifiers::InvocationUuid;

    #[test]
    fn test_encode_decode() {
        let expected_invocation_id = InvocationId::new(92, InvocationUuid::new());
        let expected_entry_index = 2_u32;

        let input_str = AwakeableIdentifier {
            invocation_id: expected_invocation_id,
            entry_index: expected_entry_index,
        }
        .to_string();
        dbg!(&input_str);

        let actual = AwakeableIdentifier::from_str(&input_str).unwrap();
        let (actual_invocation_id, actual_entry_index) = actual.into_inner();

        assert_eq!(expected_invocation_id, actual_invocation_id);
        assert_eq!(expected_entry_index, actual_entry_index);
    }
}
