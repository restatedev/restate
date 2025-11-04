// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::Engine as _;

use super::invocation::{EncodedInvocationId, WithInvocationId};
use super::{IdDecodeError, IdDecoder, IdEncoder, IdResourceType, InvocationId, ResourceId};
use crate::journal::EntryIndex;

#[derive(
    Debug, Clone, PartialEq, Eq, serde_with::SerializeDisplay, serde_with::DeserializeFromStr,
)]
pub struct ExternalSignalIdentifier {
    invocation_id: InvocationId,
    signal_index: u32,
}

impl ResourceId for ExternalSignalIdentifier {
    const RAW_BYTES_LEN: usize = size_of::<EncodedInvocationId>() + size_of::<EntryIndex>();
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Signal;

    type StrEncodedLen = ::generic_array::ConstArrayLength<
        // prefix + separator + version + suffix (38 chars)
        {
            Self::RESOURCE_TYPE.as_str().len()
                + 2
                + base64::encoded_len(
                    size_of::<EncodedInvocationId>() + size_of::<EntryIndex>(),
                    false,
                )
                .expect("awakeable id is far from usize limit")
        },
    >;

    /// We use a custom strategy for awakeable identifiers since they need to be encoded as base64
    /// for wider language support.
    fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        let mut input_buf = [0u8; Self::RAW_BYTES_LEN];
        let pos = self
            .invocation_id
            .encode_raw_bytes(&mut input_buf[..InvocationId::RAW_BYTES_LEN]);
        input_buf[pos..].copy_from_slice(&self.signal_index.to_be_bytes());

        let written = restate_base64_util::URL_SAFE
            .encode_slice(input_buf, encoder.remaining_mut())
            .expect("base64 encoding succeeds for system-generated ids");
        encoder.advance(written);
    }
}

impl ExternalSignalIdentifier {
    pub fn new(invocation_id: InvocationId, signal_index: u32) -> Self {
        Self {
            invocation_id,
            signal_index,
        }
    }

    pub fn into_inner(self) -> (InvocationId, u32) {
        (self.invocation_id, self.signal_index)
    }
}

impl std::str::FromStr for ExternalSignalIdentifier {
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
        let signal_index = u32::from_be_bytes(
            buffer[size_of::<EncodedInvocationId>()..]
                .try_into()
                // Unwrap is safe because we check the size above.
                .unwrap(),
        );

        Ok(Self {
            invocation_id,
            signal_index,
        })
    }
}

impl std::fmt::Display for ExternalSignalIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut encoder = IdEncoder::new();
        self.push_to_encoder(&mut encoder);
        f.write_str(encoder.as_str())
    }
}

impl WithInvocationId for ExternalSignalIdentifier {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn roundtrip_signal_id() {
        let expected_invocation_id = InvocationId::mock_random();
        let expected_signal_index = 2_u32;

        let input_str = ExternalSignalIdentifier {
            invocation_id: expected_invocation_id,
            signal_index: expected_signal_index,
        }
        .to_string();
        dbg!(&input_str);

        let actual = ExternalSignalIdentifier::from_str(&input_str).unwrap();
        let (actual_invocation_id, actual_signal_id) = actual.into_inner();

        assert_eq!(expected_invocation_id, actual_invocation_id);
        assert_eq!(expected_signal_index, actual_signal_id);
    }
}
