// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The object layout of metadata values.
//!
//! An object starts with a marker byte: `e` for a value, followed by the encoded value, or
//! `d` for a deletion tombstone. Deletes write tombstones because object stores cannot
//! delete an object conditionally. Values are written as bilrost, which the object's
//! content encoding records; objects without one are CBOR, the format before v1.6.

use anyhow::{Context, bail};
use bilrost::{Message, OwnedMessage};
use bytes::Bytes;
use object_store::{Attribute, Attributes, PutPayload};

use restate_types::metadata::VersionedValue;

const VALUE_MARKER: Bytes = Bytes::from_static(b"e");
const TOMBSTONE_MARKER: Bytes = Bytes::from_static(b"d");

const BILROST_ENCODING: &str = "binary/bilrost+v1";
const CBOR_ENCODING: &str = "binary/cbor";

/// The contents and attributes of an object to write.
pub(super) struct EncodedObject {
    pub(super) payload: PutPayload,
    pub(super) attributes: Attributes,
}

#[derive(Debug, Clone, bilrost::Message)]
struct SaltedVersionedValue {
    #[bilrost(1)]
    salt: u64,
    #[bilrost(2)]
    value: VersionedValue,
}

#[derive(serde::Deserialize)]
#[serde(tag = "version", content = "value")]
enum CborValue {
    V1(VersionedValue, serde::de::IgnoredAny),
}

pub(super) fn encode_value(value: VersionedValue) -> EncodedObject {
    // S3 derives ETags from the object's contents. A random salt keeps two writes of the same
    // value distinct, so a conditional write cannot succeed against an ETag it did not read.
    encode_salted(value, rand::random())
}

fn encode_salted(value: VersionedValue, salt: u64) -> EncodedObject {
    let body = SaltedVersionedValue { salt, value }.encode_to_bytes();
    let mut attributes = Attributes::new();
    attributes.insert(Attribute::ContentEncoding, BILROST_ENCODING.into());
    EncodedObject {
        payload: PutPayload::from_iter([VALUE_MARKER, body]),
        attributes,
    }
}

/// Unlike values, tombstones need no salt: a conditional write on any tombstone's tag
/// replaces a deleted key, whichever delete wrote it.
pub(super) fn tombstone() -> EncodedObject {
    EncodedObject {
        payload: PutPayload::from_bytes(TOMBSTONE_MARKER),
        attributes: Attributes::new(),
    }
}

/// Decodes an object, returning `None` for a tombstone.
pub(super) fn decode(
    attributes: &Attributes,
    mut object: Bytes,
) -> anyhow::Result<Option<VersionedValue>> {
    if object.starts_with(&TOMBSTONE_MARKER) {
        return Ok(None);
    }
    if !object.starts_with(&VALUE_MARKER) {
        bail!("metadata object starts with neither a value nor a tombstone marker");
    }
    let body = object.split_off(VALUE_MARKER.len());

    let value = match attributes
        .get(&Attribute::ContentEncoding)
        .map(|v| v.as_ref())
    {
        Some(BILROST_ENCODING) => {
            SaltedVersionedValue::decode(body)
                .context("failed to decode bilrost")?
                .value
        }
        Some(CBOR_ENCODING) | None => {
            let CborValue::V1(value, _) = ciborium::from_reader(body.as_ref())?;
            value
        }
        Some(other) => bail!("unknown metadata content encoding '{other}'"),
    };
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use restate_types::Version;

    use super::*;

    const HELLO: Bytes = Bytes::from_static(b"hello");

    fn attributes(encoding: Option<&'static str>) -> Attributes {
        let mut attributes = Attributes::new();
        if let Some(encoding) = encoding {
            attributes.insert(Attribute::ContentEncoding, encoding.into());
        }
        attributes
    }

    /// A value object as earlier versions wrote it: the `e` marker, then the body.
    fn object(body_hex: &str) -> Bytes {
        let mut object = b"e".to_vec();
        object.extend(
            (0..body_hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&body_hex[i..i + 2], 16).expect("valid hex")),
        );
        object.into()
    }

    // Written by v1.6 for version 7 of `hello` with salt 0x0123456789abcdef.
    const BILROST_BODY: &str = "04ef9aaeccf7abd0900005090407050568656c6c6f";
    // Written before v1.6 for the same value and salt.
    const CBOR_BODY: &str = "a26776657273696f6e6256316576616c756582a26776657273696f6e076576616c75654568656c6c6f1b0123456789abcdef";

    #[test]
    fn encodes_values_as_earlier_versions_did() {
        let encoded = encode_salted(
            VersionedValue::new(Version::from(7), HELLO),
            0x0123456789abcdef,
        );
        let written: Vec<u8> = encoded.payload.iter().flatten().copied().collect();

        assert_eq!(Bytes::from(written), object(BILROST_BODY));
        assert_eq!(encoded.attributes, attributes(Some(BILROST_ENCODING)));
    }

    #[test]
    fn decodes_objects_written_by_earlier_versions() {
        for (encoding, body) in [
            (Some(BILROST_ENCODING), BILROST_BODY),
            (Some(CBOR_ENCODING), CBOR_BODY),
            (None, CBOR_BODY),
        ] {
            let value = decode(&attributes(encoding), object(body))
                .unwrap()
                .expect("a value");
            assert_eq!(value.version, Version::from(7));
            assert_eq!(value.value, HELLO);
        }
    }

    /// Earlier versions write tombstones as a bare `d`, and panic on any other content when a
    /// create replaces one.
    #[test]
    fn tombstones_match_earlier_versions() {
        let tombstone = tombstone();
        let written: Vec<u8> = tombstone.payload.iter().flatten().copied().collect();

        assert_eq!(written, b"d");
        assert_eq!(tombstone.attributes, Attributes::new());
        assert!(
            decode(&Attributes::new(), Bytes::from_static(b"d"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_unknown_objects() {
        assert!(decode(&Attributes::new(), Bytes::from_static(b"x")).is_err());
        assert!(decode(&attributes(Some("text/plain")), object("00")).is_err());
    }
}
