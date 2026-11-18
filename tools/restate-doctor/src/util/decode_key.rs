// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decoding helpers for partition-store keys with custom layouts.

use restate_partition_store::keys::IndexKeyPrefix;
use restate_partition_store::registry::records::RecordKey;
use restate_partition_store::stats::AggregateKeyPrefix;

use super::hex_encode;

/// Decodes the physical index identity, leaving its index-specific payload raw.
pub fn decode_secondary_index_key(key: &[u8]) -> Option<String> {
    let (prefix, payload) = IndexKeyPrefix::decode_prefix(key).ok()?;
    Some(format!(
        "index_id={}, partition_id={}, payload=0x{}",
        prefix.index_id_raw(),
        prefix.partition_id(),
        hex_encode(payload.remaining),
    ))
}

/// Statistics are keyed by a replica-local index ID; the payload's meaning
/// depends on the registered definition, so it is shown raw.
pub fn decode_stat_key(key: &[u8]) -> Option<String> {
    let (prefix, kind, payload) = AggregateKeyPrefix::decode(key).ok()?;
    Some(format!(
        "partition={}, index={}, stat_kind={kind:?}, payload={payload:02x?}",
        prefix.partition_id(),
        prefix.id()
    ))
}

/// A registry record's key: the partition, the record kind and its ID.
pub fn decode_registry_key(key: &[u8]) -> Option<String> {
    let key = RecordKey::decode(key).ok()?;
    Some(format!(
        "partition={}, record={}, id={}",
        key.partition_id(),
        key.record().name(),
        key.id()
    ))
}

#[cfg(test)]
mod tests {
    use restate_partition_store::registry::records::RecordKind;
    use restate_types::identifiers::PartitionId;
    use zerocopy::IntoBytes;

    use super::*;

    #[test]
    fn registry_keys_show_partition_record_and_id() {
        let key = RecordKey::new(PartitionId::from(7), RecordKind::Job, 64);
        assert_eq!(
            decode_registry_key(key.as_bytes()).as_deref(),
            Some("partition=7, record=job, id=64")
        );
        assert_eq!(decode_registry_key(&key.as_bytes()[..14]), None);
    }

    /// A raw index key: `xI | padding | partition | index ID | payload`.
    fn index_key(padding: u16, partition: u16, index_id: u32, payload: &[u8]) -> Vec<u8> {
        let mut key = b"xI".to_vec();
        key.extend_from_slice(&padding.to_be_bytes());
        key.extend_from_slice(&partition.to_be_bytes());
        key.extend_from_slice(&index_id.to_be_bytes());
        key.extend_from_slice(payload);
        key
    }

    #[test]
    fn index_keys_keep_their_payload_raw_and_malformed_prefixes_are_refused() {
        // A registered index's ID says nothing about its layout: the payload stays raw.
        let key = index_key(0, 7, 64, b"payload");
        assert_eq!(
            decode_secondary_index_key(&key).as_deref(),
            Some(
                format!(
                    "index_id=64, partition_id=7, payload=0x{}",
                    hex_encode(b"payload")
                )
                .as_str()
            )
        );
        // An empty payload is still a valid prefix.
        assert_eq!(
            decode_secondary_index_key(&index_key(0, 7, 65, b"")).as_deref(),
            Some("index_id=65, partition_id=7, payload=0x")
        );
        // A truncated header, a nonzero padding or another key kind are refused.
        assert_eq!(decode_secondary_index_key(&key[..9]), None);
        assert_eq!(decode_secondary_index_key(&index_key(1, 7, 64, b"")), None);
        let mut other = key;
        other[..2].copy_from_slice(b"zR");
        assert_eq!(decode_secondary_index_key(&other), None);
    }
}
