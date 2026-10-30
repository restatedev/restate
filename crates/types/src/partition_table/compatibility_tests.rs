// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use bytes::BytesMut;
use serde_json::Value;
use serde_with::serde_as;

use super::{CfName, DbName, Partition, PartitionReplication, PartitionTable};
use crate::Version;
use crate::identifiers::PartitionId;
use crate::logs::LogId;
use crate::replication::ReplicationProperty;
use crate::sharding::KeyRange;
use crate::storage::{StorageCodec, StorageDecode, StorageEncode};

// Freeze the pre-removal wire definitions independently of the production shadows.
// These definitions were checked against crates/types/src/partition_table.rs in
// upstream tags v1.7.3, v1.7.11, v1.8.0, and v1.8.1 (lines 463-487):
// https://github.com/restatedev/restate/blob/v1.8.1/crates/types/src/partition_table.rs#L463-L487
// In particular, do not add a default or skip_serializing_if to cf_name here.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct LegacyPartition {
    log_id: Option<LogId>,
    key_range: KeyRange,
    #[serde(default)]
    db_name: Option<DbName>,
    cf_name: Option<CfName>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct LegacyPartitionTable {
    version: Version,
    num_partitions: u16,
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    partitions: Option<BTreeMap<PartitionId, LegacyPartition>>,
    replication: Option<PartitionReplication>,
}

crate::flexbuffers_storage_encode_decode!(LegacyPartitionTable);

// Generic metadata tools rewrite JSON without going through PartitionTable.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
struct MetadataDocument(Value);

crate::flexbuffers_storage_encode_decode!(MetadataDocument);

fn storage_roundtrip<S: StorageEncode, D: StorageDecode>(value: &S) -> anyhow::Result<D> {
    let mut buf = BytesMut::new();
    StorageCodec::encode(value, &mut buf)?;
    Ok(StorageCodec::decode(&mut buf)?)
}

fn legacy_table() -> LegacyPartitionTable {
    LegacyPartitionTable {
        version: Version::from(42),
        num_partitions: 2,
        partitions: Some(BTreeMap::from([
            (
                PartitionId::from(0),
                LegacyPartition {
                    log_id: None,
                    key_range: KeyRange::new(0, 1023),
                    db_name: None,
                    cf_name: None,
                },
            ),
            (
                PartitionId::from(7),
                LegacyPartition {
                    log_id: Some(LogId::from(123_u32)),
                    key_range: KeyRange::new(2048, u64::MAX),
                    db_name: None,
                    cf_name: None,
                },
            ),
        ])),
        replication: Some(PartitionReplication::Limit(
            ReplicationProperty::new_unchecked(2),
        )),
    }
}

#[test]
fn legacy_roundtrip_preserves_metadata_after_update() -> anyhow::Result<()> {
    let mut expected = legacy_table();
    let table: PartitionTable = storage_roundtrip(&expected)?;
    let replication = PartitionReplication::Limit(ReplicationProperty::new_unchecked(3));
    let mut builder = table.into_builder();
    builder.set_partition_replication(replication.clone());
    let updated = builder.build_if_modified().unwrap();

    expected.replication = Some(replication);
    expected.version = expected.version.next();
    let old_reader: LegacyPartitionTable = storage_roundtrip(&updated)?;
    assert_eq!(old_reader, expected);
    assert_eq!(
        serde_json::from_value::<LegacyPartitionTable>(serde_json::to_value(&updated)?)?,
        expected
    );

    // An old writer reintroduces null; the new reader still accepts its output.
    let rewritten: PartitionTable = storage_roundtrip(&old_reader)?;
    assert_eq!(rewritten, updated);
    Ok(())
}

fn assert_cf_name_omitted(value: &Value) {
    for entry in value["partitions"].as_array().unwrap() {
        assert!(entry[1].get("cf_name").is_none());
    }
}

#[test]
fn legacy_cf_names_are_ignored_without_cleanup_writes() -> anyhow::Result<()> {
    let expected: PartitionTable = storage_roundtrip(&legacy_table())?;
    // None means absent; Some(Null) means explicitly stored null.
    for field in [
        None,
        Some(Value::Null),
        Some(Value::String("data-7".into())),
        Some(Value::String("legacy-custom-cf".into())),
    ] {
        let mut document = serde_json::to_value(legacy_table())?;
        let partition = &mut document["partitions"][1][1];
        partition.as_object_mut().unwrap().remove("cf_name");
        if let Some(field) = &field {
            partition["cf_name"] = field.clone();
        }

        let json_reader: PartitionTable = serde_json::from_value(document.clone())?;
        let storage_reader: PartitionTable = storage_roundtrip(&MetadataDocument(document))?;
        assert_eq!(json_reader, expected);
        assert_eq!(storage_reader, expected);
        assert!(
            storage_reader
                .clone()
                .into_builder()
                .build_if_modified()
                .is_none()
        );
        assert_cf_name_omitted(&serde_json::to_value(&storage_reader)?);

        let old_reader: LegacyPartitionTable = storage_roundtrip(&storage_reader)?;
        assert_eq!(old_reader, legacy_table());

        // Partition also has its own Serde representation, outside the table shadow.
        let mut document = serde_json::to_value(expected.get(&PartitionId::from(7)).unwrap())?;
        assert!(document.get("cf_name").is_none());
        if let Some(field) = &field {
            document["cf_name"] = field.clone();
        }
        let partition: Partition = serde_json::from_value(document)?;
        assert_eq!(&partition, expected.get(&PartitionId::from(7)).unwrap());
        assert_eq!(partition.cf_name().as_ref(), "data-7");
        // A custom legacy name is tolerated, not honored. This does not assert
        // mixed-version storage equivalence for non-default overrides.
    }
    Ok(())
}
