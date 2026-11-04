// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use base64::Engine;
use bytes::Bytes;
use datafusion::arrow::array::{BooleanArray, Int64Array, LargeStringArray};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet, MessageOptions,
};
use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_types::identifiers::ServiceId;
use restate_types::schema::service::ServiceMetadata;

use super::{PROTO_DESCRIPTOR_SET_KEY, PROTO_STATE_KEY_PREFIX};
use crate::mocks::{MockQueryEngine, MockSchemas, MockStatusHandle};

#[derive(Clone, PartialEq, prost::Message)]
struct BusyMemberProto {
    #[prost(string, tag = "1")]
    member_id: String,
    #[prost(string, tag = "2")]
    email: String,
    #[prost(string, tag = "3")]
    tenant_id: String,
    #[prost(string, tag = "4")]
    phone: String,
    #[prost(string, tag = "5")]
    external_id: String,
    #[prost(string, tag = "6")]
    status: String,
    #[prost(string, tag = "7")]
    tier: String,
    #[prost(int64, tag = "8")]
    balance_cents: i64,
    #[prost(uint64, tag = "9")]
    churn_count: u64,
    #[prost(uint64, tag = "10")]
    last_churn_at: u64,
    #[prost(bool, tag = "11")]
    armed: bool,
    #[prost(uint64, tag = "12")]
    generation: u64,
    #[prost(btree_map = "string, string", tag = "15")]
    metadata: BTreeMap<String, String>,
}

fn build_busy_member_descriptor_set() -> Vec<u8> {
    let field = |name: &str, num: i32, ty: Type, label: Label, type_name: Option<&str>| {
        FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(num),
            label: Some(label as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(ToString::to_string),
            ..Default::default()
        }
    };

    let map_entry = DescriptorProto {
        name: Some("MetadataEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field("value", 2, Type::String, Label::Optional, None),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };

    let busy_member_msg = DescriptorProto {
        name: Some("BusyMemberProfile".to_string()),
        field: vec![
            field("member_id", 1, Type::String, Label::Optional, None),
            field("email", 2, Type::String, Label::Optional, None),
            field("tenant_id", 3, Type::String, Label::Optional, None),
            field("phone", 4, Type::String, Label::Optional, None),
            field("external_id", 5, Type::String, Label::Optional, None),
            field("status", 6, Type::String, Label::Optional, None),
            field("tier", 7, Type::String, Label::Optional, None),
            field("balance_cents", 8, Type::Int64, Label::Optional, None),
            field("churn_count", 9, Type::Uint64, Label::Optional, None),
            field("last_churn_at", 10, Type::Uint64, Label::Optional, None),
            field("armed", 11, Type::Bool, Label::Optional, None),
            field("generation", 12, Type::Uint64, Label::Optional, None),
            field(
                "metadata",
                15,
                Type::Message,
                Label::Repeated,
                Some(".previ.bench.BusyMemberProfile.MetadataEntry"),
            ),
        ],
        nested_type: vec![map_entry],
        ..Default::default()
    };

    let file = FileDescriptorProto {
        name: Some("busy_member.proto".to_string()),
        package: Some("previ.bench".to_string()),
        message_type: vec![busy_member_msg],
        syntax: Some("proto3".to_string()),
        ..Default::default()
    };

    let fds = FileDescriptorSet { file: vec![file] };
    fds.encode_to_vec()
}

fn make_service_metadata_with_proto(name: &str) -> ServiceMetadata {
    let fds_bytes = build_busy_member_descriptor_set();
    let fds_b64 = base64::engine::general_purpose::STANDARD.encode(&fds_bytes);
    let mut metadata = HashMap::new();
    metadata.insert(PROTO_DESCRIPTOR_SET_KEY.to_string(), fds_b64);
    metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}profile"),
        "previ.bench.BusyMemberProfile".to_string(),
    );

    let mut svc = ServiceMetadata::mock_virtual_object(name, ["seed", "tick", "get_state"]);
    svc.metadata = metadata;
    svc
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_proto_schema_registry_and_sql_udfs() {
    let mut mock_schemas = MockSchemas::default();
    mock_schemas
        .0
        .add(make_service_metadata_with_proto("BusyMember"));

    let mut engine = MockQueryEngine::create_with(MockStatusHandle::default(), mock_schemas).await;

    let mut meta = BTreeMap::new();
    meta.insert("region".to_string(), "us-west".to_string());
    meta.insert("department".to_string(), "Engineering".to_string());
    meta.insert("carrier_code".to_string(), "BCBS".to_string());

    let proto_record = BusyMemberProto {
        member_id: "mbr_000543210".to_string(),
        email: "user_543210@tenant_010.com".to_string(),
        tenant_id: "tenant_010".to_string(),
        phone: "+15550543210".to_string(),
        external_id: "EXT-543210".to_string(),
        status: "active".to_string(),
        tier: "enterprise".to_string(),
        balance_cents: 14250,
        churn_count: 87,
        last_churn_at: 1_730_000_000_000,
        armed: true,
        generation: 3,
        metadata: meta,
    };

    let encoded_bytes = proto_record.encode_to_vec();
    {
        let mut tx = engine.partition_store().transaction();
        tx.put_user_state(
            &ServiceId::new(None, "BusyMember", "mbr_000543210"),
            &Bytes::from_static(b"profile"),
            &encoded_bytes,
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    // 1. Verify sys_proto_schemas introspection table
    let schema_batches: Vec<RecordBatch> = engine
        .execute("SELECT service_name, state_key, message_name FROM sys_proto_schemas")
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(schema_batches.len(), 1);
    let sb = &schema_batches[0];
    let svc_col = sb
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    let msg_col = sb
        .column_by_name("message_name")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(svc_col.value(0), "BusyMember");
    assert_eq!(msg_col.value(0), "previ.bench.BusyMemberProfile");

    // 2. Verify proto_get_str, proto_get_int, proto_get_bool, map path, and proto_to_json
    let query = "SELECT
            service_key,
            proto_get_str(service_name, key, value, 'phone') AS phone,
            proto_get_str(service_name, key, value, 'tenant_id') AS tenant_id,
            proto_get_str(service_name, key, value, 'metadata.department') AS department,
            proto_get_int(service_name, key, value, 'balance_cents') AS balance_cents,
            proto_get_bool(service_name, key, value, 'armed') AS armed,
            proto_to_json(service_name, key, value) AS profile_json
        FROM state
        WHERE scope IS NULL AND service_name = 'BusyMember' AND service_key = 'mbr_000543210'";

    let batches: Vec<RecordBatch> = engine
        .execute(query)
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1);

    let phone = b
        .column_by_name("phone")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);
    let tenant = b
        .column_by_name("tenant_id")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);
    let dept = b
        .column_by_name("department")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);
    let balance = b
        .column_by_name("balance_cents")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let armed = b
        .column_by_name("armed")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0);
    let json_str = b
        .column_by_name("profile_json")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);

    assert_eq!(phone, "+15550543210");
    assert_eq!(tenant, "tenant_010");
    assert_eq!(dept, "Engineering");
    assert_eq!(balance, 14250);
    assert!(armed);

    let parsed_json: serde_json::Value = serde_json::from_str(json_str).unwrap();
    assert_eq!(parsed_json["member_id"], "mbr_000543210");
    assert_eq!(parsed_json["balance_cents"], 14250);
    assert_eq!(parsed_json["metadata"]["carrier_code"], "BCBS");
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn benchmark_proto_vs_json_state_scan_in_rocksdb() {
    let mut mock_schemas = MockSchemas::default();
    mock_schemas
        .0
        .add(make_service_metadata_with_proto("BusyMemberProtoSvc"));

    let mut engine = MockQueryEngine::create_with(MockStatusHandle::default(), mock_schemas).await;

    let num_records: usize = 50_000;
    let mut total_proto_bytes: usize = 0;
    let mut total_json_bytes: usize = 0;
    let state_key = Bytes::from_static(b"profile");

    {
        let mut tx = engine.partition_store().transaction();
        for i in 0..num_records {
            let tid = format!("tenant_{:03}", i % 100);
            let mbr_id = format!("mbr_{i:09}");
            let phone = format!("+1555{i:07}");
            let bal = 10_000 + ((i * 37) % 15_000) as i64;

            let mut meta = BTreeMap::new();
            meta.insert("region".to_string(), "us-west".to_string());
            meta.insert("department".to_string(), "Engineering".to_string());
            meta.insert("carrier_code".to_string(), "BCBS".to_string());
            meta.insert("plan_code".to_string(), "PLN-BCBS-US-WEST-2026".to_string());
            meta.insert(
                "notes".to_string(),
                "Previ B2B employer-sponsored benefit line eligible for automated carrier reconciliation, monthly payroll deduction ledger sync, and split-phase Restate durable state transitions.".to_string(),
            );

            let proto_rec = BusyMemberProto {
                member_id: mbr_id.clone(),
                email: format!("user_{i}@{tid}.com"),
                tenant_id: tid.clone(),
                phone: phone.clone(),
                external_id: format!("EXT-{i:06}"),
                status: if i % 10 == 0 {
                    "waived".to_string()
                } else {
                    "active".to_string()
                },
                tier: "enterprise".to_string(),
                balance_cents: bal,
                churn_count: 1,
                last_churn_at: 1_730_000_000_000,
                armed: false,
                generation: 1,
                metadata: meta.clone(),
            };
            let p_bytes = proto_rec.encode_to_vec();
            total_proto_bytes += p_bytes.len();
            tx.put_user_state(
                &ServiceId::new(None, "BusyMemberProtoSvc", mbr_id.as_str()),
                &state_key,
                &p_bytes,
            )
            .unwrap();
        }
        tx.commit().await.unwrap();
    }

    let proto_q3 = "SELECT count(*) AS total, sum(proto_get_int(service_name, key, value, 'balance_cents')) AS balance
        FROM state
        WHERE service_name = 'BusyMemberProtoSvc'
          AND proto_get_str(service_name, key, value, 'tenant_id') = 'tenant_012'";

    // Warm up Protobuf scan
    let _ = engine
        .execute(proto_q3)
        .await
        .unwrap()
        .stream
        .collect::<Vec<_>>()
        .await;

    let t0 = Instant::now();
    let proto_res: Vec<RecordBatch> = engine
        .execute(proto_q3)
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let proto_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Now overwrite the exact same 50,000 keys with JSON payloads so RocksDB holds 50k JSON rows
    {
        let mut tx = engine.partition_store().transaction();
        for i in 0..num_records {
            let tid = format!("tenant_{:03}", i % 100);
            let mbr_id = format!("mbr_{i:09}");
            let phone = format!("+1555{i:07}");
            let bal = 10_000 + ((i * 37) % 15_000) as i64;
            let mut meta = BTreeMap::new();
            meta.insert("region".to_string(), "us-west".to_string());
            meta.insert("department".to_string(), "Engineering".to_string());
            meta.insert("carrier_code".to_string(), "BCBS".to_string());
            meta.insert("plan_code".to_string(), "PLN-BCBS-US-WEST-2026".to_string());
            meta.insert(
                "notes".to_string(),
                "Previ B2B employer-sponsored benefit line eligible for automated carrier reconciliation, monthly payroll deduction ledger sync, and split-phase Restate durable state transitions.".to_string(),
            );
            let j_bytes = serde_json::to_vec(&serde_json::json!({
                "member_id": mbr_id,
                "email": format!("user_{i}@{tid}.com"),
                "tenant_id": tid,
                "phone": phone,
                "external_id": format!("EXT-{i:06}"),
                "status": if i % 10 == 0 { "waived" } else { "active" },
                "tier": "enterprise",
                "balance_cents": bal,
                "churn_count": 1,
                "last_churn_at": 1_730_000_000_000u64,
                "armed": false,
                "generation": 1,
                "metadata": meta,
            }))
            .unwrap();
            total_json_bytes += j_bytes.len();

            tx.put_user_state(
                &ServiceId::new(None, "BusyMemberProtoSvc", mbr_id.as_str()),
                &state_key,
                &j_bytes,
            )
            .unwrap();
        }
        tx.commit().await.unwrap();
    }

    let json_q3 =
        "SELECT count(*) AS total, sum(json_get_int(value_utf8, 'balance_cents')) AS balance
        FROM state
        WHERE service_name = 'BusyMemberProtoSvc'
          AND json_get_str(value_utf8, 'tenant_id') = 'tenant_012'";

    let _ = engine
        .execute(json_q3)
        .await
        .unwrap()
        .stream
        .collect::<Vec<_>>()
        .await;

    let t1 = Instant::now();
    let json_res: Vec<RecordBatch> = engine
        .execute(json_q3)
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let json_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let p_count = proto_res[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let p_bal = proto_res[0]
        .column_by_name("balance")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let j_count = json_res[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let j_bal = json_res[0]
        .column_by_name("balance")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(p_count, 500);
    assert_eq!(p_count, j_count);
    assert_eq!(p_bal, j_bal);

    eprintln!(
        "\n=== ROCKSDB STATE SCAN BENCHMARK ({num_records} records per format) ===\n\
         Protobuf total size: {:.2} MB ({:.1} B/row) vs JSON total size: {:.2} MB ({:.1} B/row)\n\
         Protobuf Q3 Scan:    {:.2} ms\n\
         JSON Q3 Scan:        {:.2} ms\n\
         Speedup:             {:.2}x faster, {:.2}x smaller in RocksDB\n",
        total_proto_bytes as f64 / (1024.0 * 1024.0),
        total_proto_bytes as f64 / num_records as f64,
        total_json_bytes as f64 / (1024.0 * 1024.0),
        total_json_bytes as f64 / num_records as f64,
        proto_ms,
        json_ms,
        json_ms / proto_ms,
        total_json_bytes as f64 / total_proto_bytes as f64,
    );
}
