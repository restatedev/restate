// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mocks::*;
use crate::row;
use bytes::Bytes;
use datafusion::arrow::array::{Int64Array, LargeStringArray, UInt32Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::all;
use googletest::prelude::{assert_that, eq};
use prost::Message;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::Transaction;
use restate_storage_api::journal_table::{JournalEntry, WriteJournalTable};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationTarget;
use restate_types::journal::enriched::{
    CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
};
use restate_types::journal::{Entry, EntryType, InputEntry};
use restate_types::service_protocol;

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_entries() {
    let mut engine = MockQueryEngine::create().await;

    let mut tx = engine.partition_store().transaction();
    let journal_invocation_id = InvocationId::mock_random();
    tx.put_journal_entry(
        &journal_invocation_id,
        0,
        &JournalEntry::Entry(ProtobufRawEntryCodec::serialize_enriched(Entry::Input(
            InputEntry {
                headers: vec![],
                value: Default::default(),
            },
        ))),
    )
    .unwrap();
    let invoked_invocation_id = InvocationId::mock_random();
    let invoked_invocation_target = InvocationTarget::mock_virtual_object();
    tx.put_journal_entry(
        &journal_invocation_id,
        1,
        &JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Call {
                is_completed: false,
                enrichment_result: Some(CallEnrichmentResult {
                    invocation_id: invoked_invocation_id,
                    invocation_target: invoked_invocation_target.clone(),
                    completion_retention_time: None,
                    span_context: Default::default(),
                }),
            },
            Bytes::new(),
        )),
    )
    .unwrap();
    tx.put_journal_entry(
        &journal_invocation_id,
        2,
        &JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Run {},
            service_protocol::RunEntryMessage {
                name: "my-side-effect".to_string(),
                result: None,
            }
            .encode_to_vec()
            .into(),
        )),
    )
    .unwrap();
    tx.commit().await.unwrap();

    let records = engine
        .execute(
            "SELECT id, index, entry_type, name, invoked_id, invoked_target FROM sys_journal ORDER BY id, index",
        )
        .await
        .unwrap()
        .collect::<Vec<Result<RecordBatch, _>>>()
        .await
        .remove(0)
        .unwrap();

    assert_that!(
        records,
        all!(
            row!(
                0,
                {
                    "id" => LargeStringArray: eq(journal_invocation_id.to_string()),
                    "index" => UInt32Array: eq(0),
                    "entry_type" => LargeStringArray: eq(EntryType::Input.to_string()),
                }
            ),
            row!(
                1,
                {
                    "id" => LargeStringArray: eq(journal_invocation_id.to_string()),
                    "index" => UInt32Array: eq(1),
                    "entry_type" => LargeStringArray: eq(EntryType::Call.to_string()),
                    "invoked_id" => LargeStringArray: eq(invoked_invocation_id.to_string()),
                    "invoked_target" => LargeStringArray: eq(invoked_invocation_target.to_string()),
                }
            ),
            row!(
                2,
                {
                    "id" => LargeStringArray: eq(journal_invocation_id.to_string()),
                    "index" => UInt32Array: eq(2),
                    "entry_type" => LargeStringArray: eq(EntryType::Run.to_string()),
                    "name" => LargeStringArray: eq("my-side-effect")
                }
            )
        )
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_count_star() {
    let mut engine = MockQueryEngine::create().await;

    let mut tx = engine.partition_store().transaction();
    let journal_invocation_id = InvocationId::mock_random();
    tx.put_journal_entry(
        &journal_invocation_id,
        0,
        &JournalEntry::Entry(ProtobufRawEntryCodec::serialize_enriched(Entry::Input(
            InputEntry {
                headers: vec![],
                value: Default::default(),
            },
        ))),
    )
    .unwrap();
    tx.put_journal_entry(
        &journal_invocation_id,
        1,
        &JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Call {
                is_completed: false,
                enrichment_result: Some(CallEnrichmentResult {
                    invocation_id: InvocationId::mock_random(),
                    invocation_target: InvocationTarget::mock_virtual_object(),
                    completion_retention_time: None,
                    span_context: Default::default(),
                }),
            },
            Bytes::new(),
        )),
    )
    .unwrap();
    tx.put_journal_entry(
        &journal_invocation_id,
        2,
        &JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Run {},
            service_protocol::RunEntryMessage {
                name: "my-side-effect".to_string(),
                result: None,
            }
            .encode_to_vec()
            .into(),
        )),
    )
    .unwrap();
    tx.commit().await.unwrap();

    let records = engine
        .execute("SELECT COUNT(*) AS count FROM sys_journal")
        .await
        .unwrap()
        .collect::<Vec<Result<RecordBatch, _>>>()
        .await
        .remove(0)
        .unwrap();

    assert_that!(
        records,
        all!(row!(
            0,
            {
                "count" => Int64Array: eq(3)
            }
        ),)
    );
}
