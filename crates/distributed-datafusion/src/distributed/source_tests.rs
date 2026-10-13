// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::metrics::Time;
use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;

use restate_core::test_env::TestCoreEnv;
use restate_storage_query_api::QueryEngineTable;
use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid};
use restate_types::vqueues::{EntryId, EntryKind, VQueueId};

use crate::state::schema::StateTable;

use super::*;

#[derive(Debug)]
struct RangeOnly;

impl ScanPartition for RangeOnly {
    fn scan_partition(
        &self,
        _: PartitionId,
        _: KeyRange,
        _: SchemaRef,
        _: Option<Arc<dyn PhysicalExpr>>,
        _: usize,
        _: Option<usize>,
        _: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        panic!("binding validation must not execute storage")
    }
}

#[restate_core::test]
async fn primary_access_codec_validates_keys_scope_and_capability() {
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let owner = core.metadata.my_node_id();
    let manager = RemoteScannerManager::local_only(core.metadata);
    manager.register_partition_scanner::<StateTable>(Arc::new(RangeOnly));
    let context = super::super::tests::environment(1, 2)
        .build_session_state()
        .unwrap()
        .task_ctx();
    let converter = DefaultPhysicalProtoConverter {};
    let codec = SourceCodec::default();
    let schema = Arc::new(Schema::empty());
    let invocation = InvocationId::from_parts(10, InvocationUuid::from_u128(1));
    let keys = [
        PrimaryKeys::Invocation(vec![invocation]),
        PrimaryKeys::VQueue(vec![VQueueId::custom(10, "primary-codec")]),
        PrimaryKeys::VQueueEntry(vec![
            BaseEntryId::new(10, EntryId::new(EntryKind::Invocation, [1; 16])).into(),
            BaseEntryId::new(10, EntryId::new(EntryKind::StateMutation, [2; 16])).into(),
        ]),
    ];
    let descriptor = |keys| SourceDescriptor {
        table: StateTable::identity(),
        owner: Some(owner),
        ordering: vec![],
        limit: None,
        work: SourceWork::MultiGet(PartitionWork::from_reads(
            Default::default(),
            vec![PrimaryLookup {
                range: ScanRange {
                    partition: PartitionId::MIN,
                    range: KeyRange::new(10, 10),
                    invocation: None,
                },
                keys,
            }],
            1,
        )),
    };
    for keys in keys {
        let source = SourcePlan::new(
            descriptor(keys.clone()),
            Arc::clone(&schema),
            None,
            None,
            None,
        )
        .unwrap()
        .into_plan();
        let mut encoded = Vec::new();
        codec.try_encode(source, &mut encoded, &converter).unwrap();
        let decoded = codec
            .try_decode(&encoded, &[], &context, &converter)
            .unwrap();
        assert!(decoded.is::<MultiGetExec>());
        assert!(decoded.properties().output_ordering().is_none());
        let SourceWork::MultiGet(work) = &source_plan(decoded.as_ref()).unwrap().descriptor.work
        else {
            unreachable!()
        };
        assert_eq!(work.lanes[0].reads[0].keys, keys);
        assert!(
            SourceCodec {
                manager: Some(manager.clone())
            }
            .try_decode(&encoded, &[], &context, &converter)
            .unwrap_err()
            .to_string()
            .contains("incompatible primary lookup capability")
        );
    }
    // Decode malformed wire directly: constructors must not mask validation gaps.
    let invalid = [
        PrimaryKeys::Unknown,
        PrimaryKeys::Invocation(vec![]),
        PrimaryKeys::Invocation(vec![invocation, invocation]),
        PrimaryKeys::Invocation(vec![InvocationId::from_parts(
            11,
            InvocationUuid::from_u128(1),
        )]),
        PrimaryKeys::VQueue(vec![]),
        PrimaryKeys::VQueueEntry(vec![]),
    ];
    for keys in invalid {
        let encoded = SourceProto {
            descriptor: descriptor(keys).encode_to_vec(),
            schema: encode_schema(&schema),
            predicate: None,
        }
        .encode_to_vec();
        assert!(
            codec
                .try_decode(&encoded, &[], &context, &converter)
                .unwrap_err()
                .to_string()
                .contains("invalid primary key set")
        );
    }
    let mut duplicate = descriptor(PrimaryKeys::Invocation(vec![invocation]));
    let SourceWork::MultiGet(work) = &mut duplicate.work else {
        unreachable!()
    };
    work.lanes.push(work.lanes[0].clone());
    let encoded = SourceProto {
        descriptor: duplicate.encode_to_vec(),
        schema: encode_schema(&schema),
        predicate: None,
    }
    .encode_to_vec();
    assert!(
        codec
            .try_decode(&encoded, &[], &context, &converter)
            .unwrap_err()
            .to_string()
            .contains("overlapping partition ranges")
    );
}
