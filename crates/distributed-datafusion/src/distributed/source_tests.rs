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
use datafusion::common::stats::Precision;
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion_distributed::{NetworkBoundary, NetworkCoalesceExec, Stage};
use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;

use restate_core::test_env::TestCoreEnv;
use restate_partition_store::PartitionStoreManager;
use restate_partition_store::lookup::LookupKeys;
use restate_partition_store::vqueue_table::VQueueMetaTable;
use restate_rocksdb::RocksDbManager;
use restate_storage_query_api::tables::vqueue_meta::VqueueMeta;
use restate_types::identifiers::BaseEntryId;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, VQueueId};

use crate::QuerySource;
use crate::table_providers::ScanPartition;
use crate::tables::state::StateTable;
use crate::tables::vqueue_entry_status::read::{EntrySelection, decode_keys, encode_keys};
use crate::tables::vqueue_entry_status::schema::{
    SysVqueueEntryStatusBuilder, SysVqueueEntryStatusTable,
};
use crate::tables::vqueue_meta::SysVqueueMetaTable;

use super::*;

#[derive(Debug)]
struct UnexecutedReader;

impl ScanPartition for UnexecutedReader {
    fn prepare_read(
        self: Arc<Self>,
        _: PrimaryRead,
        _: SchemaRef,
        _: Option<Arc<dyn PhysicalExpr>>,
        _: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        panic!("invalid_work_must_fail_before_preparation")
    }
}

#[restate_core::test]
async fn primary_access_codec_validates_keys_scope_and_capability() {
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let owner = core.metadata.my_node_id();
    let manager = SourceRegistry::local_only(core.metadata.clone());
    manager.register_partition_scanner::<StateTable>(Arc::new(UnexecutedReader));
    let context = super::super::tests::environment(1, 2)
        .build_session_state()
        .unwrap()
        .task_ctx();
    let converter = DefaultPhysicalProtoConverter {};
    let codec = SourceCodec::default();
    let schema = Arc::new(Schema::empty());
    let invocation = BaseEntryId::new(10, EntryId::new(EntryKind::Invocation, [1; 16]));
    let mutation = BaseEntryId::new(10, EntryId::new(EntryKind::StateMutation, [2; 16]));
    let keys = [
        encode_keys(&[invocation]),
        encode_keys(&[invocation, mutation]),
    ];
    let descriptor = |keys| SourceDescriptor {
        table: StateTable::identity(),
        owner: Some(owner),
        ordering: vec![],
        limit: None,
        work: SourceWork::MultiGet(PartitionWork::from_reads(
            Default::default(),
            vec![PrimaryLookup {
                partition: PartitionId::MIN,
                keys: Arc::new(keys),
            }],
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
        assert_eq!(*work.reads[0].keys, keys);
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
    // Index work round-trips, declares no ordering, and needs the scanner's support.
    let index_work = SourceDescriptor {
        table: StateTable::identity(),
        owner: Some(owner),
        ordering: vec!["partition_key".into()],
        limit: Some(7),
        work: SourceWork::IndexScan(IndexWork {
            index: ReString::from_static("_idx_entry_by_queue"),
            work: PartitionWork::from_reads(
                Default::default(),
                vec![PartitionScan {
                    partition: PartitionId::MIN,
                }],
            ),
        }),
    };
    let mut encoded = Vec::new();
    codec
        .try_encode(
            SourcePlan::new(index_work.clone(), Arc::clone(&schema), None, None, None)
                .unwrap()
                .into_plan(),
            &mut encoded,
            &converter,
        )
        .unwrap();
    let decoded = codec
        .try_decode(&encoded, &[], &context, &converter)
        .unwrap();
    assert!(decoded.is::<IndexScanExec>());
    assert!(decoded.properties().output_ordering().is_none());
    assert_eq!(
        source_plan(decoded.as_ref())
            .unwrap()
            .descriptor
            .encode_to_vec(),
        index_work.encode_to_vec(),
        "index_work_round_trips"
    );
    assert!(
        SourceCodec {
            manager: Some(manager.clone())
        }
        .try_decode(&encoded, &[], &context, &converter)
        .unwrap_err()
        .to_string()
        .contains("cannot read index '_idx_entry_by_queue'"),
        "unsupported_indexes_fail_at_binding"
    );

    // G2: a worker that predates index work decodes it with the old shape (tags
    // 1–3). It must see no executable work, and its decoder rejects that.
    #[derive(Debug, Clone, bilrost::Message, bilrost::Oneof)]
    enum FrozenSourceWork {
        Unknown,
        #[bilrost(1)]
        TableScan(PartitionWork<PartitionScan>),
        #[bilrost(2)]
        Node(()),
        #[bilrost(3)]
        MultiGet(PartitionWork<PrimaryLookup>),
    }
    #[derive(Debug, Clone, bilrost::Message)]
    struct FrozenSourceDescriptor {
        #[bilrost(1)]
        table: ReString,
        #[bilrost(2)]
        owner: Option<GenerationalNodeId>,
        #[bilrost(3)]
        work: FrozenSourceWork,
        #[bilrost(4)]
        ordering: Vec<String>,
        #[bilrost(5)]
        limit: Option<u64>,
    }
    let old = FrozenSourceDescriptor::decode(index_work.encode_to_vec().as_slice()).unwrap();
    assert!(
        matches!(old.work, FrozenSourceWork::Unknown),
        "old_workers_see_no_executable_work"
    );
    let as_old = SourceDescriptor {
        work: SourceWork::Unknown,
        ..index_work
    };
    assert_eq!(
        SourceDescriptor::decode(as_old.encode_to_vec().as_slice())
            .unwrap()
            .encode_to_vec(),
        old.encode_to_vec(),
        "the_old_view_is_the_unknown_variant"
    );
    let encoded = SourceProto {
        descriptor: old.encode_to_vec(),
        schema: encode_schema(&schema),
        predicate: None,
    }
    .encode_to_vec();
    assert!(
        codec
            .try_decode(&encoded, &[], &context, &converter)
            .unwrap_err()
            .to_string()
            .contains("unknown query source scope"),
        "unknown_work_is_a_query_error"
    );

    // Decode malformed wire directly: constructors must not mask validation gaps.
    let invalid = [
        encode_keys(&[]),
        PrimaryKeys {
            count: 1,
            encoded: vec![],
        },
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
    let mut duplicate = descriptor(encode_keys(&[invocation]));
    let SourceWork::MultiGet(work) = &mut duplicate.work else {
        unreachable!()
    };
    work.reads.push(work.reads[0].clone());
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
            .contains("duplicate partitions")
    );

    // A bound backend validates ownership independently of the global routing table.
    RocksDbManager::init();
    let stores = PartitionStoreManager::create(true).await.unwrap();
    stores
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, 99)),
            None,
        )
        .await
        .unwrap();
    let manager = SourceRegistry::local_only(core.metadata);
    manager.register_offline_reads(stores, crate::mocks::query_io());
    manager.register_partition_scanner::<SysVqueueEntryStatusTable>(Arc::new(
        SysVqueueEntryStatusTable::create_local_scanner(),
    ));
    for keys in [
        encode_keys(&[invocation, invocation]),
        encode_keys(&[mutation, invocation]),
        PrimaryKeys {
            count: 2,
            ..encode_keys(&[invocation])
        },
    ] {
        let mut malformed = descriptor(keys);
        malformed.table = SysVqueueEntryStatusTable::identity();
        let encoded = SourceProto {
            descriptor: malformed.encode_to_vec(),
            schema: encode_schema(&SysVqueueEntryStatusBuilder::schema()),
            predicate: None,
        }
        .encode_to_vec();
        assert!(
            SourceCodec {
                manager: Some(manager.clone())
            }
            .try_decode(&encoded, &[], &context, &converter)
            .is_err(),
            "the_source_validates_its_own_encoded_primary_keys"
        );
    }
    // A generic lookup source validates the same way: the stated count of
    // strictly increasing keys, and only keys of its own partition.
    manager.register_declared_scanner::<SysVqueueMetaTable>(Arc::new(
        SysVqueueMetaTable::create_local_scanner(),
    ));
    let queues = [VQueueId::custom(5, "a"), VQueueId::custom(6, "b")];
    let meta_keys = |count, ids: &[&VQueueId]| PrimaryKeys {
        count,
        encoded: LookupKeys::<VQueueMetaTable>::encode(ids.iter().copied()),
    };
    let meta_read = |keys| {
        let mut read = descriptor(keys);
        read.table = SysVqueueMetaTable::identity();
        SourceProto {
            descriptor: read.encode_to_vec(),
            schema: encode_schema(&VqueueMeta::arrow_schema()),
            predicate: None,
        }
        .encode_to_vec()
    };
    for keys in [
        meta_keys(3, &[&queues[0], &queues[1]]),
        meta_keys(2, &[&queues[1], &queues[0]]),
        meta_keys(2, &[&queues[0], &queues[0]]),
    ] {
        assert!(
            SourceCodec {
                manager: Some(manager.clone())
            }
            .try_decode(&meta_read(keys), &[], &context, &converter)
            .is_err(),
            "lookup_sources_validate_their_keys"
        );
    }
    let far = VQueueId::custom(100, "far");
    let decoded = SourceCodec {
        manager: Some(manager.clone()),
    }
    .try_decode(&meta_read(meta_keys(1, &[&far])), &[], &context, &converter)
    .unwrap();
    assert!(
        PartitionReadExec::new(vec![decoded], 1, None)
            .unwrap()
            .execute(0, Arc::clone(&context))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err()
            .to_string()
            .contains("primary keys are outside their partition"),
        "lookup_keys_must_belong_to_the_addressed_partition"
    );
    let mut outside = descriptor(encode_keys(&[BaseEntryId::new(
        100,
        EntryId::new(EntryKind::Invocation, [1; 16]),
    )]));
    outside.table = SysVqueueEntryStatusTable::identity();
    let encoded = SourceProto {
        descriptor: outside.encode_to_vec(),
        schema: encode_schema(&SysVqueueEntryStatusBuilder::schema()),
        predicate: None,
    }
    .encode_to_vec();
    let decoded = SourceCodec {
        manager: Some(manager),
    }
    .try_decode(&encoded, &[], &context, &converter)
    .unwrap();
    assert!(
        PartitionReadExec::new(vec![decoded], 1, None)
            .unwrap()
            .execute(0, context)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err()
            .to_string()
            .contains("primary keys are outside their partition"),
        "primary_keys_must_belong_to_the_addressed_partition"
    );
}

#[restate_core::test]
async fn compact_partition_templates_preserve_keys_statistics_and_codec() {
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let manager = SourceRegistry::local_only(core.metadata);
    let context = super::super::tests::environment(1, 2)
        .build_session_state()
        .unwrap()
        .task_ctx();
    let schema = Arc::new(Schema::empty());
    let mut statistics = Statistics::new_unknown(&schema);
    statistics.num_rows = Precision::Inexact(30);
    let statistics = Arc::new(statistics);
    let partitions = vec![
        (
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::new(0, 99)),
        ),
        (
            PartitionId::from(1),
            Partition::new(PartitionId::from(1), KeyRange::new(100, 199)),
        ),
    ];
    let keys = [1, 9, 105]
        .map(|key| BaseEntryId::new(key, EntryId::new(EntryKind::Invocation, [1; 16])))
        .to_vec();
    let codec = SourceCodec::default();
    let converter = DefaultPhysicalProtoConverter {};
    for access in [
        Access::Scan,
        Access::Index(ReString::from_static("_idx_test")),
        Access::Primary(Box::new(EntrySelection(keys))),
    ] {
        let lookup = matches!(access, Access::Primary(_));
        let index = matches!(access, Access::Index(_));
        let plan = plan_partition_access(
            StateTable::identity(),
            &manager,
            partitions.clone(),
            Default::default(),
            Arc::clone(&schema),
            Arc::clone(&statistics),
            vec![],
            None,
            None,
            access,
            false,
            2,
        )
        .unwrap();
        let group = plan.downcast_ref::<PartitionReadExec>().unwrap();
        assert_eq!(group.children().len(), 1, "one_template_per_owner");
        assert_eq!(plan.properties().partitioning.partition_count(), 2);
        let mut selected_partitions = Vec::new();
        let mut selected_keys = Vec::new();
        for leaf in group.children() {
            assert_eq!(leaf.is::<MultiGetExec>(), lookup);
            assert_eq!(leaf.is::<IndexScanExec>(), index);
            assert_eq!(leaf.properties().partitioning.partition_count(), 1);
            let source = source_plan(leaf.as_ref()).unwrap();
            assert_eq!(source.descriptor.owner, Some(manager.node_id()));
            match &source.descriptor.work {
                SourceWork::TableScan(work) | SourceWork::IndexScan(IndexWork { work, .. }) => {
                    assert_eq!(work.reads.len(), 2, "one_scan_per_partition");
                    selected_partitions.extend(work.reads.iter().map(|read| read.partition));
                }
                SourceWork::MultiGet(work) => {
                    assert_eq!(work.reads.len(), 2, "one_multi_get_per_partition");
                    for lookup in &work.reads {
                        selected_keys.extend(
                            decode_keys(&lookup.keys)
                                .unwrap()
                                .iter()
                                .map(BaseEntryId::partition_key),
                        );
                        selected_partitions.push(lookup.partition);
                    }
                }
                _ => unreachable!(),
            };
            let mut encoded = Vec::new();
            codec
                .try_encode(Arc::clone(leaf), &mut encoded, &converter)
                .unwrap();
            let decoded = codec
                .try_decode(&encoded, &[], &context, &converter)
                .unwrap();
            assert_eq!(
                source_plan(decoded.as_ref())
                    .unwrap()
                    .descriptor
                    .encode_to_vec(),
                source.descriptor.encode_to_vec(),
                "the_wire_descriptor_preserves_partition_ids_and_complete_keys"
            );
            assert_eq!(decoded.properties().partitioning.partition_count(), 1);
        }
        assert_eq!(
            selected_partitions,
            [PartitionId::MIN, PartitionId::from(1)]
        );
        if lookup {
            assert_eq!(selected_keys, [1, 9, 105], "do not fill the selection hole");
        }
        assert_eq!(
            StatisticsContext::new()
                .compute(plan.as_ref(), &StatisticsArgs::new())
                .unwrap()
                .num_rows,
            Precision::Inexact(if lookup { 3 } else { 30 }),
            "splitting a source must not multiply its row estimate"
        );

        // Repeated logical sources stay distinct, but compatible union branches
        // share a task on their owner. Exercise nested unions on both A/B paths.
        let repeated = UnionExec::try_new(vec![Arc::clone(&plan), plan]).unwrap();
        for pushdown in [false, true] {
            let distributed = super::super::plan_with_pushdown(
                Arc::clone(&repeated),
                pushdown,
                &ConfigOptions::default(),
            )
            .unwrap();
            let mut stages = 0;
            let mut leaves = 0;
            distributed
                .apply(|node| {
                    if let Some(boundary) = node.downcast_ref::<NetworkCoalesceExec>() {
                        stages += 1;
                        let Stage::Local(stage) = boundary.input_stage() else {
                            panic!("planning must not dispatch")
                        };
                        assert!(stage.plan.is::<UnionExec>());
                        assert_eq!(boundary.properties().partitioning.partition_count(), 4);
                    }
                    if let Some(source) = source_plan(node.as_ref()) {
                        leaves += 1;
                        assert_eq!(source.descriptor.owner, Some(manager.node_id()));
                        assert_eq!(source.properties.partitioning.partition_count(), 1);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
            assert_eq!((stages, leaves), (1, 2));
        }
    }
}
