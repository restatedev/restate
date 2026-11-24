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
use restate_partition_store::keys::FixedLengthTableKey;
use restate_partition_store::lookup::encode_unchecked;
use restate_partition_store::vqueue_table::MetaKey;
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

/// A reader that knows no index: index work reaches it as a scan.
#[derive(Debug)]
struct ScanOnlyReader;

impl ScanPartition for ScanOnlyReader {
    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        _: SchemaRef,
        _: Option<Arc<dyn PhysicalExpr>>,
        _: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        anyhow::ensure!(matches!(access, PrimaryRead::Scan), "only scans");
        Ok(Arc::new(NoRows))
    }
}

#[derive(Debug)]
struct NoRows;

impl PreparedPartitionRead for NoRows {
    fn execute(
        &self,
        _: PartitionReadContext,
        _: usize,
        _: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        unreachable!("the codec test never executes a read")
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
        orders: Vec::new(),
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
        orders: Vec::new(),
        // Two candidates, in the planner's preference order.
        work: SourceWork::IndexScan(IndexWork {
            work: PartitionWork::from_reads(
                Default::default(),
                vec![PartitionScan {
                    partition: PartitionId::MIN,
                }],
            ),
            bound: Some(51),
            candidates: vec![
                IndexCandidate {
                    index: ReString::from_static("_idx_entry_by_queue"),
                    ordering: Vec::new(),
                },
                IndexCandidate {
                    index: ReString::from_static("_idx_entry_status_by_status_created"),
                    ordering: Vec::new(),
                },
            ],
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
    let SourceWork::IndexScan(IndexWork { candidates, .. }) =
        &source_plan(decoded.as_ref()).unwrap().descriptor.work
    else {
        panic!("index work decodes as index work");
    };
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.index.as_ref())
            .collect::<Vec<&str>>(),
        ["_idx_entry_by_queue", "_idx_entry_status_by_status_created"],
        "candidates_keep_their_order"
    );
    // The fallback bound travels apart from the source's limit; work without
    // one decodes unbounded.
    let wire = &source_plan(decoded.as_ref()).unwrap().descriptor;
    assert!(matches!(
        (&wire.work, wire.limit),
        (
            SourceWork::IndexScan(IndexWork {
                bound: Some(51),
                ..
            }),
            Some(7)
        )
    ));
    let mut unbounded = index_work.clone();
    let SourceWork::IndexScan(work) = &mut unbounded.work else {
        unreachable!()
    };
    work.bound = None;
    assert!(matches!(
        SourceDescriptor::decode(unbounded.encode_to_vec().as_slice())
            .unwrap()
            .work,
        SourceWork::IndexScan(IndexWork { bound: None, .. })
    ));
    // A worker that knows none of the candidates scans instead, and the engine
    // sorts its rows under the bound: it neither fails the query nor prepares
    // an index read.
    let scanning = SourceRegistry::local_only(core.metadata.clone());
    scanning.register_partition_scanner::<StateTable>(Arc::new(ScanOnlyReader));
    assert!(
        SourceCodec {
            manager: Some(scanning)
        }
        .try_decode(&encoded, &[], &context, &converter)
        .is_ok(),
        "unknown_candidates_scan_instead"
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
        encoded: encode_unchecked(
            ids.iter()
                .map(|id| MetaKey::from(*id).to_bytes().to_vec())
                .collect(),
        ),
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
        Access::Index {
            candidates: vec![IndexCandidate {
                index: ReString::from_static("_idx_test"),
                ordering: Vec::new(),
            }],
            bound: None,
        },
        Access::Primary(Box::new(EntrySelection(keys))),
    ] {
        let lookup = matches!(access, Access::Primary(_));
        let index = matches!(access, Access::Index { .. });
        let plan = plan_partition_access(
            StateTable::identity(),
            &manager,
            partitions.clone(),
            Default::default(),
            Arc::clone(&schema),
            Arc::clone(&statistics),
            SourceOrders::default(),
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

/// A partition read keeps its template's order only when every lane reads one
/// storage partition, within the configured lanes: an ordered read doesn't open
/// more partitions at once. The order is the template's actual output order:
/// remapped through a projection, gone with the ordered column. Rewrites
/// recompute it.
#[test]
fn partition_reads_keep_template_order_only_with_a_lane_per_partition() {
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
    use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("created_at", DataType::Int64, false),
    ]));
    let descriptor = SourceDescriptor {
        table: StateTable::identity(),
        owner: None,
        ordering: Vec::new(),
        limit: None,
        orders: Vec::new(),
        work: SourceWork::IndexScan(IndexWork {
            work: PartitionWork::from_reads(
                Default::default(),
                (0..3)
                    .map(|partition| PartitionScan {
                        partition: PartitionId::from(partition),
                    })
                    .collect(),
            ),
            bound: None,
            candidates: vec![IndexCandidate {
                index: ReString::from_static("_idx_test"),
                ordering: vec![SortedColumn {
                    name: "created_at".into(),
                    descending: true,
                    nulls_first: false,
                }],
            }],
        }),
    };
    let source = SourcePlan::new(descriptor, Arc::clone(&schema), None, None, None)
        .unwrap()
        .into_plan();
    let column = |name: &str, index| ProjectionExpr::new(Arc::new(Column::new(name, index)), name);
    let swapped: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            [column("created_at", 1), column("id", 0)],
            Arc::clone(&source),
        )
        .unwrap(),
    );
    let without: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new([column("id", 0)], Arc::clone(&source)).unwrap());
    let newest_first = SortOptions {
        descending: true,
        nulls_first: false,
    };
    for (template, parallelism, lanes, ordered_by) in [
        (&source, 3, 3, Some(("created_at", 1))),
        (&source, 2, 2, None),
        (&swapped, 4, 3, Some(("created_at", 0))),
        (&without, 3, 3, None),
    ] {
        let reads: Arc<dyn ExecutionPlan> = Arc::new(
            PartitionReadExec::new(vec![Arc::clone(template)], parallelism, None).unwrap(),
        );
        let rewrite = |template: &Arc<dyn ExecutionPlan>| {
            Arc::clone(&reads)
                .replace_children(
                    vec![Arc::clone(template)],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )
                .unwrap()
        };
        // A rewrite to a template without the ordered column drops the order.
        assert!(rewrite(&without).properties().output_ordering().is_none());
        let rewritten = rewrite(template);
        for plan in [reads, rewritten] {
            assert_eq!(plan.properties().partitioning.partition_count(), lanes);
            let declared = plan.properties().output_ordering().map(|ordering| {
                ordering
                    .iter()
                    .map(|expr| {
                        let column = expr.expr.downcast_ref::<Column>().unwrap();
                        ((column.name().to_owned(), column.index()), expr.options)
                    })
                    .collect::<Vec<_>>()
            });
            assert_eq!(
                declared,
                ordered_by.map(|(name, index)| vec![((name.to_owned(), index), newest_first)]),
                "parallelism {parallelism}"
            );
        }
    }
}

/// A bounded sort's demand reaches its scan through filters and column-only
/// projections, in the scan's columns, with every filter's conditions. A scan
/// that the plan reaches twice gets no demand, whether its other consumer reads
/// every row or other rows first.
#[test]
fn demands_follow_one_consumer_through_filters_and_projections() {
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_expr::physical_exprs_bag_equal;
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};

    use super::super::demand::Demands;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("created_at", DataType::Int64, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let scan = || {
        let descriptor = SourceDescriptor {
            table: StateTable::identity(),
            owner: None,
            ordering: Vec::new(),
            limit: None,
            orders: Vec::new(),
            work: SourceWork::TableScan(PartitionWork::from_reads(
                Default::default(),
                vec![PartitionScan {
                    partition: PartitionId::MIN,
                }],
            )),
        };
        SourcePlan::new(descriptor, Arc::clone(&schema), None, None, None)
            .unwrap()
            .into_plan()
    };
    let column = |name: &str, index| Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>;
    let equals = |name: &str, index, value: &str| {
        Arc::new(BinaryExpr::new(
            column(name, index),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some(value.into())))),
        )) as Arc<dyn PhysicalExpr>
    };
    let newest_first = SortOptions {
        descending: true,
        nulls_first: false,
    };
    let newest = |expr, fetch, input| {
        let ordering = LexOrdering::new([PhysicalSortExpr {
            expr,
            options: newest_first,
        }])
        .unwrap();
        Arc::new(SortExec::new(ordering, input).with_fetch(fetch)) as Arc<dyn ExecutionPlan>
    };
    let filter = |predicate, input| {
        Arc::new(FilterExec::try_new(predicate, input).unwrap()) as Arc<dyn ExecutionPlan>
    };
    let service = || equals("service", 2, "a");

    // Two filters, the far one projecting `[created_at, id]`, then a projection
    // that swaps and renames the columns.
    let source = scan();
    let near = filter(service(), Arc::clone(&source));
    let far = FilterExecBuilder::new(equals("status", 3, "x"), near)
        .apply_projection(Some(vec![1, 0]))
        .unwrap()
        .build()
        .unwrap();
    let renamed = ProjectionExec::try_new(
        [
            ProjectionExpr::new(column("id", 1), "ident"),
            ProjectionExpr::new(column("created_at", 0), "made"),
        ],
        Arc::new(far),
    )
    .unwrap();
    let plan = newest(column("made", 1), Some(7), Arc::new(renamed));
    let demands = Demands::of(&plan);
    let demand = demands.on(&source).unwrap();
    assert_eq!(demand.fetch, 7);
    assert_eq!(
        demand.ordering,
        [PhysicalSortExpr {
            expr: column("created_at", 1),
            options: newest_first,
        }]
    );
    assert!(physical_exprs_bag_equal(
        &demand.residual,
        &[service(), equals("status", 3, "x")]
    ));

    // A computed column, or a sort without a fetch, demands nothing.
    let source = scan();
    let computed = ProjectionExec::try_new(
        [ProjectionExpr::new(
            Arc::new(BinaryExpr::new(
                column("created_at", 1),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            )),
            "later",
        )],
        Arc::clone(&source),
    )
    .unwrap();
    let plan = newest(column("later", 0), Some(7), Arc::new(computed));
    assert!(Demands::of(&plan).on(&source).is_none());
    let plan = newest(column("created_at", 1), None, Arc::clone(&source));
    assert!(Demands::of(&plan).on(&source).is_none());

    // One scan under two consumers: an unbounded one, or another bound. Two
    // scans are each their consumer's.
    let union = |left, right| UnionExec::try_new(vec![left, right]).unwrap();
    let bounded = |fetch, input: &Arc<dyn ExecutionPlan>| {
        newest(
            column("created_at", 1),
            Some(fetch),
            filter(service(), Arc::clone(input)),
        )
    };
    let shared = scan();
    for plan in [
        union(bounded(51, &shared), filter(service(), Arc::clone(&shared))),
        union(bounded(51, &shared), bounded(1, &shared)),
    ] {
        assert!(Demands::of(&plan).on(&shared).is_none());
    }
    let (first, second) = (scan(), scan());
    let plan = union(bounded(51, &first), filter(service(), Arc::clone(&second)));
    let demands = Demands::of(&plan);
    assert_eq!(demands.on(&first).map(|demand| demand.fetch), Some(51));
    assert!(demands.on(&second).is_none());
}

/// A partition that can't keep its index's order keeps its first `bound`
/// matches, newest first, within the query's memory pool. A refused
/// reservation fails the read, and the read releases everything it reserved.
#[tokio::test]
async fn bounded_fallback_sorts_within_the_query_pool() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};

    let schema = Arc::new(Schema::new(vec![
        Field::new("created_at", DataType::Int64, false),
        Field::new("service", DataType::Utf8, false),
    ]));
    // Created in key order, not time order; every other row matches.
    let created: Vec<i64> = (0..100).map(|n| (n * 37) % 100).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(created.clone())),
            Arc::new(StringArray::from_iter_values(
                created.iter().map(|at| if at % 2 == 0 { "a" } else { "b" }),
            )),
        ],
    )
    .unwrap();
    let rows = || {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter([Ok(batch.clone())]),
        )) as SendableRecordBatchStream
    };
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("service", 1)),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Utf8(Some("a".into())))),
    ));
    let ordering = LexOrdering::new([PhysicalSortExpr {
        expr: Arc::new(Column::new("created_at", 0)),
        options: SortOptions {
            descending: true,
            nulls_first: false,
        },
    }])
    .unwrap();
    let context = |pool: &Arc<GreedyMemoryPool>| {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
            .build()
            .unwrap();
        Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
    };

    let roomy = Arc::new(GreedyMemoryPool::new(1 << 20));
    let kept: Vec<_> = sorted(
        rows(),
        Some(Arc::clone(&predicate)),
        ordering.clone(),
        Some(3),
        context(&roomy),
    )
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap()
    .iter()
    .flat_map(|batch| {
        let created = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        created.values().to_vec()
    })
    .collect();
    assert_eq!(kept, [98, 96, 94]);
    assert_eq!(roomy.reserved(), 0);

    let tight = Arc::new(GreedyMemoryPool::new(64));
    let error = sorted(rows(), Some(predicate), ordering, Some(3), context(&tight))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"), "{error}");
    assert_eq!(tight.reserved(), 0);
}

/// A table or index scan without a source fetch checks its wholly safe
/// predicate before emitting rows; with a fetch, set at planning or later, or
/// for other leaves, it emits as before. The check keeps only true rows, drops
/// empty batches, and fails the read on an evaluation error.
#[tokio::test]
async fn partition_reads_filter_before_emitting_only_without_a_fetch() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, in_list, lit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("stage", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, false),
    ]));
    let stage = || Arc::new(Column::new("stage", 0)) as Arc<dyn PhysicalExpr>;
    let created_at = || Arc::new(Column::new("created_at", 1)) as Arc<dyn PhysicalExpr>;
    // Not running, and recent or exactly at 0.
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        in_list(stage(), vec![lit("running")], &true, &schema).unwrap(),
        Operator::And,
        Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(created_at(), Operator::Gt, lit(5i64))),
            Operator::Or,
            Arc::new(BinaryExpr::new(created_at(), Operator::Eq, lit(0i64))),
        )),
    ));
    let scan = |work: SourceWork, limit: Option<u64>| {
        let descriptor = SourceDescriptor {
            table: StateTable::identity(),
            owner: None,
            ordering: Vec::new(),
            limit,
            work,
            orders: Vec::new(),
        };
        SourcePlan::new(
            descriptor,
            Arc::clone(&schema),
            Some(Arc::clone(&predicate)),
            None,
            None,
        )
        .unwrap()
    };
    let reads = || {
        PartitionWork::from_reads(
            Default::default(),
            vec![PartitionScan {
                partition: PartitionId::MIN,
            }],
        )
    };
    let table = scan(SourceWork::TableScan(reads()), None);
    let index = scan(
        SourceWork::IndexScan(IndexWork {
            work: reads(),
            bound: Some(51),
            candidates: vec![IndexCandidate {
                index: ReString::from_static("_idx_test"),
                ordering: Vec::new(),
            }],
        }),
        None,
    );
    assert!(table.emission_filter().is_some() && index.emission_filter().is_some());
    assert!(
        scan(SourceWork::TableScan(reads()), Some(10))
            .emission_filter()
            .is_none()
    );
    let fetched = table.with_fetch(Some(10)).unwrap();
    assert!(fetched.emission_filter().is_none());
    let lookup = scan(
        SourceWork::MultiGet(PartitionWork::from_reads(
            Default::default(),
            vec![PrimaryLookup {
                partition: PartitionId::MIN,
                keys: Arc::new(PrimaryKeys {
                    count: 1,
                    encoded: vec![1],
                }),
            }],
        )),
        None,
    );
    assert!(lookup.emission_filter().is_none());

    // Three batches: nothing kept, part kept (NULL stage and running dropped),
    // everything kept.
    let batch = |stages: Vec<Option<&str>>, created: Vec<i64>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(stages)),
                Arc::new(Int64Array::from(created)),
            ],
        )
        .unwrap()
    };
    let rows = |batches: Vec<RecordBatch>| {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(batches.into_iter().map(Ok)),
        )) as SendableRecordBatchStream
    };
    let input = vec![
        batch(vec![Some("running"), Some("finished")], vec![9, 3]),
        batch(
            vec![Some("finished"), None, Some("running"), Some("inbox")],
            vec![9, 9, 9, 0],
        ),
        batch(vec![Some("paused")], vec![7]),
    ];
    let emitted: Vec<RecordBatch> = filtered(rows(input), table.emission_filter().unwrap())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        emitted
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(
        datafusion::arrow::util::pretty::pretty_format_batches(&emitted)
            .unwrap()
            .to_string(),
        "+----------+------------+\n\
         | stage    | created_at |\n\
         +----------+------------+\n\
         | finished | 9          |\n\
         | inbox    | 0          |\n\
         | paused   | 7          |\n\
         +----------+------------+"
    );

    // An evaluation error fails the read.
    let failing: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(BinaryExpr::new(created_at(), Operator::Divide, lit(0i64))),
        Operator::Gt,
        lit(ScalarValue::Int64(Some(1))),
    ));
    let error = filtered(rows(vec![batch(vec![Some("finished")], vec![9])]), failing)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Divide by zero"), "{error}");
}

/// A key-ordered read declares each order of its source up to the first term it
/// doesn't project or whose function it doesn't know, an order of several terms
/// as one ordering; workers that predate orders get the first of plain columns.
#[test]
fn key_ordered_reads_declare_their_orders() {
    use datafusion::arrow::datatypes::{DataType, Field};

    use crate::source_registry::{SourceOrder, SourceTerm};

    let term = |column: &str, function: Option<&str>| SourceTerm {
        column: column.into(),
        function: function.map(Into::into),
    };
    let orders = SourceOrders(vec![
        SourceOrder {
            terms: vec![term("partition_key", None)],
        },
        SourceOrder {
            terms: vec![term("id", Some("invocation_id")), term("index", None)],
        },
        SourceOrder {
            terms: vec![term("id", Some("unknown")), term("index", None)],
        },
    ]);
    let field = |name: &str| match name {
        "partition_key" => Field::new(name, DataType::UInt64, false),
        "index" => Field::new(name, DataType::UInt32, false),
        _ => Field::new(name, DataType::LargeUtf8, false),
    };
    for (columns, declared) in [
        (
            &["partition_key", "id", "index"][..],
            vec![
                vec!["partition_key@0"],
                vec!["invocation_id(id@1)", "index@2"],
            ],
        ),
        (
            &["id", "partition_key"],
            vec![vec!["partition_key@1"], vec!["invocation_id(id@0)"]],
        ),
        (&["index", "partition_key"], vec![vec!["partition_key@1"]]),
        (&["index"], vec![]),
    ] {
        let schema = Schema::new(columns.iter().map(|name| field(name)).collect::<Vec<_>>());
        let orderings: Vec<Vec<String>> = key_orderings(&orders, &schema)
            .iter()
            .map(|ordering| ordering.iter().map(|sort| sort.expr.to_string()).collect())
            .collect();
        assert_eq!(orderings, declared, "{columns:?}");
    }
    assert_eq!(orders.columns(), ["partition_key"]);
}
