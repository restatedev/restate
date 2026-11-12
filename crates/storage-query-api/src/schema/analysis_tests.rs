// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::array::{BooleanArray, LargeStringArray, UInt64Array};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::prelude::SessionContext;

use restate_types::identifiers::{BaseEntryId, CanonicalEntryId, PartitionKey};
use restate_types::vqueues::{EntryId, EntryKind, Seq, VQueueId};

use crate::selection::domain::Domain;
use crate::selection::{Selection, ValuePredicate, compile};
use crate::tables::scheduler::Scheduler;

use super::*;

crate::define_schema! {
    EntryRows {
        #[column("entry_id")]
        canonical: CanonicalEntryId => DataType::LargeUtf8,
        partition_key: PartitionKey => DataType::UInt64,
        #[internal]
        base: BaseEntryId,
    }
    derive [derive_base, derive_partition_key]
}

fn derive_base(branch: &mut EntryRowsDomains) -> anyhow::Result<()> {
    branch.base = std::mem::replace(&mut branch.base, Domain::empty())
        .intersect(branch.canonical.map_values(|id| id.to_base_entry_id()));
    Ok(())
}

fn derive_partition_key(branch: &mut EntryRowsDomains) -> anyhow::Result<()> {
    branch.partition_key = std::mem::replace(&mut branch.partition_key, Domain::empty())
        .intersect(branch.base.map_values(BaseEntryId::partition_key));
    Ok(())
}

enum Routes<'a> {
    All,
    Keys(&'a [PartitionKey]),
}

#[test]
fn simplified_native_ids_preserve_sql_truth_and_partition_targets() {
    use Routes::{All, Keys};

    const T: Option<bool> = Some(true);
    const F: Option<bool> = Some(false);
    const U: Option<bool> = None;
    let queues = [7, 9, 11].map(|key| VQueueId::custom(key, "scheduler"));
    let invocation = BaseEntryId::new(7, EntryId::new(EntryKind::Invocation, [1; 16]));
    let mutation = BaseEntryId::new(9, EntryId::new(EntryKind::StateMutation, [2; 16]));
    let schema = Scheduler::arrow_schema();
    let projection =
        ["id", "head_entry_id", "partition_key"].map(|name| schema.index_of(name).unwrap());
    let batch = RecordBatch::try_new(
        Arc::new(schema.project(&projection).unwrap()),
        vec![
            Arc::new(LargeStringArray::from_iter_values(
                queues.iter().map(ToString::to_string),
            )),
            Arc::new(LargeStringArray::from_iter([
                Some(invocation.to_string()),
                Some(mutation.to_string()),
                None,
            ])),
            Arc::new(UInt64Array::from(vec![7, 9, 11])),
        ],
    )
    .unwrap();
    let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
    let context = SessionContext::new();
    let simplifier = ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(Arc::new(schema.clone()))
            .build(),
    );
    let [a, b, _] = &queues;
    #[rustfmt::skip]
    let cases = [
        // SQL predicate                                                   SQL truth    routing keys
        (format!("id = '{a}'"),                                            [T, F, F],   Keys(&[7])),
        (format!("id >= '{a}' AND id <= '{a}'"),                             [T, F, F],   Keys(&[7])),
        (format!("id BETWEEN '{a}' AND '{a}'"),                              [T, F, F],   Keys(&[7])),
        (format!("id IN ('{a}', '{b}') AND id = '{a}'"),                      [T, F, F],   Keys(&[7])),
        ("id = 'not-a-vqueue'".into(),                                      [F, F, F],   Keys(&[])),
        (format!("id IN ('{a}', 'not-a-vqueue')"),                           [T, F, F],   Keys(&[7])),
        (format!("id IN ('vq_1', 'vq_1💥', '{a}_1', '{invocation}')"),         [F, F, F],   Keys(&[])),
        ("id NOT IN ('not-a-vqueue')".into(),                                [T, T, T],   All),
        (format!("id IN ('{a}', 'not-a-vqueue', NULL)"),                     [T, U, U],   Keys(&[7])),
        (format!("id NOT IN ('{a}', 'not-a-vqueue', NULL)"),                 [F, U, U],   Keys(&[])),
        (format!("head_entry_id = '{mutation}'"),                           [F, T, U],   Keys(&[9])),
        (format!("head_entry_id >= '{mutation}' AND head_entry_id <= '{mutation}'"), [F, T, U], Keys(&[9])),
        (format!("head_entry_id IN ('{invocation}', 'not-an-entry')"),        [T, F, U],   Keys(&[7])),
        (format!("head_entry_id IN ('inv_1', '{invocation}_1', '{a}')"),      [F, F, U],   Keys(&[])),
        ("head_entry_id NOT IN ('not-an-entry')".into(),                     [T, T, U],   All),
        ("head_entry_id IS NULL".into(),                                    [F, F, T],   All),
        (format!("head_entry_id IS NULL OR head_entry_id = '{invocation}'"), [T, F, T],   All),
        ("id > 'vq_'".into(),                                               [T, T, T],   All),
        ("id < 'vq_'".into(),                                               [F, F, F],   All),
        ("head_entry_id LIKE 'mut%'".into(),                                 [F, T, U],   All),
    ];
    for (sql, truth, expected) in cases {
        // Use DataFusion's real coercion and simplification before native analysis:
        // singleton string bounds should become equality without assuming ID order.
        let original = simplifier
            .coerce(context.parse_sql_expr(&sql, &schema).unwrap(), &schema)
            .unwrap();
        let simplified = simplifier.simplify(original.clone()).unwrap();
        let original = context.create_physical_expr(original, &schema).unwrap();
        let expr = context.create_physical_expr(simplified, &schema).unwrap();
        assert_eq!(
            RoutingFixture::truth(&original, &batch),
            truth,
            "sql_truth: {sql}"
        );
        assert_eq!(
            RoutingFixture::truth(&expr, &batch),
            truth,
            "simplification_preserves_sql_truth: {sql}"
        );
        let shard_keys = Scheduler::candidate_shard_keys(std::slice::from_ref(&expr)).unwrap();
        match expected {
            All => assert!(
                shard_keys.is_all(),
                "unsupported_or_lossy_derivations_keep_all_targets: {sql}"
            ),
            Keys(keys) => assert_eq!(
                shard_keys.clone().into_values().as_deref(),
                Some(keys),
                "native_ids_preserve_exact_routing: {sql}"
            ),
        }
        let candidates = queues
            .each_ref()
            .map(|id| shard_keys.contains(&id.partition_key()));
        let selected =
            filter_record_batch(&batch, &BooleanArray::from(candidates.to_vec())).unwrap();
        let actual = candidates
            .iter()
            .enumerate()
            .filter_map(|(row, keep)| keep.then_some(row))
            .zip(RoutingFixture::truth(&expr, &selected))
            .filter_map(|(row, value)| (value == T).then_some(row))
            .collect::<Vec<_>>();
        let expected = truth
            .iter()
            .enumerate()
            .filter_map(|(row, value)| (*value == T).then_some(row))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "routing_plus_residual_equals_sql: {sql}");
    }
}

#[test]
fn finite_domains_feed_ordered_derivations_before_alternatives_are_merged() {
    crate::define_schema! {
        FiniteRows {
            #[values([1, 2, 3])]
            choice: u64 => DataType::UInt64,
            #[values([2, 4, 6])]
            doubled: u64 => DataType::UInt64,
            #[internal]
            route: u64,
        }
        derive [derive_double, derive_route]
    }

    fn derive_double(domains: &mut FiniteRowsDomains) -> anyhow::Result<()> {
        assert!(
            domains.choice.clone().into_values().is_some(),
            "normalize before deriving"
        );
        assert!(
            !domains.choice.is_empty(),
            "discard impossible inputs before hooks"
        );
        domains.doubled = std::mem::replace(&mut domains.doubled, Domain::empty())
            .intersect(domains.choice.map_values(|choice| choice * 2));
        Ok(())
    }

    fn derive_route(domains: &mut FiniteRowsDomains) -> anyhow::Result<()> {
        assert!(
            !domains.doubled.is_empty(),
            "discard contradictions between hooks"
        );
        assert!(domains.route.is_all(), "run each hook once");
        domains.route = domains.doubled.map_values(|doubled| doubled / 4);
        Ok(())
    }

    let batch = RecordBatch::try_new(
        FiniteRows::arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            Arc::new(UInt64Array::from(vec![2, 4, 6])),
        ],
    )
    .unwrap();
    let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
    let context = SessionContext::new();
    const T: Option<bool> = Some(true);
    const F: Option<bool> = Some(false);
    type Case<'a> = (&'a str, [Option<bool>; 3], &'a [u64], usize);
    #[rustfmt::skip]
    let cases: &[Case<'_>] = &[
        // SQL predicate                                                choice: 1  2  3   routes    branches
        ("TRUE",                                                               [T, T, T], &[0, 1],  1),
        ("choice NOT IN (1, 3)",                                                [F, T, F], &[1],     1),
        ("choice NOT IN (1, 2, 3)",                                             [F, F, F], &[],      0),
        ("choice = 2 AND doubled = 6",                                          [F, F, F], &[],      0),
        ("(choice = 2 AND doubled = 6) OR (choice = 3 AND doubled = 6)",          [F, F, T], &[1],     1),
        ("choice NOT IN (1, 3) OR choice = 1",                                   [T, T, F], &[0, 1],  2),
    ];
    for &(sql, truth, routes, count) in cases {
        let expr = context
            .parse_sql_expr(sql, &schema)
            .unwrap()
            .transform(|expr| {
                Ok(match expr {
                    Expr::Literal(ScalarValue::Int64(Some(value)), metadata) => {
                        Transformed::yes(Expr::Literal(
                            ScalarValue::UInt64(Some(value.try_into().unwrap())),
                            metadata,
                        ))
                    }
                    expr => Transformed::no(expr),
                })
            })
            .unwrap()
            .data;
        let expr = context.create_physical_expr(expr, &schema).unwrap();
        assert_eq!(
            RoutingFixture::truth(&expr, &batch),
            truth,
            "SQL truth: {sql}"
        );
        let branches = FiniteRows::analyze(&[expr], NonZeroUsize::new(4).unwrap()).unwrap();
        assert_eq!(
            branches.len(),
            count,
            "surviving derived alternatives: {sql}"
        );
        for (row, choice) in [1, 2, 3].iter().enumerate() {
            assert_eq!(
                branches.iter().any(|branch| branch.choice.contains(choice)
                    && branch.doubled.contains(&(choice * 2))),
                truth[row] == Some(true),
                "typed candidates preserve SQL truth: {sql}"
            );
        }
        let route =
            FiniteRows::guarantees(&branches).map_or_else(Domain::empty, |domains| domains.route);
        assert_eq!(
            route.into_values().as_deref(),
            Some(routes),
            "derive before merging: {sql}"
        );
    }
}

#[test]
fn declared_derivations_preserve_incarnations_and_route_before_merging_branches() {
    use Routes::{All, Keys};

    const T: Option<bool> = Some(true);
    const F: Option<bool> = Some(false);
    const U: Option<bool> = None;
    let fixture = RoutingFixture::new();
    // a and next share the same base identity in partition key 7. b belongs to 9.
    let [a, next, b] = fixture.ids;
    #[rustfmt::skip]
    let cases = [
        // SQL predicate                                                       a   next b    routing keys  branches
        (format!("entry_id = '{a}'"),                                           [T,  F,  F],  Keys(&[7]),   1),
        (format!("entry_id IN ('{a}', '{next}')"),                              [T,  T,  F],  Keys(&[7]),   1),
        (format!("entry_id = '{a}' AND partition_key = 9"),                     [F,  F,  F],  Keys(&[]),    0),
        (format!("(entry_id = '{a}' AND partition_key = 9) OR \
                  (entry_id = '{b}' AND partition_key = 9)"),                  [F,  F,  T],  Keys(&[9]),   1),
        (format!("(entry_id = '{a}' AND partition_key = 9) OR \
                  (entry_id = '{b}' AND partition_key = 7)"),                  [F,  F,  F],  Keys(&[]),    0),
        (format!("entry_id = '{a}' OR partition_key = 9"),                      [T,  F,  T],  Keys(&[7, 9]),2),
        (format!("entry_id != '{a}'"),                                          [F,  T,  T],  All,          1),
        (format!("entry_id NOT IN ('{a}', '{next}')"),                          [F,  F,  T],  All,          1),
        (format!("entry_id IN ('{a}', 'not-an-id')"),                           [T,  F,  F],  Keys(&[7]),   1),
        ("entry_id IN ('not-an-id')".into(),                                    [F,  F,  F],  Keys(&[]),    0),
        ("entry_id NOT IN ('not-an-id')".into(),                                [T,  T,  T],  All,          1),
        (format!("entry_id NOT IN ('{a}', NULL)"),                              [F,  U,  U],  Keys(&[]),    0),
    ];
    for (sql, truth, routes, count) in cases {
        let branches = fixture.assert_analysis(&sql, &truth);
        assert_eq!(branches.len(), count, "surviving source branches: {sql}");
        let domain = EntryRows::guarantees(&branches)
            .map_or_else(Domain::empty, |source| source.partition_key);
        match routes {
            All => assert!(
                domain.is_all(),
                "lossy exclusions do not prune routing: {sql}"
            ),
            Keys(keys) => assert_eq!(
                domain.into_values().as_deref(),
                Some(keys),
                "routing: {sql}"
            ),
        }
    }

    // Mapping a finite candidate set preserves the original incarnation filter
    // and deduplicates the derived base identity shared by its alternatives.
    let branches = fixture.assert_analysis(&format!("entry_id IN ('{a}', '{next}')"), &[T, T, F]);
    let source = EntryRows::guarantees(&branches).unwrap();
    assert_eq!(source.base.into_values(), Some(vec![a.to_base_entry_id()]));
    assert_eq!(source.canonical.into_values(), Some(vec![a, next]));

    // A bounded fallback may broaden routing, but still retains all SQL results.
    let expr = fixture.physical(&format!("entry_id = '{a}' OR partition_key = 9"));
    let broad = EntryRows::analyze(&[expr], NonZeroUsize::new(1).unwrap()).unwrap();
    assert!(
        EntryRows::guarantees(&broad)
            .unwrap()
            .partition_key
            .is_all()
    );
    assert!(
        Domain::<CanonicalEntryId>::empty()
            .map_values(|id| id.to_base_entry_id())
            .is_empty()
    );

    assert_eq!(
        EntryRows::sql_name(EntryRowsColumn::Base),
        None,
        "internal columns have no SQL binding"
    );
    assert_eq!(EntryRows::column_from_sql("base"), None);
    assert_eq!(
        EntryRows::column_from_tag("base"),
        Some(EntryRowsColumn::Base)
    );
    assert_eq!(
        EntryRows::arrow_schema().fields().len(),
        2,
        "internal columns are omitted from Arrow"
    );
    assert_eq!(
        EntryRows::arrow_schema().field(0).name(),
        "entry_id",
        "Arrow uses the declared alias"
    );
    assert_eq!(
        EntryRows::tag(EntryRowsColumn::Canonical),
        "canonical",
        "physical bindings keep canonical tags"
    );
    let expr = fixture.physical(&format!("entry_id = '{a}'"));
    let Selection::Predicates(clauses) = compile::<EntryRows>(Some(&expr)) else {
        panic!("SQL alias must also construct a strict selection");
    };
    assert!(matches!(clauses.for_column(EntryRowsColumn::Canonical),
        [EntryRowsClause::Canonical(ValuePredicate::Equal(id))] if *id == a));
    let clause = EntryRowsClause::Base(ValuePredicate::Equal(a.to_base_entry_id()));
    assert_eq!(
        EntryRows::column(&clause),
        EntryRowsColumn::Base,
        "internal columns support native clauses"
    );
}

struct RoutingFixture {
    ids: [CanonicalEntryId; 3],
    batch: RecordBatch,
    schema: DFSchema,
    context: SessionContext,
}

impl RoutingFixture {
    fn new() -> Self {
        let a = BaseEntryId::new(7, EntryId::new(EntryKind::Invocation, [1; 16]))
            .canonicalize(Seq::new(10));
        let b = BaseEntryId::new(9, EntryId::new(EntryKind::Invocation, [2; 16]))
            .canonicalize(Seq::new(10));
        let ids = [a, a.with_seq(Seq::new(11)), b];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("entry_id", DataType::LargeUtf8, false),
                Field::new("partition_key", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(LargeStringArray::from_iter_values(
                    ids.iter().map(ToString::to_string),
                )),
                Arc::new(UInt64Array::from_iter_values(
                    ids.iter().map(CanonicalEntryId::partition_key),
                )),
            ],
        )
        .unwrap();
        Self {
            ids,
            schema: DFSchema::try_from(batch.schema().as_ref().clone()).unwrap(),
            batch,
            context: SessionContext::new(),
        }
    }

    fn physical(&self, sql: &str) -> Arc<dyn PhysicalExpr> {
        // Match the fixture's column types directly, so inserted casts do not
        // hide the literal clauses whose source binding is under test.
        let expr = self
            .context
            .parse_sql_expr(sql, &self.schema)
            .unwrap()
            .transform(|expr| match expr {
                Expr::Literal(value, metadata) => {
                    let value = match value {
                        ScalarValue::Utf8(_) | ScalarValue::Null => {
                            value.cast_to(&DataType::LargeUtf8)?
                        }
                        ScalarValue::Int64(Some(value)) => ScalarValue::UInt64(Some(
                            u64::try_from(value)
                                .expect("nonnegative partition-key fixture literal"),
                        )),
                        value => value,
                    };
                    Ok(Transformed::yes(Expr::Literal(value, metadata)))
                }
                expr => Ok(Transformed::no(expr)),
            })
            .unwrap()
            .data;
        self.context
            .create_physical_expr(expr, &self.schema)
            .unwrap()
    }

    fn truth(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Vec<Option<bool>> {
        let result = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect()
    }

    fn assert_analysis(&self, sql: &str, expected: &[Option<bool>]) -> Vec<EntryRowsDomains> {
        let expr = self.physical(sql);
        assert_eq!(
            Self::truth(&expr, &self.batch),
            expected,
            "SQL truth: {sql}"
        );
        let branches =
            EntryRows::analyze(std::slice::from_ref(&expr), NonZeroUsize::new(4).unwrap()).unwrap();
        let primary_candidates: Vec<_> = self
            .ids
            .iter()
            .map(|id| {
                branches.iter().any(|branch| {
                    branch.base.contains(&id.to_base_entry_id())
                        && branch.partition_key.contains(&id.partition_key())
                })
            })
            .collect();
        let full_candidates: Vec<_> = self
            .ids
            .iter()
            .map(|id| {
                branches.iter().any(|branch| {
                    branch.canonical.contains(id)
                        && branch.base.contains(&id.to_base_entry_id())
                        && branch.partition_key.contains(&id.partition_key())
                })
            })
            .collect();
        assert_eq!(
            full_candidates,
            expected
                .iter()
                .map(|truth| *truth == Some(true))
                .collect::<Vec<_>>(),
            "exact source candidates: {sql}"
        );

        for (row, truth) in expected.iter().enumerate() {
            assert!(
                *truth != Some(true) || primary_candidates[row],
                "derived access lost row {row}: {sql}"
            );
        }
        let selected =
            filter_record_batch(&self.batch, &BooleanArray::from(primary_candidates.clone()))
                .unwrap();
        let residual = Self::truth(&expr, &selected);
        let actual: Vec<_> = primary_candidates
            .iter()
            .enumerate()
            .filter_map(|(row, &selected)| selected.then_some(row))
            .zip(residual)
            .filter_map(|(row, truth)| (truth == Some(true)).then_some(row))
            .collect();
        let expected: Vec<_> = expected
            .iter()
            .enumerate()
            .filter_map(|(row, truth)| (*truth == Some(true)).then_some(row))
            .collect();
        assert_eq!(
            actual, expected,
            "base access plus residual preserves incarnations: {sql}"
        );
        branches
    }
}

#[test]
fn index_restrictions_keep_whole_predicate_domains() {
    use restate_storage_api::vqueue_table::Stage;

    use crate::schema::index::index_restriction;
    use crate::tables::index::EntryByQueue;
    use crate::tables::vqueue_entry_status::VqueueEntryStatus;

    let schema = DFSchema::try_from(VqueueEntryStatus::arrow_schema().as_ref().clone()).unwrap();
    let context = SessionContext::new();
    let queues: Vec<_> = (0..20)
        .map(|n| VQueueId::custom(7, format!("q{n}")).to_string())
        .collect();
    let restrict = |sql: &str| {
        let expr = context
            .parse_sql_expr(sql, &schema)
            .unwrap()
            .transform(|expr| match expr {
                Expr::Literal(ScalarValue::Utf8(value), metadata) => Ok(Transformed::yes(
                    Expr::Literal(ScalarValue::LargeUtf8(value), metadata),
                )),
                expr => Ok(Transformed::no(expr)),
            })
            .unwrap()
            .data;
        let expr = context.create_physical_expr(expr, &schema).unwrap();
        index_restriction::<EntryByQueue>(&[expr]).unwrap()
    };
    let queue = |n: usize| queues[n].parse::<VQueueId>().unwrap();
    let (a, b) = (&queues[0], &queues[1]);

    // Each alternative restricts the queue differently; their union still does.
    let domains = restrict(&format!(
        "(vqueue_id = '{a}' AND has_lock = true) OR (vqueue_id = '{b}' AND stage = 'inbox')"
    ))
    .unwrap();
    assert_eq!(
        domains.vqueue_id.into_values(),
        Some(vec![queue(0), queue(1)]),
        "alternatives_union_their_queues"
    );
    assert!(
        [Stage::Inbox, Stage::Running, Stage::Finished]
            .iter()
            .all(|stage| domains.stage.contains(stage))
            && [false, true]
                .iter()
                .all(|lock| domains.has_lock.contains(lock)),
        "an_unrestricted_alternative_admits_every_stage_and_lock"
    );

    let domains = restrict(&format!(
        "vqueue_id = '{a}' AND (stage = 'inbox' OR stage = 'running')"
    ))
    .unwrap();
    assert_eq!(
        domains.stage.into_values(),
        Some(vec![Stage::Inbox, Stage::Running]),
        "conjoined_alternatives_restrict_stages"
    );

    assert!(
        restrict(&format!("vqueue_id = '{a}' AND vqueue_id = '{b}'")).is_none(),
        "contradictions_read_nothing"
    );
    assert!(
        restrict(&format!("num_attempts > 3 OR vqueue_id = '{a}'"))
            .unwrap()
            .vqueue_id
            .is_all(),
        "an_unrestricted_alternative_admits_every_queue"
    );

    // More alternatives than the branch budget: analysis keeps the subtree intact
    // and must still admit every listed queue.
    let many = (0..20)
        .map(|n| format!("vqueue_id = '{}'", queues[n]))
        .collect::<Vec<_>>()
        .join(" OR ");
    let domains = restrict(&many).unwrap();
    assert!(
        (0..20).all(|n| domains.vqueue_id.contains(&queue(n))),
        "exhausted_budgets_widen"
    );
}
