// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Formatter};
use std::ops::{RangeBounds, RangeInclusive};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::split_conjunction;
use datafusion::physical_expr_common::physical_expr::snapshot_physical_expr;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, InListExpr, IsNullExpr, Literal};
use strum::EnumCount;

use restate_storage_api::vqueue_table::Stage;
use restate_types::PartitionedResourceId;
use restate_types::identifiers::partitioner::HashPartitioner;
use restate_types::identifiers::{InvocationId, PartitionKey, ResourceId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::VQueueEntryId;

use crate::partition_store_scanner::ScanLocalPartitionFilter;

pub trait PartitionKeyExtractor: Send + Sync + 'static + Debug {
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>>;
}

#[derive(Debug)]
pub struct FirstMatchingPartitionKeyExtractor {
    extractors: Vec<Box<dyn PartitionKeyExtractor>>,
}

impl Default for FirstMatchingPartitionKeyExtractor {
    fn default() -> Self {
        let extractors = vec![Box::new(MatchingColumnExtractor::new(
            "partition_key",
            |value: &ScalarValue| match value {
                ScalarValue::UInt64(Some(v)) => Ok(*v),
                _ => anyhow::bail!("expected UInt64 partition key"),
            },
        )) as Box<dyn PartitionKeyExtractor>];
        Self { extractors }
    }
}

impl FirstMatchingPartitionKeyExtractor {
    pub fn with_scope(self, column_name: impl Into<String>) -> Self {
        // we only use the scope value if it's not empty, otherwise we cannot
        // rely on it to get the partition key.
        let e = MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected scope")?
                .context("null scopes cannot be used for partition-key matching")?;
            Ok(HashPartitioner::compute_partition_key(value))
        });
        self.append(e)
    }

    pub fn with_partitioned_resource_id<T>(self, column_name: impl Into<String>) -> Self
    where
        T: PartitionedResourceId + ResourceId + FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let e = MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .with_context(|| format!("expected string {:?}", T::RESOURCE_TYPE))?
                .context("null values cannot be used for partition-key matching")?;
            let resource =
                T::from_str(value).with_context(|| format!("non valid {:?}", T::RESOURCE_TYPE))?;
            Ok(resource.partition_key())
        });
        self.append(e)
    }

    pub fn with_service_key(self, column_name: impl Into<String>) -> Self {
        self.append(Self::create_service_key_partition_key_extractor(
            column_name,
        ))
    }

    fn create_service_key_partition_key_extractor(
        column_name: impl Into<String>,
    ) -> MatchingColumnExtractor<fn(&ScalarValue) -> anyhow::Result<PartitionKey>> {
        MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected string service key")?
                .context("unexpected null service key")?;
            Ok(HashPartitioner::compute_partition_key(value))
        })
    }

    /// For tables sharded by `scope_column` when scoped and by `service_key_column` when
    /// unscoped (i.e. `scope_column IS NULL`). Extracts a partition key from either:
    /// - `scope = '...'` / `scope IN (...)` (sharded under `hash(scope)`), or
    /// - `scope IS NULL AND service_key = '...'` / `IN (...)` (sharded under `hash(service_key)`).
    pub fn with_scope_or_service_key(
        self,
        scope_column: impl Into<String>,
        service_key_column: impl Into<String>,
    ) -> Self {
        let scope_column: String = scope_column.into();
        let by_scope = MatchingColumnExtractor::new(scope_column.clone(), |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected scope")?
                .context("null scopes cannot be used for partition-key matching")?;
            Ok(HashPartitioner::compute_partition_key(value))
        });
        self.append(by_scope).append(WhenNullExtractor::new(
            scope_column,
            Self::create_service_key_partition_key_extractor(service_key_column),
        ))
    }

    pub fn with_invocation_id(self, column_name: impl Into<String>) -> Self {
        let e = MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected string invocation id")?
                .context("unexpected null invocation id")?;
            let invocation_id = InvocationId::from_str(value).context("non valid invocation id")?;
            Ok(invocation_id.partition_key())
        });
        self.append(e)
    }

    pub fn with_vqueue_entry_id(self, column_name: impl Into<String>) -> Self {
        let e = MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected string entry id")?
                .context("unexpected null entry id")?;

            VQueueEntryId::extract_partition_key(value)
                .map_err(|_| anyhow::anyhow!("non valid entry id"))
        });
        self.append(e)
    }

    pub fn append(mut self, extractor: impl PartitionKeyExtractor) -> Self {
        self.extractors.push(Box::new(extractor));
        self
    }
}

impl PartitionKeyExtractor for FirstMatchingPartitionKeyExtractor {
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        for extractor in &self.extractors {
            if let Some(partition_keys) = extractor.try_extract(filters)? {
                return Ok(Some(partition_keys));
            }
        }

        Ok(None)
    }
}

pub(crate) struct MatchingColumnExtractor<F> {
    column_name: String,
    extractor: F,
}

impl<F> Debug for MatchingColumnExtractor<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "MatchingColumnExtractor({:?})",
            self.column_name
        ))
    }
}

impl<F> MatchingColumnExtractor<F> {
    pub(crate) fn new(column_name: impl Into<String>, extractor: F) -> Self {
        Self {
            column_name: column_name.into(),
            extractor,
        }
    }
}

impl<F> PartitionKeyExtractor for MatchingColumnExtractor<F>
where
    F: Fn(&ScalarValue) -> anyhow::Result<PartitionKey> + Send + Sync + 'static,
{
    /// Find an expression in the form of `$column_name = <literal>`.
    /// Then use the provided extractor to convert the literal value to a partition_key.
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        for filter in filters {
            let Some(inlist) = InList::parse(filter, 5) else {
                continue;
            };

            if inlist.col.name() != self.column_name {
                continue;
            }

            let mut list_keys = BTreeSet::new();

            for value in &inlist.list {
                let pk = (self.extractor)(value)?;
                list_keys.insert(pk);
            }

            return Ok(Some(list_keys));
        }

        Ok(None)
    }
}

/// Gates an inner [`PartitionKeyExtractor`] on the presence of a top-level
/// `<null_column_name> IS NULL` conjunct.
///
/// Used for tables that are sharded differently depending on whether a column is null
/// (e.g. `state` and `sys_promise`: scoped rows live at `hash(scope)`, unscoped rows at
/// `hash(service_key)`). When the user writes `... AND <null_column> IS NULL`, scoped
/// rows are filtered out by the predicate anyway, so it's safe to narrow the scan
/// using a key derived from another column.
pub(crate) struct WhenNullExtractor<E> {
    null_column_name: String,
    inner: E,
}

impl<E> Debug for WhenNullExtractor<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "WhenNullExtractor({:?})",
            self.null_column_name
        ))
    }
}

impl<E> WhenNullExtractor<E> {
    pub(crate) fn new(null_column_name: impl Into<String>, inner: E) -> Self {
        Self {
            null_column_name: null_column_name.into(),
            inner,
        }
    }
}

impl<E> PartitionKeyExtractor for WhenNullExtractor<E>
where
    E: PartitionKeyExtractor,
{
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        // Only accept a bare top-level `IsNullExpr` against a `Column`. An `IsNullExpr`
        // nested in `Or`/`Not`/etc. does not count: e.g. `(scope IS NULL OR scope IS NOT NULL)`
        // would otherwise spuriously gate the inner extractor open.
        let has_null_check = filters.iter().any(|filter| {
            filter
                .downcast_ref::<IsNullExpr>()
                .and_then(|is_null| is_null.arg().downcast_ref::<Column>())
                .is_some_and(|column| column.name() == self.null_column_name)
        });

        if !has_null_check {
            return Ok(None);
        }

        self.inner.try_extract(filters)
    }
}

/// A normalized representation of predicates that compare a column to literal values.
/// Handles `col = lit`, `col IN (lit, ...)`, and `col = lit OR col = lit ...` patterns.
struct InList<'a> {
    col: &'a Column,
    list: HashSet<&'a ScalarValue>,
    negated: bool,
}

impl<'a> InList<'a> {
    fn parse(predicate: &'a Arc<dyn PhysicalExpr>, depth_limit: usize) -> Option<Self> {
        if depth_limit <= 1 {
            return None;
        }

        // Handle IN list: col IN ('a', 'b', ...)
        if let Some(in_list) = predicate.downcast_ref::<InListExpr>() {
            let col = in_list.expr().downcast_ref::<Column>()?;

            let mut list = HashSet::with_capacity(in_list.len());
            for lit in in_list.list() {
                let lit = lit.downcast_ref::<Literal>()?;
                list.insert(lit.value());
            }

            return Some(InList {
                col,
                list,
                negated: in_list.negated(),
            });
        }

        let binary = predicate.downcast_ref::<BinaryExpr>()?;

        match binary.op() {
            // Handle simple equality: col = 'a'
            Operator::Eq => {
                let (col, lit) = extract_column_literal(binary.left(), binary.right())
                    .or_else(|| extract_column_literal(binary.right(), binary.left()))?;

                Some(InList {
                    col,
                    list: HashSet::from_iter([lit.value()]),
                    negated: false,
                })
            }
            // Handle OR: col = 'a' OR col = 'b'
            Operator::Or => {
                let mut left = Self::parse(binary.left(), depth_limit - 1)?;
                let right = Self::parse(binary.right(), depth_limit - 1)?;

                if left.col.name() == right.col.name() && !left.negated && !right.negated {
                    left.list.extend(right.list);
                    Some(InList {
                        col: left.col,
                        list: left.list,
                        negated: false,
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

fn extract_column_literal<'a>(
    column: &'a Arc<dyn PhysicalExpr>,
    literal: &'a Arc<dyn PhysicalExpr>,
) -> Option<(&'a Column, &'a Literal)> {
    let col = column.downcast_ref::<Column>()?;
    let lit = literal.downcast_ref::<Literal>()?;
    Some((col, lit))
}

#[derive(Debug, Clone)]
pub struct VQueueFilter {
    pub partition_keys: KeyRange,
    pub stages: Option<BTreeSet<Stage>>,
}

impl ScanLocalPartitionFilter for VQueueFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        let mut stages: Option<BTreeSet<Stage>> = None;

        if let Some(predicate) = predicate
            && let Ok(predicate) = snapshot_physical_expr(predicate)
        {
            for conjunct in split_conjunction(&predicate) {
                let Some(conjunct_stages) = parse_vqueue_stages("stage", conjunct) else {
                    continue;
                };

                stages = Some(match stages {
                    Some(current) => current.intersection(&conjunct_stages).copied().collect(),
                    None => conjunct_stages,
                });
            }
        }

        Self {
            partition_keys: range,
            stages,
        }
    }
}

fn parse_vqueue_stages(
    column_name: &str,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Option<BTreeSet<Stage>> {
    // OR-chain recursion depth budget. Each `Or` node consumes one unit and the
    // leaf check requires the remaining budget to be > 1, so an N-leaf chain
    // needs depth >= N + 1. `Stage::COUNT` covers every variant (incl. `Unknown`)
    // and the `+ 1` satisfies the leaf threshold.
    let in_list = InList::parse(predicate, Stage::COUNT + 1)?;

    if in_list.col.name() != column_name || in_list.negated {
        return None;
    }

    let mut stages = BTreeSet::new();
    for literal in in_list.list {
        let Some(Some(stage_str)) = literal.try_as_str() else {
            continue;
        };

        if let Some(stage) = parse_stage_literal(stage_str) {
            stages.insert(stage);
        }
    }

    if stages.is_empty() {
        None
    } else {
        Some(stages)
    }
}

fn parse_stage_literal(value: &str) -> Option<Stage> {
    match value.to_ascii_lowercase().as_str() {
        "inbox" => Some(Stage::Inbox),
        "run" | "running" => Some(Stage::Running),
        "suspended" => Some(Stage::Suspended),
        "paused" => Some(Stage::Paused),
        "finished" => Some(Stage::Finished),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct InvocationIdFilter {
    pub partition_keys: KeyRange,
    pub invocation_ids: Option<RangeInclusive<InvocationId>>,
}

impl ScanLocalPartitionFilter for InvocationIdFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        if let Some(predicate) = predicate
            && let Ok(predicate) = snapshot_physical_expr(predicate)
        {
            for conjunct in split_conjunction(&predicate) {
                if let Some(invocation_ids) = parse_invocation_id_range("id", range, conjunct) {
                    return Self {
                        partition_keys: range,
                        invocation_ids: Some(invocation_ids),
                    };
                }
            }
        }

        Self {
            partition_keys: range,
            invocation_ids: None,
        }
    }
}

fn parse_invocation_id_range(
    column_name: &str,
    range: KeyRange,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Option<RangeInclusive<InvocationId>> {
    let in_list = InList::parse(predicate, 5)?;

    if in_list.col.name() != column_name {
        return None;
    }

    let mut invocation_ids: Option<RangeInclusive<InvocationId>> = None;
    for literal in in_list.list {
        let str = literal.try_as_str()??;
        let invocation_id = InvocationId::from_str(str).ok()?;

        if range.contains(&invocation_id.partition_key()) {
            if let Some(invocation_ids) = &mut invocation_ids {
                *invocation_ids = (*invocation_ids.start()).min(invocation_id)
                    ..=(*invocation_ids.end()).max(invocation_id);
            } else {
                invocation_ids = Some(invocation_id..=invocation_id)
            }
        }
    }

    invocation_ids
}

/// Above this many distinct entry IDs we stop building a multi-get set and
/// collapse to a `min..=max` range scan instead. Keeps the multi-get key vector
/// bounded; scattered opaque IDs are never dense, so below the threshold the
/// multi-get always wins over scanning the whole span.
const ENTRY_ID_SET_THRESHOLD: usize = 500;

/// How an `entry_id` predicate is served: either an exact set of IDs (small
/// enough for a batched multi-get) or a collapsed `min..=max` range scan.
#[derive(Debug, Clone)]
pub enum EntryIdSelection {
    Set(BTreeSet<VQueueEntryId>),
    Range(std::range::RangeInclusive<VQueueEntryId>),
}

#[derive(Debug, Clone)]
pub struct VQueueEntryIdFilter {
    pub partition_keys: KeyRange,
    pub entry_ids: Option<EntryIdSelection>,
}

impl ScanLocalPartitionFilter for VQueueEntryIdFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        if let Some(predicate) = predicate
            && let Ok(predicate) = snapshot_physical_expr(predicate)
        {
            for conjunct in split_conjunction(&predicate) {
                if let Some(entry_ids) = parse_entry_id_selection("entry_id", range, conjunct) {
                    return Self {
                        partition_keys: range,
                        entry_ids: Some(entry_ids),
                    };
                }
            }
        }

        Self {
            partition_keys: range,
            entry_ids: None,
        }
    }
}

fn parse_entry_id_selection(
    column_name: &str,
    range: KeyRange,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Option<EntryIdSelection> {
    let in_list = InList::parse(predicate, 5)?;

    if in_list.col.name() != column_name {
        return None;
    }

    // A `BTreeSet` gives us both dedup and on-disk key ordering for free:
    // `VQueueEntryId`'s `Ord` matches the encoded key byte order, so iterating
    // the set yields keys in the exact order RocksDB (and the declared table
    // sort order) expects.
    let mut entry_ids = BTreeSet::new();
    for literal in in_list.list {
        let str = literal.try_as_str()??;
        let entry_id = VQueueEntryId::from_str(str).ok()?;

        if range.contains(&entry_id.partition_key()) {
            entry_ids.insert(entry_id);
        }
    }

    if entry_ids.is_empty() {
        return None;
    }

    if entry_ids.len() <= ENTRY_ID_SET_THRESHOLD {
        Some(EntryIdSelection::Set(entry_ids))
    } else {
        // Too many IDs to multi-get: fall back to a single range scan over the
        // span they cover. `first`/`last` are the in-range min/max.
        let start = *entry_ids.first().expect("non-empty");
        let last = *entry_ids.last().expect("non-empty");
        Some(EntryIdSelection::Range(std::range::RangeInclusive {
            start,
            last,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use datafusion::common::ScalarValue;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::expressions::{
        BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
    };

    use restate_storage_api::vqueue_table::Stage;
    use restate_types::identifiers::{InvocationId, ServiceId, StateMutationId, WithPartitionKey};
    use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType};
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::VQueueEntryId;

    use crate::filter::{
        EntryIdSelection, FirstMatchingPartitionKeyExtractor, InvocationIdFilter,
        PartitionKeyExtractor, VQueueEntryIdFilter, VQueueFilter,
    };
    use crate::partition_store_scanner::ScanLocalPartitionFilter;

    fn col(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    fn utf8_lit(value: impl Into<String>) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(value.into()))))
    }

    fn is_null(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(IsNullExpr::new(col(name)))
    }

    fn is_not_null(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(IsNotNullExpr::new(col(name)))
    }

    fn eq(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            left,
            datafusion::logical_expr::Operator::Eq,
            right,
        ))
    }

    fn or(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            left,
            datafusion::logical_expr::Operator::Or,
            right,
        ))
    }

    fn in_list(col_name: &str, list: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![Field::new(col_name, DataType::LargeUtf8, true)]);
        Arc::new(InListExpr::try_new(col(col_name), list, false, &schema).expect("valid in-list"))
    }

    fn and(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            left,
            datafusion::logical_expr::Operator::And,
            right,
        ))
    }

    const FULL_RANGE: KeyRange = KeyRange::FULL;

    fn make_invocation_id(key: &str) -> InvocationId {
        let target = InvocationTarget::virtual_object(
            "svc",
            key,
            "handler",
            VirtualObjectHandlerType::Exclusive,
        );
        InvocationId::generate(&target, None)
    }

    #[test]
    fn service_key() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id = ServiceId::new(None, "greeter", "key-1");
        let expected_key = service_id.partition_key();

        let got_keys = extractor
            .try_extract(&[eq(col("service_key"), utf8_lit("key-1"))])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(1, got_keys.len());
        assert_eq!(expected_key, got_keys.into_iter().next().unwrap());
    }

    #[test]
    fn multiple_service_keys() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id_1 = ServiceId::new(None, "greeter", "key-1");
        let service_id_2 = ServiceId::new(None, "greeter", "key-2");
        let expected_key_1 = service_id_1.partition_key();
        let expected_key_2 = service_id_2.partition_key();

        let got_keys = extractor
            .try_extract(&[in_list(
                "service_key",
                vec![utf8_lit("key-1"), utf8_lit("key-2")],
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(2, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    #[test]
    fn multiple_service_keys_ored() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id_1 = ServiceId::new(None, "greeter", "key-1");
        let service_id_2 = ServiceId::new(None, "greeter", "key-2");
        let expected_key_1 = service_id_1.partition_key();
        let expected_key_2 = service_id_2.partition_key();

        let got_keys = extractor
            .try_extract(&[or(
                eq(col("service_key"), utf8_lit("key-1")),
                eq(col("service_key"), utf8_lit("key-2")),
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(2, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    #[test]
    fn multiple_service_keys_nested_or() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id_1 = ServiceId::new(None, "greeter", "key-1");
        let service_id_2 = ServiceId::new(None, "greeter", "key-2");
        let service_id_3 = ServiceId::new(None, "greeter", "key-3");
        let service_id_4 = ServiceId::new(None, "greeter", "key-4");
        let expected_key_1 = service_id_1.partition_key();
        let expected_key_2 = service_id_2.partition_key();
        let expected_key_3 = service_id_3.partition_key();
        let expected_key_4 = service_id_4.partition_key();

        let got_keys = extractor
            .try_extract(&[or(
                or(
                    eq(col("service_key"), utf8_lit("key-1")),
                    eq(col("service_key"), utf8_lit("key-2")),
                ),
                or(
                    eq(col("service_key"), utf8_lit("key-3")),
                    eq(col("service_key"), utf8_lit("key-4")),
                ),
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(4, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_4, got_keys.next().unwrap());
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_3, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    #[test]
    fn multiple_service_keys_too_deep_nesting() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let got_keys = extractor
            .try_extract(&[or(
                or(
                    eq(col("service_key"), utf8_lit("key-1")),
                    or(
                        eq(col("service_key"), utf8_lit("key-2")),
                        or(
                            eq(col("service_key"), utf8_lit("key-3")),
                            eq(col("service_key"), utf8_lit("key-4")),
                        ),
                    ),
                ),
                eq(col("service_key"), utf8_lit("key-7")),
            )])
            .expect("extract");

        assert_eq!(None, got_keys);
    }

    #[test]
    fn invocation_id() {
        let extractor = FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id");

        let invocation_id = make_invocation_id("key-2");
        let expected_key = invocation_id.partition_key();

        let got_keys = extractor
            .try_extract(&[eq(col("id"), utf8_lit(invocation_id.to_string()))])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(1, got_keys.len());
        assert_eq!(expected_key, got_keys.into_iter().next().unwrap());
    }

    #[test]
    fn multiple_invocation_ids() {
        let extractor = FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id");

        let invocation_id_1 = make_invocation_id("key-1");
        let invocation_id_2 = make_invocation_id("key-2");
        let expected_key_1 = invocation_id_1.partition_key();
        let expected_key_2 = invocation_id_2.partition_key();

        let got_keys = extractor
            .try_extract(&[in_list(
                "id",
                vec![
                    utf8_lit(invocation_id_1.to_string()),
                    utf8_lit(invocation_id_2.to_string()),
                ],
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(2, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    #[test]
    fn vqueue_entry_id_invocation_id() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_vqueue_entry_id("head_entry_id");

        let invocation_id = make_invocation_id("key-2");
        let expected_key = invocation_id.partition_key();

        let got_keys = extractor
            .try_extract(&[eq(
                col("head_entry_id"),
                utf8_lit(invocation_id.to_string()),
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(1, got_keys.len());
        assert_eq!(expected_key, got_keys.into_iter().next().unwrap());
    }

    #[test]
    fn vqueue_entry_id_state_mutation_id() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_vqueue_entry_id("head_entry_id");

        let state_mutation_id = StateMutationId::generate(42);
        let expected_key = state_mutation_id.partition_key();

        let got_keys = extractor
            .try_extract(&[eq(
                col("head_entry_id"),
                utf8_lit(state_mutation_id.to_string()),
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(1, got_keys.len());
        assert_eq!(expected_key, got_keys.into_iter().next().unwrap());
    }

    #[test]
    fn invalid_in_list() {
        let extractor = FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id");

        let invocation_id = make_invocation_id("key-1");

        // An OR where one side has a non-literal (column) should not be extractable
        let got_keys = extractor
            .try_extract(&[or(
                eq(col("id"), utf8_lit(invocation_id.to_string())),
                eq(col("id"), col("some_other_col")),
            )])
            .expect("extract");

        assert_eq!(None, got_keys);
    }

    fn scope_or_service_key_extractor() -> FirstMatchingPartitionKeyExtractor {
        FirstMatchingPartitionKeyExtractor::default()
            .with_scope_or_service_key("scope", "service_key")
    }

    #[test]
    fn service_key_when_scope_is_null_extracts_partition_key() {
        let expected = ServiceId::new(None, "svc", "k").partition_key();

        let got = scope_or_service_key_extractor()
            .try_extract(&[is_null("scope"), eq(col("service_key"), utf8_lit("k"))])
            .expect("extract")
            .expect("partition key");

        assert_eq!(1, got.len());
        assert_eq!(expected, got.into_iter().next().unwrap());
    }

    #[test]
    fn service_key_in_list_when_scope_is_null() {
        let expected_a = ServiceId::new(None, "svc", "a").partition_key();
        let expected_b = ServiceId::new(None, "svc", "b").partition_key();

        let got = scope_or_service_key_extractor()
            .try_extract(&[
                is_null("scope"),
                in_list("service_key", vec![utf8_lit("a"), utf8_lit("b")]),
            ])
            .expect("extract")
            .expect("partition keys");

        assert_eq!(2, got.len());
        assert!(got.contains(&expected_a));
        assert!(got.contains(&expected_b));
    }

    #[test]
    fn service_key_without_scope_is_null_returns_none() {
        // Without the explicit `scope IS NULL` guard, the extractor cannot narrow because
        // scoped rows for the same service_key live at hash(scope), not hash(service_key).
        let got = scope_or_service_key_extractor()
            .try_extract(&[eq(col("service_key"), utf8_lit("k"))])
            .expect("extract");

        assert_eq!(None, got);
    }

    #[test]
    fn scope_is_null_alone_returns_none() {
        let got = scope_or_service_key_extractor()
            .try_extract(&[is_null("scope")])
            .expect("extract");

        assert_eq!(None, got);
    }

    #[test]
    fn scope_is_not_null_does_not_trigger() {
        // IsNotNullExpr is a distinct type from IsNullExpr; the gate must stay closed.
        let got = scope_or_service_key_extractor()
            .try_extract(&[is_not_null("scope"), eq(col("service_key"), utf8_lit("k"))])
            .expect("extract");

        assert_eq!(None, got);
    }

    #[test]
    fn scope_is_null_inside_or_does_not_trigger() {
        // `(scope IS NULL OR scope IS NOT NULL)` is a top-level Or, not a bare IsNullExpr.
        // The gate must stay closed so we don't narrow under a tautology.
        let got = scope_or_service_key_extractor()
            .try_extract(&[
                or(is_null("scope"), is_not_null("scope")),
                eq(col("service_key"), utf8_lit("k")),
            ])
            .expect("extract");

        assert_eq!(None, got);
    }

    #[test]
    fn scope_is_null_or_service_key_does_not_trigger() {
        // Single Or conjunct: neither side is a bare top-level IsNullExpr against scope.
        let got = scope_or_service_key_extractor()
            .try_extract(&[or(is_null("scope"), eq(col("service_key"), utf8_lit("k")))])
            .expect("extract");

        assert_eq!(None, got);
    }

    #[test]
    fn scope_is_null_on_different_column_does_not_trigger() {
        let got = scope_or_service_key_extractor()
            .try_extract(&[is_null("other_col"), eq(col("service_key"), utf8_lit("k"))])
            .expect("extract");

        assert_eq!(None, got);
    }

    #[test]
    fn invocation_id_filter_single_eq() {
        let id = make_invocation_id("key-1");
        let predicate = eq(col("id"), utf8_lit(id.to_string()));

        let filter = InvocationIdFilter::new(FULL_RANGE, Some(predicate));

        let range = filter.invocation_ids.expect("should extract range");
        assert_eq!(*range.start(), id);
        assert_eq!(*range.end(), id);
    }

    #[test]
    fn invocation_id_filter_in_list() {
        let id1 = make_invocation_id("key-1");
        let id2 = make_invocation_id("key-2");
        let predicate = in_list(
            "id",
            vec![utf8_lit(id1.to_string()), utf8_lit(id2.to_string())],
        );

        let filter = InvocationIdFilter::new(FULL_RANGE, Some(predicate));

        let range = filter.invocation_ids.expect("should extract range");
        assert_eq!(*range.start(), id1.min(id2));
        assert_eq!(*range.end(), id1.max(id2));
    }

    #[test]
    fn invocation_id_filter_excludes_out_of_range() {
        let id = make_invocation_id("key-1");
        let pk = id.partition_key();
        let narrow_range = if pk > 0 {
            KeyRange::new(0, pk - 1)
        } else {
            KeyRange::new(1, 1)
        };

        let predicate = eq(col("id"), utf8_lit(id.to_string()));
        let filter = InvocationIdFilter::new(narrow_range, Some(predicate));

        assert!(filter.invocation_ids.is_none());
    }

    #[test]
    fn invocation_id_filter_and_conjunction() {
        let id = make_invocation_id("key-1");
        // id = '...' AND other_col = 'foo' — should find the id conjunct
        let predicate = and(
            eq(col("id"), utf8_lit(id.to_string())),
            eq(col("other_col"), utf8_lit("foo")),
        );

        let filter = InvocationIdFilter::new(FULL_RANGE, Some(predicate));

        let range = filter
            .invocation_ids
            .expect("should extract from conjunction");
        assert_eq!(*range.start(), id);
        assert_eq!(*range.end(), id);
    }

    #[test]
    fn invocation_id_filter_wrong_column() {
        let id = make_invocation_id("key-1");
        let predicate = eq(col("not_id"), utf8_lit(id.to_string()));

        let filter = InvocationIdFilter::new(FULL_RANGE, Some(predicate));
        assert!(filter.invocation_ids.is_none());
    }

    #[test]
    fn invocation_id_filter_no_predicate() {
        let filter = InvocationIdFilter::new(FULL_RANGE, None);

        assert!(filter.invocation_ids.is_none());
        assert_eq!(filter.partition_keys, FULL_RANGE);
    }

    #[test]
    fn invocation_id_filter_invalid_id() {
        let predicate = eq(col("id"), utf8_lit("not-a-valid-invocation-id"));

        let filter = InvocationIdFilter::new(FULL_RANGE, Some(predicate));
        assert!(filter.invocation_ids.is_none());
    }

    #[test]
    fn vqueue_entry_id_filter_set_from_in_list() {
        let id1 = make_invocation_id("key-1");
        let id2 = make_invocation_id("key-2");
        let predicate = in_list(
            "entry_id",
            vec![utf8_lit(id1.to_string()), utf8_lit(id2.to_string())],
        );

        let filter = VQueueEntryIdFilter::new(FULL_RANGE, Some(predicate));

        let expected1 = VQueueEntryId::from_str(&id1.to_string()).unwrap();
        let expected2 = VQueueEntryId::from_str(&id2.to_string()).unwrap();
        match filter.entry_ids {
            Some(EntryIdSelection::Set(ids)) => {
                assert_eq!(ids.len(), 2);
                assert!(ids.contains(&expected1));
                assert!(ids.contains(&expected2));
            }
            other => panic!("expected an entry-id set, got {other:?}"),
        }
    }

    #[test]
    fn vqueue_entry_id_filter_excludes_out_of_range() {
        let id = make_invocation_id("key-1");
        let entry_id = VQueueEntryId::from_str(&id.to_string()).unwrap();
        let pk = entry_id.partition_key();
        let narrow_range = if pk > 0 {
            KeyRange::new(0, pk - 1)
        } else {
            KeyRange::new(1, 1)
        };

        let predicate = eq(col("entry_id"), utf8_lit(id.to_string()));
        let filter = VQueueEntryIdFilter::new(narrow_range, Some(predicate));

        assert!(filter.entry_ids.is_none());
    }

    #[test]
    fn vqueue_filter_single_stage_eq() {
        let predicate = eq(col("stage"), utf8_lit("running"));

        let filter = VQueueFilter::new(FULL_RANGE, Some(predicate));
        assert_eq!(
            filter.stages,
            Some(std::collections::BTreeSet::from([Stage::Running]))
        );
    }

    #[test]
    fn vqueue_filter_in_list() {
        let predicate = in_list("stage", vec![utf8_lit("running"), utf8_lit("paused")]);

        let filter = VQueueFilter::new(FULL_RANGE, Some(predicate));
        assert_eq!(
            filter.stages,
            Some(std::collections::BTreeSet::from([
                Stage::Running,
                Stage::Paused,
            ]))
        );
    }

    #[test]
    fn vqueue_filter_or_expression() {
        let predicate = or(
            eq(col("stage"), utf8_lit("finished")),
            eq(col("stage"), utf8_lit("inbox")),
        );

        let filter = VQueueFilter::new(FULL_RANGE, Some(predicate));
        assert_eq!(
            filter.stages,
            Some(std::collections::BTreeSet::from([
                Stage::Finished,
                Stage::Inbox,
            ]))
        );
    }

    #[test]
    fn vqueue_filter_conjunction_intersection() {
        let predicate = and(
            in_list(
                "stage",
                vec![utf8_lit("running"), utf8_lit("paused"), utf8_lit("inbox")],
            ),
            eq(col("stage"), utf8_lit("paused")),
        );

        let filter = VQueueFilter::new(FULL_RANGE, Some(predicate));
        assert_eq!(
            filter.stages,
            Some(std::collections::BTreeSet::from([Stage::Paused]))
        );
    }

    #[test]
    fn vqueue_filter_invalid_stage_falls_back() {
        let predicate = eq(col("stage"), utf8_lit("not-a-stage"));

        let filter = VQueueFilter::new(FULL_RANGE, Some(predicate));
        assert!(filter.stages.is_none());
        assert_eq!(filter.partition_keys, FULL_RANGE);
    }

    #[test]
    fn vqueue_filter_no_predicate() {
        let filter = VQueueFilter::new(FULL_RANGE, None);

        assert!(filter.stages.is_none());
        assert_eq!(filter.partition_keys, FULL_RANGE);
    }
}
