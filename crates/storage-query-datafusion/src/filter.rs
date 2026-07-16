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
use std::ops::RangeBounds;
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
use restate_types::vqueues::{VQueueEntryId, VQueueId};

use crate::partition_store_scanner::ScanLocalPartitionFilter;

pub trait PartitionKeyExtractor: Send + Sync + 'static + Debug {
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>>;
}

#[derive(Debug)]
pub struct FirstMatchingPartitionKeyExtractor {
    extractors: Vec<PartitionKeyExtractorEntry>,
}

#[derive(Debug)]
struct PartitionKeyExtractorEntry {
    extractor: Box<dyn PartitionKeyExtractor>,
    fanout: PointReadFanout,
}

/// Controls how selected partition keys are mapped to physical scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PointReadFanout {
    /// Creates one scan per selected partition key.
    PerKey,
    /// Groups selected keys into one scan per Restate partition.
    PerPartition,
}

/// The partition keys and fanout produced by the first matching extractor.
#[derive(Debug)]
pub(crate) struct PartitionKeySelection {
    pub(crate) keys: BTreeSet<PartitionKey>,
    pub(crate) fanout: PointReadFanout,
}

impl Default for FirstMatchingPartitionKeyExtractor {
    fn default() -> Self {
        let extractors = vec![PartitionKeyExtractorEntry {
            extractor: Box::new(MatchingColumnExtractor::new(
                "partition_key",
                |value: &ScalarValue| match value {
                    ScalarValue::UInt64(Some(v)) => Ok(*v),
                    _ => anyhow::bail!("expected UInt64 partition key"),
                },
            )),
            fanout: PointReadFanout::PerKey,
        }];
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
        self.append(Self::create_partitioned_resource_id_extractor::<T>(
            column_name,
        ))
    }

    /// Adds a partitioned-resource-id extractor whose matches are grouped by Restate partition.
    pub fn with_grouped_partitioned_resource_id<T>(self, column_name: impl Into<String>) -> Self
    where
        T: PartitionedResourceId + ResourceId + FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        self.append_with_fanout(
            Self::create_partitioned_resource_id_extractor::<T>(column_name),
            PointReadFanout::PerPartition,
        )
    }

    fn create_partitioned_resource_id_extractor<T>(
        column_name: impl Into<String>,
    ) -> impl PartitionKeyExtractor
    where
        T: PartitionedResourceId + ResourceId + FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .with_context(|| format!("expected string {:?}", T::RESOURCE_TYPE))?
                .context("null values cannot be used for partition-key matching")?;
            let resource =
                T::from_str(value).with_context(|| format!("non valid {:?}", T::RESOURCE_TYPE))?;
            Ok(resource.partition_key())
        })
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
        self.append(Self::create_invocation_id_partition_key_extractor(
            column_name,
        ))
    }

    /// Adds an invocation-id extractor whose matches are grouped into a single
    /// scan per Restate partition.
    ///
    /// Only use this when the table's scanner re-fetches each id exactly via an
    /// exact-id filter; range-scanning tables would read every intermediate key.
    pub fn with_grouped_invocation_id(self, column_name: impl Into<String>) -> Self {
        self.append_with_fanout(
            Self::create_invocation_id_partition_key_extractor(column_name),
            PointReadFanout::PerPartition,
        )
    }

    fn create_invocation_id_partition_key_extractor(
        column_name: impl Into<String>,
    ) -> MatchingColumnExtractor<fn(&ScalarValue) -> anyhow::Result<PartitionKey>> {
        MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected string invocation id")?
                .context("unexpected null invocation id")?;
            let invocation_id = InvocationId::from_str(value).context("non valid invocation id")?;
            Ok(invocation_id.partition_key())
        })
    }

    pub fn with_vqueue_entry_id(self, column_name: impl Into<String>) -> Self {
        self.append(Self::create_vqueue_entry_id_partition_key_extractor(
            column_name,
        ))
    }

    /// Adds a vqueue-entry-id extractor whose matches are grouped into a single
    /// scan per Restate partition.
    ///
    /// Only use this when the table's scanner re-fetches each entry id exactly
    /// via an exact-id filter; range-scanning tables would read every
    /// intermediate key.
    pub fn with_grouped_vqueue_entry_id(self, column_name: impl Into<String>) -> Self {
        self.append_with_fanout(
            Self::create_vqueue_entry_id_partition_key_extractor(column_name),
            PointReadFanout::PerPartition,
        )
    }

    fn create_vqueue_entry_id_partition_key_extractor(
        column_name: impl Into<String>,
    ) -> MatchingColumnExtractor<fn(&ScalarValue) -> anyhow::Result<PartitionKey>> {
        MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected string entry id")?
                .context("unexpected null entry id")?;

            VQueueEntryId::extract_partition_key(value)
                .map_err(|_| anyhow::anyhow!("non valid entry id"))
        })
    }

    pub fn append(self, extractor: impl PartitionKeyExtractor) -> Self {
        self.append_with_fanout(extractor, PointReadFanout::PerKey)
    }

    fn append_with_fanout(
        mut self,
        extractor: impl PartitionKeyExtractor,
        fanout: PointReadFanout,
    ) -> Self {
        self.extractors.push(PartitionKeyExtractorEntry {
            extractor: Box::new(extractor),
            fanout,
        });
        self
    }

    pub(crate) fn try_extract_selection(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<PartitionKeySelection>> {
        for entry in &self.extractors {
            if let Some(keys) = entry.extractor.try_extract(filters)? {
                return Ok(Some(PartitionKeySelection {
                    keys,
                    fanout: entry.fanout,
                }));
            }
        }

        Ok(None)
    }
}

impl PartitionKeyExtractor for FirstMatchingPartitionKeyExtractor {
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        Ok(self
            .try_extract_selection(filters)?
            .map(|selection| selection.keys))
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

            // A negated list (`NOT IN`/`!=`) enumerates excluded values, so it
            // cannot narrow the partition scan.
            if inlist.col.name() != self.column_name || inlist.negated {
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
    pub entry_ids: Option<IdSelection<VQueueEntryId>>,
}

impl ScanLocalPartitionFilter for VQueueFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        let mut stages: Option<BTreeSet<Stage>> = None;
        let mut entry_ids = None;

        if let Some(predicate) = predicate
            && let Ok(predicate) = snapshot_physical_expr(predicate)
        {
            for conjunct in split_conjunction(&predicate) {
                if let Some(conjunct_stages) = parse_vqueue_stages("stage", conjunct) {
                    stages = Some(match stages {
                        Some(current) => current.intersection(&conjunct_stages).copied().collect(),
                        None => conjunct_stages,
                    });
                }

                entry_ids = entry_ids.or_else(|| {
                    parse_id_selection("entry_id", range, conjunct, VQueueEntryId::partition_key)
                });
            }
        }

        Self {
            partition_keys: range,
            stages,
            entry_ids,
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
pub struct IdSelection<T> {
    pub ids: BTreeSet<T>,
}

impl<T: Ord + Clone> IdSelection<T> {
    /// The inclusive `[min, max]` span the selected IDs cover. Used by consumers
    /// that can only range-scan, while point-lookup tables consume the exact set.
    pub fn bounds(&self) -> (T, T) {
        (
            self.ids.first().expect("selection is never empty").clone(),
            self.ids.last().expect("selection is never empty").clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct InvocationIdFilter {
    pub partition_keys: KeyRange,
    pub invocation_ids: Option<IdSelection<InvocationId>>,
}

impl ScanLocalPartitionFilter for InvocationIdFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        if let Some(predicate) = predicate
            && let Ok(predicate) = snapshot_physical_expr(predicate)
        {
            for conjunct in split_conjunction(&predicate) {
                if let Some(invocation_ids) =
                    parse_id_selection("id", range, conjunct, |id: &InvocationId| {
                        id.partition_key()
                    })
                {
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

fn parse_id_selection<T>(
    column_name: &str,
    range: KeyRange,
    predicate: &Arc<dyn PhysicalExpr>,
    partition_key_of: impl Fn(&T) -> PartitionKey,
) -> Option<IdSelection<T>>
where
    T: FromStr + Ord,
{
    let in_list = InList::parse(predicate, 5)?;

    // A negated list (`NOT IN`/`!=`) enumerates excluded IDs; using it to build
    // a lookup set would fetch exactly the rows that must be filtered out.
    if in_list.col.name() != column_name || in_list.negated {
        return None;
    }

    let mut ids = BTreeSet::new();
    for literal in in_list.list {
        let str = literal.try_as_str()??;
        let id = T::from_str(str).ok()?;

        if range.contains(&partition_key_of(&id)) {
            ids.insert(id);
        }
    }

    if ids.is_empty() {
        None
    } else {
        Some(IdSelection { ids })
    }
}

#[derive(Debug, Clone)]
pub struct VQueueEntryIdFilter {
    pub partition_keys: KeyRange,
    pub entry_ids: Option<IdSelection<VQueueEntryId>>,
}

impl ScanLocalPartitionFilter for VQueueEntryIdFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        if let Some(predicate) = predicate
            && let Ok(predicate) = snapshot_physical_expr(predicate)
        {
            for conjunct in split_conjunction(&predicate) {
                if let Some(entry_ids) =
                    parse_id_selection("entry_id", range, conjunct, VQueueEntryId::partition_key)
                {
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

/// Each vqueue id maps to exactly one metadata row, so an `id = / IN (...)`
/// predicate is served via a batched multi-get (the `Set`) instead of a full
/// partition-key-range scan. `VQueueId` is not `Copy`, but `IdSelection` only
/// requires `Ord + Clone`.
#[derive(Debug, Clone)]
pub struct VQueueMetaFilter {
    pub partition_keys: KeyRange,
    pub ids: Option<IdSelection<VQueueId>>,
}

impl ScanLocalPartitionFilter for VQueueMetaFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        if let Some(predicate) = predicate
            && let Ok(predicate) = snapshot_physical_expr(predicate)
        {
            for conjunct in split_conjunction(&predicate) {
                if let Some(ids) =
                    parse_id_selection("id", range, conjunct, |id: &VQueueId| id.partition_key())
                {
                    return Self {
                        partition_keys: range,
                        ids: Some(ids),
                    };
                }
            }
        }

        Self {
            partition_keys: range,
            ids: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
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
    use restate_types::vqueues::{VQueueEntryId, VQueueId};

    use crate::filter::{
        FirstMatchingPartitionKeyExtractor, InvocationIdFilter, PartitionKeyExtractor,
        VQueueEntryIdFilter, VQueueFilter, VQueueMetaFilter,
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
        make_in_list(col_name, list, false)
    }

    fn not_in_list(col_name: &str, list: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        make_in_list(col_name, list, true)
    }

    fn make_in_list(
        col_name: &str,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
    ) -> Arc<dyn PhysicalExpr> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![Field::new(col_name, DataType::LargeUtf8, true)]);
        Arc::new(InListExpr::try_new(col(col_name), list, negated, &schema).expect("valid in-list"))
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

        let selection = filter.invocation_ids.expect("should extract selection");
        assert_eq!(selection.ids, BTreeSet::from([id]));
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

        let selection = filter.invocation_ids.expect("should extract selection");
        assert_eq!(selection.ids, BTreeSet::from([id1, id2]));
    }

    #[test]
    fn invocation_id_filter_keeps_large_in_list_as_set() {
        let invocation_ids = (0..501)
            .map(|id| make_invocation_id(&format!("key-{id}")))
            .collect::<Vec<_>>();
        let predicate = in_list(
            "id",
            invocation_ids
                .iter()
                .map(|id| utf8_lit(id.to_string()))
                .collect(),
        );

        let filter = InvocationIdFilter::new(FULL_RANGE, Some(predicate));

        let selection = filter.invocation_ids.expect("should extract selection");
        assert_eq!(selection.ids.len(), invocation_ids.len());
        for invocation_id in invocation_ids {
            assert!(selection.ids.contains(&invocation_id));
        }
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

        let selection = filter
            .invocation_ids
            .expect("should extract from conjunction");
        assert_eq!(selection.ids, BTreeSet::from([id]));
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
    fn partition_key_extractor_rejects_negated_in_list() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let got = extractor
            .try_extract(&[not_in_list("service_key", vec![utf8_lit("key-1")])])
            .expect("extract");

        assert_eq!(None, got);
    }

    #[test]
    fn invocation_id_filter_rejects_negated_in_list() {
        let id = make_invocation_id("key-1");
        let filter = InvocationIdFilter::new(
            FULL_RANGE,
            Some(not_in_list("id", vec![utf8_lit(id.to_string())])),
        );

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
        let selection = filter.entry_ids.expect("should extract entry-id set");
        assert_eq!(selection.ids.len(), 2);
        assert!(selection.ids.contains(&expected1));
        assert!(selection.ids.contains(&expected2));
    }

    #[test]
    fn vqueue_entry_id_filter_keeps_large_in_list_as_set() {
        let invocation_ids = (0..501)
            .map(|id| make_invocation_id(&format!("key-{id}")))
            .collect::<Vec<_>>();
        let predicate = in_list(
            "entry_id",
            invocation_ids
                .iter()
                .map(|id| utf8_lit(id.to_string()))
                .collect(),
        );

        let filter = VQueueEntryIdFilter::new(FULL_RANGE, Some(predicate));

        let selection = filter.entry_ids.expect("should extract entry-id set");
        assert_eq!(selection.ids.len(), invocation_ids.len());
        for invocation_id in invocation_ids {
            let expected = VQueueEntryId::from_str(&invocation_id.to_string()).unwrap();
            assert!(selection.ids.contains(&expected));
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
    fn vqueue_entry_id_filter_rejects_negated_in_list() {
        let id = make_invocation_id("key-1");
        let filter = VQueueEntryIdFilter::new(
            FULL_RANGE,
            Some(not_in_list("entry_id", vec![utf8_lit(id.to_string())])),
        );

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
        assert!(filter.entry_ids.is_none());
        assert_eq!(filter.partition_keys, FULL_RANGE);
    }

    #[test]
    fn vqueue_filter_extracts_entry_ids_and_rejects_negated_list() {
        let id1 = make_invocation_id("key-1");
        let id2 = make_invocation_id("key-2");
        let filter = VQueueFilter::new(
            FULL_RANGE,
            Some(and(
                in_list(
                    "entry_id",
                    vec![utf8_lit(id1.to_string()), utf8_lit(id2.to_string())],
                ),
                eq(col("stage"), utf8_lit("running")),
            )),
        );

        assert_eq!(
            filter.entry_ids.expect("should extract entry ids").ids,
            BTreeSet::from([
                VQueueEntryId::from_str(&id1.to_string()).unwrap(),
                VQueueEntryId::from_str(&id2.to_string()).unwrap(),
            ])
        );
        assert_eq!(filter.stages, Some(BTreeSet::from([Stage::Running])));

        let filter = VQueueFilter::new(
            FULL_RANGE,
            Some(not_in_list("entry_id", vec![utf8_lit(id1.to_string())])),
        );
        assert!(filter.entry_ids.is_none());
    }

    #[test]
    fn vqueue_meta_filter_set_and_rejections() {
        let id1 = VQueueId::custom(1, "q1");
        let id2 = VQueueId::custom(2, "q2");

        // `id = / IN (...)` yields an exact set served via multi-get.
        let filter = VQueueMetaFilter::new(
            FULL_RANGE,
            Some(in_list(
                "id",
                vec![utf8_lit(id1.to_string()), utf8_lit(id2.to_string())],
            )),
        );
        let selection = filter.ids.expect("should extract vqueue-id set");
        assert_eq!(selection.ids, BTreeSet::from([id1.clone(), id2]));

        // No predicate and a negated list both fall back to a range scan.
        assert!(VQueueMetaFilter::new(FULL_RANGE, None).ids.is_none());
        assert!(
            VQueueMetaFilter::new(
                FULL_RANGE,
                Some(not_in_list("id", vec![utf8_lit(id1.to_string())])),
            )
            .ids
            .is_none()
        );
    }

    #[test]
    fn vqueue_meta_filter_excludes_out_of_range() {
        let id = VQueueId::custom(1, "q1");
        let pk = id.partition_key();
        let narrow_range = if pk > 0 {
            KeyRange::new(0, pk - 1)
        } else {
            KeyRange::new(1, 1)
        };

        let filter =
            VQueueMetaFilter::new(narrow_range, Some(eq(col("id"), utf8_lit(id.to_string()))));
        assert!(filter.ids.is_none());
    }

    #[test]
    fn vqueue_meta_filter_keeps_large_in_list_as_set() {
        let ids = (0..501)
            .map(|id| VQueueId::custom(id, format!("q{id}")))
            .collect::<Vec<_>>();
        let predicate = in_list(
            "id",
            ids.iter().map(|id| utf8_lit(id.to_string())).collect(),
        );

        let filter = VQueueMetaFilter::new(FULL_RANGE, Some(predicate));

        let selection = filter.ids.expect("should extract vqueue-id set");
        assert_eq!(selection.ids.len(), ids.len());
        for id in ids {
            assert!(selection.ids.contains(&id));
        }
    }
}
