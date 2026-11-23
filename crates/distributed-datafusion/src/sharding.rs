// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! V2 source declarations and resolution of candidate shard keys to partitions.
//!
//! Vocabulary (shard key, shard, shard owner, candidate shard keys, resolution)
//! is defined in [`restate_storage_query_api::schema::sharding`]. Resolution here
//! is independent of access planning and replica placement: it decides which
//! partitions may hold matching rows, not which node serves them.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::physical_plan::PhysicalExpr;

use restate_storage_query_api::ordering::NativeOrder;
use restate_storage_query_api::schema::sharding::ShardingColumn;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::{LogicalSchema, SelectionSchema, ShardingScheme};
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::partition_table::Partition;
use restate_util_string::ReString;

use crate::access::{IndexAccess, SelectPrimary};

/// A V2 source's logical schema and stable identity, independent of SQL aliases.
/// Multiple physical sources may expose the same logical schema.
pub trait QuerySource: Send + Sync + Clone + Debug + 'static {
    type Schema: LogicalSchema;

    /// Must be unique across sources and consistent on every node. Sources with
    /// distinct physical layouts override the default logical-schema identity.
    fn identity() -> ReString {
        ReString::from_static(Self::Schema::NAME)
    }
}

/// Physical access capabilities supplied by the V2 binding.
pub(crate) trait PartitionSource: QuerySource {
    const PRIMARY_ACCESS: Option<SelectPrimary> = None;
    /// Complete indexes that can replace a scan of this source.
    const INDEXES: &'static [IndexAccess<Self::Schema>] = &[];
    /// The orders the source's key-ordered reads deliver within a partition: its
    /// primary-key scans and its lookups of complete keys. A source over a storage
    /// table declares the table's claim ([`Table::NATIVE_ORDERS`]); a live source
    /// states its own. Each column term must name a column of the schema.
    ///
    /// [`Table::NATIVE_ORDERS`]: restate_storage_query_api::Table::NATIVE_ORDERS
    const ORDERS: &'static [NativeOrder] = &[];
    /// The column rendering the physical record's ID, which the function terms of
    /// [`Self::ORDERS`] take: a sharding column each of their functions applies to.
    const ID_COLUMN: Option<<Self::Schema as SelectionSchema>::Column> = None;

    fn requires_leader() -> bool {
        match Self::Schema::SHARDING {
            ShardingScheme::ByPartitionId { is_leader_required }
            | ShardingScheme::ByPartitionKey { is_leader_required } => is_leader_required,
            _ => unreachable!("partition sources require a partition-sharded declaration"),
        }
    }

    /// Whether this physical layout can contribute rows to this selection.
    fn can_read(_filters: &[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool> {
        Ok(true)
    }
}

/// Sharding columns of `S` at their ordinals in `S::arrow_schema()`. Columns
/// without an Arrow representation never appear in a plan, so they are omitted.
pub(crate) fn exposed_sharding_columns<S: LogicalSchema>() -> Vec<ShardingColumn<usize>> {
    S::SHARDING_COLUMNS
        .iter()
        .filter_map(|declared| {
            let column = S::sql_index(declared.column)?;
            debug_assert_eq!(
                Some(S::arrow_schema().field(column).name().as_str()),
                S::sql_name(declared.column),
                "an Arrow ordinal must expose its own column"
            );
            Some(ShardingColumn {
                column,
                kind: declared.kind,
                nullable: declared.nullable,
            })
        })
        .collect()
}

/// Remaps sharding columns through a provider projection and drops omitted ones,
/// so that a sharding column can never lend its identity to another ordinal.
pub(crate) fn project_sharding_columns(
    columns: &[ShardingColumn<usize>],
    projection: Option<&[usize]>,
) -> Vec<ShardingColumn<usize>> {
    let Some(projection) = projection else {
        return columns.to_vec();
    };
    columns
        .iter()
        .filter_map(|declared| {
            Some(ShardingColumn {
                column: projection.iter().position(|&i| i == declared.column)?,
                ..*declared
            })
        })
        .collect()
}

/// Candidate shard keys of a partition-sharded schema, erased over its key space
/// so that one provider serves both partition schemes.
///
/// For example, candidate keys `{0x9c3a…}` keep only the partition whose key range
/// contains `0x9c3a…`, while candidate IDs `{2, 5}` keep partitions 2 and 5.
#[derive(Debug)]
pub(crate) enum PartitionCandidates {
    /// Shard key: partition ID (`ShardingScheme::ByPartitionId`).
    Ids(Domain<PartitionId>),
    /// Shard key: partition key (`ShardingScheme::ByPartitionKey`).
    Keys(Domain<PartitionKey>),
}

impl PartitionCandidates {
    /// Resolution against one topology snapshot: the partitions that may hold a
    /// candidate shard key, in snapshot order.
    pub(crate) fn resolve(
        &self,
        topology: &[(PartitionId, Partition)],
    ) -> Vec<(PartitionId, Partition)> {
        topology
            .iter()
            .filter(|(id, partition)| self.contains(*id, partition))
            .cloned()
            .collect()
    }

    /// Resolution against one partition-table entry: whether partition `id`, with
    /// its current key range, may hold a candidate shard key.
    pub(crate) fn contains(&self, id: PartitionId, partition: &Partition) -> bool {
        match self {
            Self::Ids(ids) => ids.contains(&id),
            Self::Keys(keys) => crate::selection::key_ranges(keys, partition.key_range)
                .next()
                .is_some(),
        }
    }
}

impl From<Domain<PartitionId>> for PartitionCandidates {
    fn from(ids: Domain<PartitionId>) -> Self {
        Self::Ids(ids)
    }
}

impl From<Domain<PartitionKey>> for PartitionCandidates {
    fn from(keys: Domain<PartitionKey>) -> Self {
        Self::Keys(keys)
    }
}
