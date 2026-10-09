// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Typed primary access selected during planning and validated on worker binding.
//! SQL residuals remain independent of the selected physical read operation.

use std::ops::RangeBounds;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::logical_expr::Operator;
use datafusion::physical_plan::PhysicalExpr;

use restate_types::PartitionedResourceId;
use restate_types::identifiers::{BaseEntryId, InvocationId, PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, VQueueId};

use crate::selection::{self, Domain};

/// ID-based access supported by a source. Journal IDs bound multiple rows;
/// the other kinds identify exact primary records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryKeyKind {
    Invocation,
    /// An invocation identifies a range of journal rows, not one primary row.
    InvocationRange,
    VQueue,
    VQueueEntry,
}

impl PrimaryKeyKind {
    pub(crate) fn select(
        self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<PrimaryKeys>> {
        fn ids<T: Ord + Clone + FromStr>(
            filters: &[Arc<dyn PhysicalExpr>],
            column: &str,
        ) -> anyhow::Result<Option<Vec<T>>> {
            Ok(selection::column_domain(filters, column, |op, value| {
                if op != Operator::Eq {
                    return Ok(Domain::all());
                }
                if value.is_null() {
                    return Ok(Domain::empty());
                }
                // Failed conversion is unsupported pruning, not an empty SQL result.
                Ok(value
                    .try_as_str()
                    .flatten()
                    .and_then(|s| s.parse::<T>().ok())
                    .map_or_else(Domain::all, |id| Domain::comparison(op, id)))
            })?
            .into_values())
        }
        Ok(match self {
            Self::Invocation | Self::InvocationRange => {
                ids(filters, "id")?.map(PrimaryKeys::Invocation)
            }
            Self::VQueue => ids(filters, "id")?.map(PrimaryKeys::VQueue),
            Self::VQueueEntry => ids::<BaseEntryId>(filters, "entry_id")?
                .map(|ids| PrimaryKeys::VQueueEntry(ids.into_iter().map(EntryKey::from).collect())),
        })
    }
}

/// Inclusive storage bounds for journal rows belonging to invocations.
#[derive(Debug, Clone, Copy, bilrost::Message)]
pub struct InvocationBounds {
    #[bilrost(1)]
    pub first: InvocationId,
    #[bilrost(2)]
    pub last: InvocationId,
}

/// A fixed physical access operation. SQL predicates only filter its output.
#[derive(Debug, Clone)]
pub enum PrimaryRead {
    Range,
    InvocationRange(InvocationBounds),
    MultiGet(Arc<PrimaryKeys>),
}

/// Lossless wire representation of a vqueue primary locator. Field ordering also
/// matches BaseEntryId's storage ordering (partition, kind, remainder).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, bilrost::Message)]
pub struct EntryKey {
    #[bilrost(1)]
    partition_key: PartitionKey,
    #[bilrost(2)]
    kind: EntryKind,
    #[bilrost(tag(3), encoding(plainbytes))]
    remainder: [u8; 16],
}

impl From<BaseEntryId> for EntryKey {
    fn from(id: BaseEntryId) -> Self {
        Self {
            partition_key: id.partition_key(),
            kind: id.kind(),
            remainder: *id.as_entry_id().remainder_bytes(),
        }
    }
}

impl EntryKey {
    pub(crate) fn to_id(&self) -> anyhow::Result<BaseEntryId> {
        match self.kind {
            EntryKind::Invocation | EntryKind::StateMutation => Ok(BaseEntryId::new(
                self.partition_key,
                EntryId::new(self.kind, self.remainder),
            )),
            EntryKind::Unknown => anyhow::bail!("unknown primary entry kind"),
        }
    }
}

/// Sorted, unique keys. An empty set is valid during planning and lowers to
/// EmptyExec; an installed multi-get must always carry a nonempty validated set.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message, bilrost::Oneof)]
pub enum PrimaryKeys {
    Unknown,
    #[bilrost(1)]
    Invocation(Vec<InvocationId>),
    #[bilrost(2)]
    VQueue(Vec<VQueueId>),
    #[bilrost(3)]
    VQueueEntry(Vec<EntryKey>),
}

impl PrimaryKeys {
    pub(crate) fn kind(&self) -> Option<PrimaryKeyKind> {
        match self {
            Self::Unknown => None,
            Self::Invocation(_) => Some(PrimaryKeyKind::Invocation),
            Self::VQueue(_) => Some(PrimaryKeyKind::VQueue),
            Self::VQueueEntry(_) => Some(PrimaryKeyKind::VQueueEntry),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Unknown => 0,
            Self::Invocation(ids) => ids.len(),
            Self::VQueue(ids) => ids.len(),
            Self::VQueueEntry(ids) => ids.len(),
        }
    }

    pub(crate) fn within(&self, range: KeyRange) -> Self {
        fn subset<T: Clone>(
            ids: &[T],
            range: KeyRange,
            key: impl Fn(&T) -> PartitionKey,
        ) -> Vec<T> {
            let start = ids.partition_point(|id| key(id) < range.start());
            let end = ids.partition_point(|id| key(id) <= range.end());
            ids[start..end].to_vec()
        }
        match self {
            Self::Unknown => Self::Unknown,
            Self::Invocation(ids) => {
                Self::Invocation(subset(ids, range, WithPartitionKey::partition_key))
            }
            Self::VQueue(ids) => {
                Self::VQueue(subset(ids, range, PartitionedResourceId::partition_key))
            }
            Self::VQueueEntry(ids) => Self::VQueueEntry(subset(ids, range, |id| id.partition_key)),
        }
    }

    pub(crate) fn validate(&self, range: KeyRange) -> anyhow::Result<()> {
        fn valid<T: Ord>(ids: &[T], range: KeyRange, key: impl Fn(&T) -> PartitionKey) -> bool {
            !ids.is_empty()
                && ids.windows(2).all(|pair| pair[0] < pair[1])
                && ids.iter().all(|id| range.contains(&key(id)))
        }
        let valid = match self {
            Self::Unknown => false,
            Self::Invocation(ids) => valid(ids, range, WithPartitionKey::partition_key),
            Self::VQueue(ids) => valid(ids, range, PartitionedResourceId::partition_key),
            Self::VQueueEntry(ids) => {
                valid(ids, range, |id| id.partition_key)
                    && ids.iter().all(|id| id.kind != EntryKind::Unknown)
            }
        };
        anyhow::ensure!(
            valid,
            "invalid primary key set: expected nonempty, sorted, unique keys within the selected range"
        );
        Ok(())
    }
}
