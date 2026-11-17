// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Source-owned primary access. The engine groups complete keys by partition;
//! their representation and validation belong to the selected physical source.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::physical_plan::PhysicalExpr;

use restate_partition_store::lookup::{LookupBinding, LookupCandidates, LookupTable};
use restate_storage_query_api::schema::index::{IndexSchema, PrimarySchema, index_restriction};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::static_conjuncts;
use restate_storage_query_api::{LogicalSchema, SelectionSchema};
use restate_types::sharding::KeyRange;
use restate_util_string::ReString;

use crate::source_registry::PartitionSourceDescriptor;

pub(crate) type SelectPrimary =
    fn(&[Arc<dyn PhysicalExpr>]) -> anyhow::Result<Option<Box<dyn PrimarySelection>>>;

/// An index holding a complete reference for every row of its primary, so that
/// reading it can replace a scan of the primary, together with the planner's
/// policy for when to do so.
pub trait CompleteIndex: 'static {
    type Index: IndexSchema;

    /// Whether the restriction a query places on the index (see
    /// [`index_restriction`]) makes reading the index worthwhile. Proven-empty
    /// restrictions never reach this policy: the primary scan reads nothing too.
    fn useful(restriction: &<Self::Index as SelectionSchema>::Domains) -> bool;
}

/// A complete index declared on a source whose logical schema is `P`.
///
/// [`Self::of`] is the only constructor. It derives the identity sent with index
/// work, the restriction analysis and the policy from one [`CompleteIndex`], so
/// they cannot disagree, and it requires `P` to be that index's primary schema:
///
/// ```
/// use datafusion::arrow::datatypes::DataType;
/// use restate_distributed_datafusion::access::{CompleteIndex, IndexAccess};
/// use restate_storage_query_api::schema::index::PrimarySchema;
/// use restate_storage_query_api::tables::index::{EntryByQueue, EntryByQueueDomains};
/// use restate_storage_query_api::tables::vqueue_entry_status::VqueueEntryStatus;
/// use restate_storage_query_api::{ShardingScheme, define_schema};
/// use restate_types::identifiers::InvocationId;
///
/// struct Queues;
/// impl CompleteIndex for Queues {
///     type Index = EntryByQueue;
///     fn useful(restriction: &EntryByQueueDomains) -> bool {
///         restriction.vqueue_id.to_predicate().is_some()
///     }
/// }
/// const QUEUES: IndexAccess<VqueueEntryStatus> = IndexAccess::of::<Queues>();
///
/// // Another primary schema, used below.
/// define_schema! {
///     invocations {
///         name: "invocations",
///         sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
///         sharding_columns { id: InvocationId },
///         primary_key: id,
///         columns { id: InvocationId => DataType::LargeUtf8 }
///     }
/// }
/// fn primary<P: PrimarySchema>() {}
/// primary::<Invocations>();
/// ```
///
/// Declaring the index on that other primary does not compile:
///
/// ```compile_fail
/// # use datafusion::arrow::datatypes::DataType;
/// # use restate_distributed_datafusion::access::{CompleteIndex, IndexAccess};
/// # use restate_storage_query_api::tables::index::{EntryByQueue, EntryByQueueDomains};
/// # use restate_storage_query_api::{ShardingScheme, define_schema};
/// # use restate_types::identifiers::InvocationId;
/// # struct Queues;
/// # impl CompleteIndex for Queues {
/// #     type Index = EntryByQueue;
/// #     fn useful(restriction: &EntryByQueueDomains) -> bool {
/// #         restriction.vqueue_id.to_predicate().is_some()
/// #     }
/// # }
/// # define_schema! {
/// #     invocations {
/// #         name: "invocations",
/// #         sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
/// #         sharding_columns { id: InvocationId },
/// #         primary_key: id,
/// #         columns { id: InvocationId => DataType::LargeUtf8 }
/// #     }
/// # }
/// const QUEUES: IndexAccess<Invocations> = IndexAccess::of::<Queues>();
/// ```
pub struct IndexAccess<P> {
    declared: DeclaredIndex,
    primary: PhantomData<fn() -> P>,
}

impl<P> IndexAccess<P> {
    pub const fn of<B>() -> Self
    where
        P: PrimarySchema,
        B: CompleteIndex,
        B::Index: IndexSchema<Primary = P>,
    {
        Self {
            declared: DeclaredIndex {
                identity: <B::Index as LogicalSchema>::NAME,
                select: select::<B>,
            },
            primary: PhantomData,
        }
    }

    /// The declaration once its source is registered.
    pub(crate) fn declared(&self) -> DeclaredIndex {
        self.declared
    }
}

fn select<B: CompleteIndex>(filters: &[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool> {
    Ok(index_restriction::<B::Index>(filters)?.is_some_and(|restriction| B::useful(&restriction)))
}

/// A declared index of a registered source.
#[derive(Clone, Copy)]
pub(crate) struct DeclaredIndex {
    identity: &'static str,
    select: fn(&[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool>,
}

impl DeclaredIndex {
    /// The index schema's name, sent with index work.
    pub(crate) fn identity(&self) -> &'static str {
        self.identity
    }

    /// Whether the planner reads the index for `filters`.
    pub(crate) fn select(&self, filters: &[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool> {
        (self.select)(filters)
    }
}

/// The access path chosen for one source before distribution.
#[derive(Debug)]
pub(crate) enum Access {
    Scan,
    Primary(Box<dyn PrimarySelection>),
    Index(ReString),
}

/// One selector for ordinary and shard-JOIN inputs: complete primary keys first,
/// then the first declared index whose restriction is useful, else a scan.
///
/// Declared indexes are complete and always maintained, so selecting one needs
/// no opt-in. It doesn't depend on whether the session shows the index's table
/// either: the read goes through the primary table's source.
pub(crate) fn select_access(
    source: &PartitionSourceDescriptor,
    predicate: Option<&Arc<dyn PhysicalExpr>>,
) -> anyhow::Result<Access> {
    let filters = predicate.map(std::slice::from_ref).unwrap_or_default();
    if let Some(select) = source.primary_access
        && let Some(keys) = select(filters)?
    {
        return Ok(Access::Primary(keys));
    }
    for index in &source.indexes {
        if index.select(filters)? {
            return Ok(Access::Index(ReString::from_static(index.identity())));
        }
    }
    Ok(Access::Scan)
}

/// A complete finite set of candidate primary keys, including the empty set.
pub(crate) trait PrimarySelection: Debug + Send + Sync {
    /// Encodes the nonempty subset owned by this partition. Called during planning.
    fn within(&self, ownership: KeyRange) -> Option<PrimaryKeys>;
}

/// Complete-key lookups for a binding that supports them: the IDs a query can
/// match, when they form a finite set across all of its alternatives and every
/// one of them names a stored key. Otherwise the source is scanned: when
/// `into_values` is `None`, as for a surviving range, or when some ID can't name a
/// key, such as an invalid scope. Dynamic filters change during execution, so
/// only static conjuncts count.
pub(crate) fn select_lookup<B: LookupBinding>(
    filters: &[Arc<dyn PhysicalExpr>],
) -> anyhow::Result<Option<Box<dyn PrimarySelection>>> {
    let filters = static_conjuncts(filters);
    let Some(ids) =
        <B::Schema as SelectionSchema>::analyze(&filters, NonZeroUsize::new(16).unwrap())?
            .iter()
            .map(B::lookup_ids)
            .fold(Domain::empty(), Domain::union)
            .into_values()
    else {
        return Ok(None);
    };
    Ok(LookupCandidates::<B::Table>::prepare(&ids)
        .map(|keys| Box::new(KeyLookup(keys)) as Box<dyn PrimarySelection>))
}

/// The stored keys of a lookup, prepared once; each partition is sent its own.
#[derive(derive_more::Debug)]
struct KeyLookup<T: LookupTable>(LookupCandidates<T>);

impl<T: LookupTable> PrimarySelection for KeyLookup<T> {
    fn within(&self, ownership: KeyRange) -> Option<PrimaryKeys> {
        self.0
            .within(ownership)
            .map(|(count, encoded)| PrimaryKeys {
                count: count as u64,
                encoded,
            })
    }
}

/// Source-specific encoded keys. Workers validate the payload and cardinality
/// against the addressed source before installing a read.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct PrimaryKeys {
    #[bilrost(1)]
    pub(crate) count: u64,
    #[bilrost(tag(2), encoding(plainbytes))]
    pub(crate) encoded: Vec<u8>,
}

impl PrimaryKeys {
    pub(crate) fn len(&self) -> usize {
        self.count as usize
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.count > 0 && usize::try_from(self.count).is_ok() && !self.encoded.is_empty(),
            "invalid primary key set"
        );
        Ok(())
    }
}

/// Scans prepare navigation from the predicate. Lookups carry the selected
/// source's complete keys; native operators retain SQL residual evaluation.
/// Index reads name a declared index; the scanner derives its restriction from
/// the predicate and never chooses another access path.
#[derive(Debug, Clone)]
pub enum PrimaryRead {
    Scan,
    MultiGet(Arc<PrimaryKeys>),
    Index(ReString),
}

#[cfg(test)]
mod tests {
    use restate_partition_store::vqueue_table::VQueueMetaTable;
    use restate_types::vqueues::VQueueId;

    use super::*;

    /// Each partition is sent its own keys and nothing else, a partition without
    /// keys is sent nothing, and duplicate IDs give one key.
    #[test]
    fn lookups_send_each_partition_its_own_keys() {
        use restate_partition_store::lookup::LookupKeys;

        let ids: Vec<_> = [5, 50, 500].map(|key| VQueueId::custom(key, "q")).into();
        let lookup = KeyLookup(LookupCandidates::<VQueueMetaTable>::prepare(&ids).unwrap());
        let sent = |range| {
            lookup.within(range).map(|keys| {
                let decoded =
                    LookupKeys::<VQueueMetaTable>::decode(keys.len(), &keys.encoded).unwrap();
                (keys.len(), decoded.outside(range))
            })
        };
        assert_eq!(sent(KeyRange::new(0, 99)), Some((2, false)));
        assert_eq!(sent(KeyRange::new(100, 1000)), Some((1, false)));
        assert_eq!(sent(KeyRange::new(1001, 2000)), None);
        // Duplicate IDs give one key.
        let twice: Vec<_> = ids.iter().chain(&ids).cloned().collect();
        let deduplicated = LookupCandidates::<VQueueMetaTable>::prepare(&twice).unwrap();
        assert_eq!(deduplicated.within(KeyRange::FULL).unwrap().0, 3);
    }
}
