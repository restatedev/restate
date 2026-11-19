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

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{physical_exprs_bag_equal, split_conjunction};
use datafusion::physical_plan::PhysicalExpr;

use restate_partition_store::lookup::{LookupBinding, LookupCandidates, LookupTable};
use restate_storage_query_api::scan::ReadDirection;
use restate_storage_query_api::schema::index::{IndexSchema, PrimarySchema, index_restriction};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::static_conjuncts;
use restate_storage_query_api::{LogicalSchema, SelectionSchema};
use restate_types::SemanticRestateVersion;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::sharding::KeyRange;
use restate_util_string::ReString;

use crate::distributed::demand::Demand;
use crate::source_registry::PartitionSourceDescriptor;

pub(crate) type SelectPrimary =
    fn(&[Arc<dyn PhysicalExpr>]) -> anyhow::Result<Option<Box<dyn PrimarySelection>>>;

/// An index holding a complete reference for every row of its primary, so that
/// reading it can replace a scan of the primary, together with the planner's
/// policy for when to do so.
pub trait CompleteIndex: 'static {
    type Index: IndexSchema;

    /// The Restate version that introduced reading this index for its primary.
    /// A worker of an older version can't serve the index work, so the
    /// coordinator chooses the index only where every worker runs at least this
    /// version ([`workers_know`]). For an index of
    /// the partition store's registry, its definition's version.
    const INTRODUCED: SemanticRestateVersion;

    /// The order in which every read of the index returns a partition's rows,
    /// by columns of the primary's SQL view, whatever the restriction. The
    /// planner can then merge partitions instead of sorting them, and stop early
    /// under a LIMIT.
    const ORDERING: &'static [IndexOrder<PrimaryColumn<Self>>] = &[];

    /// Whether the restriction a query places on the index (see
    /// [`index_restriction`]) makes reading the index worthwhile. Proven-empty
    /// restrictions never reach this policy: the primary scan reads nothing too.
    fn useful(restriction: &<Self::Index as SelectionSchema>::Domains) -> bool;

    /// The primary columns a read of the index produces from what the index
    /// itself holds, its keys and the values stored with them, which always
    /// equal the primary record's. A query that needs no other column, its
    /// conditions' included, reads the index alone without fetching a primary
    /// record: a covered read ([`covers`]).
    const COVERS: &'static [PrimaryColumn<Self>] = &[];

    /// Whether a covered read pays off for `restriction`: as [`Self::useful`]
    /// says, unless the whole index costs less to read than the primary's scan,
    /// so that any restriction does.
    fn useful_covered(restriction: &<Self::Index as SelectionSchema>::Domains) -> bool {
        Self::useful(restriction)
    }

    /// Whether the index holds every row the query can return. An index of
    /// every row of its primary does. A partial index, which holds only some,
    /// admits a query only if each of its Boolean alternatives, analyzed on the
    /// primary before they are unioned for the index's restriction, proves that
    /// its rows are members.
    fn admits(_alternatives: &[<PrimaryOf<Self> as SelectionSchema>::Domains]) -> bool {
        true
    }

    /// Whether the index pays off only for a query that wants the first rows in
    /// exactly its declared [`Self::ORDERING`] or its exact reverse: a bounded
    /// sort above the source whose whole order is one of them, null placement
    /// included where a column can be NULL ([`IndexOrder::never_null`]). The
    /// read, forward or backward, then stops early. Without such a demand,
    /// reading every candidate and its primary record can cost more than the
    /// scan it replaces.
    const NEEDS_ORDERED_DEMAND: bool = false;

    /// Whether the index's policy admits a condition the query applies to the
    /// rows beyond the index's restriction. A selective condition the index
    /// can't navigate makes the read visit many candidates in vain, so a policy
    /// admits only the shapes it chooses to trust: a syntactic choice, not a
    /// proof that the read pays off. Consulted only with
    /// [`Self::NEEDS_ORDERED_DEMAND`].
    fn residual_allowed(_conjunct: &Arc<dyn PhysicalExpr>) -> bool {
        true
    }
}

type PrimaryOf<B> = <<B as CompleteIndex>::Index as IndexSchema>::Primary;
type PrimaryColumn<B> = <PrimaryOf<B> as SelectionSchema>::Column;

/// One column of an index read's declared order.
#[derive(Debug, Clone, Copy)]
pub struct IndexOrder<C> {
    pub column: C,
    pub options: SortOptions,
    /// Whether the source sets this column on every row it returns, through
    /// the index and when its partition scans instead. Then where a query puts
    /// NULLs changes nothing: `created_at DESC`, which is `NULLS FIRST`, and
    /// `created_at DESC NULLS LAST` read the index alike, and the read's rows
    /// are in the order the query asked for. Proven by the source's row
    /// writer, not by its schema: every generated column is nullable.
    pub never_null: bool,
}

/// One column of a declared order, by its SQL name, sent with index work.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub(crate) struct SortedColumn {
    #[bilrost(1)]
    pub(crate) name: String,
    #[bilrost(2)]
    pub(crate) descending: bool,
    #[bilrost(3)]
    pub(crate) nulls_first: bool,
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
/// use restate_types::SemanticRestateVersion;
/// use restate_types::identifiers::InvocationId;
///
/// struct Queues;
/// impl CompleteIndex for Queues {
///     type Index = EntryByQueue;
///     const INTRODUCED: SemanticRestateVersion = SemanticRestateVersion::new(1, 8, 2);
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
/// # use restate_types::SemanticRestateVersion;
/// # use restate_types::identifiers::InvocationId;
/// # struct Queues;
/// # impl CompleteIndex for Queues {
/// #     type Index = EntryByQueue;
/// #     const INTRODUCED: SemanticRestateVersion = SemanticRestateVersion::new(1, 8, 2);
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
                orders: orders::<B>,
                ordering: ordering::<B>,
                introduced: introduced::<B>,
            },
            primary: PhantomData,
        }
    }

    /// The declaration once its source is registered.
    pub(crate) fn declared(&self) -> DeclaredIndex {
        self.declared
    }
}

fn select<B: CompleteIndex>(
    filters: &[Arc<dyn PhysicalExpr>],
    demand: Option<&Demand>,
    projection: &Schema,
) -> anyhow::Result<bool> {
    let covered = covers::<B>(projection);
    if !index_restriction::<B::Index>(filters)?.is_some_and(|restriction| {
        if covered {
            B::useful_covered(&restriction)
        } else {
            B::useful(&restriction)
        }
    }) {
        return Ok(false);
    }
    let alternatives = <PrimaryOf<B> as SelectionSchema>::analyze(
        &static_conjuncts(filters),
        NonZeroUsize::new(16).unwrap(),
    )?;
    if !B::admits(&alternatives) {
        return Ok(false);
    }
    if !B::NEEDS_ORDERED_DEMAND {
        return Ok(true);
    }
    // Every condition, whether the source or a filter above applies it.
    Ok(demand.is_some_and(|demand| {
        orders::<B>(demand).is_some()
            && filters
                .iter()
                .flat_map(split_conjunction)
                .chain(demand.residual.iter())
                .all(B::residual_allowed)
    }))
}

/// Whether `projection`, a scan's columns, its conditions' included, lies in
/// the columns the index holds ([`CompleteIndex::COVERS`]): the coordinator and
/// the worker read the same projection, so they agree on a covered read.
pub(crate) fn covers<B: CompleteIndex>(projection: &Schema) -> bool {
    !B::COVERS.is_empty()
        && projection.fields().iter().all(|field| {
            B::COVERS
                .iter()
                .any(|&column| PrimaryOf::<B>::sql_name(column) == Some(field.name()))
        })
}

/// Which way a read of the index returns rows in `demand`'s order, if one
/// does: forward for exactly the declared order, backward for its exact
/// reverse, where NULLs go included unless the column is never NULL.
fn orders<B: CompleteIndex>(demand: &Demand) -> Option<ReadDirection> {
    if B::ORDERING.is_empty() {
        return None;
    }
    let mut columns = Vec::with_capacity(demand.ordering.len());
    for sort in demand.ordering.iter() {
        columns.push((sort.expr.downcast_ref::<Column>()?.name(), sort.options));
    }
    direction_of::<B>(&columns)
}

/// Which way `columns`, each a name and its sort options, run through the
/// index's declared order: forward, backward (every column reversed, NULLs too
/// unless never NULL), or neither. An index without a declared order reads
/// forward, unordered.
fn direction_of<B: CompleteIndex>(columns: &[(&str, SortOptions)]) -> Option<ReadDirection> {
    let declared = ordering::<B>(None);
    if columns.len() != declared.len()
        || columns
            .iter()
            .zip(&declared)
            .any(|((name, _), declared)| *name != declared.name)
    {
        return None;
    }
    let runs = |backward: bool| {
        columns.iter().zip(declared.iter().zip(B::ORDERING)).all(
            |((_, options), (declared, order))| {
                options.descending == (declared.descending != backward)
                    && (order.never_null
                        || options.nulls_first == (declared.nulls_first != backward))
            },
        )
    };
    if runs(false) {
        Some(ReadDirection::Forward)
    } else if runs(true) {
        Some(ReadDirection::Backward)
    } else {
        None
    }
}

fn introduced<B: CompleteIndex>() -> SemanticRestateVersion {
    B::INTRODUCED
}

/// The order of the index's read for `demand`: its declared order, or the
/// exact reverse where `demand` asks for that ([`orders`]). NULLs go where
/// `demand` puts them if a column is never NULL: the same rows in the same
/// order, so that the planner adds no sort for the query's own spelling.
fn ordering<B: CompleteIndex>(demand: Option<&Demand>) -> Vec<SortedColumn> {
    let asked = demand.and_then(|demand| Some((&demand.ordering, orders::<B>(demand)?)));
    let backward = matches!(asked, Some((_, ReadDirection::Backward)));
    B::ORDERING
        .iter()
        .enumerate()
        .map(|(at, order)| SortedColumn {
            name: <<B::Index as IndexSchema>::Primary as SelectionSchema>::sql_name(order.column)
                .expect("an ordered column has a SQL name")
                .to_owned(),
            descending: order.options.descending != backward,
            nulls_first: match asked {
                Some((asked, _)) if order.never_null => asked[at].options.nulls_first,
                _ => order.options.nulls_first != backward,
            },
        })
        .collect()
}

/// Which way to read `B` for work shipped with `ordering`: the order the
/// coordinator planned for, and declared to DataFusion. Any other order fails
/// the read: nothing downstream checks that rows come in the declared order.
fn shipped_direction<B: CompleteIndex>(ordering: &[SortedColumn]) -> anyhow::Result<ReadDirection> {
    let columns: Vec<_> = ordering
        .iter()
        .map(|column| {
            (
                column.name.as_str(),
                SortOptions {
                    descending: column.descending,
                    nulls_first: column.nulls_first,
                },
            )
        })
        .collect();
    direction_of::<B>(&columns).ok_or_else(|| {
        anyhow::anyhow!(
            "index '{}' can't return rows in the order {ordering:?}",
            <B::Index as LogicalSchema>::NAME
        )
    })
}

/// Whether the planner reads an index for a source's filters, under a demand.
type SelectIndex = fn(&[Arc<dyn PhysicalExpr>], Option<&Demand>, &Schema) -> anyhow::Result<bool>;

/// A declared index of a registered source.
#[derive(Clone, Copy)]
pub(crate) struct DeclaredIndex {
    identity: &'static str,
    select: SelectIndex,
    orders: fn(&Demand) -> Option<ReadDirection>,
    ordering: fn(Option<&Demand>) -> Vec<SortedColumn>,
    introduced: fn() -> SemanticRestateVersion,
}

impl DeclaredIndex {
    /// The index schema's name, sent with index work.
    pub(crate) fn identity(&self) -> &'static str {
        self.identity
    }

    /// Whether the planner reads the index for `filters` under `demand`, for a
    /// scan of `projection`.
    pub(crate) fn select(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
        demand: Option<&Demand>,
        projection: &Schema,
    ) -> anyhow::Result<bool> {
        (self.select)(filters, demand, projection)
    }

    /// Which way a read of the index returns rows in exactly `demand`'s order,
    /// if one does (see [`IndexOrder::never_null`]).
    pub(crate) fn orders(&self, demand: &Demand) -> Option<ReadDirection> {
        (self.orders)(demand)
    }

    /// The order of a partition's rows read through the index, if declared:
    /// reversed where `demand` asks for the reverse, with NULLs where `demand`
    /// puts them if that changes nothing (see [`IndexOrder::never_null`]).
    pub(crate) fn ordering(&self, demand: Option<&Demand>) -> Vec<SortedColumn> {
        (self.ordering)(demand)
    }

    /// The Restate version that introduced reading the index
    /// ([`CompleteIndex::INTRODUCED`]).
    pub(crate) fn introduced(&self) -> SemanticRestateVersion {
        (self.introduced)()
    }
}

/// The access path chosen for one source before distribution.
#[derive(Debug)]
pub(crate) enum Access {
    Scan,
    Primary(Box<dyn PrimarySelection>),
    Index {
        index: ReString,
        ordering: Vec<SortedColumn>,
        /// How many of a partition's rows a read that can't keep the order must
        /// keep, sorted: proven only where everything above the bound is in the
        /// source's predicate ([`select_access`]).
        bound: Option<usize>,
    },
}

/// One selector for ordinary and shard-JOIN inputs: complete primary keys first,
/// then the first declared index whose restriction is useful and that every
/// worker knows (see [`workers_know`]), else a scan.
///
/// Selecting a declared index needs no opt-in: either it is always maintained,
/// or its reader checks on its own snapshot that the index is complete and reads
/// the source instead, before returning anything, when it isn't. It doesn't
/// depend on whether the session shows the index's table either: the read goes
/// through the primary table's source.
///
/// `demand` is what a bounded sort above the source asks of its rows, if any
/// ([`crate::distributed::demand`]). An index that needs one
/// ([`CompleteIndex::NEEDS_ORDERED_DEMAND`]) is chosen only for it. Its fallback
/// bound is the sort's fetch when the source's predicate is exactly every
/// condition between the sort and the source: a partition that scans instead
/// applies that predicate before keeping its first rows, so none of the rows the
/// sort needs is lost.
pub(crate) fn select_access(
    source: &PartitionSourceDescriptor,
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    demand: Option<&Demand>,
    projection: &Schema,
) -> anyhow::Result<Access> {
    let filters = predicate.map(std::slice::from_ref).unwrap_or_default();
    if let Some(select) = source.primary_access
        && let Some(keys) = select(filters)?
    {
        return Ok(Access::Primary(keys));
    }
    let nodes = source.manager.metadata().nodes_config_ref();
    for index in &source.indexes {
        if index.select(filters, demand, projection)? && workers_know(&nodes, &index.introduced()) {
            let conditions = filters
                .iter()
                .flat_map(split_conjunction)
                .cloned()
                .collect::<Vec<_>>();
            let bound = demand
                .filter(|demand| {
                    index.orders(demand).is_some()
                        && physical_exprs_bag_equal(&demand.residual, &conditions)
                })
                .map(|demand| demand.fetch);
            return Ok(Access::Index {
                index: ReString::from_static(index.identity()),
                ordering: index.ordering(demand),
                bound,
            });
        }
    }
    Ok(Access::Scan)
}

/// Whether every node that may read partition data for a query knows an index
/// that `introduced` added: every worker-role node, disabled ones included
/// ([`NodesConfiguration::all_workers_including_disabled_run_at_least`]), last
/// started with that release or a newer one.
///
/// - A worker without a recorded version, or with one that doesn't parse,
///   counts as older. Other roles don't count.
/// - A pre-release of the introducing release counts as that release, so that
///   development and release-candidate clusters use their indexes. A
///   development build from before an index existed therefore passes this
///   check, and fails the query when it gets the index work.
///
/// The nodes configuration is the planner's current one: a change applies to
/// later plans, not to a plan already made.
pub(crate) fn workers_know(
    nodes: &NodesConfiguration,
    introduced: &SemanticRestateVersion,
) -> bool {
    nodes.all_workers_including_disabled_run_at_least(introduced)
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
/// Index reads name a declared index and the order planned for it; the scanner
/// derives its restriction from the predicate and never chooses another access
/// path.
#[derive(Debug, Clone)]
pub enum PrimaryRead {
    Scan,
    MultiGet(Arc<PrimaryKeys>),
    Index(IndexRead),
}

/// A read of a declared index, in the order the coordinator planned for it.
#[derive(Debug, Clone)]
pub struct IndexRead {
    name: ReString,
    ordering: Arc<[SortedColumn]>,
}

impl IndexRead {
    pub(crate) fn new(name: ReString, ordering: Arc<[SortedColumn]>) -> Self {
        Self { name, ordering }
    }

    /// The index's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Which way to read `B`, the declared index this read names: forward for
    /// its declared order, backward for the exact reverse. Any other order is
    /// an error.
    pub fn direction<B: CompleteIndex>(&self) -> anyhow::Result<ReadDirection> {
        shipped_direction::<B>(&self.ordering)
    }
}

#[cfg(test)]
mod tests {
    use restate_partition_store::vqueue_table::VQueueMetaTable;
    use restate_types::vqueues::VQueueId;

    use super::*;

    /// A source's declared indexes, as registered for planning, carry the version
    /// that introduced reading them: the entry index its storage definition's.
    #[restate_core::test]
    async fn registered_index_declarations_carry_their_versions() {
        use restate_partition_store::index::RegisteredIndex;
        use restate_partition_store::vqueue_table::EntryByServiceCreated;

        use crate::source_registry::SourceRegistry;
        use crate::tables::invocation_status::SysInvocationStatusTable;
        use crate::tables::vqueue_entry_status::schema::SysVqueueEntryStatusTable;

        let sources =
            SourceRegistry::local_only(restate_core::MetadataBuilder::default().to_metadata());
        let declared = |indexes: &[DeclaredIndex]| {
            indexes
                .iter()
                .map(|index| (index.identity(), index.introduced()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            declared(
                &sources
                    .create_declared_source::<SysInvocationStatusTable>()
                    .indexes
            ),
            [(
                "_idx_entry_by_service_created",
                EntryByServiceCreated::INTRODUCED
            )]
        );
        assert_eq!(
            declared(
                &sources
                    .create_declared_source::<SysVqueueEntryStatusTable>()
                    .indexes
            ),
            [
                ("_idx_entry_by_queue", SemanticRestateVersion::new(1, 8, 2)),
                (
                    "_idx_finished_entry_by_outcome",
                    restate_partition_store::vqueue_table::FinishedEntryByOutcome::INTRODUCED
                ),
                (
                    "_idx_active_entry_by_stage",
                    restate_partition_store::vqueue_table::ActiveEntryByStage::INTRODUCED
                ),
                ("_idx_entry_by_queue", SemanticRestateVersion::new(1, 8, 2)),
            ]
        );
    }

    /// An otherwise useful index is selected only while every worker-role node
    /// runs at least the release that introduced it; otherwise the planner
    /// scans, and complete primary keys still come first. Each plan reads the
    /// nodes configuration current at the time, through the registered source.
    /// Every plan asks for the index's order with a bound, which alone makes
    /// the index useful.
    #[restate_core::test]
    async fn indexes_are_selected_only_where_every_worker_knows_them() {
        use datafusion::arrow::compute::SortOptions;
        use datafusion::common::{DFSchema, ScalarValue};
        use datafusion::physical_expr::PhysicalSortExpr;
        use datafusion::physical_expr::expressions::col as predicate_col;
        use datafusion::prelude::{SessionContext, col, lit};
        use restate_core::test_env::TestCoreEnv;
        use restate_storage_query_api::tables::invocation_status::InvocationStatus;
        use restate_types::config::Configuration;
        use restate_types::nodes_config::{NodeConfig, Role, WorkerState};
        use restate_types::{GenerationalNodeId, RestateVersion};

        use crate::source_registry::SourceRegistry;
        use crate::tables::invocation_status::SysInvocationStatusTable;

        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let mut config = Configuration::default();
        config.common.experimental.set_track_entry_indexes(true);
        Configuration::set(config);
        let sources = SourceRegistry::local_only(env.metadata.clone());
        let source = sources.create_declared_source::<SysInvocationStatusTable>();
        let schema = DFSchema::try_from(InvocationStatus::arrow_schema().as_ref().clone()).unwrap();
        let predicate = |expr| {
            SessionContext::new()
                .create_physical_expr(expr, &schema)
                .unwrap()
        };
        let service = predicate(
            col("target_service_name").eq(lit(ScalarValue::LargeUtf8(Some("Promises".into())))),
        );
        let id = predicate(col("id").eq(lit(ScalarValue::LargeUtf8(Some(
            restate_types::identifiers::InvocationId::mock_random().to_string(),
        )))));
        // `ORDER BY created_at DESC LIMIT 51` above the source's own predicate.
        let newest = |predicate: &Arc<dyn PhysicalExpr>| Demand {
            ordering: vec![PhysicalSortExpr {
                expr: predicate_col("created_at", &InvocationStatus::arrow_schema()).unwrap(),
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            }],
            fetch: 51,
            residual: vec![Arc::clone(predicate)],
        };

        // (role, version, disabled) of each node.
        type Nodes<'a> = &'a [(Role, Option<&'a str>, bool)];
        let mut version = env.metadata.nodes_config_version();
        let mut plan_with = async |nodes: Nodes<'_>, predicate: &Arc<dyn PhysicalExpr>| {
            let current = env.metadata.nodes_config_snapshot();
            let mut config = NodesConfiguration::new(
                version,
                current.cluster_name().to_owned(),
                current.cluster_fingerprint().unwrap(),
            );
            for (id, (role, binary, disabled)) in (1u32..).zip(nodes) {
                let mut node = NodeConfig::builder()
                    .name(format!("node-{id}"))
                    .current_generation(GenerationalNodeId::new(id, 1))
                    .address("unix:/run/f.sock".parse().unwrap())
                    .roles((*role).into())
                    .binary_version(RestateVersion::current())
                    .build();
                node.binary_version = binary.map(|binary| RestateVersion::new(binary.into()));
                if *disabled {
                    node.worker_config.worker_state = WorkerState::Disabled;
                }
                config.upsert_node(node);
            }
            version = version.next();
            config.set_version(version);
            env.metadata_writer.update(Arc::new(config)).await.unwrap();
            assert_eq!(env.metadata.nodes_config_ref().version(), version);
            match select_access(
                &source,
                Some(predicate),
                Some(&newest(predicate)),
                &Schema::empty(),
            )
            .unwrap()
            {
                Access::Index { index, bound, .. } => format!("index {index} {bound:?}"),
                Access::Primary(_) => "primary".to_owned(),
                Access::Scan => "scan".to_owned(),
            }
        };
        let index = "index _idx_entry_by_service_created Some(51)";
        use Role::{Admin, MetadataServer, Worker};
        for (nodes, expected) in [
            (&[(Worker, Some("1.8.2"), false)][..], index),
            (
                &[
                    (Worker, Some("1.9.0"), false),
                    (Worker, Some("1.8.2-dev"), false),
                ],
                index,
            ),
            (
                &[
                    (Worker, Some("1.8.2"), false),
                    (Worker, Some("1.8.1"), false),
                ],
                "scan",
            ),
            (
                &[(Worker, Some("1.8.2"), false), (Worker, None, false)],
                "scan",
            ),
            (
                &[
                    (Worker, Some("1.8.2"), false),
                    (Worker, Some("not a version"), false),
                ],
                "scan",
            ),
            // Other roles don't veto.
            (
                &[
                    (Worker, Some("1.8.2"), false),
                    (MetadataServer, Some("1.7.0"), false),
                    (Admin, None, false),
                ],
                index,
            ),
            // A disabled worker may still serve its partitions: it counts.
            (
                &[
                    (Worker, Some("1.8.2"), false),
                    (Worker, Some("1.8.1"), true),
                ],
                "scan",
            ),
            // The next plan sees the upgrade.
            (
                &[
                    (Worker, Some("1.8.2"), false),
                    (Worker, Some("1.8.2"), true),
                ],
                index,
            ),
        ] {
            assert_eq!(plan_with(nodes, &service).await, expected, "{nodes:?}");
        }
        // The index pays off only for its own order, or its exact reverse, and
        // conditions that keep most candidates; the fallback bound needs every
        // condition above the source in its predicate.
        let condition = |column: &str, value: &str| {
            predicate(col(column).eq(lit(ScalarValue::LargeUtf8(Some(value.into())))))
        };
        let oldest = Demand {
            ordering: vec![PhysicalSortExpr {
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
                ..newest(&service).ordering[0].clone()
            }],
            ..newest(&service)
        };
        let mut scheduled = newest(&service);
        scheduled.residual.push(condition("status", "scheduled"));
        let mut typed = newest(&service);
        typed
            .residual
            .push(condition("target_service_ty", "workflow"));
        let chosen = |demand: Option<&Demand>| match select_access(
            &source,
            Some(&service),
            demand,
            &Schema::empty(),
        )
        .unwrap()
        {
            Access::Index { bound, .. } => format!("index {bound:?}"),
            _ => "scan".to_owned(),
        };
        assert_eq!(chosen(None), "scan");
        assert_eq!(chosen(Some(&oldest)), "index Some(51)");
        assert_eq!(chosen(Some(&scheduled)), "scan");
        assert_eq!(chosen(Some(&typed)), "index None");
        assert_eq!(chosen(Some(&newest(&service))), "index Some(51)");
        // `created_at` is never NULL: `DESC`, which puts NULLs first, reads the
        // index too, and the read delivers that placement, so no sort is added.
        let nulls_first = Demand {
            ordering: vec![PhysicalSortExpr {
                options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
                ..newest(&service).ordering[0].clone()
            }],
            ..newest(&service)
        };
        let Access::Index {
            ordering, bound, ..
        } = select_access(
            &source,
            Some(&service),
            Some(&nulls_first),
            &Schema::empty(),
        )
        .unwrap()
        else {
            panic!("the index reads either placement");
        };
        assert_eq!((ordering[0].nulls_first, bound), (true, Some(51)));
        // The exact reverse reads it backwards, oldest first, with the same
        // bound: the order shipped to workers says so.
        let Access::Index {
            ordering, bound, ..
        } = select_access(&source, Some(&service), Some(&oldest), &Schema::empty()).unwrap()
        else {
            panic!("the index reads backwards");
        };
        assert_eq!(
            (ordering[0].descending, ordering[0].nulls_first, bound),
            (false, false, Some(51))
        );
        // A column that can be NULL keeps its placement: the other one needs a
        // sort.
        struct Nullable;
        impl CompleteIndex for Nullable {
            type Index = restate_storage_query_api::tables::index::EntryByServiceCreated;
            const INTRODUCED: SemanticRestateVersion = SemanticRestateVersion::new(1, 8, 2);
            const ORDERING: &'static [IndexOrder<PrimaryColumn<Self>>] = &[IndexOrder {
                column: restate_storage_query_api::tables::invocation_status::InvocationStatusColumn::CreatedAt,
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
                never_null: false,
            }];
            fn useful(
                _: &restate_storage_query_api::tables::index::EntryByServiceCreatedDomains,
            ) -> bool {
                true
            }
        }
        let spelled = |descending, nulls_first| Demand {
            ordering: vec![PhysicalSortExpr {
                options: SortOptions {
                    descending,
                    nulls_first,
                },
                ..newest(&service).ordering[0].clone()
            }],
            ..newest(&service)
        };
        use ReadDirection::{Backward, Forward};
        // DESC NULLS LAST, DESC NULLS FIRST, ASC NULLS FIRST, ASC NULLS LAST.
        let spellings = [(true, false), (true, true), (false, true), (false, false)];
        assert_eq!(
            spellings.map(|(descending, nulls)| orders::<Nullable>(&spelled(descending, nulls))),
            [Some(Forward), None, Some(Backward), None]
        );
        assert!(!super::ordering::<Nullable>(Some(&nulls_first))[0].nulls_first);
        assert!(super::ordering::<Nullable>(Some(&spelled(false, true)))[0].nulls_first);
        // A worker reads in the direction of the order shipped to it, and
        // refuses any other order.
        let shipped = |descending, nulls_first| {
            IndexRead::new(
                ReString::from_static("i"),
                vec![SortedColumn {
                    name: "created_at".to_owned(),
                    descending,
                    nulls_first,
                }]
                .into(),
            )
            .direction::<Nullable>()
            .ok()
        };
        assert_eq!(
            spellings.map(|(descending, nulls)| shipped(descending, nulls)),
            [Some(Forward), None, Some(Backward), None]
        );
        // Complete primary keys come first, whatever the versions.
        let older = [(Worker, Some("1.8.1"), false)];
        assert_eq!(plan_with(&older, &id).await, "primary");
        assert_eq!(plan_with(&older, &service).await, "scan");
        assert!(matches!(
            select_access(&source, Some(&id), None, &Schema::empty()).unwrap(),
            Access::Primary(_)
        ));
    }

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
