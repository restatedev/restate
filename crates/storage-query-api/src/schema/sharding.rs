// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! How logical schemas are sharded, and which shards a predicate can touch.
//!
//! # Concepts
//!
//! - **Shard key**: the value a row carries in its schema's key space. It is a
//!   property of the row alone and never depends on cluster topology; for example,
//!   an invocation keeps the partition key encoded in its ID forever.
//! - **Shard**: the unit of data one server holds completely, such as one Restate
//!   partition or one node. A shard holds a set of shard keys *under one topology
//!   snapshot*: splitting a partition changes which shard holds a key, never the
//!   key itself.
//! - **Shard owner**: the node chosen to serve a shard for one query, under the
//!   schema's hard serving requirements (such as a mandatory leader) and the
//!   session's placement preferences. It is a per-query choice, not replication
//!   ownership.
//! - **Routing** (placement): mapping a shard to its owner. It depends on topology
//!   and policy, and is not decided here.
//! - **Candidate shard keys** ([`Sharding::Candidates`]): for a predicate, a set of
//!   shard keys that contains the shard key of *every* row that can satisfy it.
//!   They are a superset: an extra key costs work, a missing key would lose rows.
//!   They are topology-free, computed per Boolean alternative, and then unioned.
//! - **Resolution**: mapping candidate shard keys to candidate shards against one
//!   topology snapshot, for example by intersecting a partition-key domain with
//!   each partition's key range.
//!
//! "Partition" always means a Restate partition: a [`PartitionId`] owning a
//! contiguous range of [`PartitionKey`]s. It never means a shard in general, nor a
//! DataFusion output partition.
//!
//! | [`ShardingScheme`](crate::ShardingScheme) | Shard key | Shard | Shard owner |
//! |---|---|---|---|
//! | `ByPartitionKey` | partition key | the partition whose key range contains it | its leader or a replica |
//! | `ByPartitionId` | partition ID | that partition | its leader or a replica |
//! | `ByNode` | plain node ID | that node | the node's current generation |
//! | `Unsharded`, `Coordinator` | `()` | the single logical shard | an eligible node, or the coordinator |
//!
//! # Example
//!
//! An invocation in `sys_invocation_status` whose ID encodes partition key
//! `0x9c3a…`, in a cluster with four equally sized partitions:
//!
//! ```text
//! id = 'inv_1…'   shard key   = 0x9c3a…  (encoded in the ID; never changes)
//!                 shard       = partition 2, owning keys 0x8000… ..= 0xbfff…
//!                 shard owner = N3, partition 2's leader chosen for this query
//!
//! partition 2 splits into partition 4 (0x8000… ..= 0x9fff…) and partition 5:
//!                 shard key   = 0x9c3a…  (unchanged)
//!                 shard       = partition 4
//! ```
//!
//! Candidate shard keys of predicates on that table, and their resolution against
//! the four original partitions:
//!
//! ```text
//! predicate                                  candidate shard keys          partitions
//! id IN ('inv_1…', 'inv_2…')                 { key(inv_1…), key(inv_2…) }  those holding either key
//! id = 'inv_1…' OR status = 'running'        all (2nd alternative is open) all four
//! partition_key = 7 AND id = 'inv_1…'        { 7 } ∩ { key(inv_1…) } = {}  none
//! ```
//!
//! Candidates never replace the predicate: readers still evaluate the original SQL
//! on every row they produce.
//!
//! # Sharding columns
//!
//! A **sharding column** is a column whose non-NULL value *alone* determines its
//! row's shard key, through a declared [derivation](shard_key). A schema may have
//! several. Each is an independent determinant, not part of a composite key, and
//! NULL determines nothing.
//!
//! ```text
//! sharding_columns { partition_key: PartitionKey, id: InvocationId, scope: Scope }
//!
//! row: partition_key = 0x9c3a…, id = 'inv_1…', scope = 'tenant-a'
//!      PartitionKey(0x9c3a…)    = 0x9c3a…
//!      InvocationId('inv_1…')   = 0x9c3a…   (the key encoded in the ID)
//!      Scope('tenant-a')        = hash('tenant-a') = 0x9c3a…
//! ```
//!
//! A declaration states a fact about *every* row: all of its non-NULL sharding
//! columns agree on its shard key. Two consumers rely on that one fact:
//!
//! - **Candidate shard keys** are generated from it. Each column's candidate domain
//!   is mapped through its derivation, and the results are intersected, so
//!   `id IN ('inv_1…')` yields `{0x9c3a…}`.
//! - **Shard-local JOINs** are justified by it. When both sides of an equality are
//!   sharding columns with the same derivation, matching rows have equal shard keys
//!   and therefore share a shard under any topology.
//!
//! Only declarations count. Native type, SQL type, column name, and primary-key
//! status are never evidence: `invoked_by_id` also holds an invocation ID, but it
//! names the *caller*, whose shard may differ. Conditional rules are not sharding
//! columns either: `state.service_key` determines the shard key only when `scope`
//! is NULL. Such rules live in a hand-written `shard_keys:` closure, which may prune
//! shards but never proves colocation.

use restate_types::GenerationalNodeId;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_util_string::{ReString, ToReString};

use crate::selection::domain::Domain;

/// A sharding scheme at the type level: its shard-key space determines how
/// candidate shard keys are represented.
pub trait Sharding: Send + Sync + 'static {
    /// Candidate shard keys of one predicate, in this scheme's key space.
    type Candidates: Candidates;
}

/// A set of candidate shard keys.
pub trait Candidates: Sized {
    /// Every shard key: nothing restricts the shards.
    fn all() -> Self;
    /// No shard key: no row can satisfy the predicate.
    fn empty() -> Self;
    /// Shard keys admitted by both sets, used to combine restrictions within one
    /// Boolean alternative.
    fn intersect(self, other: Self) -> Self;
    /// Shard keys admitted by either set, used to combine Boolean alternatives.
    fn union(self, other: Self) -> Self;
}

impl<T: Ord + Clone> Candidates for Domain<T> {
    fn all() -> Self {
        Self::all()
    }
    fn empty() -> Self {
        Self::empty()
    }
    fn intersect(self, other: Self) -> Self {
        self.intersect(other)
    }
    fn union(self, other: Self) -> Self {
        self.union(other)
    }
}

/// A column whose non-NULL value alone determines its row's shard key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardingColumn<C> {
    /// The column: a typed identity, or an ordinal once bound to a physical scan.
    pub column: C,
    /// How the column's value determines the shard key.
    pub kind: ShardKeyKind,
    /// Whether the column can be NULL. NULL determines nothing, so two NULLs never
    /// prove equal shard keys.
    pub nullable: bool,
}

pub use shard_key::ShardKeyKind;

/// Shard-key derivations: how a sharding column's value determines its row's shard
/// key. `sharding_columns { column: Derivation }` names one of these types.
///
/// Each [`Derivation`](shard_key::Derivation) belongs to one sharding scheme and
/// accepts one native column type as its [`Input`](shard_key::Input), or that
/// type's `Option` for a nullable column:
///
/// ```text
/// sharding_columns { id: InvocationId, scope: Scope }
///
/// id:    InvocationId      : Input<shard_key::InvocationId>   (ByPartitionKey)
/// scope: Option<ReString>  : Input<shard_key::Scope>          (ByPartitionKey, nullable)
/// ```
///
/// Declaring a derivation of another scheme, or on a column of another native type,
/// fails to compile.
pub mod shard_key {
    use restate_types::identifiers::WithPartitionKey;
    use restate_types::identifiers::partitioner::HashPartitioner;
    use restate_util_string::ReString;

    use super::{
        NodeCandidates, NodeSharding, PartitionIdSharding, PartitionKeySharding, Sharding,
    };
    use crate::selection::domain::Domain;

    /// A shard-key derivation as a type.
    pub trait Derivation: 'static {
        /// The scheme whose shard keys the derivation produces.
        type Sharding: Sharding;
        /// Runtime identity, comparable across schemas.
        const KIND: ShardKeyKind;
    }

    /// A native column type that derivation `D` accepts.
    pub trait Input<D: Derivation>: Ord + Clone {
        /// Whether the column can hold NULL.
        const NULLABLE: bool = false;
        /// Candidate shard keys of the rows whose value lies in `values`.
        fn shard_keys(values: &Domain<Self>) -> <D::Sharding as Sharding>::Candidates;
    }

    /// A nullable sharding column: only non-NULL values determine a shard key.
    impl<D: Derivation, V: Input<D>> Input<D> for Option<V> {
        const NULLABLE: bool = true;

        fn shard_keys(values: &Domain<Self>) -> <D::Sharding as Sharding>::Candidates {
            // A NULL candidate leaves its row's shard key unknown, which widens the
            // mapped domain (and so the candidates) to everything.
            V::shard_keys(&values.try_map_values(Clone::clone))
        }
    }

    macro_rules! derivations {
        ($($(#[doc = $doc:expr])* $name:ident: $value:ty, $sharding:ty, $shard_keys:expr;)+) => {
            /// The runtime identity of a [`Derivation`].
            ///
            /// Columns of the same kind with equal non-NULL SQL values have equal
            /// shard keys, in any schema. Distinct kinds never prove equality, even
            /// where renderings could coincide, such as an invocation ID and the
            /// entry ID of the same invocation.
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub enum ShardKeyKind {
                $($(#[doc = $doc])* $name,)+
            }

            $(
                $(#[doc = $doc])*
                #[derive(Debug)]
                pub enum $name {}

                impl Derivation for $name {
                    type Sharding = $sharding;
                    const KIND: ShardKeyKind = ShardKeyKind::$name;
                }

                impl Input<$name> for $value {
                    fn shard_keys(values: &Domain<Self>) -> <$sharding as Sharding>::Candidates {
                        let shard_keys: fn(&Domain<Self>) -> <$sharding as Sharding>::Candidates =
                            $shard_keys;
                        shard_keys(values)
                    }
                }
            )+
        };
    }

    // Value types use full paths: the derivation types shadow their names here.
    derivations! {
        /// `ByPartitionKey`: the row's partition key itself.
        PartitionKey: restate_types::identifiers::PartitionKey, PartitionKeySharding, Domain::clone;
        /// `ByPartitionKey`: the partition key encoded in an invocation ID.
        InvocationId: restate_types::identifiers::InvocationId, PartitionKeySharding,
            |ids| ids.map_values(WithPartitionKey::partition_key);
        /// `ByPartitionKey`: the partition key encoded in a vqueue ID.
        VQueueId: restate_types::vqueues::VQueueId, PartitionKeySharding,
            |ids| ids.map_values(restate_types::vqueues::VQueueId::partition_key);
        /// `ByPartitionKey`: the partition key encoded in a vqueue entry ID.
        BaseEntryId: restate_types::identifiers::BaseEntryId, PartitionKeySharding,
            |ids| ids.map_values(restate_types::identifiers::BaseEntryId::partition_key);
        /// `ByPartitionKey`: the partition key encoded in a canonical vqueue entry ID.
        CanonicalEntryId: restate_types::identifiers::CanonicalEntryId, PartitionKeySharding,
            |ids| ids.map_values(restate_types::identifiers::CanonicalEntryId::partition_key);
        /// `ByPartitionKey`: the hash of a scope name, equal to the partition key of
        /// the corresponding `restate_types::Scope`.
        Scope: ReString, PartitionKeySharding,
            |scopes| scopes.map_values(|scope| HashPartitioner::compute_partition_key(scope));
        /// `ByPartitionId`: the row's partition ID itself.
        PartitionId: restate_types::identifiers::PartitionId, PartitionIdSharding, Domain::clone;
        /// `ByNode`: a plain node ID such as `N1`, compared as a SQL string.
        PlainNodeId: ReString, NodeSharding,
            |nodes| NodeCandidates::new(nodes.clone(), Domain::all());
        /// `ByNode`: a generational node ID such as `N1:3`, compared as a SQL string.
        /// It determines the plain node and constrains the owner's generation.
        GenerationalNodeId: ReString, NodeSharding,
            |nodes| NodeCandidates::new(Domain::all(), nodes.clone());
    }
}

/// Shard key: [`PartitionId`]. Each candidate ID is its own partition.
#[derive(Debug)]
pub enum PartitionIdSharding {}
/// Shard key: [`PartitionKey`]. Candidate keys resolve to the partitions whose
/// key ranges contain them.
#[derive(Debug)]
pub enum PartitionKeySharding {}
/// Shard key: plain node ID. See [`NodeCandidates`].
#[derive(Debug)]
pub enum NodeSharding {}
/// One logical shard with the unit shard key `()`. Candidates are either empty
/// (no row can match) or `{()}`.
#[derive(Debug)]
pub enum Unsharded {}

impl Sharding for PartitionIdSharding {
    type Candidates = Domain<PartitionId>;
}

impl Sharding for PartitionKeySharding {
    type Candidates = Domain<PartitionKey>;
}

impl Sharding for NodeSharding {
    type Candidates = NodeCandidates;
}

impl Sharding for Unsharded {
    type Candidates = Domain<()>;
}

/// Candidate shard keys of a node-sharded schema, plus a constraint on the
/// generation that may own them.
///
/// Each Boolean alternative pairs two domains of SQL strings:
///
/// - a **plain** domain such as `{'N1'}`, which constrains the shard key; and
/// - a **generation** domain such as `{'N1:3'}`, which constrains the incarnation
///   that may serve the shard. Rows record the generation that produced them, so a
///   stale generation matches no live owner.
///
/// Pairs stay together: `(plain_node_id = 'N1' AND gen_node_id = 'N1:3') OR
/// plain_node_id = 'N2'` admits `N1:3` and any generation of `N2`, but not `N1:4`.
/// Node identities keep SQL string semantics. Resolve them against actual
/// generations, rather than parsing aliases or mapping a stale generation to its
/// replacement.
#[derive(Debug, Clone)]
pub struct NodeCandidates {
    alternatives: Vec<(Domain<ReString>, Domain<ReString>)>,
}

impl NodeCandidates {
    /// Candidates of one alternative.
    pub fn new(plain: Domain<ReString>, generation: Domain<ReString>) -> Self {
        Self {
            alternatives: vec![(plain, generation)],
        }
    }

    /// Resolution: whether this live node generation may serve a candidate shard.
    pub fn contains(&self, node: GenerationalNodeId) -> bool {
        let plain = node.as_plain().to_restring();
        let generation = node.to_restring();
        self.alternatives
            .iter()
            .any(|(p, g)| p.contains(&plain) && g.contains(&generation))
    }
}

impl Candidates for NodeCandidates {
    fn all() -> Self {
        Self::new(Domain::all(), Domain::all())
    }
    fn empty() -> Self {
        Self {
            alternatives: Vec::new(),
        }
    }
    /// Pairs every alternative of one side with every alternative of the other.
    fn intersect(self, other: Self) -> Self {
        let alternatives = self
            .alternatives
            .iter()
            .flat_map(|(plain, generation)| {
                other
                    .alternatives
                    .iter()
                    .map(|(other_plain, other_generation)| {
                        (
                            plain.clone().intersect(other_plain.clone()),
                            generation.clone().intersect(other_generation.clone()),
                        )
                    })
            })
            .collect();
        Self { alternatives }
    }
    fn union(mut self, other: Self) -> Self {
        self.alternatives.extend(other.alternatives);
        self
    }
}
