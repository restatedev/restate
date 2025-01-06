// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{hash_map, BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::num::NonZeroU8;
use std::str::FromStr;

use anyhow::Context;
use bytestring::ByteString;
use enum_map::Enum;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use smallvec::SmallVec;
use xxhash_rust::xxh3::Xxh3Builder;

use super::builder::LogsBuilder;
use super::LogletId;
use crate::config::Configuration;
use crate::logs::{LogId, Lsn, SequenceNumber};
use crate::protobuf::cluster_configuration::{
    NodeSetSelectionStrategy as ProtoNodeSetSelectionStrategy, NodeSetSelectionStrategyKind,
};
use crate::replicated_loglet::{ReplicatedLogletParams, ReplicationProperty};
use crate::{flexbuffers_storage_encode_decode, Version, Versioned};

// Starts with 0 being the oldest loglet in the chain.
#[derive(
    Default,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Debug,
    Hash,
    Serialize,
    Deserialize,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct SegmentIndex(pub(crate) u32);

impl SegmentIndex {
    pub const OLDEST: SegmentIndex = SegmentIndex(0);

    pub fn next(&self) -> SegmentIndex {
        SegmentIndex(
            self.0
                .checked_add(1)
                .expect("we should never create more than 2^32 segments"),
        )
    }
}

#[derive(Debug, Clone)]
pub struct LogletRef<P> {
    pub params: P,
    pub references: SmallVec<[(LogId, SegmentIndex); 1]>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct LookupIndex {
    pub(super) replicated_loglets:
        HashMap<LogletId, LogletRef<ReplicatedLogletParams>, Xxh3Builder>,
}

impl LookupIndex {
    pub fn add_replicated_loglet(
        &mut self,
        log_id: LogId,
        segment_index: SegmentIndex,
        params: ReplicatedLogletParams,
    ) {
        self.replicated_loglets
            .entry(params.loglet_id)
            .or_insert_with(|| LogletRef {
                params,
                references: Default::default(),
            })
            .references
            .push((log_id, segment_index));
    }

    pub fn rm_replicated_loglet_reference(
        &mut self,
        log_id: LogId,
        segment_index: SegmentIndex,
        loglet_id: LogletId,
    ) {
        if let hash_map::Entry::Occupied(mut entry) = self.replicated_loglets.entry(loglet_id) {
            entry
                .get_mut()
                .references
                .retain(|(l, s)| *l != log_id && *s != segment_index);
            if entry.get().references.is_empty() {
                entry.remove();
            }
        }
    }

    pub fn get_replicated_loglet(
        &self,
        loglet_id: &LogletId,
    ) -> Option<&LogletRef<ReplicatedLogletParams>> {
        self.replicated_loglets.get(loglet_id)
    }
}

/// Node set selection strategy for picking cluster members to host replicated logs. Note that this
/// concerns loglet replication configuration across storage servers during log bootstrap or cluster
/// reconfiguration, for example when expanding capacity.
///
/// It is expected that the Bifrost data plane will deal with short-term server unavailability.
/// Therefore, we can afford to aim high with our nodeset selections and optimise for maximum
/// possible fault tolerance. It is the data plane's responsibility to achieve availability within
/// this nodeset during periods of individual node downtime.
///
/// Finally, nodeset selection is orthogonal to log sequencer placement.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[serde(rename_all = "kebab-case")]
pub enum NodeSetSelectionStrategy {
    /// Selects an optimal nodeset size based on the replication factor. The nodeset size is at
    /// least `2f+1`, where `f` is the number of tolerable failures.
    ///
    /// It's calculated by working backwards from a replication factor of `f+1`. If there are
    /// more nodes available in the cluster, the strategy will use them.
    ///
    /// This strategy will never suggest a nodeset smaller than `2f+1`, thus ensuring that there is
    /// always plenty of fault tolerance built into the loglet. This is a safe default choice.
    ///
    /// Example: For a replication factor of (2) `F=f+1=2`, `f` will equal to 1, and hence
    /// the node set size will be `2f+1 => 3`.
    ///
    /// For an `F=3` (and hence `f=2`), then `2f+1` => 5 nodes
    #[default]
    StrictFaultTolerantGreedy,
}

impl Display for NodeSetSelectionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StrictFaultTolerantGreedy => write!(f, "strict-fault-tolerant-greedy"),
        }
    }
}

impl FromStr for NodeSetSelectionStrategy {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "strict-fault-tolerant-greedy" => Ok(Self::StrictFaultTolerantGreedy),
            _ => Err("Unknown node set selection strategy"),
        }
    }
}

impl From<NodeSetSelectionStrategy> for ProtoNodeSetSelectionStrategy {
    fn from(value: NodeSetSelectionStrategy) -> Self {
        match value {
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy => Self {
                kind: NodeSetSelectionStrategyKind::StrictFaultTolerantGreedy.into(),
            },
        }
    }
}

impl TryFrom<ProtoNodeSetSelectionStrategy> for NodeSetSelectionStrategy {
    type Error = anyhow::Error;

    fn try_from(value: ProtoNodeSetSelectionStrategy) -> Result<Self, Self::Error> {
        if value.kind == i32::from(NodeSetSelectionStrategyKind::StrictFaultTolerantGreedy) {
            Ok(Self::StrictFaultTolerantGreedy)
        } else {
            anyhow::bail!("Unknown nodeset selection strategy kind")
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DefaultProvider {
    #[cfg(any(test, feature = "memory-loglet"))]
    InMemory,
    #[default]
    Local,
    Replicated(ReplicatedLogletConfig),
}

impl DefaultProvider {
    pub fn as_provider_kind(&self) -> ProviderKind {
        match self {
            #[cfg(any(test, feature = "memory-loglet"))]
            Self::InMemory => ProviderKind::InMemory,
            Self::Local => ProviderKind::Local,
            Self::Replicated(_) => ProviderKind::Replicated,
        }
    }
}

impl From<DefaultProvider> for crate::protobuf::cluster_configuration::DefaultProvider {
    fn from(value: DefaultProvider) -> Self {
        use crate::protobuf::cluster_configuration;

        let mut result = crate::protobuf::cluster_configuration::DefaultProvider::default();

        match value {
            DefaultProvider::Local => result.provider = ProviderKind::Local.to_string(),
            #[cfg(any(test, feature = "memory-loglet"))]
            DefaultProvider::InMemory => result.provider = ProviderKind::InMemory.to_string(),
            DefaultProvider::Replicated(config) => {
                result.provider = ProviderKind::Replicated.to_string();
                result.replicated_config = Some(cluster_configuration::ReplicatedProviderConfig {
                    replication_property: config.replication_property.to_string(),
                    nodeset_selection_strategy: Some(config.nodeset_selection_strategy.into()),
                })
            }
        };

        result
    }
}

impl TryFrom<crate::protobuf::cluster_configuration::DefaultProvider> for DefaultProvider {
    type Error = anyhow::Error;
    fn try_from(
        value: crate::protobuf::cluster_configuration::DefaultProvider,
    ) -> Result<Self, Self::Error> {
        let provider_kind: ProviderKind = value.provider.parse()?;

        match provider_kind {
            ProviderKind::Local => Ok(Self::Local),
            #[cfg(any(test, feature = "memory-loglet"))]
            ProviderKind::InMemory => Ok(Self::InMemory),
            ProviderKind::Replicated => {
                let config = value.replicated_config.ok_or_else(|| {
                    anyhow::anyhow!("replicate_config is required with replicated provider")
                })?;

                Ok(Self::Replicated(ReplicatedLogletConfig {
                    replication_property: config.replication_property.parse()?,
                    nodeset_selection_strategy: config
                        .nodeset_selection_strategy
                        .context("NodeSet selection strategy is required")?
                        .try_into()?,
                }))
            }
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReplicatedLogletConfig {
    pub nodeset_selection_strategy: NodeSetSelectionStrategy,
    #[serde_as(as = "DisplayFromStr")]
    pub replication_property: ReplicationProperty,
}

#[derive(Debug, Clone, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct LogsConfiguration {
    pub default_provider: DefaultProvider,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "LogsSerde", into = "LogsSerde")]
pub struct Logs {
    pub(super) version: Version,
    pub(super) logs: HashMap<LogId, Chain, Xxh3Builder>,
    pub(super) lookup_index: LookupIndex,
    pub(super) config: LogsConfiguration,
}

impl Default for Logs {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            logs: Default::default(),
            lookup_index: Default::default(),
            config: LogsConfiguration::default(),
        }
    }
}

impl From<Logs> for LogsSerde {
    fn from(value: Logs) -> Self {
        Self {
            version: value.version,
            logs: value.logs.into_iter().collect(),
            config: Some(value.config),
        }
    }
}

impl TryFrom<LogsSerde> for Logs {
    type Error = anyhow::Error;

    fn try_from(value: LogsSerde) -> Result<Self, Self::Error> {
        let mut logs = HashMap::with_capacity_and_hasher(value.logs.len(), Xxh3Builder::new());
        let mut lookup_index = LookupIndex::default();

        let mut config = value.config;
        for (log_id, chain) in value.logs {
            for loglet_config in chain.chain.values() {
                if let ProviderKind::Replicated = loglet_config.kind {
                    let params =
                        ReplicatedLogletParams::deserialize_from(loglet_config.params.as_bytes())?;
                    lookup_index.add_replicated_loglet(log_id, loglet_config.index, params);

                    if config.is_none() {
                        // no config in LogsSerde but we are using replicated loglets already
                        // this means we are migrating from an older setup that had replication-property
                        // hardcoded to {node:2}
                        config = Some(LogsConfiguration {
                            default_provider: DefaultProvider::Replicated(ReplicatedLogletConfig {
                                nodeset_selection_strategy: NodeSetSelectionStrategy::default(),
                                replication_property: ReplicationProperty::new(
                                    NonZeroU8::new(2).expect("2 is not 0"),
                                ),
                            }),
                        })
                    }
                }
            }
            logs.insert(log_id, chain);
        }

        Ok(Self {
            version: value.version,
            logs,
            lookup_index,
            config: config.unwrap_or_default(),
        })
    }
}

/// Log metadata is the map of logs known to the system with the corresponding chain.
/// Metadata updates are versioned and atomic.
///
/// This structure is what gets serialized in metadata store.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogsSerde {
    version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    logs: Vec<(LogId, Chain)>,
    config: Option<LogsConfiguration>,
}

/// the chain is a list of segments in (from Lsn) order.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chain {
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub(super) chain: BTreeMap<Lsn, LogletConfig>,
}

#[derive(Debug, Clone)]
pub struct Segment<'a> {
    /// The offset of the first record in the segment (if exists).
    /// A segment on a clean chain is created with Lsn::OLDEST but this doesn't mean that this
    /// record exists. It only means that we want to offset the loglet offsets by base_lsn -
    /// Loglet::Offset::OLDEST.
    pub base_lsn: Lsn,
    /// Exclusive, if unset, this is the tail/writeable segment of the log.
    pub tail_lsn: Option<Lsn>,
    pub config: &'a LogletConfig,
}

impl<'a> Segment<'a> {
    pub fn index(&'a self) -> SegmentIndex {
        self.config.index()
    }
}

/// A segment in the chain of loglet instances.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogletConfig {
    pub kind: ProviderKind,
    pub params: LogletParams,
    /// This is a cheap and collision-free way to identify loglets within the same log without
    /// using random numbers. Globally, the tuple (log_id, index) is unique.
    ///
    // serde(default) to v1.0 compatibility. This can be removed once we are confident that all
    // persisted metadata have this index set. For v1.0 multi-segment logs are not supported so we
    // only expect logs with 1 segment. Therefore, a default of 0 matches what we create on new
    // loglet chains already.
    #[serde(default)]
    index: SegmentIndex,
}

impl LogletConfig {
    #[cfg(any(test, feature = "memory-loglet"))]
    pub fn for_testing() -> Self {
        Self {
            kind: ProviderKind::InMemory,
            params: LogletParams(ByteString::default()),
            index: 0.into(),
        }
    }
}

/// The configuration of a single loglet segment. This holds information needed
/// for a loglet kind to construct a configured loglet instance modulo the log-id
/// and start-lsn. It's provided by bifrost on loglet creation. This allows the
/// parameters to be shared between segments and logs if needed.
#[derive(
    Debug, Clone, Hash, Eq, PartialEq, derive_more::From, derive_more::Deref, Serialize, Deserialize,
)]
pub struct LogletParams(ByteString);

impl From<String> for LogletParams {
    fn from(value: String) -> Self {
        Self(ByteString::from(value))
    }
}

impl From<&'static str> for LogletParams {
    fn from(value: &'static str) -> Self {
        Self(ByteString::from_static(value))
    }
}

/// An enum with the list of supported loglet providers.
#[derive(
    Debug,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    Enum,
    strum::EnumIter,
    strum::Display,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum ProviderKind {
    /// A local rocksdb-backed loglet.
    Local,
    /// An in-memory loglet, primarily for testing.
    #[cfg(any(test, feature = "memory-loglet"))]
    InMemory,
    /// Replicated loglet implementation. This requires log-server role to run on
    /// enough nodes in the cluster.
    Replicated,
}

impl FromStr for ProviderKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "local" => Ok(Self::Local),
            #[cfg(any(test, feature = "memory-loglet"))]
            "in-memory" | "in_memory" | "memory" => Ok(Self::InMemory),
            "replicated" => Ok(Self::Replicated),
            _ => anyhow::bail!("Unknown provider kind"),
        }
    }
}

impl LogletConfig {
    pub(crate) fn new(index: SegmentIndex, kind: ProviderKind, params: LogletParams) -> Self {
        Self {
            kind,
            params,
            index,
        }
    }

    pub fn index(&self) -> SegmentIndex {
        self.index
    }
}

impl Logs {
    pub fn from_configuration(config: &Configuration) -> Self {
        Self::with_logs_configuration(LogsConfiguration {
            default_provider: match config.bifrost.default_provider {
                #[cfg(any(test, feature = "memory-loglet"))]
                ProviderKind::InMemory => DefaultProvider::InMemory,
                ProviderKind::Local => DefaultProvider::Local,
                ProviderKind::Replicated => DefaultProvider::Replicated(ReplicatedLogletConfig {
                    nodeset_selection_strategy: NodeSetSelectionStrategy::default(),
                    replication_property: ReplicationProperty::new(
                        NonZeroU8::new(1).expect("1 is not zero"),
                    ),
                }),
            },
        })
    }

    pub fn with_logs_configuration(logs_configuration: LogsConfiguration) -> Self {
        Logs {
            config: logs_configuration,
            ..Default::default()
        }
    }

    /// empty metadata with an invalid version
    pub fn empty() -> Self {
        Default::default()
    }

    pub fn num_logs(&self) -> usize {
        self.logs.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&LogId, &Chain)> {
        self.logs.iter()
    }

    pub fn chain(&self, log_id: &LogId) -> Option<&Chain> {
        self.logs.get(log_id)
    }

    pub fn into_builder(self) -> LogsBuilder {
        self.into()
    }

    pub fn configuration(&self) -> &LogsConfiguration {
        &self.config
    }

    pub fn get_replicated_loglet(
        &self,
        loglet_id: &LogletId,
    ) -> Option<&LogletRef<ReplicatedLogletParams>> {
        self.lookup_index.get_replicated_loglet(loglet_id)
    }
}

impl Versioned for Logs {
    fn version(&self) -> Version {
        self.version
    }
}

flexbuffers_storage_encode_decode!(Logs);

/// Result of a segment lookup in the chain
#[derive(Debug)]
pub enum MaybeSegment<'a> {
    /// Segment is found in the chain.
    Some(Segment<'a>),
    /// No segment was found, the log is trimmed until (at least) the next_base_lsn. When
    /// generating trim gaps, this value should be considered exclusive (next_base_lsn doesn't
    /// necessarily point to a gap)
    Trim { next_base_lsn: Lsn },
}

impl Chain {
    /// Creates a new chain starting from Lsn(1) with a given loglet config.
    pub fn new(kind: ProviderKind, config: LogletParams) -> Self {
        Self::with_base_lsn(Lsn::OLDEST, kind, config)
    }

    /// Create a chain with `base_lsn` as its oldest Lsn.
    pub fn with_base_lsn(base_lsn: Lsn, kind: ProviderKind, config: LogletParams) -> Self {
        let mut chain = BTreeMap::new();
        chain.insert(
            base_lsn,
            LogletConfig::new(SegmentIndex::default(), kind, config),
        );
        Self { chain }
    }

    #[track_caller]
    pub fn tail(&self) -> Segment<'_> {
        self.chain
            .last_key_value()
            .map(|(base_lsn, config)| Segment {
                base_lsn: *base_lsn,
                tail_lsn: None,
                config,
            })
            .expect("Chain must have at least one segment")
    }

    #[track_caller]
    pub fn tail_index(&self) -> SegmentIndex {
        self.chain
            .last_key_value()
            .map(|(_, v)| v.index())
            .expect("Chain must have at least one segment")
    }

    #[track_caller]
    pub fn head(&self) -> Segment<'_> {
        let mut iter = self.chain.iter();
        let maybe_head = iter.next();
        let tail_lsn = iter.next().map(|(k, _)| *k);

        maybe_head
            .map(|(base_lsn, config)| Segment {
                base_lsn: *base_lsn,
                tail_lsn,
                config,
            })
            .expect("Chain must have at least one segment")
    }

    pub fn num_segments(&self) -> usize {
        self.chain.len()
    }

    /// Finds the segment that contains the given Lsn.
    /// Returns `MaybeSegment::Trim` if the Lsn is behind the oldest segment (trimmed).
    pub fn find_segment_for_lsn(&self, lsn: Lsn) -> MaybeSegment<'_> {
        // Ensure we don't actually consider INVALID as INVALID.
        let lsn = lsn.max(Lsn::OLDEST);
        // NOTE: Hopefully at some point we will use the nightly Cursor API for
        // efficient cursor seeks in the chain (or use nightly channel)
        // Reference: https://github.com/rust-lang/rust/issues/107540

        // The tail lsn is the base_lsn of the next segment (if exists)
        let mut tail_lsn = None;
        // linear backward search until we can use the Cursor API.
        let mut range = self.chain.range(..);
        // walking backwards.
        while let Some((base_lsn, config)) = range.next_back() {
            if lsn >= *base_lsn {
                return MaybeSegment::Some(Segment {
                    base_lsn: *base_lsn,
                    tail_lsn,
                    config,
                });
            }
            tail_lsn = Some(*base_lsn);
        }

        MaybeSegment::Trim {
            next_base_lsn: tail_lsn.expect("Chain is not empty"),
        }
    }

    /// Note that this is a special case, we don't set tail_lsn on segments, why?
    /// - It adds complexity
    /// - Tail LSN can be established by visiting the next item in the iterator externally
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Segment<'_>> + '_ {
        self.chain.iter().map(|(lsn, loglet_config)| Segment {
            base_lsn: *lsn,
            // See note above
            tail_lsn: None,
            config: loglet_config,
        })
    }
}

flexbuffers_storage_encode_decode!(Chain);

/// Creates appropriate [`LogletParams`] value that can be used to start a fresh
/// single-node loglet instance.
///
/// This is used in single-node bootstrap scenarios and assumes a non-running system.
/// It must generate params that uniquely identify the new loglet instance on every call.
pub fn new_single_node_loglet_params(default_provider: ProviderKind) -> LogletParams {
    let loglet_id = rand::thread_rng().next_u64().to_string();
    match default_provider {
        ProviderKind::Local => LogletParams::from(loglet_id),
        #[cfg(any(test, feature = "memory-loglet"))]
        ProviderKind::InMemory => LogletParams::from(loglet_id),
        ProviderKind::Replicated => panic!(
            "replicated-loglet is still in development and cannot be used as default-provider in this version. Pleae use 'local' instead."
        ),
    }
}

/// Initializes the bifrost metadata with static log metadata, it creates a log for every partition
/// with a chain of the default loglet provider kind.
pub fn bootstrap_logs_metadata(
    default_provider: ProviderKind,
    default_loglet_params: Option<String>,
    num_partitions: u16,
) -> Logs {
    // Get metadata from somewhere
    let mut builder = LogsBuilder::default();
    #[allow(clippy::mutable_key_type)]
    let mut generated_params: HashSet<_> = HashSet::new();
    // pre-fill with all possible logs up to `num_partitions`
    (0..num_partitions).for_each(|i| {
        // a little paranoid about collisions
        let params = default_loglet_params
            .clone()
            .map(LogletParams::from)
            .unwrap_or_else(|| loop {
                let params = new_single_node_loglet_params(default_provider);
                if !generated_params.contains(&params) {
                    generated_params.insert(params.clone());
                    break params;
                }
            });
        builder
            .add_log(LogId::from(i), Chain::new(default_provider, params))
            .unwrap();
    });

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_new() {
        let chain = Chain::new(ProviderKind::Local, LogletParams::from("test".to_string()));
        assert_eq!(chain.chain.len(), 1);
        let Segment {
            base_lsn,
            tail_lsn,
            config,
        } = chain.tail();
        assert_eq!(Lsn::OLDEST, base_lsn);
        assert_eq!(None, tail_lsn);
        assert_eq!(ProviderKind::Local, config.kind);
        assert_eq!("test".to_string(), config.params.0.to_string());
    }
}
