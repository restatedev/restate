// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet, hash_map};
use std::num::NonZeroU8;
use std::ops::Bound;
use std::str::FromStr;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use anyhow::Context;
use bytestring::ByteString;
use enum_map::Enum;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use smallvec::SmallVec;

use restate_clock::UniqueTimestamp;
use restate_encoding::BilrostNewType;

use super::LogletId;
use super::builder::LogsBuilder;
use crate::config::Configuration;
use crate::logs::builder::BuilderError;
use crate::logs::{LogId, Lsn, SequenceNumber};
use crate::metadata::GlobalMetadata;
use crate::net::metadata::{MetadataContainer, MetadataKind};
use crate::replicated_loglet::ReplicatedLogletParams;
use crate::replication::ReplicationProperty;
use crate::time::MillisSinceEpoch;
use crate::{GenerationalNodeId, Version, Versioned, flexbuffers_storage_encode_decode};

// Starts with 0 being the oldest loglet in the chain.
#[derive(
    Default,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    derive_more::Debug,
    BilrostNewType,
)]
#[repr(transparent)]
#[serde(transparent)]
#[debug("{}", _0)]
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
    pub(super) replicated_loglets: HashMap<LogletId, LogletRef<ReplicatedLogletParams>>,
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
                .retain(|(l, s)| (*l, *s) != (log_id, segment_index));
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

    fn iter(&self) -> impl Iterator<Item = (&LogletId, &LogletRef<ReplicatedLogletParams>)> {
        self.replicated_loglets.iter()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderConfiguration {
    InMemory,
    Local,
    Replicated(ReplicatedLogletConfig),
}

impl Default for ProviderConfiguration {
    fn default() -> Self {
        Self::Replicated(ReplicatedLogletConfig {
            replication_property: ReplicationProperty::new_unchecked(1),
            target_nodeset_size: NodeSetSize::new(1).expect("is valid nodeset size"),
        })
    }
}

impl ProviderConfiguration {
    pub fn kind(&self) -> ProviderKind {
        match self {
            Self::InMemory => ProviderKind::InMemory,
            Self::Local => ProviderKind::Local,
            Self::Replicated(_) => ProviderKind::Replicated,
        }
    }

    pub fn from_configuration(configuration: &Configuration) -> Self {
        ProviderConfiguration::from((
            configuration.bifrost.default_provider,
            configuration.common.default_replication.clone(),
            configuration.bifrost.replicated_loglet.default_nodeset_size,
        ))
    }

    pub fn replication(&self) -> Option<&ReplicationProperty> {
        match self {
            ProviderConfiguration::InMemory => None,
            ProviderConfiguration::Local => None,
            ProviderConfiguration::Replicated(config) => Some(&config.replication_property),
        }
    }

    pub fn target_nodeset_size(&self) -> Option<NodeSetSize> {
        match self {
            ProviderConfiguration::InMemory => None,
            ProviderConfiguration::Local => None,
            ProviderConfiguration::Replicated(config) => Some(config.target_nodeset_size),
        }
    }
}

impl From<(ProviderKind, ReplicationProperty, NodeSetSize)> for ProviderConfiguration {
    fn from(
        (provider_kind, log_replication, target_nodeset_size): (
            ProviderKind,
            ReplicationProperty,
            NodeSetSize,
        ),
    ) -> Self {
        match provider_kind {
            ProviderKind::Local => ProviderConfiguration::Local,
            ProviderKind::InMemory => ProviderConfiguration::InMemory,
            ProviderKind::Replicated => ProviderConfiguration::Replicated(ReplicatedLogletConfig {
                replication_property: log_replication,
                target_nodeset_size,
            }),
        }
    }
}

impl From<ProviderConfiguration> for crate::protobuf::cluster::BifrostProvider {
    fn from(value: ProviderConfiguration) -> Self {
        use crate::protobuf::cluster;

        let mut result = cluster::BifrostProvider::default();

        match value {
            ProviderConfiguration::Local => result.provider = ProviderKind::Local.to_string(),
            ProviderConfiguration::InMemory => result.provider = ProviderKind::InMemory.to_string(),
            ProviderConfiguration::Replicated(config) => {
                result.provider = ProviderKind::Replicated.to_string();
                result.replication_property = Some(cluster::ReplicationProperty {
                    replication_property: config.replication_property.to_string(),
                });
                result.target_nodeset_size = config.target_nodeset_size.as_u32();
            }
        };

        result
    }
}

impl TryFrom<crate::protobuf::cluster::BifrostProvider> for ProviderConfiguration {
    type Error = anyhow::Error;
    fn try_from(value: crate::protobuf::cluster::BifrostProvider) -> Result<Self, Self::Error> {
        let provider_kind: ProviderKind = value.provider.parse()?;

        match provider_kind {
            ProviderKind::Local => Ok(Self::Local),
            ProviderKind::InMemory => Ok(Self::InMemory),
            ProviderKind::Replicated => {
                let config = value.replication_property.ok_or_else(|| {
                    anyhow::anyhow!("replicate_config is required with replicated provider")
                })?;

                Ok(Self::Replicated(ReplicatedLogletConfig {
                    replication_property: config.replication_property.parse()?,
                    target_nodeset_size: value
                        .target_nodeset_size
                        .try_into()
                        // the error message helps the user learn about the logical maximum rather
                        // than the type max limit.
                        .context("target_nodeset_size is too big, please keep it under 128")?,
                }))
            }
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReplicatedLogletConfig {
    #[serde_as(as = "DisplayFromStr")]
    pub replication_property: ReplicationProperty,
    /// The default target for new nodesets. 0 (default) auto-chooses a nodeset-size that
    /// balances read and write availability. It's a reasonable default for most cases.
    pub target_nodeset_size: NodeSetSize,
}

/// New type that enforces that the nodeset size is never larger than 128.
#[derive(
    Debug,
    Clone,
    Copy,
    derive_more::Into,
    serde::Serialize,
    serde::Deserialize,
    Eq,
    PartialEq,
    derive_more::Display,
)]
#[serde(try_from = "u16", into = "u16")]
#[display("{_0}")]
pub struct NodeSetSize(u16);

impl NodeSetSize {
    pub const ZERO: NodeSetSize = NodeSetSize(0);
    pub const MAX: NodeSetSize = NodeSetSize(128);

    pub fn new(value: u16) -> Option<NodeSetSize> {
        Self::try_from(value).ok()
    }

    pub const fn as_u16(&self) -> u16 {
        self.0
    }

    pub const fn as_u32(&self) -> u32 {
        self.0 as u32
    }
}

impl Default for NodeSetSize {
    fn default() -> Self {
        NodeSetSize::ZERO
    }
}

impl TryFrom<u16> for NodeSetSize {
    type Error = NodeSetSizeError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if value > NodeSetSize::MAX.as_u16() {
            return Err(NodeSetSizeError);
        }

        Ok(NodeSetSize(value))
    }
}

impl TryFrom<u32> for NodeSetSize {
    type Error = NodeSetSizeError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value > NodeSetSize::MAX.as_u32() {
            return Err(NodeSetSizeError);
        }

        Ok(NodeSetSize(value as u16))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("node set size is too big, please keep it under {}", NodeSetSize::MAX.as_u16())]
pub struct NodeSetSizeError;

#[derive(
    Debug, Clone, Eq, PartialEq, Default, derive_more::From, serde::Serialize, serde::Deserialize,
)]
pub struct LogsConfiguration {
    pub default_provider: ProviderConfiguration,
}

#[derive(derive_more::Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "LogsSerde", into = "LogsSerde")]
pub struct Logs {
    pub(super) version: Version,
    pub(super) logs: HashMap<LogId, Chain>,
    #[debug(skip)]
    pub(super) lookup_index: LookupIndex,
    pub(super) config: LogsConfiguration,
    // The last modification time.
    pub(super) modified_at: Option<UniqueTimestamp>,
}

impl Default for Logs {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            logs: Default::default(),
            lookup_index: Default::default(),
            config: LogsConfiguration::default(),
            modified_at: None,
        }
    }
}

impl GlobalMetadata for Logs {
    const KEY: &'static str = "bifrost_config";

    const KIND: MetadataKind = MetadataKind::Logs;

    fn into_container(self: Arc<Self>) -> MetadataContainer {
        MetadataContainer::Logs(self)
    }
}

impl From<Logs> for LogsSerde {
    fn from(value: Logs) -> Self {
        Self {
            version: value.version,
            logs: value.logs.into_iter().collect(),
            config: Some(value.config),
            modified_at: value.modified_at,
        }
    }
}

impl TryFrom<LogsSerde> for Logs {
    type Error = anyhow::Error;

    fn try_from(value: LogsSerde) -> Result<Self, Self::Error> {
        let mut logs = HashMap::with_capacity(value.logs.len());
        let mut lookup_index = LookupIndex::default();

        let mut config = value.config;
        for (log_id, chain) in value.logs {
            for loglet_config in chain.chain.values() {
                if ProviderKind::Replicated == loglet_config.kind {
                    let params =
                        ReplicatedLogletParams::deserialize_from(loglet_config.params.as_bytes())?;
                    lookup_index.add_replicated_loglet(log_id, loglet_config.index, params);

                    if config.is_none() {
                        // no config in LogsSerde but we are using replicated loglets already
                        // this means we are migrating from an older setup that had replication-property
                        // hardcoded to {node:2}
                        config = Some(LogsConfiguration {
                            default_provider: ProviderConfiguration::Replicated(
                                ReplicatedLogletConfig {
                                    target_nodeset_size: NodeSetSize::default(),
                                    replication_property: ReplicationProperty::new(
                                        NonZeroU8::new(2).expect("2 is not 0"),
                                    ),
                                },
                            ),
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
            modified_at: value.modified_at,
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
    #[serde(default)]
    modified_at: Option<UniqueTimestamp>,
}

/// Seal metadata is json-serialized and stored as loglet params of the seal marker segment.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SealMetadata {
    /// If seal is permanent, the chain will not be allowed to grow.
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub permanent_seal: bool,
    pub sealed_at: MillisSinceEpoch,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub context: std::collections::HashMap<String, String>,
    pub tail_ts: Option<UniqueTimestamp>,
}

impl SealMetadata {
    pub fn new(source: impl Into<String>, node_id: GenerationalNodeId) -> Self {
        Self {
            context: std::collections::HashMap::from_iter([
                ("node".to_owned(), node_id.to_string()),
                ("source".to_owned(), source.into()),
            ]),
            ..Default::default()
        }
    }

    pub fn with_context(
        permanent_seal: bool,
        context: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            permanent_seal,
            context,
            ..Default::default()
        }
    }

    pub fn deserialize_from(slice: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(slice)
    }

    pub fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl Default for SealMetadata {
    fn default() -> Self {
        Self {
            permanent_seal: false,
            sealed_at: MillisSinceEpoch::now(),
            context: Default::default(),
            tail_ts: None,
        }
    }
}

impl TryFrom<&SealMetadata> for LogletParams {
    type Error = serde_json::Error;

    fn try_from(value: &SealMetadata) -> Result<Self, Self::Error> {
        Ok(LogletParams::from(serde_json::to_string(value)?))
    }
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
    pub kind: InternalKind,
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
    #[serde(default)]
    pub created_at: Option<UniqueTimestamp>,
}

impl LogletConfig {
    #[cfg(feature = "test-util")]
    pub fn for_testing() -> Self {
        Self {
            kind: InternalKind::InMemory,
            params: LogletParams(ByteString::default()),
            index: 0.into(),
            created_at: None,
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
///
/// Internal kinds may include special loglets that are not exposed to the user.
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
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum InternalKind {
    /// A local rocksdb-backed loglet.
    Local,
    /// An in-memory loglet, for testing.
    InMemory,
    /// Replicated loglets are restate's native log replication system. This requires
    /// `log-server` role to run on enough nodes in the cluster.
    Replicated,

    /// A loglet that's always sealed and has no records. Used as a seal marker for a sealed chain.
    Sealed,
}

impl InternalKind {
    pub fn is_seal_marker(&self) -> bool {
        matches!(self, Self::Sealed)
    }
}

impl From<ProviderKind> for InternalKind {
    fn from(value: ProviderKind) -> Self {
        match value {
            ProviderKind::Local => Self::Local,
            ProviderKind::InMemory => Self::InMemory,
            ProviderKind::Replicated => Self::Replicated,
        }
    }
}

impl TryFrom<InternalKind> for ProviderKind {
    type Error = anyhow::Error;
    fn try_from(value: InternalKind) -> Result<Self, Self::Error> {
        match value {
            InternalKind::Local => Ok(Self::Local),
            InternalKind::InMemory => Ok(Self::InMemory),
            InternalKind::Replicated => Ok(Self::Replicated),
            InternalKind::Sealed => Err(anyhow::anyhow!(
                "a special loglet kind that cannot be converted into user-facing kind"
            )),
        }
    }
}

impl PartialEq<InternalKind> for ProviderKind {
    fn eq(&self, other: &InternalKind) -> bool {
        match self {
            ProviderKind::Local => matches!(other, InternalKind::Local),
            ProviderKind::InMemory => matches!(other, InternalKind::InMemory),
            ProviderKind::Replicated => matches!(other, InternalKind::Replicated),
        }
    }
}

impl PartialEq<ProviderKind> for InternalKind {
    fn eq(&self, other: &ProviderKind) -> bool {
        match other {
            ProviderKind::Local => matches!(self, InternalKind::Local),
            ProviderKind::InMemory => matches!(self, InternalKind::InMemory),
            ProviderKind::Replicated => matches!(self, InternalKind::Replicated),
        }
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
    /// An in-memory loglet, for testing.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[cfg_attr(feature = "clap", clap(hide = true))]
    InMemory,
    /// Replicated loglets are restate's native log replication system. This requires
    /// `log-server` role to run on enough nodes in the cluster.
    Replicated,
}

impl FromStr for ProviderKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "in-memory" | "in_memory" | "memory" => Ok(Self::InMemory),
            "replicated" => Ok(Self::Replicated),
            _ => anyhow::bail!("Unknown provider kind"),
        }
    }
}

impl LogletConfig {
    pub(crate) fn new(index: SegmentIndex, kind: ProviderKind, params: LogletParams) -> Self {
        Self {
            kind: InternalKind::from(kind),
            params,
            index,
            created_at: None,
        }
    }

    pub(crate) fn new_sealed(
        index: SegmentIndex,
        metadata: &SealMetadata,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            kind: InternalKind::Sealed,
            params: LogletParams::try_from(metadata)?,
            index,
            created_at: None,
        })
    }

    pub fn index(&self) -> SegmentIndex {
        self.index
    }
}

impl Logs {
    pub fn from_configuration(config: &Configuration) -> Self {
        Self::with_logs_configuration(LogsConfiguration {
            default_provider: ProviderConfiguration::from_configuration(config),
        })
    }

    pub fn with_logs_configuration(logs_configuration: LogsConfiguration) -> Self {
        Logs {
            config: logs_configuration,
            version: Version::MIN,
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

    pub fn try_into_builder(self) -> Result<LogsBuilder, BuilderError> {
        self.try_into()
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

    pub fn iter_writeable(&self) -> impl Iterator<Item = (&LogId, Segment<'_>)> {
        self.logs
            .iter()
            .map(|(log_id, chain)| (log_id, chain.tail()))
    }

    pub fn iter_replicated_loglets(
        &self,
    ) -> impl Iterator<Item = (&LogletId, &LogletRef<ReplicatedLogletParams>)> {
        self.lookup_index.iter()
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

    /// Returns the sealed tail if the chain is sealed.
    pub fn sealed_tail(&self) -> Option<Lsn> {
        if let Some((&lsn, config)) = self.chain.last_key_value()
            && config.kind.is_seal_marker()
        {
            Some(lsn)
        } else {
            None
        }
    }

    /// Is the chain sealed?
    pub fn is_sealed(&self) -> bool {
        self.sealed_tail().is_some()
    }

    /// Finds the last non-special segment in the chain if it exists.
    pub fn non_special_tail(&self) -> Option<Segment<'_>> {
        let (&base_lsn, config) = self
            .chain
            .iter()
            .rfind(|(_, config)| !config.kind.is_seal_marker())?;

        // Does it have a next segment? then it's sealed.
        //
        // Checkout the comment in `find_segment_for_lsn()` for why we use `range()` here.
        let tail_lsn = self
            .chain
            .range(Bound::Excluded(base_lsn)..Bound::Unbounded)
            .next()
            .map(|(lsn, _)| *lsn);

        Some(Segment {
            base_lsn,
            tail_lsn,
            config,
        })
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
        let (&base_lsn, config) = iter.next().expect("Chain must have at least one segment");
        let tail_lsn = iter.next().map(|(&lsn, _)| lsn);

        Segment {
            base_lsn,
            tail_lsn,
            config,
        }
    }

    pub fn num_segments(&self) -> usize {
        self.chain.len()
    }

    pub fn find_segment_by_index(
        &self,
        index: SegmentIndex,
        provider: ProviderKind,
    ) -> Option<Segment<'_>> {
        let (&base_lsn, config) = self
            .chain
            .iter()
            .rfind(|(_, config)| config.index() == index && config.kind == provider)?;

        // Checkout the comment in `find_segment_for_lsn()` for why we use `range()` here.
        let tail_lsn = self
            .chain
            .range(Bound::Excluded(base_lsn)..Bound::Unbounded)
            .next()
            .map(|(lsn, _)| *lsn);

        Some(Segment {
            base_lsn,
            tail_lsn,
            config,
        })
    }

    /// Finds the segment that contains the given Lsn.
    /// Returns `MaybeSegment::Trim` if the Lsn is behind the oldest segment (trimmed).
    #[track_caller]
    pub fn find_segment_for_lsn(&self, lsn: Lsn) -> MaybeSegment<'_> {
        // Ensure we don't actually consider INVALID as INVALID.
        let lsn = lsn.max(Lsn::OLDEST);
        // NOTE: Hopefully at some point we will use the nightly Cursor API for
        // efficient cursor seeks in the chain (or use nightly channel)
        // Reference: https://github.com/rust-lang/rust/issues/107540

        // linear backward search until we can use the Cursor API.
        // walking backwards.
        let Some((&base_lsn, config)) = self.chain.iter().rfind(|(base_lsn, _)| lsn >= **base_lsn)
        else {
            let (&next_base_lsn, _) = self.chain.iter().next().expect("Chain is not empty");
            return MaybeSegment::Trim { next_base_lsn };
        };

        // Why not we just drive the iter() iterator forward instead of creating a new one?
        // glad you asked. Rust iterators do not guarantee that they will drive the iterator
        // forward once they return None. When `rfind()` stops searching, it might have hit a
        // None. In the case of btreemap range iterator, calling next() on that iterator will
        // not advance it and we'll always see None.
        let tail_lsn = {
            let next = self
                .chain
                .range(Bound::Excluded(base_lsn)..Bound::Unbounded)
                .next();
            next.map(|(&lsn, _)| lsn)
        };

        MaybeSegment::Some(Segment {
            base_lsn,
            tail_lsn,
            config,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = Segment<'_>> + '_ {
        let mut iter = self.chain.iter();
        let mut maybe_current = iter.next();

        std::iter::from_fn(move || {
            let (&base_lsn, config) = maybe_current?;
            let tmp_next = iter.next();
            let tail_lsn = tmp_next.map(|(&lsn, _)| lsn);

            let segment = Segment {
                base_lsn,
                tail_lsn,
                config,
            };
            maybe_current = tmp_next;

            Some(segment)
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
    match default_provider {
        ProviderKind::Local => {
            use rand::RngCore;
            let loglet_id = rand::rng().next_u64().to_string();
            LogletParams::from(loglet_id)
        }
        ProviderKind::InMemory => {
            use rand::RngCore;
            let loglet_id = rand::rng().next_u64().to_string();
            LogletParams::from(loglet_id)
        }
        ProviderKind::Replicated => panic!(
            "replicated-loglet is still in development and cannot be used as default-provider in this version. Please use 'local' instead."
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
            .unwrap_or_else(|| {
                loop {
                    let params = new_single_node_loglet_params(default_provider);
                    if !generated_params.contains(&params) {
                        generated_params.insert(params.clone());
                        break params;
                    }
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
