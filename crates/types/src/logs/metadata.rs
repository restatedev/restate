// Copyright (c) 2024-2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use bytestring::ByteString;
use enum_map::Enum;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::builder::LogsBuilder;
use crate::logs::{LogId, Lsn, SequenceNumber};
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
    derive_more::Display,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct SegmentIndex(pub(crate) u32);

/// Log metadata is the map of logs known to the system with the corresponding chain.
/// Metadata updates are versioned and atomic.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Logs {
    pub(super) version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub(super) logs: HashMap<LogId, Chain>,
}

impl Default for Logs {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            logs: Default::default(),
        }
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

/// The configuration of a single loglet segment. This holds information needed
/// for a loglet kind to construct a configured loglet instance modulo the log-id
/// and start-lsn. It's provided by bifrost on loglet creation. This allows the
/// parameters to be shared between segments and logs if needed.
#[derive(Debug, Clone, Hash, Eq, PartialEq, derive_more::From, Serialize, Deserialize)]
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
pub enum ProviderKind {
    /// A local rocksdb-backed loglet.
    Local,
    /// An in-memory loglet, primarily for testing.
    #[cfg(any(test, feature = "memory-loglet"))]
    InMemory,
    #[cfg(feature = "replicated-loglet")]
    /// [IN DEVELOPMENT]
    /// Replicated loglet implementation. This requires log-server role to run on
    /// enough nodes in the cluster.
    Replicated,

    #[cfg(feature = "kafka-loglet")]
    Kafka,
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

impl LogletParams {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &Bytes {
        self.0.as_bytes()
    }
}

impl Logs {
    pub fn new(version: Version, logs: HashMap<LogId, Chain>) -> Self {
        Self { version, logs }
    }

    /// empty metadata with an invalid version
    pub fn empty() -> Self {
        Self {
            version: Version::INVALID,
            logs: Default::default(),
        }
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
    pub fn iter(&self) -> impl Iterator<Item = Segment<'_>> + '_ {
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
        #[cfg(feature = "replicated-loglet")]
        ProviderKind::Replicated => panic!(
            "replicated-loglet cannot be used as default-provider in a single-node setup.\
            To use replicated loglet, the node must be running in cluster-mode"
        ),
        #[cfg(feature = "kafka-loglet")]
        ProviderKind::Kafka => LogletParams::from(loglet_id),
    }
}

/// Initializes the bifrost metadata with static log metadata, it creates a log for every partition
/// with a chain of the default loglet provider kind.
pub fn bootstrap_logs_metadata(default_provider: ProviderKind, num_partitions: u64) -> Logs {
    // Get metadata from somewhere
    let mut builder = LogsBuilder::default();
    #[allow(clippy::mutable_key_type)]
    let mut generated_params: HashSet<_> = HashSet::new();
    // pre-fill with all possible logs up to `num_partitions`
    (0..num_partitions).for_each(|i| {
        // a little paranoid about collisions
        let params = loop {
            let params = new_single_node_loglet_params(default_provider);
            if !generated_params.contains(&params) {
                generated_params.insert(params.clone());
                break params;
            }
        };
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
