// Copyright (c) 2024-2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Remove after fleshing the code out.
#![allow(dead_code)]

use crate::logs::{LogId, Lsn, SequenceNumber};
use crate::{flexbuffers_storage_encode_decode, Version, Versioned};
use enum_map::Enum;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Log metadata is the map of logs known to the system with the corresponding chain.
/// Metadata updates are versioned and atomic.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Logs {
    pub version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub logs: HashMap<LogId, Chain>,
}

/// the chain is a list of segments in (from Lsn) order.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chain {
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub chain: BTreeMap<Lsn, Arc<LogletConfig>>,
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub base_lsn: Lsn,
    pub config: Arc<LogletConfig>,
}

/// A segment in the chain of loglet instances.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogletConfig {
    pub kind: ProviderKind,
    pub params: LogletParams,
}

/// The configuration of a single loglet segment. This holds information needed
/// for a loglet kind to construct a configured loglet instance modulo the log-id
/// and start-lsn. It's provided by bifrost on loglet creation. This allows the
/// parameters to be shared between segments and logs if needed.
#[derive(Debug, Clone, Hash, Eq, PartialEq, derive_more::From, Serialize, Deserialize)]
pub struct LogletParams(String);

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
    strum_macros::EnumIter,
    strum_macros::Display,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    /// A local rocksdb-backed loglet.
    Local,
    /// An in-memory loglet, primarily for testing.
    InMemory,
}

impl LogletConfig {
    pub fn new(kind: ProviderKind, params: LogletParams) -> Self {
        Self { kind, params }
    }
}

impl LogletParams {
    pub fn id(&self) -> &str {
        &self.0
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

    pub fn tail_segment(&self, log_id: LogId) -> Option<Segment> {
        self.logs
            .get(&log_id)
            .and_then(|chain| chain.tail())
            .map(|(base_lsn, config)| Segment {
                base_lsn: *base_lsn,
                config: Arc::clone(config),
            })
    }

    pub fn find_segment_for_lsn(&self, log_id: LogId, _lsn: Lsn) -> Option<Segment> {
        // [Temporary implementation] At the moment, we have the hard assumption
        // that the chain contains a single segment so we always return this segment.
        //
        // NOTE: Hopefully at some point we will use the nightly Cursor API for
        // effecient cursor seeks in the chain (or use nightly channel)
        // Reference: https://github.com/rust-lang/rust/issues/107540
        //
        self.logs.get(&log_id).map(|chain| {
            let config = chain
                .chain
                .get(&Lsn::OLDEST)
                .expect("Chain should always have one segment");
            Segment {
                base_lsn: Lsn::OLDEST,
                config: Arc::clone(config),
            }
        })
    }
}

impl Versioned for Logs {
    fn version(&self) -> Version {
        self.version
    }
}

flexbuffers_storage_encode_decode!(Logs);

impl Chain {
    /// Creates a new chain starting from Lsn(1) with a given loglet config.
    pub fn new(kind: ProviderKind, config: LogletParams) -> Self {
        let mut chain = BTreeMap::new();
        let from_lsn = Lsn::OLDEST;
        chain.insert(from_lsn, Arc::new(LogletConfig::new(kind, config)));
        Self { chain }
    }

    pub fn tail(&self) -> Option<(&Lsn, &Arc<LogletConfig>)> {
        self.chain.last_key_value()
    }
}

/// Initializes the bifrost metadata with static log metadata, it creates a log for every partition
/// with a chain of the default loglet provider kind.
pub fn create_static_metadata(default_provider: ProviderKind, num_partitions: u64) -> Logs {
    // Get metadata from somewhere
    let mut log_chain: HashMap<LogId, Chain> = HashMap::with_capacity(num_partitions as usize);

    // pre-fill with all possible logs up to `num_partitions`
    (0..num_partitions).for_each(|i| {
        // fixed config that uses the log-id as loglet identifier/config
        let config = LogletParams::from(i.to_string());
        log_chain.insert(LogId::from(i), Chain::new(default_provider, config));
    });

    Logs::new(Version::MIN, log_chain)
}

#[cfg(test)]
mod tests {
    use restate_test_util::let_assert;

    use super::*;
    #[test]
    fn test_chain_new() {
        let chain = Chain::new(ProviderKind::Local, LogletParams::from("test".to_string()));
        assert_eq!(chain.chain.len(), 1);
        let_assert!(Some((lsn, loglet_config)) = chain.tail());
        assert_eq!(Lsn::OLDEST, *lsn);
        assert_eq!(ProviderKind::Local, loglet_config.kind);
        assert_eq!("test".to_string(), loglet_config.params.0);
    }
}
