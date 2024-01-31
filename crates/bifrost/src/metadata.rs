// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};

use bytestring::ByteString;

use crate::types::Version;
use crate::{LogId, Lsn};

/// An enum with the list of supported loglet implementations.
/// For each variant we must have a corresponding implementation of the
/// [`crate::loglet::Loglet`] trait
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum LogletKind {
    /// A file-backed loglet.
    File,
    #[cfg(test)]
    Memory,
}

/// Log metadata is the map of logs known to the system with the corresponding chain.
/// Metadata updates are versioned and atomic.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Logs {
    pub(crate) version: Version,
    pub(crate) logs: HashMap<LogId, Chain>,
}

//// the chain is a list of segments in (from Lsn) order.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Chain {
    // NOTE: Hopefully at some point we will use the nightly Cursor API for
    // effecient cursor seeks in the chain (or use nightly channel)
    // Reference: https://github.com/rust-lang/rust/issues/107540
    pub(crate) chain: BTreeMap<Lsn, LogSegment>,
}

/// A segment in the chain of loglet instances.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum LogSegment {
    /// A segment that is owned by this chain
    Owned { config: LogletConfig },
}

/// The configuration of a single loglet segment. This holds information needed
/// for a loglet kind to construct a configured loglet instance modulo the log-id
/// and start-lsn. is provided by bifrost on loglet creation. This allows the
/// parameters to be shared between segments and logs if needed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogletConfig {
    kind: LogletKind,
    /// Opaque to bifrost. This config must be interpretable by this loglet kind
    params: ByteString,
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

    /// log metadata is considered initialized if the version is valid but with
    /// no guarantee that we have the latest version.
    pub fn is_initialized(&self) -> bool {
        self.version != Version::INVALID
    }
}

impl LogletConfig {
    pub fn new(kind: LogletKind, params: ByteString) -> Self {
        Self { kind, params }
    }
}

impl Chain {
    /// Creates a new chain starting from Lsn(1) with a given loglet config.
    pub fn new(config: LogletConfig) -> Self {
        let mut chain = BTreeMap::new();
        chain.insert(Lsn::OLDEST, LogSegment::Owned { config });
        Self { chain }
    }
}
