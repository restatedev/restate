// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains the core types used by various Restate components.

mod base62_util;
mod id_util;
mod locking;
mod macros;
mod node_id;
mod restate_version;
mod version;

pub mod art;
pub mod cluster;

pub mod cluster_state;
pub mod health;

pub mod config;
pub mod config_loader;
pub mod deployment;
pub mod endpoint_manifest;
pub mod epoch;
pub mod errors;
pub mod identifiers;
pub mod invocation;
pub mod journal;
pub mod journal_events;
pub mod journal_v2;
pub mod live;
pub mod locality;
pub mod logs;
pub mod message;
pub mod metadata;
pub mod metadata_store;
pub mod net;
pub mod nodes_config;
pub mod partition_table;
pub mod partitions;
pub mod protobuf;
pub mod rate;
pub mod replicated_loglet;
pub mod replication;
pub mod retries;
pub mod schema;
pub mod service_discovery;
pub mod service_protocol;
pub mod state_mut;
pub mod storage;
pub mod timer;
pub mod vqueues;

pub use id_util::IdResourceType;
pub use identifiers::PartitionedResourceId;
pub use locking::*;
pub use node_id::*;
use restate_encoding::BilrostNewType;
use restate_util_string::InternedReString;
pub use restate_version::*;
pub use version::*;

// Re-export of the old time module by delegating to the restate-clock crate.
pub use restate_clock::time;

use self::identifiers::partitioner::HashPartitioner;
use self::identifiers::{PartitionKey, WithPartitionKey};
pub mod clock {
    pub use restate_clock::*;
}

// Re-export restate-memory crate for memory management utilities.
pub mod memory {
    pub use restate_memory::*;
}

// Re-export metrics' SharedString (Space-efficient Cow + RefCounted variant)
pub type SharedString = metrics::SharedString;

/// An interned service name
#[derive(
    derive_more::Display,
    derive_more::Debug,
    derive_more::AsRef,
    derive_more::From,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    BilrostNewType,
)]
#[debug("{}", _0)]
#[display("{}", _0)]
#[repr(transparent)]
pub struct ServiceName(InternedReString);

impl ServiceName {
    #[inline]
    pub fn new(value: &str) -> Self {
        assert!(!value.is_empty());
        Self(InternedReString::new(value))
    }

    #[inline]
    pub const fn from_static(value: &'static str) -> Self {
        assert!(!value.is_empty());
        Self(InternedReString::from_static(value))
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<str> for ServiceName {
    #[inline]
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::borrow::Borrow<str> for ServiceName {
    #[inline]
    fn borrow(&self) -> &str {
        self.0.as_ref()
    }
}

/// An interned scope
///
/// A scope defines the partitioning boundary for sets of service instances and
/// invocations.
#[derive(
    derive_more::Display,
    derive_more::Debug,
    derive_more::AsRef,
    derive_more::From,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    BilrostNewType,
)]
#[debug("{}", _0)]
#[display("{}", _0)]
#[repr(transparent)]
pub struct Scope(InternedReString);

impl Scope {
    #[inline]
    pub fn new(value: &str) -> Self {
        assert!(!value.is_empty());
        Self(InternedReString::new(value))
    }

    #[inline]
    pub const fn from_static(value: &'static str) -> Self {
        assert!(!value.is_empty());
        Self(InternedReString::from_static(value))
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<str> for Scope {
    #[inline]
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::borrow::Borrow<str> for Scope {
    #[inline]
    fn borrow(&self) -> &str {
        self.0.as_ref()
    }
}

impl WithPartitionKey for Scope {
    #[inline]
    fn partition_key(&self) -> PartitionKey {
        // the partition key is calculated directly from the scope value
        HashPartitioner::compute_partition_key(&self.0)
    }
}

/// Trait for merging two attributes
pub trait Merge {
    /// Return true if the value was mutated as a result of the merge
    fn merge(&mut self, other: Self) -> bool;
}

impl Merge for bool {
    fn merge(&mut self, other: Self) -> bool {
        if *self != other {
            *self |= other;
            true
        } else {
            false
        }
    }
}
