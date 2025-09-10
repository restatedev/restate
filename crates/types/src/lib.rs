// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
pub mod time;
pub mod timer;

pub use id_util::IdResourceType;
pub use node_id::*;
pub use restate_version::*;
pub use version::*;

// Re-export metrics' SharedString (Space-efficient Cow + RefCounted variant)
pub type SharedString = metrics::SharedString;

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
