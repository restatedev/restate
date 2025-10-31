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
pub mod invocation;
pub mod journal;
pub mod journal_events;
pub mod journal_v2;
pub mod live;
pub mod locality;
pub mod logs;
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

pub use restate_ty::*;

// remapping of the new type placement into the old module until call sites are updated
pub mod identifiers {
    // re-export identifiers from restate_ty to avoid large-scale refactoring
    pub use restate_ty::identifiers::*;
    // todo: remove this alias after call sites are updated
    pub use restate_ty::invocation::ServiceId;
    pub use restate_ty::journal::EntryIndex;
    pub use restate_ty::partitions::{
        LeaderEpoch, PartitionId, PartitionKey, PartitionLeaderEpoch, WithPartitionKey,
    };
}

pub mod message {
    pub use restate_ty::MessageIndex;
}

// Re-export metrics' SharedString (Space-efficient Cow + RefCounted variant)
pub type SharedString = metrics::SharedString;
