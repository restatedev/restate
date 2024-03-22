// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
mod version;

pub mod dedup;
pub mod deployment;
pub mod errors;
pub mod identifiers;
pub mod ingress;
pub mod invocation;
pub mod journal;
pub mod logs;
pub mod message;
pub mod nodes_config;
pub mod partition_table;
pub mod retries;
pub mod state_mut;
pub mod subscription;
pub mod time;
pub mod timer;

pub use id_util::{IdDecoder, IdEncoder, IdResourceType, IdStrCursor};
pub use node_id::*;
pub use version::*;

pub const DEFAULT_STORAGE_DIRECTORY: &str = "./restate-data";
